"""SQLite-backed relational store for inspection documents.

Storage model
-------------
Three live tables hold the whole document shape:

``device``
    one row per device: the inspection-section state (absent / null / empty
    array / present array) and the root slot id when present.

``section_slot``
    one row per section array element, including holes.  Besides its scalar
    fields and its own attachment-array state it carries the structural links
    of the section array.

``attachment_slot``
    one row per attachment array element, including holes.  It points at the
    *stable id* of its owning section slot, never at a section position.

A fourth, append-only table holds the version history:

``device_version``
    one row per committed, content-changing version of a device: the full
    identified draft (stable ids, fields, shapes, order and attachment
    ownership) plus metadata and a content hash.  Rows are never updated or
    deleted; restoring an old version appends a new version instead.

Array order is represented as an implicit treap.  Each slot row stores
``parent_id`` / ``left_id`` / ``right_id`` / ``priority`` / ``subtree_size``.
In-order traversal is array order; subtree sizes give zero-based ranks.  A
middle insert or delete rewrites only the O(log n) rows on the search path, so
the untouched suffix is never renumbered.  No ``position`` column exists in
the live schema.  Monotonic positions are generated only while streaming an
official export.

Stable slot ids are a separate namespace from position.  They are globally
unique across the two slot tables (enforced by triggers) and never change.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from .codec import (
    ABSENT,
    CODE_ABSENT,
    CODE_EMPTY,
    CODE_NULL,
    CODE_PRESENT,
    StoreError,
    ValidationError,
    check_slot_fields,
    dumps_value,
    loads_scalar,
    normalize_device_doc,
)
from .merge import (
    AttDraft,
    Draft,
    MergeConflict,
    SlotDraft,
    StaleLocal,
    draft_ids,
    draft_import_ids,
    draft_to_json,
    draft_to_plain_document,
    first_difference,
    merge_drafts,
    parse_draft,
)

SCHEMA_VERSION = 2

SHAPE_NAMES = {
    CODE_ABSENT: "absent",
    CODE_NULL: "null",
    CODE_EMPTY: "empty",
    CODE_PRESENT: "present",
}


# ---------------------------------------------------------------------------
# Errors


class NotFound(StoreError):
    """A device or slot does not exist (any more)."""


class DuplicateIdentity(StoreError):
    """A slot id was used twice, in this batch or in storage."""


class BatchAborted(StoreError):
    """A batch failed validation; nothing from it became visible."""


class IntegrityError(StoreError):
    """The stored relations do not describe a valid document."""


# ---------------------------------------------------------------------------
# Schema

# Append-only history.  One row per committed, content-changing version of a
# device.  Rows are never updated or deleted: restoring an old version appends
# a new version instead of rewriting history, so version numbers always grow
# in time order and stay readable.  The payload is the full identified draft
# (every slot's stable id, fields, hole flag, attachment ownership and array
# order), which is exactly what the merge write-back replays.
_HISTORY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS device_version (
    device_key      TEXT NOT NULL,
    version_no      INTEGER NOT NULL CHECK (version_no >= 1),
    created_at      TEXT NOT NULL,
    kind            TEXT NOT NULL,
    content_hash    TEXT NOT NULL,
    section_slots   INTEGER NOT NULL CHECK (section_slots >= 0),
    attachment_slots INTEGER NOT NULL CHECK (attachment_slots >= 0),
    draft_json      TEXT NOT NULL,
    PRIMARY KEY (device_key, version_no)
);
"""

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    name  TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS device (
    device_key    TEXT PRIMARY KEY,
    section_shape INTEGER NOT NULL CHECK (section_shape IN (0, 1, 2, 3)),
    section_root  TEXT,
    CHECK ((section_shape = 3 AND section_root IS NOT NULL)
        OR (section_shape <> 3 AND section_root IS NULL))
);

CREATE TABLE IF NOT EXISTS section_slot (
    slot_id      TEXT PRIMARY KEY,
    device_key   TEXT NOT NULL REFERENCES device(device_key) ON DELETE CASCADE,
    parent_id    TEXT REFERENCES section_slot(slot_id) DEFERRABLE INITIALLY DEFERRED,
    left_id      TEXT REFERENCES section_slot(slot_id) DEFERRABLE INITIALLY DEFERRED,
    right_id     TEXT REFERENCES section_slot(slot_id) DEFERRABLE INITIALLY DEFERRED,
    priority     INTEGER NOT NULL,
    subtree_size INTEGER NOT NULL CHECK (subtree_size >= 1),
    is_hole      INTEGER NOT NULL CHECK (is_hole IN (0, 1)),
    fields_json  TEXT NOT NULL,
    att_shape    INTEGER NOT NULL CHECK (att_shape IN (0, 1, 2, 3)),
    att_root     TEXT,
    CHECK (is_hole = 0 OR (fields_json = '{}' AND att_shape = 0 AND att_root IS NULL)),
    CHECK ((att_shape = 3 AND att_root IS NOT NULL)
        OR (att_shape <> 3 AND att_root IS NULL))
);

CREATE INDEX IF NOT EXISTS section_slot_device ON section_slot(device_key);
CREATE INDEX IF NOT EXISTS section_slot_parent ON section_slot(parent_id);

CREATE TABLE IF NOT EXISTS attachment_slot (
    slot_id          TEXT PRIMARY KEY,
    section_slot_id  TEXT NOT NULL REFERENCES section_slot(slot_id) ON DELETE CASCADE,
    parent_id        TEXT REFERENCES attachment_slot(slot_id) DEFERRABLE INITIALLY DEFERRED,
    left_id          TEXT REFERENCES attachment_slot(slot_id) DEFERRABLE INITIALLY DEFERRED,
    right_id         TEXT REFERENCES attachment_slot(slot_id) DEFERRABLE INITIALLY DEFERRED,
    priority         INTEGER NOT NULL,
    subtree_size     INTEGER NOT NULL CHECK (subtree_size >= 1),
    is_hole          INTEGER NOT NULL CHECK (is_hole IN (0, 1)),
    fields_json      TEXT NOT NULL,
    CHECK (is_hole = 0 OR fields_json = '{}')
);

CREATE INDEX IF NOT EXISTS attachment_slot_owner ON attachment_slot(section_slot_id);
CREATE INDEX IF NOT EXISTS attachment_slot_parent ON attachment_slot(parent_id);

CREATE TRIGGER IF NOT EXISTS section_id_not_attachment
BEFORE INSERT ON section_slot
WHEN EXISTS (SELECT 1 FROM attachment_slot WHERE slot_id = NEW.slot_id)
BEGIN
    SELECT RAISE(ABORT, 'duplicate slot identity');
END;

CREATE TRIGGER IF NOT EXISTS attachment_id_not_section
BEFORE INSERT ON attachment_slot
WHEN EXISTS (SELECT 1 FROM section_slot WHERE slot_id = NEW.slot_id)
BEGIN
    SELECT RAISE(ABORT, 'duplicate slot identity');
END;
""" + _HISTORY_TABLE_SQL

# The v1 schema, kept verbatim so tests can build a legacy database and prove
# that opening it upgrades the data in place.  Version 2 only adds the
# device_version table; the three live relations are byte-for-byte the same.
_LEGACY_SCHEMA_V1 = """
CREATE TABLE IF NOT EXISTS meta (
    name  TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS device (
    device_key    TEXT PRIMARY KEY,
    section_shape INTEGER NOT NULL CHECK (section_shape IN (0, 1, 2, 3)),
    section_root  TEXT,
    CHECK ((section_shape = 3 AND section_root IS NOT NULL)
        OR (section_shape <> 3 AND section_root IS NULL))
);

CREATE TABLE IF NOT EXISTS section_slot (
    slot_id      TEXT PRIMARY KEY,
    device_key   TEXT NOT NULL REFERENCES device(device_key) ON DELETE CASCADE,
    parent_id    TEXT REFERENCES section_slot(slot_id) DEFERRABLE INITIALLY DEFERRED,
    left_id      TEXT REFERENCES section_slot(slot_id) DEFERRABLE INITIALLY DEFERRED,
    right_id     TEXT REFERENCES section_slot(slot_id) DEFERRABLE INITIALLY DEFERRED,
    priority     INTEGER NOT NULL,
    subtree_size INTEGER NOT NULL CHECK (subtree_size >= 1),
    is_hole      INTEGER NOT NULL CHECK (is_hole IN (0, 1)),
    fields_json  TEXT NOT NULL,
    att_shape    INTEGER NOT NULL CHECK (att_shape IN (0, 1, 2, 3)),
    att_root     TEXT,
    CHECK (is_hole = 0 OR (fields_json = '{}' AND att_shape = 0 AND att_root IS NULL)),
    CHECK ((att_shape = 3 AND att_root IS NOT NULL)
        OR (att_shape <> 3 AND att_root IS NULL))
);

CREATE INDEX IF NOT EXISTS section_slot_device ON section_slot(device_key);
CREATE INDEX IF NOT EXISTS section_slot_parent ON section_slot(parent_id);

CREATE TABLE IF NOT EXISTS attachment_slot (
    slot_id          TEXT PRIMARY KEY,
    section_slot_id  TEXT NOT NULL REFERENCES section_slot(slot_id) ON DELETE CASCADE,
    parent_id        TEXT REFERENCES attachment_slot(slot_id) DEFERRABLE INITIALLY DEFERRED,
    left_id          TEXT REFERENCES attachment_slot(slot_id) DEFERRABLE INITIALLY DEFERRED,
    right_id         TEXT REFERENCES attachment_slot(slot_id) DEFERRABLE INITIALLY DEFERRED,
    priority         INTEGER NOT NULL,
    subtree_size     INTEGER NOT NULL CHECK (subtree_size >= 1),
    is_hole          INTEGER NOT NULL CHECK (is_hole IN (0, 1)),
    fields_json      TEXT NOT NULL,
    CHECK (is_hole = 0 OR fields_json = '{}')
);

CREATE INDEX IF NOT EXISTS attachment_slot_owner ON attachment_slot(section_slot_id);
CREATE INDEX IF NOT EXISTS attachment_slot_parent ON attachment_slot(parent_id);

CREATE TRIGGER IF NOT EXISTS section_id_not_attachment
BEFORE INSERT ON section_slot
WHEN EXISTS (SELECT 1 FROM attachment_slot WHERE slot_id = NEW.slot_id)
BEGIN
    SELECT RAISE(ABORT, 'duplicate slot identity');
END;

CREATE TRIGGER IF NOT EXISTS attachment_id_not_section
BEFORE INSERT ON attachment_slot
WHEN EXISTS (SELECT 1 FROM section_slot WHERE slot_id = NEW.slot_id)
BEGIN
    SELECT RAISE(ABORT, 'duplicate slot identity');
END;
"""


# ---------------------------------------------------------------------------
# Stable ids and priorities


def new_slot_id(prefix: str = "s") -> str:
    """A fresh stable slot id.  It is opaque and has no position meaning."""
    return f"{prefix}_{uuid.uuid4().hex}"


def priority_for(slot_id: str) -> int:
    """Deterministic 63-bit priority.

    Deriving it from the id keeps the tree shape reproducible and avoids a
    separate random source, while hashing prevents callers inserting ids in
    sorted order from degenerating the treap.  Ties are broken by the id.
    """
    digest = hashlib.sha256(slot_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") & ((1 << 63) - 1)


def _stored_fields(fields_json: str) -> Tuple[Tuple[str, Any], ...]:
    """A stored fields_json as a sorted (name, scalar) tuple."""
    return tuple(sorted(loads_scalar(fields_json).items()))


def _canonical_draft_json(draft: "Draft") -> str:
    """Canonical JSON of an identified draft.

    The canonical text carries everything a version must preserve: every
    field, the four-way array shape (absent / null / empty / present, holes
    included), each slot's stable identity, array order and attachment
    ownership.  Two drafts with the same text are the same content, so it also
    defines "no content change" for the history.
    """
    return json.dumps(
        draft_to_json(draft),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _draft_slot_counts(draft: "Draft") -> Tuple[int, int]:
    section_slots = 0
    attachment_slots = 0
    if isinstance(draft.section, tuple):
        section_slots = len(draft.section)
        for slot in draft.section:
            if isinstance(slot.attachments, tuple):
                attachment_slots += len(slot.attachments)
    return section_slots, attachment_slots


def _walk_rows(
    rows_by_id: Dict[str, Tuple[Any, ...]],
    root: Optional[str],
    left_index: int,
    right_index: int,
    label: str,
) -> List[str]:
    """In-order slot ids of one array from already-fetched rows.

    Used by the draft reader: the rows come from a handful of bulk queries
    inside one transaction, and the treap links are followed in memory.
    """
    order: List[str] = []
    stack: List[Tuple[Any, ...]] = []
    visited: set = set()
    current = root
    while stack or current is not None:
        while current is not None:
            if current in visited:
                raise IntegrityError(f"{label}: cycle at slot {current!r}")
            visited.add(current)
            row = rows_by_id.get(current)
            if row is None:
                raise IntegrityError(f"{label}: dangling slot id {current!r}")
            stack.append(row)
            current = row[left_index]
        row = stack.pop()
        order.append(row[0])
        current = row[right_index]
    if len(order) != len(rows_by_id):
        raise IntegrityError(
            f"{label}: {len(rows_by_id) - len(order)} slot rows are unreachable"
        )
    return order


# ---------------------------------------------------------------------------
# Implicit treap with lazy row loading


class _Node:
    __slots__ = ("id", "parent", "left", "right", "priority", "size", "dirty")

    def __init__(
        self,
        slot_id: str,
        parent: Optional[str] = None,
        left: Optional[str] = None,
        right: Optional[str] = None,
        priority: int = 0,
        size: int = 1,
    ) -> None:
        self.id = slot_id
        self.parent = parent
        self.left = left
        self.right = right
        self.priority = priority
        self.size = size
        self.dirty = False

    def __repr__(self) -> str:  # pragma: no cover
        return f"_Node({self.id!r}, l={self.left!r}, r={self.right!r}, n={self.size})"


def _higher(a: _Node, b: _Node) -> bool:
    if a.priority != b.priority:
        return a.priority > b.priority
    return a.id > b.id


class Meter:
    """Counts read work so page queries can be checked for linear scans."""

    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.sql_statements = 0
        self.selects = 0
        self.node_fetches = 0

    def on_sql(self, statement: str) -> None:
        self.sql_statements += 1
        if statement.lstrip()[:6].upper() == "SELECT":
            self.selects += 1


def _root_id(node: Optional[_Node]) -> Optional[str]:
    return None if node is None else node.id


class Treap:
    """One array, loaded lazily from one table.

    Only the nodes touched by an operation are read and the changed ones are
    written back.  ``loads`` and ``writes`` expose the work done, which the
    scale test uses to prove that an insert does not rewrite the suffix.
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        table: str,
        id_column: str,
        root: Optional[str] = None,
        scope_column: Optional[str] = None,
        scope_value: Optional[str] = None,
        meter: Optional["Meter"] = None,
    ) -> None:
        self.conn = conn
        self.meter = meter
        self.table = table
        self.id_column = id_column
        self.root = root
        self.scope_column = scope_column
        self.scope_value = scope_value
        self.cache: Dict[str, _Node] = {}
        self.loads = 0
        self.new_nodes: List[_Node] = []
        # Upper bound on rows touched; a corrupted structure cannot make an
        # operation recurse or walk without end.
        self.walk_budget = None if root is None else 4 * Treap._peek_size(conn, table, id_column, root) + 64

    @staticmethod
    def _peek_size(conn, table, id_column, root: str) -> int:
        row = conn.execute(
            f"SELECT subtree_size FROM {table} WHERE {id_column} = ?", (root,)
        ).fetchone()
        return row[0] if row is not None else 0

    # -- row access ------------------------------------------------------
    def load(self, slot_id: Optional[str]) -> Optional[_Node]:
        if slot_id is None:
            return None
        node = self.cache.get(slot_id)
        if node is not None:
            return node
        if self.walk_budget is not None and self.loads >= self.walk_budget:
            raise IntegrityError("array structure exceeded its walk budget")
        row = self.conn.execute(
            f"SELECT {self.id_column}, parent_id, left_id, right_id, priority, "
            f"subtree_size FROM {self.table} WHERE {self.id_column} = ?",
            (slot_id,),
        ).fetchone()
        if row is None:
            raise NotFound(f"slot {slot_id!r} not found in {self.table}")
        node = _Node(row[0], row[1], row[2], row[3], row[4], row[5])
        self.cache[slot_id] = node
        self.loads += 1
        if self.meter is not None:
            self.meter.node_fetches += 1
        return node

    def make_node(
        self,
        slot_id: str,
        priority: Optional[int] = None,
        size: int = 1,
    ) -> _Node:
        node = _Node(slot_id, priority=priority_for(slot_id) if priority is None else priority, size=size)
        node.dirty = True
        self.cache[slot_id] = node
        self.new_nodes.append(node)
        if self.walk_budget is not None:
            # New rows are cached, not fetched, but a batch can then walk a
            # larger tree; keep the guard proportional to the array.
            self.walk_budget += 4
        return node

    def _set_left(self, node: _Node, child: Optional[_Node]) -> None:
        new_id = child.id if child is not None else None
        if node.left != new_id:
            node.left = new_id
            node.dirty = True
        if child is not None and child.parent != node.id:
            child.parent = node.id
            child.dirty = True

    def _set_right(self, node: _Node, child: Optional[_Node]) -> None:
        new_id = child.id if child is not None else None
        if node.right != new_id:
            node.right = new_id
            node.dirty = True
        if child is not None and child.parent != node.id:
            child.parent = node.id
            child.dirty = True

    def _clear_parent(self, node: Optional[_Node]) -> None:
        if node is not None and node.parent is not None:
            node.parent = None
            node.dirty = True

    def _pull(self, node: _Node) -> int:
        left = self.load(node.left)
        right = self.load(node.right)
        size = 1 + (left.size if left else 0) + (right.size if right else 0)
        if node.size != size:
            node.size = size
            node.dirty = True
        return size

    # -- treap operations ------------------------------------------------
    def merge(self, a: Optional[_Node], b: Optional[_Node]) -> Optional[_Node]:
        if a is None:
            self._clear_parent(b)
            return b
        if b is None:
            self._clear_parent(a)
            return a
        if _higher(a, b):
            child = self.merge(self.load(a.right), b)
            self._set_right(a, child)
            self._pull(a)
            self._clear_parent(a)
            return a
        child = self.merge(a, self.load(b.left))
        self._set_left(b, child)
        self._pull(b)
        self._clear_parent(b)
        return b

    def split(self, root: Optional[_Node], k: int) -> Tuple[Optional[_Node], Optional[_Node]]:
        """Split into the first ``k`` slots and the rest."""
        if root is None:
            return None, None
        left = self.load(root.left)
        left_size = left.size if left is not None else 0
        if k <= left_size:
            a, b = self.split(left, k)
            self._set_left(root, b)
            self._pull(root)
            self._clear_parent(root)
            if a is not None:
                self._clear_parent(a)
            return a, root
        a, b = self.split(self.load(root.right), k - left_size - 1)
        self._set_right(root, a)
        self._pull(root)
        self._clear_parent(root)
        if b is not None:
            self._clear_parent(b)
        return root, b

    def build(self, nodes: Sequence[_Node]) -> Optional[_Node]:
        """Build a treap from nodes already in array order, O(k)."""
        stack: List[_Node] = []
        for node in nodes:
            node.left = None
            node.right = None
            node.parent = None
            node.dirty = True
            last: Optional[_Node] = None
            while stack and _higher(node, stack[-1]):
                last = stack.pop()
            if last is not None:
                node.left = last.id
                last.parent = node.id
                last.dirty = True
            if stack:
                stack[-1].right = node.id
                node.parent = stack[-1].id
                stack[-1].dirty = True
            stack.append(node)
        if not stack:
            return None
        root = stack[0]
        # Compute sizes bottom-up without recursion (a long right spine is
        # possible in a crafted tree).
        order: List[_Node] = []
        todo = [root]
        while todo:
            n = todo.pop()
            order.append(n)
            if n.left is not None:
                todo.append(self.cache[n.left])
            if n.right is not None:
                todo.append(self.cache[n.right])
        for n in reversed(order):
            left = self.cache[n.left] if n.left is not None else None
            right = self.cache[n.right] if n.right is not None else None
            n.size = 1 + (left.size if left else 0) + (right.size if right else 0)
            n.dirty = True
        return root

    def insert(self, node: _Node, position: int) -> Optional[_Node]:
        a, b = self.split(self.load(self.root), position)
        return self.merge(self.merge(a, node), b)

    def remove_at(self, position: int) -> Tuple[Optional[_Node], Optional[_Node]]:
        """Detach and return (new_root, removed_node)."""
        a, bc = self.split(self.load(self.root), position)
        mid, c = self.split(bc, 1)
        if mid is None:
            raise IntegrityError(f"no slot at position {position}")
        return self.merge(a, c), mid

    def root_size(self) -> int:
        root = self.load(self.root)
        return root.size if root is not None else 0

    def select(self, position: int) -> _Node:
        if position < 0:
            raise ValueError("position must be >= 0")
        node = self.load(self.root)
        if node is None:
            raise IndexError("array is empty")
        k = position
        steps = 0
        while True:
            steps += 1
            if self.walk_budget is not None and steps > self.walk_budget:
                raise IntegrityError("array structure exceeded its walk budget")
            left = self.load(node.left)
            left_size = left.size if left is not None else 0
            if k < left_size:
                node = left
            elif k == left_size:
                return node
            else:
                k -= left_size + 1
                if node.right is None:
                    raise IndexError("position past end of array")
                node = self.load(node.right)

    def rank(self, slot_id: str) -> int:
        node = self.load(slot_id)
        if node is None:
            raise NotFound(slot_id)
        left = self.load(node.left)
        rank = left.size if left is not None else 0
        current = node
        steps = 0
        while current.parent is not None:
            steps += 1
            if self.walk_budget is not None and steps > self.walk_budget:
                raise IntegrityError("array structure exceeded its walk budget")
            parent = self.load(current.parent)
            if parent.right == current.id:
                pleb = self.load(parent.left)
                rank += 1 + (pleb.size if pleb is not None else 0)
            current = parent
        return rank

    def successor(self, node: _Node) -> Optional[_Node]:
        steps = 0
        if node.right is not None:
            current = self.load(node.right)
            while current.left is not None:
                steps += 1
                self._check_steps(steps)
                current = self.load(current.left)
            return current
        current = node
        while current.parent is not None:
            steps += 1
            self._check_steps(steps)
            parent = self.load(current.parent)
            if parent.left == current.id:
                return parent
            current = parent
        return None

    def _check_steps(self, steps: int) -> None:
        if self.walk_budget is not None and steps > self.walk_budget:
            raise IntegrityError("array structure exceeded its walk budget")

    def iter_slice(self, start: int, stop: int) -> Iterator[Tuple[int, _Node]]:
        """Yield (absolute position, node) for the half-open range."""
        if start < 0 or stop < start:
            raise ValueError("invalid half-open range")
        root = self.load(self.root)
        if root is None or start >= root.size:
            return
        node = self.select(start)
        if stop is not None and stop > root.size:
            stop = root.size
        pos = start
        while node is not None and pos < stop:
            yield pos, node
            pos += 1
            if pos >= stop:
                break
            node = self.successor(node)

    def forget(self, slot_id: str) -> None:
        self.cache.pop(slot_id, None)

    def inorder_ids(self) -> Iterator[str]:
        """Stream slot ids in array order using O(log n) memory.

        Uses only primary-key lookups, so a huge array can be traversed
        without materializing either the whole document or a node cache.
        """
        stack: List[str] = []
        visited = 0
        current: Optional[str] = self.root
        while stack or current is not None:
            while current is not None:
                if (
                    self.walk_budget is not None
                    and len(stack) >= self.walk_budget
                ):
                    raise IntegrityError("array structure exceeded its walk budget")
                stack.append(current)
                current = self._child(current, "left_id")
            current = stack.pop()
            yield current
            visited += 1
            if self.walk_budget is not None and visited > self.walk_budget:
                raise IntegrityError("array structure exceeded its walk budget")
            current = self._child(current, "right_id")

    def _child(self, slot_id: str, column: str) -> Optional[str]:
        row = self.conn.execute(
            f"SELECT {column} FROM {self.table} WHERE {self.id_column} = ?",
            (slot_id,),
        ).fetchone()
        if row is None:
            raise IntegrityError(f"dangling {column} reference to {slot_id!r}")
        if self.meter is not None:
            self.meter.node_fetches += 1
        return row[0]

    # -- persistence -----------------------------------------------------
    def flush(self) -> int:
        """Write changed structural rows; returns the number of updates."""
        dirty = [n for n in self.cache.values() if n.dirty]
        if dirty:
            self.conn.executemany(
                f"UPDATE {self.table} SET parent_id = ?, left_id = ?, right_id = ?, "
                f"priority = ?, subtree_size = ? WHERE {self.id_column} = ?",
                [
                    (n.parent, n.left, n.right, n.priority, n.size, n.id)
                    for n in dirty
                ],
            )
        for n in dirty:
            n.dirty = False
        return len(dirty)

# ---------------------------------------------------------------------------
# Edit description


@dataclass(frozen=True)
class Edit:
    """One edit inside a batch.

    Use the class methods rather than constructing directly.  ``before`` is a
    stable slot id (insert before it) or ``None`` (append at the tail).  Slot
    payloads use the same nested vocabulary as documents, but an edit may
    only *initialize* an attachment array to empty; it may not create a
    non-empty attachment array in one step.
    """

    op: str
    slot_id: Optional[str] = None
    section: Optional[str] = None
    before: Optional[str] = None
    value: Any = None

    @classmethod
    def init_section_empty(cls) -> "Edit":
        return cls("init_section_empty")

    @classmethod
    def init_attachments_empty(cls, section_id: str) -> "Edit":
        return cls("init_attachments_empty", section=section_id)

    @classmethod
    def insert_section(
        cls,
        slot_id: str,
        before: Optional[str] = None,
        slot: Any = None,
    ) -> "Edit":
        return cls("insert_section", slot_id=slot_id, before=before, value=slot)

    @classmethod
    def delete_section(cls, slot_id: str) -> "Edit":
        return cls("delete_section", slot_id=slot_id)

    @classmethod
    def insert_attachment(
        cls,
        section_id: str,
        slot_id: str,
        before: Optional[str] = None,
        slot: Any = None,
    ) -> "Edit":
        return cls(
            "insert_attachment",
            section=section_id,
            slot_id=slot_id,
            before=before,
            value=slot,
        )

    @classmethod
    def delete_attachment(cls, section_id: str, slot_id: str) -> "Edit":
        return cls("delete_attachment", section=section_id, slot_id=slot_id)

    @classmethod
    def from_json(cls, raw: Mapping[str, Any]) -> "Edit":
        if not isinstance(raw, Mapping):
            raise ValidationError("edit must be an object")
        op = raw.get("op")
        allowed = {
            "init_section_empty": set(),
            "init_attachments_empty": {"section"},
            "insert_section": {"id", "before", "slot"},
            "delete_section": {"id"},
            "insert_attachment": {"section", "id", "before", "slot"},
            "delete_attachment": {"section", "id"},
        }
        if op not in allowed:
            raise ValidationError(f"unknown edit op {op!r}")
        extra = set(raw) - allowed[op] - {"op"}
        if extra:
            raise ValidationError(f"unknown keys for {op}: {sorted(extra)}")
        if op == "init_section_empty":
            return cls(op)
        if op == "init_attachments_empty":
            if "section" not in raw:
                raise ValidationError("init_attachments_empty needs 'section'")
            return cls(op, section=raw["section"])
        if op == "insert_section":
            if "id" not in raw:
                raise ValidationError("insert_section needs 'id'")
            return cls(
                op,
                slot_id=raw["id"],
                before=raw.get("before"),
                value=raw.get("slot"),
            )
        if op == "delete_section":
            if "id" not in raw:
                raise ValidationError("delete_section needs 'id'")
            return cls(op, slot_id=raw["id"])
        if op == "insert_attachment":
            if "section" not in raw or "id" not in raw:
                raise ValidationError("insert_attachment needs 'section' and 'id'")
            return cls(
                op,
                section=raw["section"],
                slot_id=raw["id"],
                before=raw.get("before"),
                value=raw.get("slot"),
            )
        if op == "delete_attachment":
            if "section" not in raw or "id" not in raw:
                raise ValidationError("delete_attachment needs 'section' and 'id'")
            return cls(op, section=raw["section"], slot_id=raw["id"])
        raise AssertionError(op)  # pragma: no cover


def _coerce_edit(edit: Any) -> Edit:
    if isinstance(edit, Edit):
        return edit
    if isinstance(edit, Mapping):
        return Edit.from_json(edit)
    raise ValidationError(f"edit must be an Edit or object, got {type(edit).__name__}")


# ---------------------------------------------------------------------------
# Section slot payloads used by edits


def _parse_section_slot_payload(value: Any) -> Tuple[int, str, int, Optional[str]]:
    """Return (is_hole, fields_json, att_shape, att_root)."""
    if value is None:
        return 1, "{}", CODE_ABSENT, None
    if not isinstance(value, Mapping):
        raise ValidationError("section slot payload must be null or an object")
    extra = set(value) - {"fields", "attachments"}
    if extra:
        raise ValidationError(f"unknown section slot keys: {sorted(extra)}")
    fields = check_slot_fields(value.get("fields", {}))
    att = value["attachments"] if "attachments" in value else ABSENT
    if att is ABSENT:
        shape = CODE_ABSENT
    elif att is None:
        shape = CODE_NULL
    elif isinstance(att, list):
        if att:
            raise ValidationError(
                "an edit may only insert an empty attachment array; "
                "initialize it and insert attachment slots"
            )
        shape = CODE_EMPTY
    else:
        raise ValidationError("attachments must be absent, null, or an empty list")
    return 0, dumps_value(dict(fields)), shape, None


def _parse_attachment_slot_payload(value: Any) -> Tuple[int, str]:
    """Return (is_hole, fields_json)."""
    if value is None:
        return 1, "{}"
    fields = check_slot_fields(value)
    return 0, dumps_value(dict(fields))


# ---------------------------------------------------------------------------
# Batch session


class _Session:
    def __init__(self, store: "DeviceStore", device_key: str) -> None:
        self.store = store
        self.conn = store.conn
        self.device_key = device_key
        row = self.conn.execute(
            "SELECT section_shape, section_root FROM device WHERE device_key = ?",
            (device_key,),
        ).fetchone()
        if row is None:
            raise NotFound(f"device {device_key!r} does not exist")
        self.section_shape = row[0]
        self.section_root = row[1]
        self.section_treap: Optional[Treap] = None
        if self.section_shape in (CODE_EMPTY, CODE_PRESENT):
            self.section_treap = Treap(
                self.conn, "section_slot", "slot_id", root=self.section_root
            )
        # sid -> (att_shape, att_root, is_hole)
        self.att_state: Dict[str, Tuple[int, Optional[str], int]] = {}
        self.att_treaps: Dict[str, Treap] = {}
        self.att_dirty: Set[str] = set()
        self.device_dirty = False
        self.used_ids: Set[str] = set()
        self.stats = {
            "inserted_rows": 0,
            "deleted_rows": 0,
            "cascaded_attachment_rows": 0,
            "treap_updated_rows": 0,
            "control_updated_rows": 0,
            "nodes_loaded": 0,
            "rows_written": 0,
        }

    # -- helpers ---------------------------------------------------------
    def _require_array(self, what: str) -> Treap:
        if self.section_treap is None:
            raise ValidationError(
                f"{what}: section is {SHAPE_NAMES[self.section_shape]}; "
                "initialize it to an empty array first"
            )
        return self.section_treap

    def _register_new_id(self, slot_id: Any) -> str:
        if not isinstance(slot_id, str) or not slot_id:
            raise ValidationError("slot id must be a non-empty string")
        if slot_id in self.used_ids or self.store._slot_id_exists(slot_id):
            raise DuplicateIdentity(f"slot id {slot_id!r} already exists")
        self.used_ids.add(slot_id)
        return slot_id

    def _assert_section_owner(self, slot_id: str) -> None:
        row = self.conn.execute(
            "SELECT device_key FROM section_slot WHERE slot_id = ?", (slot_id,)
        ).fetchone()
        if row is None:
            if self.conn.execute(
                "SELECT 1 FROM attachment_slot WHERE slot_id = ?", (slot_id,)
            ).fetchone():
                raise ValidationError(
                    f"{slot_id!r} is an attachment slot, not a section slot"
                )
            raise NotFound(f"section slot {slot_id!r} does not exist")
        if row[0] != self.device_key:
            raise ValidationError(
                f"section slot {slot_id!r} belongs to another device"
            )

    def _assert_attachment_owner(self, slot_id: str, section_id: str) -> None:
        row = self.conn.execute(
            "SELECT section_slot_id FROM attachment_slot WHERE slot_id = ?",
            (slot_id,),
        ).fetchone()
        if row is None:
            if self.conn.execute(
                "SELECT 1 FROM section_slot WHERE slot_id = ?", (slot_id,)
            ).fetchone():
                raise ValidationError(
                    f"{slot_id!r} is a section slot, not an attachment slot"
                )
            raise NotFound(f"attachment slot {slot_id!r} does not exist")
        if row[0] != section_id:
            raise ValidationError(
                f"attachment slot {slot_id!r} belongs to a different section"
            )

    def _load_att_state(self, section_id: str) -> Tuple[int, Optional[str], int]:
        state = self.att_state.get(section_id)
        if state is not None:
            return state
        row = self.conn.execute(
            "SELECT att_shape, att_root, is_hole, device_key "
            "FROM section_slot WHERE slot_id = ?",
            (section_id,),
        ).fetchone()
        if row is None:
            raise NotFound(f"section slot {section_id!r} does not exist")
        if row[3] != self.device_key:
            raise ValidationError(
                f"section slot {section_id!r} belongs to another device"
            )
        state = (row[0], row[1], row[2])
        self.att_state[section_id] = state
        return state

    def _att_treap(self, section_id: str) -> Treap:
        treap = self.att_treaps.get(section_id)
        if treap is None:
            state = self._load_att_state(section_id)
            root = state[1] if state[0] == CODE_PRESENT else None
            treap = Treap(
                self.conn,
                "attachment_slot",
                "slot_id",
                root=root,
                scope_column="section_slot_id",
                scope_value=section_id,
            )
            self.att_treaps[section_id] = treap
        return treap

    def _set_att_state(
        self, section_id: str, shape: int, root: Optional[str]
    ) -> None:
        state = self.att_state.get(section_id)
        is_hole = state[2] if state is not None else 0
        self.att_state[section_id] = (shape, root, is_hole)
        self.att_dirty.add(section_id)

    # -- operations ------------------------------------------------------
    def apply(self, edit: Edit) -> None:
        handler = getattr(self, "_op_" + edit.op, None)
        if handler is None:
            raise ValidationError(f"unknown edit op {edit.op!r}")
        handler(edit)

    def _op_init_section_empty(self, edit: Edit) -> None:
        if self.section_shape not in (CODE_ABSENT, CODE_NULL):
            raise ValidationError(
                "section is already an array; it cannot be re-initialized"
            )
        self.section_shape = CODE_EMPTY
        self.section_root = None
        self.section_treap = Treap(self.conn, "section_slot", "slot_id", root=None)
        self.device_dirty = True

    def _op_init_attachments_empty(self, edit: Edit) -> None:
        section_id = edit.section
        if not isinstance(section_id, str) or not section_id:
            raise ValidationError("init_attachments_empty needs a section id")
        shape, root, is_hole = self._load_att_state(section_id)
        if is_hole:
            raise ValidationError("a hole has no attachment array")
        if shape not in (CODE_ABSENT, CODE_NULL):
            raise ValidationError(
                "attachments are already an array; they cannot be re-initialized"
            )
        self._set_att_state(section_id, CODE_EMPTY, None)

    def _op_insert_section(self, edit: Edit) -> None:
        treap = self._require_array("insert_section")
        slot_id = self._register_new_id(edit.slot_id)
        if edit.before is not None:
            self._assert_section_owner(edit.before)
        is_hole, fields_json, att_shape, att_root = _parse_section_slot_payload(
            edit.value
        )
        self.conn.execute(
            "INSERT INTO section_slot (slot_id, device_key, parent_id, left_id, "
            "right_id, priority, subtree_size, is_hole, fields_json, att_shape, "
            "att_root) VALUES (?, ?, NULL, NULL, NULL, ?, 1, ?, ?, ?, NULL)",
            (
                slot_id,
                self.device_key,
                priority_for(slot_id),
                is_hole,
                fields_json,
                att_shape,
            ),
        )
        self.stats["inserted_rows"] += 1
        node = treap.make_node(slot_id)
        position = treap.rank(edit.before) if edit.before is not None else treap.root_size()
        treap.root = _root_id(treap.insert(node, position))
        self.section_root = treap.root
        self.section_shape = CODE_PRESENT
        self.device_dirty = True
        self.att_state[slot_id] = (att_shape, att_root, is_hole)

    def _op_delete_section(self, edit: Edit) -> None:
        treap = self._require_array("delete_section")
        slot_id = edit.slot_id
        if not isinstance(slot_id, str) or not slot_id:
            raise ValidationError("delete_section needs a slot id")
        self._assert_section_owner(slot_id)
        position = treap.rank(slot_id)
        count = self.conn.execute(
            "SELECT COUNT(*) FROM attachment_slot WHERE section_slot_id = ?",
            (slot_id,),
        ).fetchone()[0]
        new_root, removed = treap.remove_at(position)
        treap.root = _root_id(new_root)
        treap.forget(slot_id)
        self.att_treaps.pop(slot_id, None)
        # mid should be exactly the requested slot.
        if removed is None or removed.id != slot_id:  # pragma: no cover
            raise IntegrityError("treap rank did not identify the requested slot")
        self.conn.execute("DELETE FROM section_slot WHERE slot_id = ?", (slot_id,))
        self.stats["deleted_rows"] += 1
        self.stats["cascaded_attachment_rows"] += count
        self.used_ids.add(slot_id)
        self.att_state.pop(slot_id, None)
        self.att_treaps.pop(slot_id, None)
        self.att_dirty.discard(slot_id)
        self.section_root = treap.root
        self.section_shape = CODE_PRESENT if treap.root is not None else CODE_EMPTY
        self.device_dirty = True

    def _op_insert_attachment(self, edit: Edit) -> None:
        section_id = edit.section
        if not isinstance(section_id, str) or not section_id:
            raise ValidationError("insert_attachment needs a section id")
        shape, root, is_hole = self._load_att_state(section_id)
        if is_hole:
            raise ValidationError("a hole has no attachment array")
        if shape not in (CODE_EMPTY, CODE_PRESENT):
            raise ValidationError(
                "attachments are absent or null; initialize them to an empty "
                "array first"
            )
        slot_id = self._register_new_id(edit.slot_id)
        if edit.before is not None:
            self._assert_attachment_owner(edit.before, section_id)
        is_att_hole, fields_json = _parse_attachment_slot_payload(edit.value)
        self.conn.execute(
            "INSERT INTO attachment_slot (slot_id, section_slot_id, parent_id, "
            "left_id, right_id, priority, subtree_size, is_hole, fields_json) "
            "VALUES (?, ?, NULL, NULL, NULL, ?, 1, ?, ?)",
            (
                slot_id,
                section_id,
                priority_for(slot_id),
                is_att_hole,
                fields_json,
            ),
        )
        self.stats["inserted_rows"] += 1
        treap = self._att_treap(section_id)
        node = treap.make_node(slot_id)
        position = treap.rank(edit.before) if edit.before is not None else treap.root_size()
        treap.root = _root_id(treap.insert(node, position))
        self._set_att_state(section_id, CODE_PRESENT, treap.root)

    def _op_delete_attachment(self, edit: Edit) -> None:
        section_id = edit.section
        slot_id = edit.slot_id
        if not isinstance(section_id, str) or not section_id:
            raise ValidationError("delete_attachment needs a section id")
        if not isinstance(slot_id, str) or not slot_id:
            raise ValidationError("delete_attachment needs a slot id")
        shape, root, is_hole = self._load_att_state(section_id)
        if is_hole:
            raise ValidationError("a hole has no attachment array")
        if shape not in (CODE_EMPTY, CODE_PRESENT):
            raise ValidationError("attachments are not an array")
        self._assert_attachment_owner(slot_id, section_id)
        treap = self._att_treap(section_id)
        position = treap.rank(slot_id)
        new_root, removed = treap.remove_at(position)
        treap.root = _root_id(new_root)
        treap.forget(slot_id)
        if removed is None or removed.id != slot_id:  # pragma: no cover
            raise IntegrityError("treap rank did not identify the requested slot")
        self.conn.execute("DELETE FROM attachment_slot WHERE slot_id = ?", (slot_id,))
        self.stats["deleted_rows"] += 1
        self.used_ids.add(slot_id)
        self._set_att_state(
            section_id,
            CODE_PRESENT if treap.root is not None else CODE_EMPTY,
            treap.root,
        )

    # -- finalize --------------------------------------------------------
    def finalize(self) -> Dict[str, int]:
        updates = 0
        if self.section_treap is not None:
            updates += self.section_treap.flush()
            self.stats["nodes_loaded"] += self.section_treap.loads
        for treap in self.att_treaps.values():
            updates += treap.flush()
            self.stats["nodes_loaded"] += treap.loads
        self.stats["treap_updated_rows"] = updates

        if self.device_dirty:
            self.conn.execute(
                "UPDATE device SET section_shape = ?, section_root = ? "
                "WHERE device_key = ?",
                (self.section_shape, self.section_root, self.device_key),
            )
            self.stats["control_updated_rows"] += 1
        for section_id in sorted(self.att_dirty):
            state = self.att_state.get(section_id)
            if state is None:
                continue
            shape, root, _ = state
            cur = self.conn.execute(
                "UPDATE section_slot SET att_shape = ?, att_root = ? "
                "WHERE slot_id = ?",
                (shape, root, section_id),
            )
            if cur.rowcount:
                self.stats["control_updated_rows"] += 1

        self.stats["rows_written"] = (
            self.stats["inserted_rows"]
            + self.stats["deleted_rows"]
            + self.stats["cascaded_attachment_rows"]
            + self.stats["treap_updated_rows"]
            + self.stats["control_updated_rows"]
        )
        return self.stats


# ---------------------------------------------------------------------------
# Store


class DeviceStore:
    """A file-backed relational store for device inspection documents.

    One SQLite file holds many devices.  All public mutating methods are
    transactional and batch atomic.
    """

    def __init__(self, path: str, *, create: bool = False) -> None:
        self.path = os.fspath(path)
        if not create and self.path != ":memory:" and not os.path.exists(self.path):
            raise StoreError(f"store does not exist: {self.path}")
        self.conn = sqlite3.connect(self.path, isolation_level=None)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA busy_timeout = 5000")
        self.meter = Meter()
        self._trace_callback: Optional[Any] = None
        if create:
            # executescript() issues its own COMMIT, so keep it outside the
            # explicit transaction wrapper.
            self.conn.executescript(_SCHEMA)
            with self._transaction():
                self.conn.execute(
                    "INSERT OR IGNORE INTO meta(name, value) VALUES ('schema_version', ?)",
                    (str(SCHEMA_VERSION),),
                )
        else:
            self._check_schema()

    # -- lifecycle -------------------------------------------------------
    def _check_schema(self) -> None:
        try:
            row = self.conn.execute(
                "SELECT value FROM meta WHERE name = 'schema_version'"
            ).fetchone()
        except sqlite3.DatabaseError as exc:
            raise StoreError("not an inspection store") from exc
        if row is None:
            raise StoreError("not a supported inspection store")
        try:
            version = int(row[0])
        except ValueError as exc:
            raise StoreError("not a supported inspection store") from exc
        if version > SCHEMA_VERSION:
            raise StoreError(
                f"store schema version {version} is newer than this software "
                f"({SCHEMA_VERSION})"
            )
        if version < SCHEMA_VERSION:
            self._migrate(version)

    def _migrate(self, from_version: int) -> None:
        """Upgrade an older database in place, inside one transaction.

        The upgrade only *adds* history.  Every device already present becomes
        version 1: its current rows are read with the ordinary draft reader and
        snapshotted, so old data and every existing usage keep working while
        the same history machinery serves new writes.  A failure rolls the
        whole upgrade back and leaves the old database untouched.
        """
        if from_version != 1:
            raise StoreError(
                f"no migration path from schema version {from_version}"
            )
        with self._transaction():
            self.conn.execute(_HISTORY_TABLE_SQL)
            keys = [
                r[0]
                for r in self.conn.execute(
                    "SELECT device_key FROM device ORDER BY device_key"
                )
            ]
            for key in keys:
                draft = self._read_draft(key)
                self._insert_version(draft, "baseline")
            self.conn.execute(
                "UPDATE meta SET value = ? WHERE name = 'schema_version'",
                (str(SCHEMA_VERSION),),
            )

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "DeviceStore":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    class _Txn:
        def __init__(self, conn: sqlite3.Connection) -> None:
            self.conn = conn

        def __enter__(self) -> sqlite3.Connection:
            self.conn.execute("BEGIN IMMEDIATE")
            return self.conn

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            if exc_type is None:
                self.conn.execute("COMMIT")
            else:
                self.conn.execute("ROLLBACK")

    def _transaction(self) -> "DeviceStore._Txn":
        return DeviceStore._Txn(self.conn)

    # -- introspection ---------------------------------------------------
    def _slot_id_exists(self, slot_id: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM section_slot WHERE slot_id = ? "
            "UNION ALL SELECT 1 FROM attachment_slot WHERE slot_id = ?",
            (slot_id, slot_id),
        ).fetchone()
        return row is not None

    def enable_meter(self) -> Meter:
        """Count SQL reads/writes made through this connection."""
        self.meter.reset()
        self._trace_callback = self.meter.on_sql
        self.conn.set_trace_callback(self._trace_callback)
        return self.meter

    def disable_meter(self) -> None:
        self._trace_callback = None
        self.conn.set_trace_callback(None)

    def counts(self) -> Dict[str, int]:
        return {
            "device": self.conn.execute("SELECT COUNT(*) FROM device").fetchone()[0],
            "section_slot": self.conn.execute(
                "SELECT COUNT(*) FROM section_slot"
            ).fetchone()[0],
            "attachment_slot": self.conn.execute(
                "SELECT COUNT(*) FROM attachment_slot"
            ).fetchone()[0],
        }

    def dump_tables(self) -> Dict[str, List[Tuple[Any, ...]]]:
        """Raw ordered snapshots of the four relations (for inspection/tests)."""
        return {
            "device": self.conn.execute(
                "SELECT device_key, section_shape, section_root FROM device "
                "ORDER BY device_key"
            ).fetchall(),
            "section_slot": self.conn.execute(
                "SELECT slot_id, device_key, parent_id, left_id, right_id, "
                "priority, subtree_size, is_hole, fields_json, att_shape, "
                "att_root FROM section_slot ORDER BY slot_id"
            ).fetchall(),
            "attachment_slot": self.conn.execute(
                "SELECT slot_id, section_slot_id, parent_id, left_id, right_id, "
                "priority, subtree_size, is_hole, fields_json "
                "FROM attachment_slot ORDER BY slot_id"
            ).fetchall(),
            "device_version": self.conn.execute(
                "SELECT device_key, version_no, created_at, kind, content_hash, "
                "section_slots, attachment_slots, draft_json "
                "FROM device_version ORDER BY device_key, version_no"
            ).fetchall(),
        }

    def device_keys(self) -> List[str]:
        return [
            r[0]
            for r in self.conn.execute(
                "SELECT device_key FROM device ORDER BY device_key"
            )
        ]

    def array_shape(self, device_key: str, section_id: Optional[str] = None) -> str:
        """'absent' | 'null' | 'empty' | 'present' for a section or attachment array."""
        if section_id is None:
            row = self.conn.execute(
                "SELECT section_shape FROM device WHERE device_key = ?",
                (device_key,),
            ).fetchone()
            if row is None:
                raise NotFound(f"device {device_key!r} does not exist")
            return SHAPE_NAMES[row[0]]
        row = self.conn.execute(
            "SELECT device_key, is_hole, att_shape FROM section_slot WHERE slot_id = ?",
            (section_id,),
        ).fetchone()
        if row is None:
            raise NotFound(f"section slot {section_id!r} does not exist")
        if row[0] != device_key:
            raise ValidationError("section slot belongs to another device")
        if row[1]:
            raise ValidationError("a hole has no attachment array")
        return SHAPE_NAMES[row[2]]

    # -- import ----------------------------------------------------------
    def import_document(
        self,
        device_key: str,
        document: Mapping[str, Any],
        *,
        id_factory: Optional[Any] = None,
        replace: bool = False,
    ) -> Dict[str, int]:
        """Create (or with ``replace`` overwrite) a device from a document.

        ``document`` is the logical nested form: ``{"section": ...}`` with
        the section absent, null, or a list.  Stable slot ids are generated;
        pass ``id_factory(kind, ordinal)`` to control them.
        """
        normalized = normalize_device_doc(device_key, document)
        if id_factory is None:
            counter = {"section": 0, "attachment": 0}

            def id_factory(kind: str, ordinal: int) -> str:  # type: ignore[misc]
                return new_slot_id(kind[0])

        stats = {"section_slots": 0, "attachment_slots": 0}
        with self._transaction():
            existed = self.conn.execute(
                "SELECT 1 FROM device WHERE device_key = ?", (device_key,)
            ).fetchone() is not None
            previous = self._read_draft(device_key) if existed else None
            self._replace_document_rows(
                device_key, normalized, id_factory, replace=replace, stats=stats
            )
            recorded = self._record_version(
                device_key, "import" if previous is None else "replace", previous
            )
            stats.update(recorded)
        return stats

    def _replace_document_rows(
        self,
        device_key: str,
        normalized: Any,
        id_factory: Any,
        *,
        replace: bool,
        stats: Dict[str, int],
    ) -> None:
        """Insert one device's rows; must run inside an open transaction."""
        exists = self.conn.execute(
            "SELECT 1 FROM device WHERE device_key = ?", (device_key,)
        ).fetchone()
        if exists is not None:
            if not replace:
                raise StoreError(f"device {device_key!r} already exists")
            # Cascades remove the live slots; device_version rows are kept so
            # the pre-replacement history stays readable.
            self.conn.execute(
                "DELETE FROM attachment_slot WHERE section_slot_id IN "
                "(SELECT slot_id FROM section_slot WHERE device_key = ?)",
                (device_key,),
            )
            self.conn.execute(
                "DELETE FROM section_slot WHERE device_key = ?", (device_key,)
            )
            self.conn.execute(
                "DELETE FROM device WHERE device_key = ?", (device_key,)
            )

        if normalized.section is ABSENT:
            shape, root = CODE_ABSENT, None
        elif normalized.section is None:
            shape, root = CODE_NULL, None
        elif not normalized.section:
            shape, root = CODE_EMPTY, None
        else:
            shape = CODE_PRESENT
            root = None
        # A present array's root is not known until its slots exist; the
        # device row temporarily records an empty array inside this same
        # transaction, then is updated to "present".
        self.conn.execute(
            "INSERT INTO device(device_key, section_shape, section_root) "
            "VALUES (?, ?, ?)",
            (device_key, CODE_EMPTY if shape == CODE_PRESENT else shape, root),
        )
        if shape == CODE_PRESENT:
            root = self._import_section_array(
                device_key, normalized.section, id_factory, stats
            )
            self.conn.execute(
                "UPDATE device SET section_shape = ?, section_root = ? "
                "WHERE device_key = ?",
                (CODE_PRESENT, root, device_key),
            )

    def _import_section_array(
        self,
        device_key: str,
        slots: Sequence[Any],
        id_factory: Any,
        stats: Dict[str, int],
    ) -> str:
        from .codec import SlotDoc

        treap = Treap(self.conn, "section_slot", "slot_id", root=None)
        nodes: List[_Node] = []
        for ordinal, slot in enumerate(slots):
            assert isinstance(slot, SlotDoc)
            sid = id_factory("section", ordinal)
            if not isinstance(sid, str) or not sid:
                raise ValidationError("id_factory must return a non-empty string")
            is_hole = 1 if slot.fields is None else 0
            fields_json = dumps_value(dict(slot.fields or ()))
            if is_hole or slot.attachments is ABSENT:
                att_shape, att_root = CODE_ABSENT, None
            elif slot.attachments is None:
                att_shape, att_root = CODE_NULL, None
            elif not slot.attachments:
                att_shape, att_root = CODE_EMPTY, None
            else:
                # Insert the section row first (the attachment rows need it
                # as a foreign key and its root needs them), then fill in the
                # attachment array and update the row.
                self.conn.execute(
                    "INSERT INTO section_slot(slot_id, device_key, parent_id, "
                    "left_id, right_id, priority, subtree_size, is_hole, "
                    "fields_json, att_shape, att_root) "
                    "VALUES (?, ?, NULL, NULL, NULL, ?, 1, ?, ?, 2, NULL)",
                    (sid, device_key, priority_for(sid), is_hole, fields_json),
                )
                stats["section_slots"] += 1
                att_root = self._import_attachment_array(
                    sid, slot.attachments, id_factory, ordinal, stats
                )
                self.conn.execute(
                    "UPDATE section_slot SET att_shape = 3, att_root = ? "
                    "WHERE slot_id = ?",
                    (att_root, sid),
                )
                nodes.append(treap.make_node(sid))
                continue
            self.conn.execute(
                "INSERT INTO section_slot(slot_id, device_key, parent_id, left_id, "
                "right_id, priority, subtree_size, is_hole, fields_json, "
                "att_shape, att_root) VALUES (?, ?, NULL, NULL, NULL, ?, 1, ?, ?, ?, ?)",
                (
                    sid,
                    device_key,
                    priority_for(sid),
                    is_hole,
                    fields_json,
                    att_shape,
                    att_root,
                ),
            )
            stats["section_slots"] += 1
            nodes.append(treap.make_node(sid))
        root_node = treap.build(nodes)
        treap.flush()
        if root_node is None:
            raise IntegrityError("present section imported with no root")
        return root_node.id

    def _import_attachment_array(
        self,
        section_id: str,
        atts: Sequence[Any],
        id_factory: Any,
        ordinal: int,
        stats: Dict[str, int],
    ) -> str:
        treap = Treap(
            self.conn,
            "attachment_slot",
            "slot_id",
            root=None,
            scope_column="section_slot_id",
            scope_value=section_id,
        )
        nodes: List[_Node] = []
        for index, att in enumerate(atts):
            aid = id_factory("attachment", index)
            if not isinstance(aid, str) or not aid:
                raise ValidationError("id_factory must return a non-empty string")
            if att is None:
                is_hole, fields_json = 1, "{}"
            else:
                is_hole, fields_json = 0, dumps_value(dict(att))
            self.conn.execute(
                "INSERT INTO attachment_slot(slot_id, section_slot_id, parent_id, "
                "left_id, right_id, priority, subtree_size, is_hole, fields_json) "
                "VALUES (?, ?, NULL, NULL, NULL, ?, 1, ?, ?)",
                (aid, section_id, priority_for(aid), is_hole, fields_json),
            )
            stats["attachment_slots"] += 1
            nodes.append(treap.make_node(aid))
        root_node = treap.build(nodes)
        treap.flush()
        if root_node is None:
            raise IntegrityError("present attachment array imported with no root")
        return root_node.id

    # -- batching --------------------------------------------------------
    def apply_batch(self, device_key: str, edits: Iterable[Any]) -> Dict[str, int]:
        """Apply a homogeneous batch atomically.

        Every edit is validated and applied inside one transaction.  Any
        error rolls the whole batch back, so a bad batch leaves the stored
        view exactly as it was, while a successful one is visible to a fresh
        process.
        """
        try:
            edit_list = [_coerce_edit(e) for e in edits]
        except StoreError as exc:
            raise BatchAborted(str(exc)) from exc
        if not edit_list:
            return {
                "inserted_rows": 0,
                "deleted_rows": 0,
                "cascaded_attachment_rows": 0,
                "treap_updated_rows": 0,
                "control_updated_rows": 0,
                "nodes_loaded": 0,
                "rows_written": 0,
                "versions_added": 0,
            }
        with self._transaction():
            previous = self._read_draft(device_key)
            session = _Session(self, device_key)
            try:
                for edit in edit_list:
                    session.apply(edit)
            except (StoreError, ValueError, IndexError, sqlite3.Error) as exc:
                # Leave the store untouched; convert to a batch error so the
                # caller sees why the whole batch vanished.
                raise BatchAborted(str(exc)) from exc
            stats = session.finalize()
            stats.update(self._record_version(device_key, "batch", previous))
            return stats

    # -- reads -----------------------------------------------------------
    def _open_section_treap(self, device_key: str) -> Tuple[int, Optional[Treap]]:
        row = self.conn.execute(
            "SELECT section_shape, section_root FROM device WHERE device_key = ?",
            (device_key,),
        ).fetchone()
        if row is None:
            raise NotFound(f"device {device_key!r} does not exist")
        shape, root = row
        if shape in (CODE_EMPTY, CODE_PRESENT):
            return shape, Treap(
                self.conn, "section_slot", "slot_id", root=root, meter=self.meter
            )
        return shape, None

    def _open_attachment_treap(self, section_id: str) -> Tuple[int, Optional[Treap], int]:
        row = self.conn.execute(
            "SELECT att_shape, att_root, is_hole FROM section_slot WHERE slot_id = ?",
            (section_id,),
        ).fetchone()
        if row is None:
            raise NotFound(f"section slot {section_id!r} does not exist")
        shape, root, is_hole = row
        if shape == CODE_ABSENT:
            return shape, None, is_hole
        return shape, Treap(
            self.conn,
            "attachment_slot",
            "slot_id",
            root=root,
            scope_column="section_slot_id",
            scope_value=section_id,
            meter=self.meter,
        ), is_hole

    def _section_row(self, slot_id: str) -> Tuple[int, str, int]:
        row = self.conn.execute(
            "SELECT is_hole, fields_json, att_shape FROM section_slot "
            "WHERE slot_id = ?",
            (slot_id,),
        ).fetchone()
        if row is None:
            raise NotFound(f"section slot {slot_id!r} does not exist")
        return row[0], row[1], row[2]

    def _attachment_fields(self, slot_id: str) -> Any:
        row = self.conn.execute(
            "SELECT is_hole, fields_json FROM attachment_slot WHERE slot_id = ?",
            (slot_id,),
        ).fetchone()
        if row is None:
            raise NotFound(f"attachment slot {slot_id!r} does not exist")
        if row[0]:
            return None
        return loads_scalar(row[1])

    def _slot_document(self, slot_id: str) -> Any:
        """Nested representation of one section slot, attachments included."""
        is_hole, fields_json, att_shape = self._section_row(slot_id)
        if is_hole:
            return None
        out: Dict[str, Any] = {"fields": loads_scalar(fields_json)}
        if att_shape == CODE_ABSENT:
            return out
        if att_shape == CODE_NULL:
            out["attachments"] = None
            return out
        att_treap = Treap(
            self.conn,
            "attachment_slot",
            "slot_id",
            root=self.conn.execute(
                "SELECT att_root FROM section_slot WHERE slot_id = ?", (slot_id,)
            ).fetchone()[0],
            scope_column="section_slot_id",
            scope_value=slot_id,
            meter=self.meter,
        )
        out["attachments"] = [
            self._attachment_fields(aid) for aid in att_treap.inorder_ids()
        ]
        return out

    def load_document(self, device_key: str) -> Dict[str, Any]:
        """Rebuild the logical nested document for one device."""
        shape, treap = self._open_section_treap(device_key)
        doc: Dict[str, Any] = {"key": device_key}
        if shape == CODE_ABSENT:
            return doc
        if shape == CODE_NULL:
            doc["section"] = None
            return doc
        if treap is None:  # pragma: no cover - non-array shape handled above
            raise IntegrityError("section array shape without a treap")
        # inorder_ids() walks by primary-key lookups, so rebuilding a huge
        # array needs O(log n) memory rather than a full node cache.
        doc["section"] = [self._slot_document(sid) for sid in treap.inorder_ids()]
        return doc

    # -- identified drafts: checkout and three-way merge -------------------
    def checkout_document(self, device_key: str) -> Dict[str, Any]:
        """The identified draft of one device, from a single consistent read.

        Every array element (holes included) carries its stable slot id, so
        the draft can serve as the base, local, or returned input of an
        offline three-way merge.  The whole read runs inside one
        transaction: sections and attachments never come from different
        moments of a concurrent edit.
        """
        with self._transaction():
            draft = self._read_draft(device_key)
        return draft_to_json(draft)

    def _read_draft(self, device_key: str) -> Draft:
        """Bulk-read one device into a normalized draft (in a transaction)."""
        row = self.conn.execute(
            "SELECT section_shape, section_root FROM device WHERE device_key = ?",
            (device_key,),
        ).fetchone()
        if row is None:
            raise NotFound(f"device {device_key!r} does not exist")
        shape, root = row
        if shape == CODE_ABSENT:
            return Draft(key=device_key, section=ABSENT)
        if shape == CODE_NULL:
            return Draft(key=device_key, section=None)
        section_rows = self.conn.execute(
            "SELECT slot_id, left_id, right_id, is_hole, fields_json, "
            "att_shape, att_root FROM section_slot WHERE device_key = ?",
            (device_key,),
        ).fetchall()
        section_by_id = {r[0]: r for r in section_rows}
        order = _walk_rows(section_by_id, root, 1, 2, f"section of {device_key}")
        attachment_rows = self.conn.execute(
            "SELECT a.slot_id, a.section_slot_id, a.left_id, a.right_id, "
            "a.is_hole, a.fields_json FROM attachment_slot a "
            "JOIN section_slot s ON a.section_slot_id = s.slot_id "
            "WHERE s.device_key = ?",
            (device_key,),
        ).fetchall()
        attachments_by_owner: Dict[str, Dict[str, Tuple[Any, ...]]] = {}
        for r in attachment_rows:
            attachments_by_owner.setdefault(r[1], {})[r[0]] = r
        slots: List[SlotDraft] = []
        for slot_id in order:
            _, _, _, is_hole, fields_json, att_shape, att_root = section_by_id[slot_id]
            if is_hole:
                slots.append(SlotDraft(slot_id, True, (), ABSENT))
                continue
            fields = _stored_fields(fields_json)
            if att_shape == CODE_ABSENT:
                attachments: Any = ABSENT
            elif att_shape == CODE_NULL:
                attachments = None
            elif att_shape == CODE_EMPTY:
                attachments = ()
            else:
                owner_rows = attachments_by_owner.get(slot_id, {})
                att_order = _walk_rows(
                    owner_rows, att_root, 2, 3, f"attachments of {slot_id}"
                )
                attachments = tuple(
                    AttDraft(
                        aid,
                        bool(owner_rows[aid][4]),
                        () if owner_rows[aid][4] else _stored_fields(owner_rows[aid][5]),
                    )
                    for aid in att_order
                )
            if att_shape != CODE_PRESENT and attachments_by_owner.get(slot_id):
                raise IntegrityError(
                    f"{slot_id}: non-present attachments still have slot rows"
                )
            slots.append(SlotDraft(slot_id, False, fields, attachments))
        return Draft(key=device_key, section=tuple(slots))

    def merge_document(
        self,
        device_key: str,
        base_raw: Any,
        local_raw: Any,
        theirs_raw: Any,
    ) -> Dict[str, int]:
        """Three-way merge of one device's drafts, written back atomically.

        Planning is pure and happens before any write.  The write-back is a
        compare-and-swap: inside one transaction the store's current draft
        must still equal the submitted local draft, and no merged slot id
        may belong to another device; otherwise nothing changes.  Only the
        target device's rows are rewritten, with their original identities.
        """
        base = parse_draft(base_raw, "base")
        local = parse_draft(local_raw, "local")
        theirs = parse_draft(theirs_raw, "theirs")
        for label, draft in (("base", base), ("local", local), ("theirs", theirs)):
            if draft.key != device_key:
                raise ValidationError(
                    f"{label} draft is for device {draft.key!r}, not {device_key!r}"
                )
        merged = merge_drafts(base, local, theirs)
        with self._transaction():
            current = self._read_draft(device_key)
            if current != local:
                raise StaleLocal(
                    "the store no longer matches the local draft: "
                    + first_difference(current, local)
                )
            self._assert_ids_not_owned_elsewhere(device_key, merged)
            stats = self._replace_identified(device_key, merged)
            stats.update(self._record_version(device_key, "merge", current))
        return stats

    def _assert_ids_not_owned_elsewhere(
        self, device_key: str, draft: Draft
    ) -> None:
        """No merged slot id may collide with another device in this store."""
        ids = list(draft_ids(draft))
        chunk = 500
        for start in range(0, len(ids), chunk):
            part = ids[start : start + chunk]
            marks = ",".join("?" * len(part))
            rows = self.conn.execute(
                f"SELECT slot_id, device_key FROM section_slot "
                f"WHERE slot_id IN ({marks})",
                part,
            ).fetchall()
            rows += self.conn.execute(
                f"SELECT a.slot_id, s.device_key FROM attachment_slot a "
                f"JOIN section_slot s ON a.section_slot_id = s.slot_id "
                f"WHERE a.slot_id IN ({marks})",
                part,
            ).fetchall()
            for slot_id, owner in rows:
                if owner != device_key:
                    raise MergeConflict(
                        f"merged slot id {slot_id!r} already belongs to "
                        f"device {owner!r}"
                    )

    def _replace_identified(self, device_key: str, draft: Draft) -> Dict[str, int]:
        """Replace one device with a merged draft, keeping its slot ids.

        Runs inside the caller's transaction.  The id plan replays the
        draft's identities through the ordinary import path in exactly the
        order the import consumes them.
        """
        normalized = normalize_device_doc(device_key, draft_to_plain_document(draft))
        plan = draft_import_ids(draft)
        cursor = {"position": 0}

        def id_factory(kind: str, ordinal: int) -> str:
            position = cursor["position"]
            if position >= len(plan):
                raise IntegrityError("merge id plan exhausted during import")
            expected_kind, slot_id = plan[position]
            if kind != expected_kind:
                raise IntegrityError(
                    f"merge id plan mismatch at {position}: expected "
                    f"{expected_kind}, got {kind}"
                )
            cursor["position"] = position + 1
            return slot_id

        stats = {"section_slots": 0, "attachment_slots": 0}
        self._replace_document_rows(
            device_key, normalized, id_factory, replace=True, stats=stats
        )
        if cursor["position"] != len(plan):
            raise IntegrityError("merge id plan was not fully consumed")
        return stats

    # -- history -----------------------------------------------------------
    def _insert_version(self, draft: Draft, kind: str) -> int:
        """Append one version row for an already-current draft.

        Runs inside the caller's open transaction.  Version numbers are
        per-device and gapless; the table is append-only, so no version row is
        ever updated or deleted.
        """
        row = self.conn.execute(
            "SELECT COALESCE(MAX(version_no), 0) FROM device_version "
            "WHERE device_key = ?",
            (draft.key,),
        ).fetchone()
        next_no = int(row[0]) + 1
        canonical = _canonical_draft_json(draft)
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        section_slots, attachment_slots = _draft_slot_counts(draft)
        created_at = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            "INSERT INTO device_version (device_key, version_no, created_at, "
            "kind, content_hash, section_slots, attachment_slots, draft_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                draft.key,
                next_no,
                created_at,
                kind,
                digest,
                section_slots,
                attachment_slots,
                canonical,
            ),
        )
        return next_no

    def _record_version(
        self, device_key: str, kind: str, previous: Optional[Draft]
    ) -> Dict[str, int]:
        """Snapshot the device's current state unless it did not change.

        Called only after a write transaction has put the rows in their final
        state.  A failed write never gets here (its transaction rolls back),
        and a write whose content is identical to the previous committed
        state appends nothing — one batch/merge/import is one change, and no
        change means no new version.  Returns the counters callers report.
        """
        current = self._read_draft(device_key)
        current_canonical = _canonical_draft_json(current)
        if previous is not None:
            previous_canonical = _canonical_draft_json(previous)
            if current_canonical == previous_canonical:
                return {"versions_added": 0, "version_no": 0}
        version_no = self._insert_version(current, kind)
        return {"versions_added": 1, "version_no": version_no}

    def _require_device(self, device_key: str) -> None:
        row = self.conn.execute(
            "SELECT 1 FROM device WHERE device_key = ?", (device_key,)
        ).fetchone()
        if row is None:
            raise NotFound(f"device {device_key!r} does not exist")

    def list_versions(self, device_key: str) -> List[Dict[str, Any]]:
        """Metadata for every version of one device, oldest version first.

        Version numbers are per-device, gapless and ordered by commit time.
        The last entry is the current record.
        """
        self._require_device(device_key)
        rows = self.conn.execute(
            "SELECT version_no, created_at, kind, content_hash, "
            "section_slots, attachment_slots FROM device_version "
            "WHERE device_key = ? ORDER BY version_no",
            (device_key,),
        ).fetchall()
        return [
            {
                "device_key": device_key,
                "version_no": row[0],
                "created_at": row[1],
                "kind": row[2],
                "content_hash": row[3],
                "section_slots": row[4],
                "attachment_slots": row[5],
            }
            for row in rows
        ]

    def _version_row(self, device_key: str, version_no: Any) -> Tuple[Any, ...]:
        if isinstance(version_no, bool) or not isinstance(version_no, int):
            raise ValidationError(
                f"version must be an integer, got {type(version_no).__name__}"
            )
        if version_no < 1:
            raise ValidationError(f"version numbers start at 1, got {version_no}")
        row = self.conn.execute(
            "SELECT version_no, created_at, kind, content_hash, section_slots, "
            "attachment_slots, draft_json FROM device_version "
            "WHERE device_key = ? AND version_no = ?",
            (device_key, version_no),
        ).fetchone()
        if row is None:
            # Distinguish an unknown device from an out-of-range version so the
            # reason is always reported precisely.
            self._require_device(device_key)
            latest = self.conn.execute(
                "SELECT MAX(version_no) FROM device_version WHERE device_key = ?",
                (device_key,),
            ).fetchone()[0]
            raise NotFound(
                f"device {device_key!r} has no version {version_no} "
                f"(versions 1..{latest})"
            )
        return row

    def _load_version_draft(self, device_key: str, version_no: int) -> Draft:
        row = self._version_row(device_key, version_no)
        try:
            return parse_draft(json.loads(row[6]), f"version {version_no}")
        except (json.JSONDecodeError, StoreError) as exc:
            raise IntegrityError(
                f"stored version {version_no} of {device_key!r} is unreadable: {exc}"
            ) from exc

    def load_version(self, device_key: str, version_no: int) -> Dict[str, Any]:
        """The full plain record (the same nested form as ``load_document``)
        stored at one version, plus its version metadata.

        Read-only: the current record and the history are never modified.
        """
        row = self._version_row(device_key, version_no)
        draft = self._load_version_draft(device_key, version_no)
        if draft.key != device_key:  # pragma: no cover - defensive
            raise IntegrityError(
                f"version {version_no} is stored under {device_key!r} but "
                f"contains device {draft.key!r}"
            )
        document = draft_to_plain_document(draft)
        document["key"] = device_key
        return {
            "device_key": device_key,
            "version_no": row[0],
            "created_at": row[1],
            "kind": row[2],
            "content_hash": row[3],
            "section_slots": row[4],
            "attachment_slots": row[5],
            "document": document,
        }

    def checkout_version(self, device_key: str, version_no: int) -> Dict[str, Any]:
        """The full identified draft (stable slot ids included) of one version.

        This is the historical counterpart of :meth:`checkout_document`.
        """
        self._version_row(device_key, version_no)
        return draft_to_json(self._load_version_draft(device_key, version_no))

    def restore_version(
        self, device_key: str, version_no: int
    ) -> Dict[str, Any]:
        """Restore one old version as the current record.

        The current rows are rebuilt from the version's identified draft via
        the same identity-preserving write-back as an offline merge: fields,
        array shapes, hole membership, slot identities, order and attachment
        ownership all return to what that version held.  History itself never
        moves backwards and no old version is deleted: the restore is appended
        as a new latest version.  Restoring content identical to the current
        record changes nothing and appends nothing.
        """
        with self._transaction():
            previous = self._read_draft(device_key)
            draft = self._load_version_draft(device_key, version_no)
            if draft.key != device_key:  # pragma: no cover - defensive
                raise IntegrityError(
                    f"version {version_no} belongs to device {draft.key!r}, "
                    f"not {device_key!r}"
                )
            stats = self._replace_identified(device_key, draft)
            recorded = self._record_version(device_key, "restore", previous)
            stats.update(recorded)
            stats["restored_from"] = version_no
        return stats


    def slot_view(self, slot_id: str) -> Dict[str, Any]:
        """Current identity, kind, owner and absolute position of a slot."""
        sec = self.conn.execute(
            "SELECT device_key, is_hole, att_shape FROM section_slot "
            "WHERE slot_id = ?",
            (slot_id,),
        ).fetchone()
        if sec is not None:
            device_key, is_hole, att_shape = sec
            shape, treap = self._open_section_treap(device_key)
            if treap is None:  # pragma: no cover - row implies an array
                raise IntegrityError("section row without a section array")
            position = treap.rank(slot_id)
            # A hole has no attachment array; its attachment state is ABSENT.
            return {
                "slot_id": slot_id,
                "kind": "section",
                "device_key": device_key,
                "position": position,
                "is_hole": bool(is_hole),
                "attachments": SHAPE_NAMES[att_shape],
            }
        att = self.conn.execute(
            "SELECT section_slot_id, is_hole FROM attachment_slot WHERE slot_id = ?",
            (slot_id,),
        ).fetchone()
        if att is not None:
            section_id, is_hole = att
            shape, treap, _ = self._open_attachment_treap(section_id)
            if treap is None:  # pragma: no cover - row implies an array
                raise IntegrityError("attachment row without an attachment array")
            device_key = self.conn.execute(
                "SELECT device_key FROM section_slot WHERE slot_id = ?",
                (section_id,),
            ).fetchone()[0]
            return {
                "slot_id": slot_id,
                "kind": "attachment",
                "device_key": device_key,
                "owner_section_slot_id": section_id,
                "position": treap.rank(slot_id),
                "is_hole": bool(is_hole),
            }
        raise NotFound(f"slot {slot_id!r} does not exist")

    def slot_position(self, device_key: str, slot_id: str) -> int:
        view = self.slot_view(slot_id)
        if view["device_key"] != device_key:
            raise ValidationError("slot belongs to another device")
        return view["position"]

    def page(
        self,
        device_key: str,
        start: int,
        stop: int,
        *,
        section_id: Optional[str] = None,
        stats: Optional[Dict[str, int]] = None,
    ) -> List[Dict[str, Any]]:
        """A half-open absolute range of one array, without scanning from 0.

        ``stats``, when given, is filled with the number of slot rows fetched
        and SQL SELECTs issued while answering this call, so a caller can
        demonstrate that the cost depends on the page size and tree height,
        not on ``start``.
        """
        if start < 0 or stop < start:
            raise ValidationError("page needs 0 <= start <= stop")
        read_meter = Meter()
        self.conn.set_trace_callback(read_meter.on_sql)
        try:
            result = self._page(
                device_key, start, stop, section_id=section_id, meter=read_meter
            )
        finally:
            self.conn.set_trace_callback(self._trace_callback)
        if stats is not None:
            stats.clear()
            stats.update(
                {
                    "rows_fetched": read_meter.node_fetches,
                    "selects": read_meter.selects,
                    "statements": read_meter.sql_statements,
                }
            )
        return result

    def _page(
        self,
        device_key: str,
        start: int,
        stop: int,
        *,
        section_id: Optional[str],
        meter: Meter,
    ) -> List[Dict[str, Any]]:
        if section_id is None:
            shape, treap = self._open_section_treap(device_key)
            if treap is None:
                raise ValidationError(
                    f"section is {SHAPE_NAMES[shape]}, not an array"
                )
            if shape == CODE_EMPTY:
                return []
            treap.meter = meter
            out = []
            for pos, node in treap.iter_slice(start, stop):
                is_hole, fields_json, att_shape = self._section_row(node.id)
                out.append(
                    {
                        "position": pos,
                        "slot_id": node.id,
                        "is_hole": bool(is_hole),
                        "fields": {} if is_hole else loads_scalar(fields_json),
                        "attachments": SHAPE_NAMES[att_shape] if not is_hole else "absent",
                    }
                )
            return out
        row = self.conn.execute(
            "SELECT device_key, is_hole, att_shape, att_root FROM section_slot "
            "WHERE slot_id = ?",
            (section_id,),
        ).fetchone()
        if row is None:
            raise NotFound(f"section slot {section_id!r} does not exist")
        if row[0] != device_key:
            raise ValidationError("section slot belongs to another device")
        if row[1]:
            raise ValidationError("a hole has no attachment array")
        if row[2] not in (CODE_EMPTY, CODE_PRESENT):
            raise ValidationError(
                f"attachments are {SHAPE_NAMES[row[2]]}, not an array"
            )
        if row[2] == CODE_EMPTY:
            return []
        treap = Treap(
            self.conn,
            "attachment_slot",
            "slot_id",
            root=row[3],
            scope_column="section_slot_id",
            scope_value=section_id,
            meter=meter,
        )
        out = []
        for pos, node in treap.iter_slice(start, stop):
            is_hole, fields_json = self.conn.execute(
                "SELECT is_hole, fields_json FROM attachment_slot WHERE slot_id = ?",
                (node.id,),
            ).fetchone()
            out.append(
                {
                    "position": pos,
                    "slot_id": node.id,
                    "is_hole": bool(is_hole),
                    "fields": {} if is_hole else loads_scalar(fields_json),
                }
            )
        return out

    # -- integrity -------------------------------------------------------
    def verify(self) -> Dict[str, int]:
        """Check all treap, shape and history invariants; raise on any break."""
        checked = {
            "devices": 0,
            "section_arrays": 0,
            "attachment_arrays": 0,
            "history_devices": 0,
            "history_versions": 0,
        }
        rows = self.conn.execute(
            "SELECT device_key, section_shape, section_root FROM device "
            "ORDER BY device_key"
        ).fetchall()
        for device_key, shape, root in rows:
            checked["devices"] += 1
            if shape in (CODE_ABSENT, CODE_NULL, CODE_EMPTY):
                if root is not None:
                    raise IntegrityError(
                        f"{device_key}: non-present section has a root"
                    )
                count = self.conn.execute(
                    "SELECT COUNT(*) FROM section_slot WHERE device_key = ?",
                    (device_key,),
                ).fetchone()[0]
                if count:
                    raise IntegrityError(
                        f"{device_key}: non-array section still has slot rows"
                    )
                if shape != CODE_EMPTY:
                    continue
            else:
                if root is None:
                    raise IntegrityError(f"{device_key}: present section has no root")
                self._verify_array(
                    "section_slot",
                    "slot_id",
                    root,
                    scope_where=("device_key", device_key),
                    label=f"section of {device_key}",
                )
                checked["section_arrays"] += 1
            for (sid, att_shape, att_root) in self.conn.execute(
                "SELECT slot_id, att_shape, att_root FROM section_slot "
                "WHERE device_key = ?",
                (device_key,),
            ):
                if att_shape in (CODE_ABSENT, CODE_NULL, CODE_EMPTY):
                    if att_root is not None:
                        raise IntegrityError(f"{sid}: non-present attachments with root")
                    count = self.conn.execute(
                        "SELECT COUNT(*) FROM attachment_slot WHERE section_slot_id = ?",
                        (sid,),
                    ).fetchone()[0]
                    if count:
                        raise IntegrityError(
                            f"{sid}: non-array attachments still have slot rows"
                        )
                else:
                    self._verify_array(
                        "attachment_slot",
                        "slot_id",
                        att_root,
                        scope_where=("section_slot_id", sid),
                        label=f"attachments of {sid}",
                    )
                    checked["attachment_arrays"] += 1
        self._verify_history(checked)
        return checked

    def _verify_history(self, checked: Dict[str, int]) -> None:
        """History invariants: every device has gapless versions, stored payloads
        are valid and self-consistent, and the newest version equals the live
        rows, so a committed record and its history version always coexist.
        """
        version_rows = self.conn.execute(
            "SELECT device_key, version_no, kind, content_hash, section_slots, "
            "attachment_slots, draft_json FROM device_version "
            "ORDER BY device_key, version_no"
        ).fetchall()
        grouped: Dict[str, List[Tuple[Any, ...]]] = {}
        for row in version_rows:
            grouped.setdefault(row[0], []).append(row)
        live_keys = set(self.device_keys())
        if set(grouped) != live_keys:
            missing = sorted(live_keys - set(grouped))
            extra = sorted(set(grouped) - live_keys)
            raise IntegrityError(
                f"history coverage mismatch (missing={missing}, extra={extra})"
            )
        for device_key, rows in grouped.items():
            checked["history_devices"] += 1
            expected_no = 1
            newest_payload: Optional[str] = None
            for _, version_no, kind, content_hash, sec_count, att_count, payload in rows:
                if version_no != expected_no:
                    raise IntegrityError(
                        f"{device_key}: history jumps from "
                        f"{expected_no - 1} to {version_no}"
                    )
                expected_no += 1
                # The stored text is itself the canonical serialization, so the
                # hash can be checked without a parse/reserialize round trip.
                if (
                    hashlib.sha256(payload.encode("utf-8")).hexdigest()
                    != content_hash
                ):
                    raise IntegrityError(
                        f"{device_key}: version {version_no} content hash does not "
                        "match its payload"
                    )
                try:
                    draft = parse_draft(json.loads(payload), f"version {version_no}")
                except (json.JSONDecodeError, StoreError) as exc:
                    raise IntegrityError(
                        f"{device_key}: version {version_no} payload is invalid: {exc}"
                    ) from exc
                if draft.key != device_key:
                    raise IntegrityError(
                        f"{device_key}: version {version_no} payload is for "
                        f"{draft.key!r}"
                    )
                sec_slots, att_slots = _draft_slot_counts(draft)
                if (sec_slots, att_slots) != (sec_count, att_count):
                    raise IntegrityError(
                        f"{device_key}: version {version_no} slot counters disagree "
                        f"with its payload"
                    )
                newest_payload = payload
                checked["history_versions"] += 1
            # The newest historical version must be exactly what is live now:
            # one canonicalization of the live rows, compared as text.
            live_draft = self._read_draft(device_key)
            if _canonical_draft_json(live_draft) != newest_payload:
                raise IntegrityError(
                    f"{device_key}: newest history version does not match the "
                    "current record"
                )

    def _verify_array(
        self,
        table: str,
        id_column: str,
        root: str,
        *,
        scope_where: Tuple[str, str],
        label: str,
    ) -> None:
        scope_col, scope_val = scope_where
        placeholders = []
        ids: List[str] = []
        stack = [root]
        seen: set = set()
        while stack:
            sid = stack.pop()
            if sid in seen:
                raise IntegrityError(f"{label}: cycle or shared child at {sid!r}")
            seen.add(sid)
            row = self.conn.execute(
                f"SELECT parent_id, left_id, right_id, priority, subtree_size, "
                f"{scope_col} FROM {table} WHERE {id_column} = ?",
                (sid,),
            ).fetchone()
            if row is None:
                raise IntegrityError(f"{label}: dangling id {sid!r}")
            if row[5] != scope_val:
                raise IntegrityError(f"{label}: slot {sid!r} has wrong owner")
            if row[3] != priority_for(sid):
                raise IntegrityError(f"{label}: slot {sid!r} has a wrong priority")
            if row[4] < 1:
                raise IntegrityError(f"{label}: slot {sid!r} has an empty subtree")
            if row[1] is not None:
                if row[1] == sid:
                    raise IntegrityError(f"{label}: self left child at {sid!r}")
                stack.append(row[1])
            if row[2] is not None:
                if row[2] == sid:
                    raise IntegrityError(f"{label}: self right child at {sid!r}")
                stack.append(row[2])
        # Root must not have a parent, and every non-root must be pointed at.
        root_parent = self.conn.execute(
            f"SELECT parent_id FROM {table} WHERE {id_column} = ?", (root,)
        ).fetchone()[0]
        if root_parent is not None:
            raise IntegrityError(f"{label}: root {root!r} has a parent")
        total = self.conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {scope_col} = ?", (scope_val,)
        ).fetchone()[0]
        if total != len(seen):
            raise IntegrityError(f"{label}: {total - len(seen)} orphan slot rows")
        # Inorder walk: priorities, sizes and parent back-pointers.
        treap = Treap(self.conn, table, id_column, root=root)
        order: List[str] = []
        for sid in treap.inorder_ids():
            order.append(sid)
        if len(order) != total:
            raise IntegrityError(f"{label}: inorder walk has {len(order)} of {total}")
        parent_of: Dict[str, Optional[str]] = {}
        left_of: Dict[str, Optional[str]] = {}
        right_of: Dict[str, Optional[str]] = {}
        prio: Dict[str, int] = {}
        size: Dict[str, int] = {}
        for sid in order:
            row = self.conn.execute(
                f"SELECT parent_id, left_id, right_id, priority, subtree_size "
                f"FROM {table} WHERE {id_column} = ?",
                (sid,),
            ).fetchone()
            parent_of[sid], left_of[sid], right_of[sid] = row[0], row[1], row[2]
            prio[sid], size[sid] = row[3], row[4]
        # Size check, bottom-up over the inorder-independent postorder.
        def check_size(sid: str) -> int:
            stack2 = [(sid, False)]
            computed: Dict[str, int] = {}
            while stack2:
                current, done = stack2.pop()
                if done:
                    left = left_of[current]
                    right = right_of[current]
                    expected = 1 + (computed[left] if left else 0) + (
                        computed[right] if right else 0
                    )
                    if size[current] != expected:
                        raise IntegrityError(
                            f"{label}: slot {current!r} size {size[current]} != {expected}"
                        )
                    computed[current] = expected
                else:
                    stack2.append((current, True))
                    for child in (right_of[current], left_of[current]):
                        if child:
                            stack2.append((child, False))
            return computed[sid]

        check_size(root)
        # Parent back-pointers.
        for sid in order:
            for child in (left_of[sid], right_of[sid]):
                if child is not None and parent_of[child] != sid:
                    raise IntegrityError(
                        f"{label}: {child!r} does not point back to {sid!r}"
                    )
        # Treap heap property and inorder = array order via sequence check.
        for sid in order:
            for child in (left_of[sid], right_of[sid]):
                if child is not None and not (
                    prio[sid] > prio[child]
                    or (prio[sid] == prio[child] and sid > child)
                ):
                    raise IntegrityError(f"{label}: heap property broken at {sid!r}")
        return
