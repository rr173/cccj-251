"""Streaming export of the formal relational tables.

The live tables intentionally contain *no* position column.  Official exports
materialize one: the section and attachment arrays are walked in order and
every element gets ``position = 0,1,2,...`` at that moment.  Sequences are a
property of an export, not of storage, so they cannot go stale and never need
to be maintained across edits.

Exports are written to files chosen by the caller.  Nothing big is written
into the repository; the helper writes to a scratch directory by default only
when explicitly asked.
"""

from __future__ import annotations

import csv
import os
from typing import Any, Dict, Iterator, Optional, Sequence, Tuple

from .codec import CODE_ABSENT, CODE_EMPTY, CODE_NULL, CODE_PRESENT
from .store import DeviceStore, Treap


SECTION_HEADER = [
    "device_key",
    "section_slot_id",
    "device_section_shape",
    "position",
    "is_hole",
    "fields_json",
    "attachment_shape",
    "section_slot_count",
]

ATTACHMENT_HEADER = [
    "device_key",
    "owner_section_slot_id",
    "attachment_slot_id",
    "section_position",
    "position",
    "is_hole",
    "fields_json",
]

DEVICE_HEADER = [
    "device_key",
    "section_shape",
    "section_slot_count",
]


def _shape_name(code: int) -> str:
    return {CODE_ABSENT: "absent", CODE_NULL: "null", CODE_EMPTY: "empty",
            CODE_PRESENT: "present"}[code]


def iter_section_rows(
    store: DeviceStore, device_keys: Optional[Sequence[str]] = None
) -> Iterator[Tuple[Any, ...]]:
    """Yield final section rows for every device, one array order at a time."""
    keys = list(device_keys) if device_keys is not None else store.device_keys()
    for key in keys:
        device_row = store.conn.execute(
            "SELECT section_shape, section_root FROM device WHERE device_key = ?",
            (key,),
        ).fetchone()
        if device_row is None:
            raise KeyError(key)
        shape, root = device_row
        if shape not in (CODE_EMPTY, CODE_PRESENT):
            # No section rows; the shape is still visible in the device export.
            continue
        count = store.conn.execute(
            "SELECT COUNT(*) FROM section_slot WHERE device_key = ?", (key,)
        ).fetchone()[0]
        treap = Treap(store.conn, "section_slot", "slot_id", root=root)
        for position, sid in enumerate(treap.inorder_ids()):
            is_hole, fields_json, att_shape = store.conn.execute(
                "SELECT is_hole, fields_json, att_shape FROM section_slot "
                "WHERE slot_id = ?",
                (sid,),
            ).fetchone()
            yield (
                key,
                sid,
                _shape_name(shape),
                position,
                is_hole,
                fields_json,
                _shape_name(att_shape),
                count,
            )


def iter_attachment_rows(
    store: DeviceStore, device_keys: Optional[Sequence[str]] = None
) -> Iterator[Tuple[Any, ...]]:
    """Yield final attachment rows, carrying their owner's *final* position."""
    keys = list(device_keys) if device_keys is not None else store.device_keys()
    for key in keys:
        device_row = store.conn.execute(
            "SELECT section_shape, section_root FROM device WHERE device_key = ?",
            (key,),
        ).fetchone()
        if device_row is None:
            raise KeyError(key)
        shape, root = device_row
        if shape not in (CODE_EMPTY, CODE_PRESENT):
            continue
        section_treap = Treap(store.conn, "section_slot", "slot_id", root=root)
        for section_position, sid in enumerate(section_treap.inorder_ids()):
            att_shape, att_root = store.conn.execute(
                "SELECT att_shape, att_root FROM section_slot WHERE slot_id = ?",
                (sid,),
            ).fetchone()
            if att_shape not in (CODE_EMPTY, CODE_PRESENT):
                continue
            if att_shape == CODE_EMPTY:
                continue
            if att_root is None:
                raise AssertionError(f"{sid}: present attachments without root")
            att_treap = Treap(store.conn, "attachment_slot", "slot_id", root=att_root)
            for position, aid in enumerate(att_treap.inorder_ids()):
                is_hole, fields_json = store.conn.execute(
                    "SELECT is_hole, fields_json FROM attachment_slot "
                    "WHERE slot_id = ?",
                    (aid,),
                ).fetchone()
                yield (key, sid, aid, section_position, position, is_hole, fields_json)


def iter_device_rows(
    store: DeviceStore, device_keys: Optional[Sequence[str]] = None
) -> Iterator[Tuple[Any, ...]]:
    keys = list(device_keys) if device_keys is not None else store.device_keys()
    for key in keys:
        device_row = store.conn.execute(
            "SELECT section_shape, section_root FROM device WHERE device_key = ?",
            (key,),
        ).fetchone()
        if device_row is None:
            raise KeyError(key)
        shape, root = device_row
        count = store.conn.execute(
            "SELECT COUNT(*) FROM section_slot WHERE device_key = ?", (key,)
        ).fetchone()[0]
        yield (key, _shape_name(shape), count)


def export_csv(
    store: DeviceStore,
    directory: str,
    device_keys: Optional[Sequence[str]] = None,
) -> Dict[str, str]:
    """Write the three formal tables as CSV files; returns their paths."""
    os.makedirs(directory, exist_ok=True)
    paths = {
        "device": os.path.join(directory, "device.csv"),
        "section_slot": os.path.join(directory, "section_slot.csv"),
        "attachment_slot": os.path.join(directory, "attachment_slot.csv"),
    }
    specs = [
        ("device", DEVICE_HEADER, iter_device_rows),
        ("section_slot", SECTION_HEADER, iter_section_rows),
        ("attachment_slot", ATTACHMENT_HEADER, iter_attachment_rows),
    ]
    for name, header, iterator in specs:
        with open(paths[name], "w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(header)
            for row in iterator(store, device_keys):
                writer.writerow(row)
    return paths
