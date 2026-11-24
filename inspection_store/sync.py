"""Incremental synchronization of an inspection store into an analysis database.

Analysts run SQL against the three formal relations (``device``,
``section_slot`` and ``attachment_slot``) without touching the live inspection
store.  This module maintains a separate, read-only-for-business SQLite
database that mirrors the source store's *current* records together with the
source version number each record came from.

Only stores whose version history is enabled are accepted, because the
per-device latest version number in ``device_version`` is the change marker.
The source is opened through a read-only connection and is never modified.

Incremental contract
--------------------

A sync first opens one read transaction on the source and reads the latest
version number of every device, so the whole sync corresponds to a single
committed read view: array shapes, holes, stable identities, attachment
ownership and final positions all come from that one version.  The version
list is the only full-store scan; the record bodies (live rows) are read only
for devices whose version differs from what the analysis database already
published.  Historical version payloads are never read.

On the target, the three relations and the synchronized-version bookkeeping
are replaced inside one write transaction, per changed device:

* unchanged devices are not rewritten, so their rows keep their rowids;
* a changed device's slot rows are deleted (children first) and reinserted, so
  a slot id the old device deleted and another device now owns is never
  misbound to a stale row;
* the whole publish either commits together or rolls back, leaving the
  previously published result queryable; no progress is written ahead of the
  commit, so a retry cannot miss an update.

Two syncs never interleave on one target (the write transaction starts
``BEGIN IMMEDIATE`` and any second sync fails fast with :class:`SyncBusy`).
Before publishing, the snapshot is checked against the rows already
published: an older source read view can never overwrite a newer one — that
raises :class:`SyncConflict` and changes nothing.
"""

from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Tuple
from urllib.parse import quote

from .codec import (
    CODE_ABSENT,
    CODE_EMPTY,
    CODE_NULL,
    CODE_PRESENT,
    StoreError,
    dumps_value,
)
from .merge import Draft
from .store import (
    SCHEMA_VERSION,
    SHAPE_NAMES,
    IntegrityError,
    _read_draft_from_conn,
)

ANALYSIS_SCHEMA_VERSION = 1

# Column order is identical to the formal CSV export (see export.py); the
# device relation additionally carries the source version number each record
# was synchronized from.  Positions are materialized here on purpose: the
# analysis database is a frozen mirror, so its snapshots never go stale.
_ANALYSIS_SCHEMA = """
CREATE TABLE IF NOT EXISTS analysis_meta (
    name  TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS device (
    device_key         TEXT PRIMARY KEY,
    section_shape      TEXT NOT NULL,
    section_slot_count INTEGER NOT NULL CHECK (section_slot_count >= 0),
    source_version     INTEGER NOT NULL CHECK (source_version >= 1)
);

CREATE TABLE IF NOT EXISTS section_slot (
    device_key            TEXT NOT NULL,
    section_slot_id       TEXT NOT NULL,
    device_section_shape  TEXT NOT NULL,
    position              INTEGER NOT NULL CHECK (position >= 0),
    is_hole               INTEGER NOT NULL CHECK (is_hole IN (0, 1)),
    fields_json           TEXT NOT NULL,
    attachment_shape      TEXT NOT NULL,
    section_slot_count    INTEGER NOT NULL CHECK (section_slot_count >= 0),
    PRIMARY KEY (device_key, section_slot_id),
    FOREIGN KEY (device_key) REFERENCES device(device_key) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS section_slot_position
    ON section_slot(device_key, position);

CREATE TABLE IF NOT EXISTS attachment_slot (
    device_key              TEXT NOT NULL,
    owner_section_slot_id   TEXT NOT NULL,
    attachment_slot_id      TEXT NOT NULL,
    section_position        INTEGER NOT NULL CHECK (section_position >= 0),
    position                INTEGER NOT NULL CHECK (position >= 0),
    is_hole                 INTEGER NOT NULL CHECK (is_hole IN (0, 1)),
    fields_json             TEXT NOT NULL,
    PRIMARY KEY (
        device_key,
        owner_section_slot_id,
        attachment_slot_id
    ),
    FOREIGN KEY (device_key, owner_section_slot_id)
        REFERENCES section_slot(device_key, section_slot_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS attachment_slot_position
    ON attachment_slot(device_key, section_position, position);
CREATE INDEX IF NOT EXISTS attachment_slot_owner
    ON attachment_slot(device_key, owner_section_slot_id);
"""


class SyncError(StoreError):
    """The synchronization cannot run (bad source or target)."""


class SyncBusy(StoreError):
    """Another sync currently holds the analysis database write lock."""


class SyncConflict(StoreError):
    """An older source read view would overwrite a newer published result."""


@dataclass(frozen=True)
class SourceSnapshot:
    """One consistent read view of the whole source store."""

    versions: Mapping[str, int]
    drafts: Mapping[str, Draft]  # only devices whose bodies had to be read


# ---------------------------------------------------------------------------
# Source access (read-only)


def open_source_connection(path: str) -> sqlite3.Connection:
    """Open the inspection source store strictly read-only.

    The connection uses SQLite's ``mode=ro`` URI, so even a bug cannot mutate
    the source.  Only a history-enabled store (schema version
    :data:`~inspection_store.store.SCHEMA_VERSION`) is accepted: older stores
    would have to be migrated, which a read-only connection must not do.
    """
    real_path = os.path.abspath(os.fspath(path))
    if not os.path.exists(real_path):
        raise SyncError(f"source store does not exist: {real_path}")
    uri = "file:" + quote(real_path) + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, isolation_level=None)
    conn.execute("PRAGMA query_only = ON")
    try:
        row = conn.execute(
            "SELECT value FROM meta WHERE name = 'schema_version'"
        ).fetchone()
    except sqlite3.DatabaseError as exc:
        conn.close()
        raise SyncError(f"not an inspection store: {real_path}") from exc
    if row is None:
        conn.close()
        raise SyncError(f"not a supported inspection store: {real_path}")
    try:
        version = int(row[0])
    except ValueError as exc:
        conn.close()
        raise SyncError(f"unsupported schema version in {real_path}") from exc
    if version < SCHEMA_VERSION:
        conn.close()
        raise SyncError(
            f"{real_path} has no version history (schema version {version}); "
            f"open it once with a current DeviceStore to migrate it before "
            f"synchronizing"
        )
    if version > SCHEMA_VERSION:
        conn.close()
        raise SyncError(
            f"{real_path} uses schema version {version}, newer than this "
            f"software ({SCHEMA_VERSION})"
        )
    return conn


def read_source_snapshot(
    source: sqlite3.Connection, published: Mapping[str, int]
) -> SourceSnapshot:
    """Read one consistent source view and the bodies of changed devices.

    Runs inside a single read transaction so every observed version and row
    comes from the same committed view of the source.  ``published`` maps the
    device keys the target already holds to their published source versions;
    only devices missing from it or carrying a newer version are read in full.
    """
    return read_source_snapshot_from(source, published)


def read_source_snapshot_from(
    source: Any, published: Mapping[str, int]
) -> SourceSnapshot:
    """Same as :func:`read_source_snapshot` over any connection-like object.

    Accepts a wrapper exposing ``execute`` so tests can trace the exact
    statements and bound parameters the snapshot reads.
    """
    source.execute("BEGIN")
    try:
        rows = source.execute(
            "SELECT d.device_key, "
            "       (SELECT MAX(v.version_no) FROM device_version v "
            "        WHERE v.device_key = d.device_key) AS version_no "
            "FROM device d ORDER BY d.device_key"
        ).fetchall()
        versions: Dict[str, int] = {}
        drafts: Dict[str, Draft] = {}
        for device_key, version_no in rows:
            if version_no is None:
                raise IntegrityError(
                    f"device {device_key!r} has no history version; the store "
                    f"is history-enabled but its history is incomplete"
                )
            versions[device_key] = int(version_no)
            if published.get(device_key) != int(version_no):
                drafts[device_key] = _read_draft_from_conn(source, device_key)
        return SourceSnapshot(versions=versions, drafts=drafts)
    except BaseException:
        source.execute("ROLLBACK")
        raise
    else:
        source.execute("COMMIT")


# ---------------------------------------------------------------------------
# Target access


def open_analysis_connection(path: str) -> sqlite3.Connection:
    """Open (creating when absent) the analysis database maintained here."""
    real_path = os.path.abspath(os.fspath(path))
    parent = os.path.dirname(real_path)
    if parent and not os.path.isdir(parent):
        raise SyncError(f"target directory does not exist: {parent}")
    if os.path.exists(real_path) and not os.path.isfile(real_path):
        raise SyncError(f"analysis target is not a file: {real_path}")
    conn = sqlite3.connect(real_path, isolation_level=None)
    conn.execute("PRAGMA busy_timeout = 5000")
    # WAL lets analysts keep reading while a sync swaps the published rows.
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    fresh = _analysis_tables_absent(conn)
    if fresh:
        conn.executescript(_ANALYSIS_SCHEMA)
        with conn:
            conn.execute(
                "INSERT INTO analysis_meta(name, value) VALUES (?, ?)",
                ("schema_version", str(ANALYSIS_SCHEMA_VERSION)),
            )
    else:
        _check_analysis_schema(conn)
    return conn


def _analysis_tables_absent(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' "
        "AND name IN ('analysis_meta', 'device', 'section_slot', "
        "'attachment_slot')"
    ).fetchone()[0]
    if row == 0:
        return True
    if row != 4:
        raise SyncError(
            "target database is not an analysis database maintained by the "
            "sync tool (partially overlapping tables)"
        )
    return False


def _check_analysis_schema(conn: sqlite3.Connection) -> None:
    row = conn.execute(
        "SELECT value FROM analysis_meta WHERE name = 'schema_version'"
    ).fetchone()
    if row is None:
        raise SyncError("analysis database has no schema metadata")
    try:
        version = int(row[0])
    except ValueError as exc:
        raise SyncError("analysis database carries an invalid schema version") from exc
    if version != ANALYSIS_SCHEMA_VERSION:
        raise SyncError(
            f"analysis database schema version {version} is not supported by "
            f"this software ({ANALYSIS_SCHEMA_VERSION})"
        )


def read_published_versions(conn: sqlite3.Connection) -> Dict[str, int]:
    """The source version currently published per device key."""
    return {
        key: int(version)
        for key, version in conn.execute(
            "SELECT device_key, source_version FROM device"
        ).fetchall()
    }


# ---------------------------------------------------------------------------
# Rendering drafts into the three formal relations


def _shape_name(code: int) -> str:
    return SHAPE_NAMES[code]


def _section_shape_code(draft: Draft) -> int:
    if draft.section is None:
        return CODE_NULL
    if isinstance(draft.section, tuple):
        return CODE_PRESENT if draft.section else CODE_EMPTY
    return CODE_ABSENT


def render_device_rows(
    key: str, version: int, draft: Draft
) -> Tuple[Tuple[Any, ...], List[Tuple[Any, ...]], List[Tuple[Any, ...]]]:
    """Render one draft as the three relation row sets, in export column order.

    Positions are generated by in-order enumeration here, exactly as the
    streaming CSV export generates them from the treap.
    """
    shape = _section_shape_code(draft)
    section_rows: List[Tuple[Any, ...]] = []
    attachment_rows: List[Tuple[Any, ...]] = []
    count = len(draft.section) if isinstance(draft.section, tuple) else 0
    device_row = (key, _shape_name(shape), count, version)
    if shape not in (CODE_EMPTY, CODE_PRESENT):
        return device_row, section_rows, attachment_rows
    for position, slot in enumerate(draft.section):
        if slot.hole:
            section_rows.append(
                (
                    key,
                    slot.slot_id,
                    _shape_name(shape),
                    position,
                    1,
                    "{}",
                    _shape_name(CODE_ABSENT),
                    count,
                )
            )
            continue
        attachments = slot.attachments
        if attachments is None:
            att_shape = CODE_NULL
        elif isinstance(attachments, tuple):
            att_shape = CODE_PRESENT if attachments else CODE_EMPTY
        else:
            att_shape = CODE_ABSENT
        section_rows.append(
            (
                key,
                slot.slot_id,
                _shape_name(shape),
                position,
                0,
                dumps_value(dict(slot.fields)),
                _shape_name(att_shape),
                count,
            )
        )
        if isinstance(attachments, tuple):
            for att_position, attachment in enumerate(attachments):
                attachment_rows.append(
                    (
                        key,
                        slot.slot_id,
                        attachment.slot_id,
                        position,
                        att_position,
                        1 if attachment.hole else 0,
                        "{}" if attachment.hole else dumps_value(
                            dict(attachment.fields)
                        ),
                    )
                )
    return device_row, section_rows, attachment_rows


# ---------------------------------------------------------------------------
# Publishing


def _write_sync_metadata(conn: sqlite3.Connection, source_identity: str) -> None:
    """Record one successful, fully committed sync (runs inside the publish)."""
    conn.execute(
        "INSERT INTO analysis_meta(name, value) VALUES ('source_store', ?) "
        "ON CONFLICT(name) DO UPDATE SET value = excluded.value",
        (source_identity,),
    )
    conn.execute(
        "INSERT INTO analysis_meta(name, value) VALUES "
        "('last_synced_at', ?) "
        "ON CONFLICT(name) DO UPDATE SET value = excluded.value",
        (datetime.now(timezone.utc).isoformat(),),
    )
    conn.execute(
        "INSERT INTO analysis_meta(name, value) VALUES "
        "('last_synced_generation', "
        "CAST(COALESCE((SELECT value FROM analysis_meta "
        " WHERE name = 'last_synced_generation'), '0') AS INTEGER) + 1) "
        "ON CONFLICT(name) DO UPDATE SET value = excluded.value"
    )


def _is_dominated(
    snapshot: Mapping[str, int], published: Mapping[str, int]
) -> Optional[Tuple[str, int, int]]:
    """Return the first device where a newer published result would be lost.

    A snapshot may only replace the published state if, for every device the
    published result contains, the snapshot carries that device with a
    version at least as new.  Both forms of an older read view are rejected:

    * the snapshot still has the device but at an older version number;
    * the snapshot never saw the device at all (it was added to the source
      after this snapshot's read view began and another sync already
      published it), which must not be mistaken for a deletion.

    The supported source API never deletes a device, so a missing published
    device can only mean the read view is older than the published one.
    """
    for key, current in published.items():
        snapshot_version = snapshot.get(key)
        if snapshot_version is None or snapshot_version < current:
            return key, snapshot_version or 0, current
    return None


def publish_snapshot(
    target: sqlite3.Connection,
    snapshot: SourceSnapshot,
    *,
    source_identity: str,
    busy_timeout_ms: Optional[int] = None,
) -> Dict[str, int]:
    """Publish one source snapshot atomically; returns change counters.

    Raises :class:`SyncBusy` if another sync holds the write lock and
    :class:`SyncConflict` if the snapshot is older than the published result.
    """
    if busy_timeout_ms is not None:
        target.execute(f"PRAGMA busy_timeout = {int(busy_timeout_ms)}")
    try:
        target.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError as exc:
        raise SyncBusy(
            "another synchronization is writing the analysis database"
        ) from exc
    try:
        # Re-check the source binding inside the write lock: an early check in
        # synchronize() alone could race with another first-time sync.
        stored_identity = target.execute(
            "SELECT value FROM analysis_meta WHERE name = 'source_store'"
        ).fetchone()
        if stored_identity is not None and stored_identity[0] != source_identity:
            raise SyncError(
                f"analysis database already mirrors {stored_identity[0]!r}; "
                f"it is maintained for one fixed source and cannot be "
                f"pointed at {source_identity!r}"
            )
        published = read_published_versions(target)
        dominated = _is_dominated(snapshot.versions, published)
        if dominated is not None:
            key, snapshot_version, published_version = dominated
            raise SyncConflict(
                f"refusing to publish version {snapshot_version} of device "
                f"{key!r}: the analysis database already holds version "
                f"{published_version} from a newer sync"
            )

        changed = [
            key
            for key, version in snapshot.versions.items()
            if published.get(key) != version
        ]
        if not changed:
            # Even with zero devices, a successful sync marks this read view
            # as published so the analysis metadata reflects every run.
            _write_sync_metadata(target, source_identity)
            target.execute("COMMIT")
            return {
                "devices_total": len(snapshot.versions),
                "devices_changed": 0,
                "devices_inserted": 0,
                "devices_updated": 0,
                "devices_deleted": 0,
                "section_slots_written": 0,
                "attachment_slots_written": 0,
                "already_current": True,
            }

        section_written = 0
        attachment_written = 0
        inserted = 0
        updated = 0
        for key in changed:
            draft = snapshot.drafts.get(key)
            if draft is None:
                raise IntegrityError(
                    f"internal sync error: no snapshot body for changed {key!r}"
                )
            if key in published:
                updated += 1
                # Replace the whole device atomically, children first.  This
                # is what makes a slot id the old device deleted and another
                # device now owns resolve to the new owner alone: no stale row
                # survives the publish.
                target.execute(
                    "DELETE FROM attachment_slot WHERE device_key = ?", (key,)
                )
                target.execute(
                    "DELETE FROM section_slot WHERE device_key = ?", (key,)
                )
                target.execute("DELETE FROM device WHERE device_key = ?", (key,))
            else:
                inserted += 1
            device_row, section_rows, attachment_rows = render_device_rows(
                key, snapshot.versions[key], draft
            )
            target.execute(
                "INSERT INTO device(device_key, section_shape, "
                "section_slot_count, source_version) VALUES (?, ?, ?, ?)",
                device_row,
            )
            if section_rows:
                target.executemany(
                    "INSERT INTO section_slot(device_key, section_slot_id, "
                    "device_section_shape, position, is_hole, fields_json, "
                    "attachment_shape, section_slot_count) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    section_rows,
                )
            if attachment_rows:
                target.executemany(
                    "INSERT INTO attachment_slot(device_key, "
                    "owner_section_slot_id, attachment_slot_id, "
                    "section_position, position, is_hole, fields_json) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    attachment_rows,
                )
            section_written += len(section_rows)
            attachment_written += len(attachment_rows)

        _write_sync_metadata(target, source_identity)
        target.execute("COMMIT")
    except BaseException:
        target.execute("ROLLBACK")
        raise
    return {
        "devices_total": len(snapshot.versions),
        "devices_changed": len(changed),
        "devices_inserted": inserted,
        "devices_updated": updated,
        # The supported source API never deletes a device; a snapshot missing
        # a published device is an older read view and is rejected above.
        "devices_deleted": 0,
        "section_slots_written": section_written,
        "attachment_slots_written": attachment_written,
        "already_current": False,
    }


# ---------------------------------------------------------------------------
# Entry point


def synchronize(source_path: str, analysis_path: str) -> Dict[str, Any]:
    """Synchronize one inspection store into its independent analysis database.

    The first run creates the analysis database from an empty target; later
    runs against the same pair only read and rewrite changed devices.  The
    source is opened read-only and never modified, and the target's published
    relations are swapped in one transaction.
    """
    source_identity = os.path.abspath(os.fspath(source_path))
    source = open_source_connection(source_path)
    try:
        target = open_analysis_connection(analysis_path)
        try:
            stored_identity = target.execute(
                "SELECT value FROM analysis_meta WHERE name = 'source_store'"
            ).fetchone()
            if stored_identity is not None and stored_identity[0] != source_identity:
                raise SyncError(
                    f"analysis database already mirrors {stored_identity[0]!r}; "
                    f"it is maintained for one fixed source and cannot be "
                    f"pointed at {source_identity!r}"
                )
            published = read_published_versions(target)
            snapshot = read_source_snapshot(source, published)
            report = publish_snapshot(
                target, snapshot, source_identity=source_identity
            )
            report["source"] = source_identity
            report["analysis"] = os.path.abspath(os.fspath(analysis_path))
            return report
        finally:
            target.close()
    finally:
        source.close()
