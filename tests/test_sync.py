"""Tests for the incremental synchronization into the analysis database.

The contract under test:

* the first sync builds the three formal relations (``device``,
  ``section_slot``, ``attachment_slot``) from an empty target, with columns
  matching the official CSV export plus the per-device ``source_version``;
* later syncs only read and rewrite changed devices — version-unchanged
  devices keep their rows (rowids), historical version payloads are never
  read, and the source is opened strictly read-only;
* every successful sync reflects one committed read view (shapes, holes,
  stable identities, attachment ownership, positions), including restores to
  old content that mint a new version and slot ids deleted by one device and
  reused by another;
* the three relations and the version bookkeeping publish atomically, a
  failure leaves the previous result queryable and a retry cannot lose an
  update;
* an older read view can never overwrite a newer published result, and two
  overlapping syncs cannot interleave.
"""

from __future__ import annotations

import csv
import os
import sqlite3
import threading
import unittest

from helpers import TempStore
from inspection_store import (
    DeviceStore,
    Edit,
    SyncBusy,
    SyncConflict,
    SyncError,
    synchronize,
)
from inspection_store.export import (
    ATTACHMENT_HEADER,
    DEVICE_HEADER,
    SECTION_HEADER,
    export_csv,
)
from inspection_store import sync as sync_mod

DOCUMENT = {
    "section": [
        {"fields": {"n": 0}, "attachments": [{"a": 1}, None]},
        None,
        {"fields": {"n": 2}, "attachments": []},
        {"fields": {"n": 3}, "attachments": None},
    ]
}


class SyncFixture:
    def __init__(self) -> None:
        self.tmp = TempStore()
        self.source_path = self.tmp.path
        self.analysis_path = os.path.join(self.tmp.directory, "analysis.db")
        self.csv_dir = os.path.join(self.tmp.directory, "csv")

    def analysis(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.analysis_path)
        conn.row_factory = sqlite3.Row
        return conn

    def sync(self) -> dict:
        return synchronize(self.source_path, self.analysis_path)


class FirstSyncTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fx = SyncFixture()
        store = DeviceStore(self.fx.source_path, create=True)
        store.import_document("D1", DOCUMENT)
        store.import_document("ABS", {})
        store.import_document("NUL", {"section": None})
        store.import_document("EMP", {"section": []})
        store.verify()
        store.close()

    def test_first_sync_builds_relations_matching_the_csv_export(self) -> None:
        report = self.fx.sync()
        self.assertFalse(report["already_current"])
        self.assertEqual(report["devices_inserted"], 4)
        self.assertEqual(report["devices_updated"], 0)
        self.assertEqual(report["devices_deleted"], 0)

        store = DeviceStore(self.fx.source_path)
        paths = export_csv(store, self.fx.csv_dir)
        store.close()

        conn = self.fx.analysis()
        with open(paths["device"], newline="", encoding="utf-8") as handle:
            csv_rows = list(csv.reader(handle))
        self.assertEqual(csv_rows[0], DEVICE_HEADER)
        db_rows = [
            list(map(str, row))
            for row in conn.execute(
                "SELECT device_key, section_shape, section_slot_count "
                "FROM device ORDER BY device_key"
            )
        ]
        self.assertEqual(db_rows, sorted(r for r in csv_rows[1:]))
        # The analysis relation adds the source version column.
        columns = [r[1] for r in conn.execute("PRAGMA table_info(device)")]
        self.assertEqual(columns, DEVICE_HEADER + ["source_version"])

        for table, header, query in (
            (
                "section_slot",
                SECTION_HEADER,
                "SELECT device_key, section_slot_id, device_section_shape, "
                "position, is_hole, fields_json, attachment_shape, "
                "section_slot_count FROM section_slot",
            ),
            (
                "attachment_slot",
                ATTACHMENT_HEADER,
                "SELECT device_key, owner_section_slot_id, attachment_slot_id, "
                "section_position, position, is_hole, fields_json "
                "FROM attachment_slot",
            ),
        ):
            with open(paths[table], newline="", encoding="utf-8") as handle:
                csv_rows = list(csv.reader(handle))
            self.assertEqual(csv_rows[0], header)
            db_rows = [list(map(str, row)) for row in conn.execute(query)]
            self.assertEqual(sorted(db_rows), sorted(csv_rows[1:]), table)
        conn.close()

    def test_every_device_carries_its_latest_source_version(self) -> None:
        self.fx.sync()
        conn = self.fx.analysis()
        versions = dict(
            conn.execute("SELECT device_key, source_version FROM device")
        )
        conn.close()
        store = DeviceStore(self.fx.source_path)
        for key in ("D1", "ABS", "NUL", "EMP"):
            latest = store.list_versions(key)[-1]["version_no"]
            self.assertEqual(versions[key], latest, key)
        store.close()

    def test_all_four_shapes_and_holes_survive(self) -> None:
        self.fx.sync()
        conn = self.fx.analysis()
        shapes = dict(conn.execute("SELECT device_key, section_shape FROM device"))
        self.assertEqual(
            shapes, {"D1": "present", "ABS": "absent", "NUL": "null", "EMP": "empty"}
        )
        holes = conn.execute(
            "SELECT position FROM section_slot WHERE is_hole = 1 ORDER BY position"
        ).fetchall()
        self.assertEqual([r[0] for r in holes], [1])
        # The section hole has no attachment rows and is recorded as absent.
        hole_shape = conn.execute(
            "SELECT attachment_shape FROM section_slot WHERE is_hole = 1"
        ).fetchone()[0]
        self.assertEqual(hole_shape, "absent")
        # The attachment hole keeps its position and stable owner.
        att_hole = conn.execute(
            "SELECT section_position, position FROM attachment_slot "
            "WHERE is_hole = 1"
        ).fetchone()
        self.assertEqual(tuple(att_hole), (0, 1))
        conn.close()

    def test_sync_from_empty_source(self) -> None:
        empty = TempStore()
        empty_store = empty.create()
        empty_store.close()
        analysis = os.path.join(empty.directory, "analysis.db")
        report = synchronize(empty.path, analysis)
        self.assertTrue(report["already_current"])
        self.assertEqual(report["devices_total"], 0)
        conn = sqlite3.connect(analysis)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM device").fetchone()[0], 0)
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM section_slot").fetchone()[0], 0
        )
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM attachment_slot").fetchone()[0], 0
        )
        # The empty target still records that a sync happened.
        self.assertIsNotNone(
            conn.execute(
                "SELECT value FROM analysis_meta WHERE name = 'last_synced_at'"
            ).fetchone()
        )
        conn.close()


class IncrementalSyncTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fx = SyncFixture()
        store = DeviceStore(self.fx.source_path, create=True)
        store.import_document("D1", DOCUMENT)
        # D2 carries attachment rows too, so rowid preservation can be proven
        # in both slot tables for the device that never changes.
        store.import_document(
            "D2",
            {"section": [{"fields": {"k": 1}, "attachments": [{"p": 0}, {"p": 1}]}]},
        )
        store.close()
        self.fx.sync()

    def test_repeat_sync_without_changes_is_a_noop(self) -> None:
        again = self.fx.sync()
        self.assertTrue(again["already_current"])
        self.assertEqual(again["devices_changed"], 0)
        self.assertEqual(again["section_slots_written"], 0)
        self.assertEqual(again["attachment_slots_written"], 0)

    def test_unchanged_device_rows_are_not_rewritten(self) -> None:
        conn = self.fx.analysis()
        before = {
            table: dict(
                (row[0], row[1])
                for row in conn.execute(
                    f"SELECT device_key, rowid FROM {table} ORDER BY rowid"
                )
            )
            for table in ("section_slot", "attachment_slot")
        }
        device_rowids = dict(
            conn.execute("SELECT device_key, rowid FROM device")
        )
        conn.close()

        store = DeviceStore(self.fx.source_path)
        store.apply_batch(
            "D1", [Edit.insert_section("NEW", slot={"fields": {"n": 99}})]
        )
        store.close()
        report = self.fx.sync()
        self.assertEqual(report["devices_updated"], 1)
        self.assertEqual(report["devices_changed"], 1)

        conn = self.fx.analysis()
        after = {
            table: dict(
                (row[0], row[1])
                for row in conn.execute(
                    f"SELECT device_key, rowid FROM {table} ORDER BY rowid"
                )
            )
            for table in ("section_slot", "attachment_slot")
        }
        # The incremental contract is about the unchanged device: D2's rows
        # (in every table where it has any) keep exactly their rowids, proving
        # they were never deleted or rewritten.  D1's rowids are not asserted
        # because SQLite may recycle rowids after a delete/reinsert in the
        # same transaction when no other device's rows remain in that table.
        for table in before:
            for key, rowid in before[table].items():
                if key == "D2":
                    self.assertEqual(after[table].get(key), rowid, table)
        # The unchanged device row itself keeps its rowid; D1's is replaced.
        after_device_rowids = dict(
            conn.execute("SELECT device_key, rowid FROM device")
        )
        self.assertEqual(after_device_rowids["D2"], device_rowids["D2"])
        self.assertNotEqual(after_device_rowids["D1"], device_rowids["D1"])
        conn.close()

    def test_only_changed_device_bodies_are_read(self) -> None:
        # The version scan may read device/device_version; reading live slot
        # rows of an unchanged device is forbidden by the incremental contract.
        live_tables = ("SECTION_SLOT", "ATTACHMENT_SLOT")

        def trace_snapshot(source, published):
            traced: list = []

            class TracingConn:
                """Wraps the real connection and records every execute call."""

                def __init__(self, real):
                    self.real = real

                def execute(self, sql, parameters=()):
                    params = tuple(parameters) if parameters else ()
                    traced.append((sql, params))
                    return self.real.execute(sql, params)

                def __getattr__(self, name):
                    return getattr(self.real, name)

            # The module-level draft reader receives this wrapper as its
            # connection, so the live-table SELECTs are recorded here too.
            snapshot = sync_mod.read_source_snapshot_from(
                TracingConn(source), published
            )
            live_reads = [
                (sql, params)
                for sql, params in traced
                if sql.lstrip().upper().startswith("SELECT")
                and any(table in sql.upper() for table in live_tables)
            ]
            return snapshot, live_reads

        # Phase 1: nothing changed since the publish in setUp.
        source = sync_mod.open_source_connection(self.fx.source_path)
        target = sync_mod.open_analysis_connection(self.fx.analysis_path)
        published = sync_mod.read_published_versions(target)
        snapshot, live_reads = trace_snapshot(source, published)
        target.close()
        source.close()
        self.assertEqual(live_reads, [])
        self.assertEqual(set(snapshot.drafts), set())

        # Phase 2: exactly one device changes; only its body is read.
        store = DeviceStore(self.fx.source_path)
        store.apply_batch(
            "D1", [Edit.insert_section("NEW2", slot={"fields": {"n": 7}})]
        )
        store.close()
        source = sync_mod.open_source_connection(self.fx.source_path)
        target = sync_mod.open_analysis_connection(self.fx.analysis_path)
        published = sync_mod.read_published_versions(target)
        snapshot, live_reads = trace_snapshot(source, published)
        target.close()
        source.close()
        self.assertEqual(set(snapshot.drafts), {"D1"})
        self.assertTrue(live_reads)
        # Every live-table read is parameterized with the changed device key;
        # D2's body (its section and attachment rows) is never read.
        for sql, params in live_reads:
            self.assertIn("D1", params, sql)
            self.assertNotIn("D2", params, sql)
        # No live-table statement mentions the unchanged device inline either.
        self.assertTrue(all("D2" not in sql for sql, _ in live_reads), live_reads)

    def test_historical_version_payloads_are_never_read(self) -> None:
        statements: list = []
        source = sync_mod.open_source_connection(self.fx.source_path)
        target = sync_mod.open_analysis_connection(self.fx.analysis_path)
        try:
            source.set_trace_callback(statements.append)
            published = sync_mod.read_published_versions(target)
            sync_mod.read_source_snapshot(source, published)
            source.set_trace_callback(None)
        finally:
            target.close()
            source.close()
        self.assertTrue(statements)
        for sql in statements:
            self.assertNotIn("draft_json", sql)
            self.assertNotIn("content_hash", sql)

    def test_multiple_changes_between_syncs_publish_only_the_latest(self) -> None:
        store = DeviceStore(self.fx.source_path)
        first_id = store.page("D1", 0, 1)[0]["slot_id"]
        for n in range(3):
            store.apply_batch(
                "D1",
                [Edit.insert_section(f"M{n}", before=first_id,
                                     slot={"fields": {"n": n}})],
            )
        versions = [v["version_no"] for v in store.list_versions("D1")]
        store.close()
        self.assertEqual(versions, [1, 2, 3, 4])

        report = self.fx.sync()
        self.assertEqual(report["devices_changed"], 1)
        conn = self.fx.analysis()
        self.assertEqual(
            conn.execute(
                "SELECT source_version FROM device WHERE device_key = 'D1'"
            ).fetchone()[0],
            4,
        )
        positions = conn.execute(
            "SELECT section_slot_id, position FROM section_slot "
            "WHERE device_key = 'D1' ORDER BY position"
        ).fetchall()
        self.assertEqual([r[1] for r in positions], list(range(7)))
        # Each insert was before the original first slot, so the inserts keep
        # their chronological order ahead of the untouched original slots.
        self.assertEqual(
            [r[0] for r in positions[:3]], ["M0", "M1", "M2"]
        )
        self.assertEqual(
            [r[0] for r in positions[3:]],
            [
                r[0]
                for r in conn.execute(
                    "SELECT section_slot_id FROM section_slot "
                    "WHERE device_key = 'D1' AND section_slot_id NOT IN "
                    "('M0', 'M1', 'M2') ORDER BY position"
                )
            ],
        )
        conn.close()

    def test_restore_to_old_content_mints_a_new_version_and_syncs(self) -> None:
        store = DeviceStore(self.fx.source_path)
        original = store.load_document("D1")
        store.apply_batch(
            "D1", [Edit.insert_section("LATER", slot={"fields": {"n": 11}})]
        )
        stats = store.restore_version("D1", 1)
        self.assertEqual(stats["versions_added"], 1)
        restored_versions = [v["version_no"] for v in store.list_versions("D1")]
        store.close()
        self.assertEqual(restored_versions, [1, 2, 3])

        report = self.fx.sync()
        self.assertEqual(report["devices_changed"], 1)
        conn = self.fx.analysis()
        self.assertEqual(
            conn.execute(
                "SELECT source_version FROM device WHERE device_key = 'D1'"
            ).fetchone()[0],
            3,
        )
        # Content equals version 1 even though the source version number is 3.
        rows = conn.execute(
            "SELECT section_slot_id FROM section_slot WHERE device_key = 'D1' "
            "ORDER BY position"
        ).fetchall()
        self.assertEqual(len(rows), len(DOCUMENT["section"]))
        self.assertIsNone(
            conn.execute(
                "SELECT 1 FROM section_slot WHERE section_slot_id = 'LATER'"
            ).fetchone()
        )
        conn.close()
        store = DeviceStore(self.fx.source_path)
        self.assertEqual(store.load_document("D1"), original)
        store.close()

    def test_slot_id_deleted_then_reused_by_another_device(self) -> None:
        store = DeviceStore(self.fx.source_path)
        first = store.page("D1", 0, 1)[0]["slot_id"]
        # Device A owns a well-known id ...
        store.import_document(
            "DA", {"section": [{"fields": {"who": "A"}}]},
            id_factory=lambda kind, ordinal: "SHARED" if kind == "section" else f"a{ordinal}",
        )
        self.assertEqual(
            store.slot_view("SHARED")["device_key"], "DA"
        )
        # ... then deletes it, and device B reuses the same stable id.
        store.apply_batch("DA", [Edit.delete_section("SHARED")])
        store.import_document(
            "DB", {"section": [{"fields": {"who": "B"}}]},
            id_factory=lambda kind, ordinal: "SHARED" if kind == "section" else f"b{ordinal}",
        )
        self.assertEqual(
            store.slot_view("SHARED")["device_key"], "DB"
        )
        store.verify()
        store.close()

        report = self.fx.sync()
        # D1/D2 unchanged; A and B are new, neither fails on the reused id.
        self.assertEqual(report["devices_inserted"], 2)
        conn = self.fx.analysis()
        owner = conn.execute(
            "SELECT device_key, fields_json FROM section_slot "
            "WHERE section_slot_id = 'SHARED'"
        ).fetchall()
        self.assertEqual(len(owner), 1)
        self.assertEqual(owner[0][0], "DB")
        self.assertEqual(owner[0][1], '{"who":"B"}')
        # No stale attachment rows can be left pointing at the old owner.
        dangling = conn.execute(
            "SELECT 1 FROM attachment_slot a WHERE NOT EXISTS ("
            "SELECT 1 FROM section_slot s WHERE s.device_key = a.device_key "
            "AND s.section_slot_id = a.owner_section_slot_id)"
        ).fetchall()
        self.assertEqual(dangling, [])
        conn.close()

    def test_new_device_is_inserted_and_published_by_a_fresh_sync(self) -> None:
        store = DeviceStore(self.fx.source_path)
        store.import_document("D3", {"section": [{"fields": {"z": 0}}]})
        store.close()

        report = self.fx.sync()
        self.assertEqual(report["devices_inserted"], 1)
        self.assertEqual(report["devices_deleted"], 0)
        conn = self.fx.analysis()
        keys = [r[0] for r in conn.execute("SELECT device_key FROM device")]
        self.assertEqual(sorted(keys), ["D1", "D2", "D3"])
        self.assertEqual(
            conn.execute(
                "SELECT source_version FROM device WHERE device_key = 'D3'"
            ).fetchone()[0],
            1,
        )
        conn.close()

    def test_stale_view_missing_a_published_device_is_rejected_not_deleted(self) -> None:
        # A fresh sync publishes a brand-new device D3.
        store = DeviceStore(self.fx.source_path)
        store.import_document("D3", {"section": [{"fields": {"z": 0}}]})
        store.close()
        self.fx.sync()

        # An older read view (e.g. a sync whose source snapshot began before
        # D3 existed) must not be allowed to delete D3 on publish; the strict
        # ordering guard treats the missing published device as a conflict.
        target = sync_mod.open_analysis_connection(self.fx.analysis_path)
        stale = sync_mod.SourceSnapshot(
            versions={"D1": 1, "D2": 1}, drafts={}
        )
        with self.assertRaises(SyncConflict):
            sync_mod.publish_snapshot(
                target,
                stale,
                source_identity=os.path.abspath(self.fx.source_path),
            )
        target.close()
        conn = self.fx.analysis()
        # D3 and its version are untouched after the rejected publish.
        self.assertEqual(
            conn.execute(
                "SELECT source_version FROM device WHERE device_key = 'D3'"
            ).fetchone()[0],
            1,
        )
        conn.close()


class AtomicityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fx = SyncFixture()
        store = DeviceStore(self.fx.source_path, create=True)
        store.import_document("D1", DOCUMENT)
        store.import_document("D2", {"section": [{"fields": {"k": 1}}]})
        store.close()
        self.fx.sync()

    def test_failed_publish_keeps_the_previous_result_and_retries_cleanly(self) -> None:
        conn = self.fx.analysis()
        before = {
            table: conn.execute(f"SELECT * FROM {table} ORDER BY 1, 2, 3").fetchall()
            for table in ("device", "section_slot", "attachment_slot")
        }
        generation = conn.execute(
            "SELECT value FROM analysis_meta WHERE name = 'last_synced_generation'"
        ).fetchone()[0]
        conn.close()

        store = DeviceStore(self.fx.source_path)
        store.apply_batch(
            "D1", [Edit.insert_section("BOOM", slot={"fields": {"n": 1}})]
        )
        store.close()

        # Force the next publish to fail after some rows have been staged:
        # progress must roll back, not leak halfway.  A wrapper around the
        # connection injects the failure when the sync reaches its final
        # metadata write, which happens only after every data row is staged.
        real_target = sync_mod.open_analysis_connection(self.fx.analysis_path)
        source = sync_mod.open_source_connection(self.fx.source_path)

        class FailingConnection:
            def __init__(self, real):
                self.real = real
                self.armed = True

            def execute(self, sql, parameters=()):
                if (
                    self.armed
                    and sql.lstrip().upper().startswith("INSERT INTO ANALYSIS_META")
                ):
                    self.armed = False
                    raise sqlite3.OperationalError("injected publish failure")
                return self.real.execute(sql, parameters)

            def __getattr__(self, name):
                return getattr(self.real, name)

        failing = FailingConnection(real_target)
        with self.assertRaises(sqlite3.OperationalError):
            published = sync_mod.read_published_versions(real_target)
            snapshot = sync_mod.read_source_snapshot(source, published)
            sync_mod.publish_snapshot(
                failing, snapshot,
                source_identity=os.path.abspath(self.fx.source_path),
            )
        real_target.close()
        source.close()

        conn = self.fx.analysis()
        after = {
            table: conn.execute(f"SELECT * FROM {table} ORDER BY 1, 2, 3").fetchall()
            for table in ("device", "section_slot", "attachment_slot")
        }
        self.assertEqual(after, before)
        self.assertEqual(
            conn.execute(
                "SELECT value FROM analysis_meta WHERE name = 'last_synced_generation'"
            ).fetchone()[0],
            generation,
        )
        self.assertIsNone(
            conn.execute(
                "SELECT 1 FROM section_slot WHERE section_slot_id = 'BOOM'"
            ).fetchone()
        )
        conn.close()

        # A plain retry must publish everything the failed attempt staged.
        report = self.fx.sync()
        self.assertFalse(report["already_current"])
        self.assertEqual(report["devices_updated"], 1)
        conn = self.fx.analysis()
        self.assertEqual(
            conn.execute(
                "SELECT source_version FROM device WHERE device_key = 'D1'"
            ).fetchone()[0],
            2,
        )
        self.assertIsNotNone(
            conn.execute(
                "SELECT 1 FROM section_slot WHERE section_slot_id = 'BOOM'"
            ).fetchone()
        )
        conn.close()

    def test_reader_never_sees_a_partial_publish(self) -> None:
        # WAL readers see only committed snapshots; assert the journal mode and
        # that every visible state passes an inter-table consistency check.
        conn = self.fx.analysis()
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0].lower()
        self.assertEqual(mode, "wal")
        store = DeviceStore(self.fx.source_path)
        for n in range(5):
            store.apply_batch(
                "D1",
                [Edit.insert_section(f"P{n}", slot={"fields": {"n": n}})],
            )
        store.close()
        self.fx.sync()
        # Every attachment row's owner exists at the same published position.
        orphans = conn.execute(
            "SELECT COUNT(*) FROM attachment_slot a WHERE NOT EXISTS ("
            "SELECT 1 FROM section_slot s WHERE s.device_key = a.device_key "
            "AND s.section_slot_id = a.owner_section_slot_id "
            "AND s.position = a.section_position)"
        ).fetchone()[0]
        self.assertEqual(orphans, 0)
        # Positions are dense per device and array.
        max_position = conn.execute(
            "SELECT MAX(c) FROM (SELECT device_key, COUNT(*) AS c "
            "FROM section_slot GROUP BY device_key)"
        ).fetchone()[0]
        counts = conn.execute(
            "SELECT device_key, section_slot_count FROM device"
        ).fetchall()
        for key, count in counts:
            actual = conn.execute(
                "SELECT COUNT(*) FROM section_slot WHERE device_key = ?", (key,)
            ).fetchone()[0]
            self.assertEqual(actual, count)
        self.assertGreaterEqual(max_position, 1)
        conn.close()


class ConcurrencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fx = SyncFixture()
        store = DeviceStore(self.fx.source_path, create=True)
        # WAL lets a long read transaction coexist with a source writer.
        store.conn.execute("PRAGMA journal_mode = WAL")
        store.import_document("D1", DOCUMENT)
        store.close()
        self.fx.sync()

    def test_overlapping_syncs_do_not_interleave(self) -> None:
        # While one transaction owns the analysis database write lock, a
        # second publish must fail fast with SyncBusy instead of interleaving
        # its rows with the in-flight one.
        holder = sync_mod.open_analysis_connection(self.fx.analysis_path)
        holder.execute("BEGIN IMMEDIATE")

        result: dict = {"error": None, "report": None}

        def worker() -> None:
            target = sync_mod.open_analysis_connection(self.fx.analysis_path)
            source = sync_mod.open_source_connection(self.fx.source_path)
            try:
                published = sync_mod.read_published_versions(target)
                snapshot = sync_mod.read_source_snapshot(source, published)
                result["report"] = sync_mod.publish_snapshot(
                    target,
                    snapshot,
                    source_identity=os.path.abspath(self.fx.source_path),
                    busy_timeout_ms=20,
                )
            except SyncBusy as exc:
                result["error"] = exc
            finally:
                source.close()
                target.close()

        thread = threading.Thread(target=worker)
        thread.start()
        thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertIsInstance(result["error"], SyncBusy)
        self.assertIsNone(result["report"])

        # The holder's snapshot remains fully visible and intact.
        held = holder.execute("SELECT COUNT(*) FROM device").fetchone()[0]
        self.assertEqual(held, 1)
        holder.execute("COMMIT")
        holder.close()

        # The rejected sync left nothing behind: foreign keys still check.
        conn = self.fx.analysis()
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM device").fetchone()[0], 1
        )
        self.assertIsNone(conn.execute("PRAGMA foreign_key_check").fetchone())
        conn.close()

        # Once the lock is released, a new sync publishes normally.
        self.fx.sync()

    def test_older_read_view_cannot_overwrite_newer_publish(self) -> None:
        # Hold one read transaction open at version 1.
        old_source = sync_mod.open_source_connection(self.fx.source_path)
        old_source.execute("BEGIN")
        old_snapshot_rows = old_source.execute(
            "SELECT d.device_key, MAX(v.version_no) FROM device d "
            "JOIN device_version v ON v.device_key = d.device_key GROUP BY d.device_key"
        ).fetchall()
        self.assertEqual(old_snapshot_rows, [("D1", 1)])

        # A separate writer advances the source and a normal sync publishes v2.
        writer = DeviceStore(self.fx.source_path)
        writer.apply_batch(
            "D1", [Edit.insert_section("FRESH", slot={"fields": {"n": 5}})]
        )
        writer.close()
        self.fx.sync()

        # Build the stale snapshot from the still-open old read view, then try
        # to publish it against a target that already holds the newer result.
        from inspection_store.store import _read_draft_from_conn

        stale_draft = _read_draft_from_conn(old_source, "D1")
        old_source.execute("ROLLBACK")
        old_source.close()
        stale = sync_mod.SourceSnapshot(
            versions={"D1": 1}, drafts={"D1": stale_draft}
        )
        target = sync_mod.open_analysis_connection(self.fx.analysis_path)
        with self.assertRaises(SyncConflict):
            sync_mod.publish_snapshot(
                target,
                stale,
                source_identity=os.path.abspath(self.fx.source_path),
            )
        target.close()

        conn = self.fx.analysis()
        self.assertEqual(
            conn.execute(
                "SELECT source_version FROM device WHERE device_key = 'D1'"
            ).fetchone()[0],
            2,
        )
        self.assertIsNotNone(
            conn.execute(
                "SELECT 1 FROM section_slot WHERE section_slot_id = 'FRESH'"
            ).fetchone()
        )
        conn.close()

    def test_full_synchronize_raises_sync_busy_under_contention(self) -> None:
        target = sync_mod.open_analysis_connection(self.fx.analysis_path)
        target.execute("BEGIN IMMEDIATE")
        try:
            with self.assertRaises(SyncBusy):
                # A second, independent connection cannot start its publish.
                conn = sync_mod.open_analysis_connection(self.fx.analysis_path)
                try:
                    source = sync_mod.open_source_connection(self.fx.source_path)
                    try:
                        published = sync_mod.read_published_versions(conn)
                        snapshot = sync_mod.read_source_snapshot(source, published)
                        sync_mod.publish_snapshot(
                            conn,
                            snapshot,
                            source_identity=os.path.abspath(self.fx.source_path),
                            busy_timeout_ms=20,
                        )
                    finally:
                        source.close()
                finally:
                    conn.close()
        finally:
            target.execute("ROLLBACK")
            target.close()

    def test_source_binding_is_enforced_inside_the_publish_transaction(self) -> None:
        # Even a snapshot built from the real data must not publish under a
        # foreign source identity once the target is bound.  The check runs
        # inside BEGIN IMMEDIATE so racing first-time syncs cannot bypass it.
        source = sync_mod.open_source_connection(self.fx.source_path)
        published = sync_mod.read_published_versions(
            sync_mod.open_analysis_connection(self.fx.analysis_path)
        )
        snapshot = sync_mod.read_source_snapshot(source, published)
        source.close()
        target = sync_mod.open_analysis_connection(self.fx.analysis_path)
        with self.assertRaises(SyncError):
            sync_mod.publish_snapshot(
                target,
                snapshot,
                source_identity=os.path.abspath(
                    os.path.join(self.fx.tmp.directory, "foreign-store.db")
                ),
            )
        target.close()
        # The rejected publish rolled back; the original binding survives.
        conn = self.fx.analysis()
        self.assertEqual(
            conn.execute(
                "SELECT value FROM analysis_meta WHERE name = 'source_store'"
            ).fetchone()[0],
            os.path.abspath(self.fx.source_path),
        )
        conn.close()


class ReadOnlySourceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fx = SyncFixture()
        store = DeviceStore(self.fx.source_path, create=True)
        store.import_document("D1", DOCUMENT)
        store.close()

    def test_source_file_is_not_modified_by_sync(self) -> None:
        before = os.stat(self.fx.source_path).st_mtime_ns
        # Any attempted write through a mode=ro connection raises immediately.
        conn = sync_mod.open_source_connection(self.fx.source_path)
        with self.assertRaises(sqlite3.OperationalError):
            conn.execute("CREATE TABLE attempted_write(x INTEGER)")
        conn.close()
        self.fx.sync()
        # A read-only sync cannot touch the source file's modification time.
        self.assertEqual(os.stat(self.fx.source_path).st_mtime_ns, before)

    def test_any_write_statement_is_rejected_by_the_authorizer(self) -> None:
        conn = sync_mod.open_source_connection(self.fx.source_path)
        self.assertEqual(conn.execute("PRAGMA query_only").fetchone()[0], 1)
        for statement in (
            "INSERT INTO device DEFAULT VALUES",
            "UPDATE device SET section_shape = 0",
            "DELETE FROM device",
        ):
            with self.assertRaises(sqlite3.OperationalError):
                conn.execute(statement)
        conn.close()

    def test_legacy_store_without_history_is_rejected(self) -> None:
        from inspection_store.store import _LEGACY_SCHEMA_V1

        legacy = TempStore()
        raw = sqlite3.connect(legacy.path)
        raw.executescript(_LEGACY_SCHEMA_V1)
        raw.execute("INSERT INTO meta(name, value) VALUES ('schema_version', '1')")
        raw.execute(
            "INSERT INTO device(device_key, section_shape, section_root) "
            "VALUES ('D', 0, NULL)"
        )
        raw.commit()
        raw.close()
        analysis = os.path.join(legacy.directory, "analysis.db")
        with self.assertRaises(SyncError) as caught:
            synchronize(legacy.path, analysis)
        self.assertIn("version history", str(caught.exception))
        self.assertFalse(os.path.exists(analysis))

    def test_missing_source_and_bad_target_are_reported(self) -> None:
        with self.assertRaises(SyncError):
            sync_mod.open_source_connection(
                os.path.join(self.fx.tmp.directory, "nope.db")
            )
        # A foreign SQLite file that is not an inspection store.
        foreign = os.path.join(self.fx.tmp.directory, "foreign.db")
        other = sqlite3.connect(foreign)
        other.execute("CREATE TABLE unrelated(x)")
        other.commit()
        other.close()
        with self.assertRaises(SyncError):
            sync_mod.open_source_connection(foreign)

        # A target already holding unrelated tables is rejected, not overwritten.
        bad_target = os.path.join(self.fx.tmp.directory, "bad-target.db")
        other = sqlite3.connect(bad_target)
        other.execute("CREATE TABLE device(x)")
        other.commit()
        other.close()
        with self.assertRaises(SyncError):
            synchronize(self.fx.source_path, bad_target)

    def test_target_is_bound_to_one_source(self) -> None:
        self.fx.sync()
        other_source = os.path.join(self.fx.tmp.directory, "other-store.db")
        other = DeviceStore(other_source, create=True)
        other.import_document("X", {})
        other.close()
        with self.assertRaises(SyncError) as caught:
            synchronize(other_source, self.fx.analysis_path)
        self.assertIn("one fixed source", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
