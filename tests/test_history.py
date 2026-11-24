"""Version-history tests.

Covers the traceability contract:

* a fresh import is version 1 and an upgraded legacy database gets a baseline
  version per device, while all old data stays readable and writable;
* each successful content change appends exactly one version, for every write
  path (import, replace, batch, offline merge write-back and restore);
* failed writes and content-identical writes append nothing and leave both the
  current record and the history exactly as they were;
* versions survive closing and reopening the database;
* any historical version is fully readable, including fields, array shape,
  holes, order, stable slot ids and attachment ownership;
* restore rebuilds the old content and identities but appends a new latest
  version — history never moves backwards or loses a version;
* invalid devices or versions report the reason and change nothing.
"""

from __future__ import annotations

import json
import os
import sqlite3
import unittest

from helpers import TempStore
from inspection_store import (
    BatchAborted,
    DeviceStore,
    Edit,
    IntegrityError,
    NotFound,
    ValidationError,
)
from inspection_store.merge import (
    draft_import_ids,
    draft_to_plain_document,
    parse_draft,
)
from inspection_store.store import _LEGACY_SCHEMA_V1

DOCUMENT = {
    "section": [
        {"fields": {"n": 0}, "attachments": [{"a": 1}, None]},
        None,
        {"fields": {"n": 2}, "attachments": []},
        {"fields": {"n": 3}, "attachments": None},
    ]
}


def replay_ids(draft):
    """An id_factory that replays a draft's own stable ids on import."""
    plan = draft_import_ids(draft)
    cursor = {"i": 0}

    def factory(kind, ordinal):
        expected_kind, slot_id = plan[cursor["i"]]
        assert expected_kind == kind
        cursor["i"] += 1
        return slot_id

    return factory


class FreshHistoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TempStore()
        self.store = self.tmp.create()

    def tearDown(self) -> None:
        self.store.close()

    def test_first_import_is_version_one_for_every_shape(self) -> None:
        for key, doc in [
            ("ABS", {}),
            ("NUL", {"section": None}),
            ("EMP", {"section": []}),
            ("FUL", DOCUMENT),
        ]:
            self.store.import_document(key, doc)
            versions = self.store.list_versions(key)
            self.assertEqual([v["version_no"] for v in versions], [1])
            self.assertEqual(versions[0]["kind"], "import")
            self.assertIn("created_at", versions[0])
            self.assertIn("content_hash", versions[0])
            section = doc.get("section")
            expected_sections = len(section) if isinstance(section, list) else 0
            self.assertEqual(versions[0]["section_slots"], expected_sections)
            expected_atts = 0
            if isinstance(section, list):
                for slot in section:
                    if isinstance(slot, dict) and isinstance(
                        slot.get("attachments"), list
                    ):
                        expected_atts += len(slot["attachments"])
            self.assertEqual(versions[0]["attachment_slots"], expected_atts)
            # The stored version reads back as exactly the imported record.
            record = self.store.load_version(key, 1)["document"]
            self.assertEqual(record, {"key": key, **doc})
        self.store.verify()

    def test_successful_batch_appends_one_version_for_the_whole_batch(self) -> None:
        self.store.import_document("D", DOCUMENT)
        section = self.store.page("D", 0, 1)[0]["slot_id"]
        stats = self.store.apply_batch(
            "D",
            [
                Edit.insert_section("S1", before=section, slot={"fields": {"n": 9}}),
                Edit.init_attachments_empty("S1"),
                Edit.insert_attachment("S1", "A9", slot={"x": 1}),
                Edit.insert_attachment("S1", "H9", slot=None),
            ],
        )
        self.assertEqual(stats["versions_added"], 1)
        self.assertEqual(stats["version_no"], 2)
        self.assertEqual(
            [v["version_no"] for v in self.store.list_versions("D")], [1, 2]
        )
        # Version 1 still holds the pre-batch document and identities.
        v1 = self.store.load_version("D", 1)["document"]
        self.assertEqual(v1, {"key": "D", **DOCUMENT})
        # The version's identified draft keeps every stable id and order.
        draft_v1 = parse_draft(self.store.checkout_version("D", 1))
        draft_now = parse_draft(self.store.checkout_document("D"))
        old_ids = list(draft_import_ids(draft_v1))
        new_ids = list(draft_import_ids(draft_now))
        # Old ids all survive, new ones are appended into the same identities.
        self.assertTrue(set(old_ids).issubset(set(new_ids)))
        self.assertEqual(len(new_ids) - len(old_ids), 3)
        self.store.verify()

    def test_failed_batch_adds_no_version_and_changes_nothing(self) -> None:
        self.store.import_document("D", {"section": [{"fields": {"n": 0}}]})
        before_tables = self.store.dump_tables()
        before_doc = self.store.load_document("D")
        before_versions = self.store.list_versions("D")
        with self.assertRaises(BatchAborted):
            self.store.apply_batch(
                "D",
                [
                    Edit.insert_section("BAD", slot=None),
                    Edit.insert_section("BAD", slot=None),  # duplicate id
                ],
            )
        self.assertEqual(self.store.dump_tables(), before_tables)
        self.assertEqual(self.store.load_document("D"), before_doc)
        self.assertEqual(self.store.list_versions("D"), before_versions)
        # The failure's partial id never leaked into history or live tables.
        with self.assertRaises(NotFound):
            self.store.slot_view("BAD")

    def test_empty_batch_adds_no_version(self) -> None:
        self.store.import_document("D", DOCUMENT)
        stats = self.store.apply_batch("D", [])
        self.assertEqual(stats["versions_added"], 0)
        self.assertEqual(
            [v["version_no"] for v in self.store.list_versions("D")], [1]
        )

    def test_content_identical_replace_adds_no_version(self) -> None:
        self.store.import_document("D", DOCUMENT)
        draft = parse_draft(self.store.checkout_document("D"))
        before = self.store.list_versions("D")
        # Replace with an equal document, replaying the exact same identities.
        stats = self.store.import_document(
            "D",
            draft_to_plain_document(draft),
            replace=True,
            id_factory=replay_ids(draft),
        )
        self.assertEqual(stats["versions_added"], 0)
        self.assertEqual(self.store.list_versions("D"), before)
        self.assertEqual(
            self.store.load_document("D"), {"key": "D", **DOCUMENT}
        )

    def test_changing_replace_adds_one_version_and_keeps_old_one(self) -> None:
        self.store.import_document("D", DOCUMENT)
        self.store.import_document("D", {"section": None}, replace=True)
        versions = self.store.list_versions("D")
        self.assertEqual([v["kind"] for v in versions], ["import", "replace"])
        self.assertIsNone(self.store.load_document("D").get("section"))
        self.assertEqual(
            self.store.load_version("D", 1)["document"], {"key": "D", **DOCUMENT}
        )
        self.assertIsNone(self.store.load_version("D", 2)["document"]["section"])

    def test_versions_persist_after_reopen(self) -> None:
        self.store.import_document("D", DOCUMENT)
        self.store.apply_batch(
            "D", [Edit.insert_section("S1", slot={"fields": {"n": 7}})]
        )
        self.store.apply_batch(
            "D", [Edit.insert_section("S2", before="S1", slot=None)]
        )
        path = self.tmp.path
        self.store.close()

        reopened = DeviceStore(path)
        try:
            self.assertEqual(
                [v["version_no"] for v in reopened.list_versions("D")], [1, 2, 3]
            )
            for no in (1, 2, 3):
                record = reopened.load_version("D", no)
                self.assertEqual(record["version_no"], no)
                # Every version is readable both as a record and identified.
                draft = reopened.checkout_version("D", no)
                self.assertEqual(draft["key"], "D")
            reopened.verify()
        finally:
            reopened.close()

    def test_restore_rebuilds_content_and_identities_then_appends(self) -> None:
        self.store.import_document("D", DOCUMENT)
        original_draft = parse_draft(self.store.checkout_document("D"))
        original_ids = list(draft_import_ids(original_draft))
        section = self.store.page("D", 0, 1)[0]["slot_id"]
        self.store.apply_batch(
            "D",
            [
                Edit.delete_section(section),
                Edit.insert_section("LATER", slot={"fields": {"n": 99}}),
            ],
        )
        self.assertEqual(
            [v["version_no"] for v in self.store.list_versions("D")], [1, 2]
        )
        stats = self.store.restore_version("D", 1)
        self.assertEqual(stats["versions_added"], 1)
        self.assertEqual(stats["restored_from"], 1)

        versions = self.store.list_versions("D")
        self.assertEqual([v["version_no"] for v in versions], [1, 2, 3])
        self.assertEqual(versions[-1]["kind"], "restore")
        # Nothing was deleted or reordered in history.
        self.assertEqual(
            self.store.load_version("D", 1)["document"], {"key": "D", **DOCUMENT}
        )
        self.assertEqual(
            self.store.load_document("D"), {"key": "D", **DOCUMENT}
        )
        # Stable slot identities come back too, not just values.
        restored_ids = list(
            draft_import_ids(parse_draft(self.store.checkout_document("D")))
        )
        self.assertEqual(restored_ids, original_ids)
        # The newest stored version is exactly the live record.
        self.assertEqual(
            self.store.load_version("D", 3)["document"],
            {"key": "D", **DOCUMENT},
        )
        self.store.verify()

    def test_restore_same_content_adds_no_version(self) -> None:
        self.store.import_document("D", DOCUMENT)
        before = self.store.list_versions("D")
        stats = self.store.restore_version("D", 1)
        self.assertEqual(stats["versions_added"], 0)
        self.assertEqual(self.store.list_versions("D"), before)

    def test_restore_chain_keeps_every_version_readable(self) -> None:
        self.store.import_document("D", {"section": [{"fields": {"v": 1}}]})
        self.store.import_document(
            "D", {"section": [{"fields": {"v": 2}}]}, replace=True
        )
        self.store.import_document(
            "D", {"section": [{"fields": {"v": 3}}]}, replace=True
        )
        self.restore_and_check("D", 1, 1)
        self.restore_and_check("D", 2, 2)
        self.restore_and_check("D", 1, 1)
        self.assertEqual(
            [v["version_no"] for v in self.store.list_versions("D")],
            [1, 2, 3, 4, 5, 6],
        )
        expected = {1: 1, 2: 2, 3: 3, 4: 1, 5: 2, 6: 1}
        for no, value in expected.items():
            self.assertEqual(
                self.store.load_version("D", no)["document"]["section"][0][
                    "fields"
                ]["v"],
                value,
            )

    def restore_and_check(self, device, source, expected_value) -> None:
        self.store.restore_version(device, source)
        self.assertEqual(
            self.store.load_document(device)["section"][0]["fields"]["v"],
            expected_value,
        )

    def test_restore_invalid_version_changes_nothing(self) -> None:
        self.store.import_document("D", DOCUMENT)
        before_tables = self.store.dump_tables()
        for bad, error in (
            (0, ValidationError),
            (-1, ValidationError),
            (7, NotFound),
            ("1", ValidationError),
            (1.0, ValidationError),
            (True, ValidationError),
        ):
            with self.assertRaises(error):
                self.store.restore_version("D", bad)
        self.assertEqual(self.store.dump_tables(), before_tables)
        self.assertEqual(
            [v["version_no"] for v in self.store.list_versions("D")], [1]
        )

    def test_unknown_device_reports_not_found_and_writes_nothing(self) -> None:
        self.store.import_document("D", DOCUMENT)
        with self.assertRaises(NotFound):
            self.store.list_versions("GHOST")
        with self.assertRaises(NotFound):
            self.store.load_version("GHOST", 1)
        with self.assertRaises(NotFound):
            self.store.restore_version("GHOST", 1)
        self.assertEqual(self.store.device_keys(), ["D"])

    def test_version_payload_preserves_shapes_holes_order_and_ownership(self) -> None:
        doc = {
            "section": [
                {"fields": {"n": 0}, "attachments": [{"a": 1}, None]},
                None,
                {"fields": {"n": 2}, "attachments": []},
                {"fields": {"n": 3}, "attachments": None},
                {"fields": {"n": 4}},  # attachments absent
            ]
        }
        self.store.import_document("S", doc)
        draft = self.store.checkout_version("S", 1)
        section = draft["section"]
        # The hole keeps a stable id while its slot payload is null.
        self.assertIsNone(section[1]["slot"])
        self.assertTrue(section[1]["id"])
        # Attachment hole is a full member with its own id and null payload.
        self.assertEqual(
            section[0]["slot"]["attachments"][1],
            {"id": section[0]["slot"]["attachments"][1]["id"], "slot": None},
        )
        # The four attachment states stay distinct in the historical payload.
        self.assertIsInstance(section[0]["slot"]["attachments"], list)
        self.assertEqual(section[2]["slot"]["attachments"], [])  # empty
        self.assertIsNone(section[3]["slot"]["attachments"])     # null
        self.assertNotIn("attachments", section[4]["slot"])      # absent
        # Order and section identities match the live array exactly.
        self.assertEqual(
            [e["id"] for e in section],
            [r["slot_id"] for r in self.store.page("S", 0, 10)],
        )
        # Attachment ids are nested under their owning section id.
        owner = section[0]["id"]
        attachment_ids = [e["id"] for e in section[0]["slot"]["attachments"]]
        for aid in attachment_ids:
            self.assertEqual(
                self.store.slot_view(aid)["owner_section_slot_id"], owner
            )
        self.store.verify()

    def test_content_hash_changes_only_with_content(self) -> None:
        self.store.import_document("D", DOCUMENT)
        h1 = self.store.list_versions("D")[0]["content_hash"]
        draft = parse_draft(self.store.checkout_document("D"))
        self.store.import_document(
            "D",
            draft_to_plain_document(draft),
            replace=True,
            id_factory=replay_ids(draft),
        )
        self.assertEqual(
            self.store.list_versions("D")[0]["content_hash"], h1
        )
        self.store.import_document(
            "D", {"section": [{"fields": {"n": 1}}]}, replace=True
        )
        self.assertNotEqual(
            self.store.list_versions("D")[-1]["content_hash"], h1
        )

    def test_devices_have_independent_version_sequences(self) -> None:
        self.store.import_document("D1", {"section": []})
        self.store.import_document("D2", {"section": [None]})
        self.store.apply_batch(
            "D1", [Edit.insert_section("X", slot={"fields": {}})]
        )
        self.assertEqual(
            [v["version_no"] for v in self.store.list_versions("D1")], [1, 2]
        )
        self.assertEqual(
            [v["version_no"] for v in self.store.list_versions("D2")], [1]
        )
        self.store.verify()

    def test_failed_import_adds_no_version_and_changes_nothing(self) -> None:
        self.store.import_document("D", DOCUMENT)
        before_tables = self.store.dump_tables()
        before_doc = self.store.load_document("D")
        # A document with a non-scalar value is rejected before any write.
        with self.assertRaises(Exception):
            self.store.import_document(
                "D", {"section": [{"fields": {"bad": [1, 2]}}]}, replace=True
            )
        # A non-replacing import into an existing key is rejected too.
        with self.assertRaises(Exception):
            self.store.import_document("D", {"section": []})
        self.assertEqual(self.store.dump_tables(), before_tables)
        self.assertEqual(self.store.load_document("D"), before_doc)
        self.assertEqual(
            [v["version_no"] for v in self.store.list_versions("D")], [1]
        )

    def test_restore_reproduces_every_array_shape(self) -> None:
        shapes = [
            ("ABS", {}),
            ("NUL", {"section": None}),
            ("EMP", {"section": []}),
            ("HOLE", {"section": [None]}),
        ]
        for key, first_doc in shapes:
            self.store.import_document(key, first_doc)
            self.store.import_document(
                key, {"section": [{"fields": {"x": 1}}]}, replace=True
            )
            self.assertEqual(
                [v["version_no"] for v in self.store.list_versions(key)], [1, 2]
            )
            stats = self.store.restore_version(key, 1)
            self.assertEqual(stats["versions_added"], 1)
            self.assertEqual(
                self.store.load_document(key), {"key": key, **first_doc}
            )
            # The restored version 3 is byte-identical in content to version 1.
            self.assertEqual(
                self.store.load_version(key, 3)["document"],
                self.store.load_version(key, 1)["document"],
            )
        self.store.verify()


class MergeHistoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TempStore()
        self.store = self.tmp.create()
        self.store.import_document(
            "D",
            {"section": [{"fields": {"torque": 10}}, None, {"fields": {"r": 3}}]},
        )
        self.base = self.store.checkout_document("D")

    def tearDown(self) -> None:
        self.store.close()

    def test_successful_merge_appends_one_version(self) -> None:
        returned = json.loads(json.dumps(self.base))
        returned["section"][0]["slot"]["fields"]["torque"] = 12
        returned["section"].insert(
            1, {"id": "NEW-1", "slot": {"fields": {"r": 4}}}
        )
        stats = self.store.merge_document("D", self.base, self.base, returned)
        self.assertEqual(stats["versions_added"], 1)
        versions = self.store.list_versions("D")
        self.assertEqual([v["kind"] for v in versions], ["import", "merge"])
        # Version 1 still holds the pre-merge base document.
        self.assertEqual(
            self.store.load_version("D", 1)["document"],
            {"key": "D", **_plain(self.base)},
        )

    def test_stale_or_conflicting_merge_adds_no_version(self) -> None:
        returned = json.loads(json.dumps(self.base))
        returned["section"][0]["slot"]["fields"]["torque"] = 12
        # First merge succeeds.
        self.store.merge_document("D", self.base, self.base, returned)
        history_after_success = list(self.store.dump_tables()["device_version"])
        # Replaying it is stale: nothing may change, no third version.
        from inspection_store import StaleLocal

        with self.assertRaises(StaleLocal):
            self.store.merge_document("D", self.base, self.base, returned)
        self.assertEqual(
            [v["version_no"] for v in self.store.list_versions("D")], [1, 2]
        )
        # A content conflict likewise leaves history untouched.
        local = json.loads(json.dumps(self.base))
        local["section"][0]["slot"]["fields"]["torque"] = 99
        conflicted = json.loads(json.dumps(self.base))
        conflicted["section"][0]["slot"]["fields"]["torque"] = 12
        from inspection_store import MergeConflict

        with self.assertRaises(MergeConflict):
            self.store.merge_document("D", self.base, local, conflicted)
        self.assertEqual(
            [v["version_no"] for v in self.store.list_versions("D")], [1, 2]
        )
        self.assertEqual(
            self.store.dump_tables()["device_version"], history_after_success
        )

    def test_merge_that_changes_nothing_adds_no_version(self) -> None:
        # theirs == local == base: merged content equals the current record.
        stats = self.store.merge_document("D", self.base, self.base, self.base)
        self.assertEqual(stats["versions_added"], 0)
        self.assertEqual(
            [v["version_no"] for v in self.store.list_versions("D")], [1]
        )


def _plain(draft):
    return draft_to_plain_document(parse_draft(draft))


class LegacyUpgradeTests(unittest.TestCase):
    def build_legacy(self, path: str) -> None:
        """Create a genuine schema-v1 database (no device_version table)."""
        conn = sqlite3.connect(path)
        try:
            conn.executescript(_LEGACY_SCHEMA_V1)
            conn.execute(
                "INSERT INTO meta(name, value) VALUES ('schema_version', '1')"
            )
            conn.commit()
        finally:
            conn.close()

    def seed_as_v1(self, legacy_path: str) -> None:
        """Build data with the current code, then strip it back to v1 shape."""
        seed_path = legacy_path + ".seed"
        seed = DeviceStore(seed_path, create=True)
        try:
            seed.import_document(
                "OLD",
                {
                    "section": [
                        {"fields": {"v": 1}, "attachments": [{"a": 0}, None]},
                        None,
                    ]
                },
            )
            seed.import_document("EMPTY", {"section": []})
            seed.import_document("NULLD", {"section": None})
            seed.import_document("ABSD", {})
        finally:
            seed.close()
        source = sqlite3.connect(seed_path)
        target = sqlite3.connect(legacy_path)
        try:
            source.execute("DROP TABLE device_version")
            source.execute(
                "UPDATE meta SET value = '1' WHERE name = 'schema_version'"
            )
            source.commit()
            source.backup(target)
            target.commit()
        finally:
            target.close()
            source.close()
        os.remove(seed_path)

    def test_upgrade_baselines_every_device_and_keeps_data_usable(self) -> None:
        tmp = TempStore()
        self.build_legacy(tmp.path)
        self.seed_as_v1(tmp.path)

        store = DeviceStore(tmp.path)  # opening runs the migration
        try:
            self.assertEqual(
                store.conn.execute(
                    "SELECT value FROM meta WHERE name = 'schema_version'"
                ).fetchone()[0],
                "2",
            )
            for key in ("OLD", "EMPTY", "NULLD", "ABSD"):
                versions = store.list_versions(key)
                self.assertEqual(len(versions), 1)
                self.assertEqual(versions[0]["kind"], "baseline")
                self.assertEqual(versions[0]["version_no"], 1)
            # Original record still reads exactly as written.
            self.assertEqual(
                store.load_document("OLD")["section"],
                [
                    {"fields": {"v": 1}, "attachments": [{"a": 0}, None]},
                    None,
                ],
            )
            self.assertEqual(store.load_document("EMPTY")["section"], [])
            self.assertIsNone(store.load_document("NULLD")["section"])
            self.assertNotIn("section", store.load_document("ABSD"))
            # And it can still be edited, paged, exported and verified.
            first = store.page("OLD", 0, 1)[0]["slot_id"]
            store.apply_batch(
                "OLD", [Edit.insert_section("NEW", before=first, slot=None)]
            )
            self.assertEqual(
                [v["version_no"] for v in store.list_versions("OLD")], [1, 2]
            )
            self.assertEqual(
                store.load_version("OLD", 1)["document"]["section"][0],
                {"fields": {"v": 1}, "attachments": [{"a": 0}, None]},
            )
            store.verify()
        finally:
            store.close()

        # A second open is an ordinary v2 open: no duplicate baselines.
        again = DeviceStore(tmp.path)
        try:
            again.verify()
            self.assertEqual(
                [v["version_no"] for v in again.list_versions("OLD")], [1, 2]
            )
        finally:
            again.close()

    def test_upgrade_is_atomic(self) -> None:
        tmp = TempStore()
        self.build_legacy(tmp.path)
        self.seed_as_v1(tmp.path)
        marker_before = sqlite3.connect(tmp.path).execute(
            "SELECT value FROM meta WHERE name = 'schema_version'"
        ).fetchone()[0]
        self.assertEqual(marker_before, "1")
        # Corrupt the live rows so the baseline snapshot fails mid-migration.
        conn = sqlite3.connect(tmp.path)
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute(
            "UPDATE section_slot SET left_id = 'ghost-id' WHERE 1=1"
        )
        conn.commit()
        conn.close()
        with self.assertRaises(Exception):
            DeviceStore(tmp.path)
        conn = sqlite3.connect(tmp.path)
        try:
            # The whole upgrade rolled back: still v1, no history table.
            self.assertEqual(
                conn.execute(
                    "SELECT value FROM meta WHERE name = 'schema_version'"
                ).fetchone()[0],
                "1",
            )
            self.assertIsNone(
                conn.execute(
                    "SELECT name FROM sqlite_master WHERE name = 'device_version'"
                ).fetchone()
            )
        finally:
            conn.close()

    def test_empty_legacy_database_upgrades_then_imports_version_one(self) -> None:
        tmp = TempStore()
        self.build_legacy(tmp.path)
        store = DeviceStore(tmp.path)
        try:
            store.import_document("D", DOCUMENT)
            self.assertEqual(
                [v["kind"] for v in store.list_versions("D")], ["import"]
            )
            store.verify()
        finally:
            store.close()


class HistoryVerifyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TempStore()
        self.store = self.tmp.create()
        self.store.import_document("D", DOCUMENT)
        self.store.apply_batch(
            "D", [Edit.insert_section("S1", slot={"fields": {"n": 1}})]
        )

    def tearDown(self) -> None:
        self.store.close()

    def test_verify_detects_tampered_history(self) -> None:
        self.store.verify()
        # Delete a version: gapless sequence breaks and coverage shifts.
        self.store.conn.execute(
            "DELETE FROM device_version WHERE device_key = 'D' AND version_no = 1"
        )
        with self.assertRaises(IntegrityError):
            self.store.verify()

    def test_verify_detects_payload_hash_mismatch(self) -> None:
        self.store.conn.execute(
            "UPDATE device_version SET content_hash = 'deadbeef' "
            "WHERE device_key = 'D' AND version_no = 1"
        )
        with self.assertRaises(IntegrityError):
            self.store.verify()

    def test_verify_detects_live_record_drifting_from_newest_version(self) -> None:
        # A live edit performed outside the history machinery must be caught.
        self.store.conn.execute(
            "UPDATE section_slot SET fields_json = '{\"z\":1}' "
            "WHERE slot_id = ("
            "SELECT slot_id FROM section_slot WHERE is_hole = 0 LIMIT 1)"
        )
        with self.assertRaises(IntegrityError):
            self.store.verify()


if __name__ == "__main__":
    unittest.main()
