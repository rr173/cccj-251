"""Atomicity and cross-process visibility tests."""

import os
import subprocess
import sys
import textwrap
import unittest

from helpers import ROOT, TempStore
from inspection_store import BatchAborted, DeviceStore, Edit


class BatchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TempStore()
        self.store = self.tmp.create()

    def tearDown(self) -> None:
        self.store.close()

    def test_bad_batch_is_invisible_after_reopen(self) -> None:
        self.store.import_document(
            "D",
            {"section": [{"fields": {"n": 0}, "attachments": [{"a": 0}]}]},
        )
        section = self.store.page("D", 0, 10)[0]["slot_id"]
        attachment = self.store.page(
            "D", 0, 10, section_id=section
        )[0]["slot_id"]
        before_tables = self.store.dump_tables()
        before_doc = self.store.load_document("D")
        before_counts = self.store.counts()

        cases = [
            # delete the same slot twice
            [Edit.delete_section(section), Edit.delete_section(section)],
            # duplicate identity
            [
                Edit.insert_section("DUP", slot=None),
                Edit.insert_section("DUP", slot=None),
            ],
            # bad reference
            [Edit.insert_section("X", before="missing-id", slot=None)],
            # attachment of another section
            [Edit.delete_attachment("no-such-section", attachment)],
            # delete an attachment that does not exist
            [Edit.delete_attachment(section, "missing-attachment")],
        ]
        for edits in cases:
            with self.assertRaises(BatchAborted):
                self.store.apply_batch("D", edits)
            self.assertEqual(self.store.dump_tables(), before_tables)
            self.assertEqual(self.store.load_document("D"), before_doc)
            self.assertEqual(self.store.counts(), before_counts)

    def test_batch_is_atomic_across_arrays(self) -> None:
        self.store.import_document(
            "D", {"section": [{"fields": {"n": 0}}]}
        )
        section = self.store.page("D", 0, 1)[0]["slot_id"]
        # The second edit is invalid, so the first (attachment init) must not
        # survive either.
        with self.assertRaises(BatchAborted):
            self.store.apply_batch(
                "D",
                [
                    Edit.init_attachments_empty(section),
                    Edit.insert_attachment(section, "A", slot={"x": 1}),
                    Edit.delete_section("does-not-exist"),
                ],
            )
        self.assertEqual(self.store.array_shape("D", section), "absent")

    def test_successful_batch_visible_to_new_process(self) -> None:
        self.store.import_document(
            "D", {"section": [{"fields": {"n": 0}}]}
        )
        self.store.apply_batch(
            "D",
            [
                Edit.insert_section("S1", before=None, slot={"fields": {"n": 1}}),
                Edit.insert_section("S2", before="S1", slot=None),
            ],
        )
        script = textwrap.dedent(
            f"""
            import json, sys
            sys.path.insert(0, {ROOT!r})
            from inspection_store import DeviceStore
            store = DeviceStore({self.tmp.path!r})
            print(json.dumps(store.load_document("D"), sort_keys=True))
            print(json.dumps(store.slot_view("S1"), sort_keys=True))
            store.close()
            """
        )
        result = subprocess.run(
            [sys.executable, "-I", "-c", script],
            capture_output=True,
            text=True,
            cwd=self.tmp.directory,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        lines = result.stdout.strip().splitlines()
        import json

        document = json.loads(lines[0])
        self.assertEqual(
            document["section"],
            [{"fields": {"n": 0}}, None, {"fields": {"n": 1}}],
        )
        view = json.loads(lines[1])
        self.assertEqual(view["position"], 2)

    def test_reader_sees_committed_state_while_writer_closed(self) -> None:
        self.store.import_document("D", {"section": [None]})
        self.store.apply_batch("D", [Edit.insert_section("T", slot=None)])
        other = DeviceStore(self.tmp.path)
        try:
            self.assertEqual(other.load_document("D")["section"], [None, None])
        finally:
            other.close()


class StoreFileTests(unittest.TestCase):
    def test_missing_store_is_rejected(self) -> None:
        tmp = TempStore()
        with self.assertRaises(Exception):
            DeviceStore(tmp.path)

    def test_non_store_file_is_rejected(self) -> None:
        tmp = TempStore()
        with open(tmp.path, "w") as handle:
            handle.write("not sqlite")
        with self.assertRaises(Exception):
            DeviceStore(tmp.path)

    def test_no_history_tables(self) -> None:
        tmp = TempStore()
        store = tmp.create()
        try:
            names = {
                row[0]
                for row in store.conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
            self.assertEqual(names, {"meta", "device", "section_slot", "attachment_slot"})
            columns = [
                row[1]
                for row in store.conn.execute("PRAGMA table_info(section_slot)")
            ]
            self.assertNotIn("position", columns)
            self.assertNotIn("pos", columns)
        finally:
            store.close()


if __name__ == "__main__":
    unittest.main()
