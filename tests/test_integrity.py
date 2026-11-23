"""Integrity and corruption tests."""

import unittest

from helpers import TempStore
from inspection_store import IntegrityError


class IntegrityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TempStore()
        self.store = self.tmp.create()
        self.store.import_document(
            "D",
            {
                "section": [
                    {"fields": {"i": 0}, "attachments": [{"a": 0}, None]},
                    {"fields": {"i": 1}},
                    None,
                ]
            },
        )

    def tearDown(self) -> None:
        self.store.close()

    def test_verify_accepts_valid_store(self) -> None:
        report = self.store.verify()
        self.assertEqual(report["devices"], 1)
        self.assertEqual(report["section_arrays"], 1)
        self.assertEqual(report["attachment_arrays"], 1)

    def test_cycle_is_detected_and_does_not_hang(self) -> None:
        ids = [r["slot_id"] for r in self.store.page("D", 0, 3)]
        self.store.conn.execute(
            "UPDATE section_slot SET parent_id = ? WHERE slot_id = ?",
            (ids[1], ids[1]),
        )
        # Any parent-chain walk terminates with an error instead of looping.
        with self.assertRaises(IntegrityError):
            self.store.slot_view(ids[1])
        with self.assertRaises(IntegrityError):
            self.store.verify()

    def test_dangling_child_is_detected(self) -> None:
        ids = [r["slot_id"] for r in self.store.page("D", 0, 3)]
        # The schema's foreign keys reject a dangling id outright; with them
        # off the verifier must still catch it.
        self.store.conn.execute("PRAGMA foreign_keys = OFF")
        self.store.conn.execute(
            "UPDATE section_slot SET left_id = 'ghost' WHERE slot_id = ?",
            (ids[0],),
        )
        with self.assertRaises(IntegrityError):
            self.store.verify()

    def test_wrong_subtree_size_is_detected(self) -> None:
        ids = [r["slot_id"] for r in self.store.page("D", 0, 3)]
        self.store.conn.execute(
            "UPDATE section_slot SET subtree_size = 99 WHERE slot_id = ?",
            (ids[0],),
        )
        with self.assertRaises(IntegrityError):
            self.store.verify()

    def test_attachment_bound_to_wrong_section_is_detected(self) -> None:
        ids = [r["slot_id"] for r in self.store.page("D", 0, 3)]
        self.store.conn.execute(
            "UPDATE attachment_slot SET section_slot_id = ? WHERE section_slot_id != ?",
            (ids[1], ids[1]),
        )
        with self.assertRaises(IntegrityError):
            self.store.verify()


if __name__ == "__main__":
    unittest.main()
