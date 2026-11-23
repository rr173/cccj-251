"""Shape tests: absent / null / empty / present are all preserved.

The critical distinction is ``[null]`` (a present array with one hole) versus
``[]`` (a present, empty array), and the same for attachment arrays.
"""

import unittest

from helpers import TempStore


class ShapeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TempStore()
        self.store = self.tmp.create()

    def tearDown(self) -> None:
        self.store.close()

    def test_section_absent(self) -> None:
        self.store.import_document("D", {})
        self.assertEqual(self.store.array_shape("D"), "absent")
        self.assertEqual(self.store.load_document("D"), {"key": "D"})
        # Absent is not an array, so it has no positions to page over.
        with self.assertRaises(Exception):
            self.store.page("D", 0, 5)

    def test_section_null(self) -> None:
        self.store.import_document("D", {"section": None})
        self.assertEqual(self.store.array_shape("D"), "null")
        self.assertEqual(
            self.store.load_document("D"), {"key": "D", "section": None}
        )

    def test_section_empty_vs_one_hole(self) -> None:
        self.store.import_document("EMPTY", {"section": []})
        self.store.import_document("HOLE", {"section": [None]})
        self.assertEqual(self.store.array_shape("EMPTY"), "empty")
        self.assertEqual(self.store.array_shape("HOLE"), "present")
        self.assertEqual(self.store.load_document("EMPTY")["section"], [])
        self.assertEqual(self.store.load_document("HOLE")["section"], [None])
        # A hole still has a stable identity and a position.
        hole = self.store.page("HOLE", 0, 1)[0]
        self.assertTrue(hole["is_hole"])
        view = self.store.slot_view(hole["slot_id"])
        self.assertEqual(view["position"], 0)
        self.assertEqual(view["kind"], "section")

    def test_attachment_shapes_are_per_slot(self) -> None:
        document = {
            "section": [
                {"fields": {"n": 0}},
                {"fields": {"n": 1}, "attachments": None},
                {"fields": {"n": 2}, "attachments": []},
                {"fields": {"n": 3}, "attachments": [None]},
                {"fields": {"n": 4}, "attachments": [{"a": 1}, None]},
            ]
        }
        self.store.import_document("D", document)
        rebuilt = self.store.load_document("D")
        self.assertEqual(rebuilt["section"], document["section"])
        page = self.store.page("D", 0, 10)
        shapes = [row["attachments"] for row in page]
        self.assertEqual(
            shapes, ["absent", "null", "empty", "present", "present"]
        )
        for row, expected in zip(page, shapes):
            if row["is_hole"]:
                continue
            self.assertEqual(
                self.store.array_shape("D", row["slot_id"]), expected
            )

    def test_hole_has_no_attachment_array(self) -> None:
        self.store.import_document("D", {"section": [None]})
        sid = self.store.page("D", 0, 1)[0]["slot_id"]
        with self.assertRaises(Exception):
            self.store.array_shape("D", sid)

    def test_hole_versus_empty_field_map(self) -> None:
        self.store.import_document(
            "D", {"section": [None, {"fields": {}}]}
        )
        section = self.store.load_document("D")["section"]
        self.assertIsNone(section[0])
        self.assertEqual(section[1], {"fields": {}})

    def test_scalars_only_and_bool_rejected(self) -> None:
        for bad in [True, 1.5, {"x": [1]}, None]:
            doc = {"section": [{"fields": {"bad": bad}}]}
            with self.assertRaises(Exception, msg=repr(bad)):
                self.store.import_document("D", doc)

    def test_no_arbitrary_depth(self) -> None:
        doc = {
            "section": [
                {
                    "fields": {},
                    "attachments": [{"fields": {"nested": 1}}],
                }
            ]
        }
        with self.assertRaises(Exception):
            self.store.import_document("D", doc)

    def test_duplicate_import_rejected_unless_replace(self) -> None:
        self.store.import_document("D", {"section": []})
        with self.assertRaises(Exception):
            self.store.import_document("D", {"section": [None]})
        self.store.import_document("D", {"section": [None]}, replace=True)
        self.assertEqual(self.store.load_document("D")["section"], [None])


if __name__ == "__main__":
    unittest.main()
