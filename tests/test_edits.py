"""Edit tests: positional semantics, identity stability, initialization rules,
cascade deletion and lookup failure.
"""

import unittest

from helpers import TempStore
from inspection_store import Edit


def ids(store, device, start=0, stop=100, section=None):
    return [
        row["slot_id"]
        for row in store.page(device, start, stop, section_id=section)
    ]


class EditTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TempStore()
        self.store = self.tmp.create()

    def tearDown(self) -> None:
        self.store.close()

    def doc(self, device="D"):
        return self.store.load_document(device)

    def test_insert_before_and_tail(self) -> None:
        self.store.import_document(
            "D", {"section": [{"fields": {"n": 0}}, {"fields": {"n": 2}}]}
        )
        section = ids(self.store, "D")
        self.store.apply_batch(
            "D",
            [
                Edit.insert_section("B1", before=section[1], slot={"fields": {"n": 1}}),
                Edit.insert_section("TAIL", before=None, slot={"fields": {"n": 3}}),
                Edit.insert_section("HEAD", before=section[0], slot=None),
            ],
        )
        self.assertEqual(
            self.doc()["section"],
            [
                None,
                {"fields": {"n": 0}},
                {"fields": {"n": 1}},
                {"fields": {"n": 2}},
                {"fields": {"n": 3}},
            ],
        )
        self.assertEqual(
            [self.store.slot_view(s)["position"] for s in ("HEAD", "B1", "TAIL")],
            [0, 2, 4],
        )

    def test_section_insert_moves_positions_but_not_attachment_identity(self) -> None:
        self.store.import_document(
            "D",
            {
                "section": [
                    {"fields": {"n": 0}, "attachments": [{"a": 0}, {"a": 1}]},
                    {"fields": {"n": 1}, "attachments": [{"b": 0}]},
                ]
            },
        )
        section = ids(self.store, "D")
        att_of_second = ids(self.store, "D", section=section[1])
        before = [self.store.slot_view(a) for a in att_of_second]
        self.store.apply_batch(
            "D", [Edit.insert_section("NEW", before=section[0], slot={"fields": {}})]
        )
        after = [self.store.slot_view(a) for a in att_of_second]
        for old, new in zip(before, after):
            self.assertEqual(old["slot_id"], new["slot_id"])
            self.assertEqual(old["owner_section_slot_id"], new["owner_section_slot_id"])
            self.assertEqual(old["position"], new["position"])  # array is independent
        # The owner section moved by one in the section array.
        self.assertEqual(self.store.slot_view(section[1])["position"], 2)

    def test_delete_section_cascades_and_lookup_fails(self) -> None:
        self.store.import_document(
            "D",
            {
                "section": [
                    {"fields": {"n": 0}, "attachments": [{"a": 0}, None]},
                    {"fields": {"n": 1}},
                ]
            },
        )
        section = ids(self.store, "D")
        attachments = ids(self.store, "D", section=section[0])
        self.assertEqual(self.store.counts()["attachment_slot"], 2)
        self.store.apply_batch("D", [Edit.delete_section(section[0])])
        self.assertEqual(self.store.counts()["attachment_slot"], 0)
        for aid in attachments:
            with self.assertRaises(Exception):
                self.store.slot_view(aid)
        with self.assertRaises(Exception):
            self.store.slot_view(section[0])
        self.assertEqual(self.doc()["section"], [{"fields": {"n": 1}}])
        # Positions after deletion reflect the new array only.
        self.assertEqual(self.store.slot_view(section[1])["position"], 0)

    def test_delete_last_slot_leaves_empty_not_absent(self) -> None:
        self.store.import_document("D", {"section": [{"fields": {}}]})
        sid = ids(self.store, "D")[0]
        self.store.apply_batch("D", [Edit.delete_section(sid)])
        self.assertEqual(self.store.array_shape("D"), "empty")
        self.assertEqual(self.doc()["section"], [])
        # And the empty array can be inserted into again.
        self.store.apply_batch(
            "D", [Edit.insert_section("N", before=None, slot={"fields": {}})]
        )
        self.assertEqual(self.store.array_shape("D"), "present")

    def test_delete_last_attachment_leaves_empty(self) -> None:
        self.store.import_document(
            "D", {"section": [{"fields": {}, "attachments": [{"a": 1}]}]}
        )
        section = ids(self.store, "D")[0]
        attachment = ids(self.store, "D", section=section)[0]
        self.store.apply_batch(
            "D", [Edit.delete_attachment(section, attachment)]
        )
        self.assertEqual(self.store.array_shape("D", section), "empty")
        self.assertEqual(
            self.doc()["section"], [{"fields": {}, "attachments": []}]
        )

    def test_init_required_for_absent_and_null(self) -> None:
        for value in ({}, {"section": None}):
            self.store.import_document("D", value, replace=True)
            with self.assertRaises(Exception):
                self.store.apply_batch(
                    "D",
                    [Edit.insert_section("S", before=None, slot={"fields": {}})],
                )
            self.store.apply_batch("D", [Edit.init_section_empty()])
            self.assertEqual(self.store.array_shape("D"), "empty")
            self.store.apply_batch(
                "D", [Edit.insert_section("S", before=None, slot={"fields": {}})]
            )
            self.assertEqual(self.store.array_shape("D"), "present")
            with self.assertRaises(Exception):
                self.store.apply_batch("D", [Edit.init_section_empty()])

    def test_attachments_init_required(self) -> None:
        self.store.import_document(
            "D", {"section": [{"fields": {}}, {"fields": {}, "attachments": None}]}
        )
        section = ids(self.store, "D")
        for index, sid in enumerate(section):
            aid = f"A{index}"
            with self.assertRaises(Exception):
                self.store.apply_batch(
                    "D", [Edit.insert_attachment(sid, aid, before=None, slot={"a": 1})]
                )
            self.store.apply_batch(
                "D",
                [Edit.init_attachments_empty(sid),
                 Edit.insert_attachment(sid, aid, before=None, slot={"a": 1})],
            )
            self.assertEqual(self.store.array_shape("D", sid), "present")
            with self.assertRaises(Exception):
                self.store.apply_batch("D", [Edit.init_attachments_empty(sid)])

    def test_attachment_insert_before_and_delete(self) -> None:
        self.store.import_document(
            "D", {"section": [{"fields": {}, "attachments": []}]}
        )
        sid = ids(self.store, "D")[0]
        self.store.apply_batch(
            "D",
            [
                Edit.insert_attachment(sid, "A1", slot={"v": 1}),
                Edit.insert_attachment(sid, "A3", slot={"v": 3}),
                Edit.insert_attachment(sid, "A2", before="A3", slot={"v": 2}),
            ],
        )
        self.assertEqual(
            self.doc()["section"][0]["attachments"],
            [{"v": 1}, {"v": 2}, {"v": 3}],
        )
        self.store.apply_batch("D", [Edit.delete_attachment(sid, "A2")])
        self.assertEqual(
            self.doc()["section"][0]["attachments"], [{"v": 1}, {"v": 3}]
        )
        with self.assertRaises(Exception):
            self.store.slot_view("A2")
        with self.assertRaises(Exception):
            self.store.apply_batch("D", [Edit.delete_attachment(sid, "A2")])

    def test_attachment_belongs_to_its_section_even_as_order_changes(self) -> None:
        self.store.import_document(
            "D",
            {
                "section": [
                    {"fields": {"n": 0}, "attachments": [{"a": 0}]},
                    {"fields": {"n": 1}, "attachments": [{"b": 0}, {"b": 1}]},
                ]
            },
        )
        section = ids(self.store, "D")
        att_second = ids(self.store, "D", section=section[1])
        # Move the second section to the front; attachments follow identity.
        self.store.apply_batch(
            "D", [Edit.insert_section("FIRST", before=section[0], slot=None)]
        )
        self.assertEqual(
            [self.store.slot_view(a)["owner_section_slot_id"] for a in att_second],
            [section[1], section[1]],
        )
        page = self.store.page("D", 0, 10, section_id=section[1])
        self.assertEqual([r["slot_id"] for r in page], att_second)
        # Cross-section references are rejected.
        with self.assertRaises(Exception):
            self.store.apply_batch(
                "D", [Edit.delete_attachment(section[0], att_second[0])]
            )
        with self.assertRaises(Exception):
            self.store.apply_batch(
                "D",
                [Edit.insert_attachment(section[0], "X", before=att_second[0],
                                        slot={"q": 1})],
            )

    def test_hole_does_not_accept_attachments(self) -> None:
        self.store.import_document("D", {"section": [None]})
        sid = ids(self.store, "D")[0]
        with self.assertRaises(Exception):
            self.store.apply_batch("D", [Edit.init_attachments_empty(sid)])

    def test_cannot_initialize_already_present_arrays(self) -> None:
        self.store.import_document(
            "D", {"section": [{"fields": {}, "attachments": [{"a": 1}]}]}
        )
        with self.assertRaises(Exception):
            self.store.apply_batch("D", [Edit.init_section_empty()])
        sid = ids(self.store, "D")[0]
        with self.assertRaises(Exception):
            self.store.apply_batch("D", [Edit.init_attachments_empty(sid)])

    def test_insert_with_nonempty_attachment_array_rejected(self) -> None:
        self.store.import_document("D", {"section": []})
        with self.assertRaises(Exception):
            self.store.apply_batch(
                "D",
                [
                    Edit.insert_section(
                        "S", slot={"fields": {}, "attachments": [{"a": 1}]}
                    )
                ],
            )


if __name__ == "__main__":
    unittest.main()
