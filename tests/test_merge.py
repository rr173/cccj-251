"""Three-way offline merge tests: planning rules, CAS write-back, CLI."""

import json
import os
import subprocess
import sys
import threading
import time
import unittest

from helpers import ROOT, TempStore
from inspection_store import (
    DeviceStore,
    Edit,
    MergeConflict,
    StaleLocal,
    ValidationError,
)
from inspection_store.export import export_csv
from inspection_store.merge import draft_to_json, merge_drafts, parse_draft

ABSENT = object()  # builder sentinel: omit the key entirely


def slot(sid, fields=None, attachments=ABSENT, hole=False):
    if hole:
        return {"id": sid, "slot": None}
    payload = {"fields": dict(fields or {})}
    if attachments is not ABSENT:
        payload["attachments"] = attachments
    return {"id": sid, "slot": payload}


def att(aid, fields=None, hole=False):
    if hole:
        return {"id": aid, "slot": None}
    return {"id": aid, "slot": dict(fields or {})}


def draft(key, *elements, section=True):
    doc = {"key": key}
    if section is True:
        doc["section"] = list(elements)
    elif section is not False:
        doc["section"] = section
    return doc


def ids_of(draft_doc):
    return [element["id"] for element in draft_doc["section"]]


def plan(base, local, theirs):
    return draft_to_json(
        merge_drafts(
            parse_draft(base, "base"),
            parse_draft(local, "local"),
            parse_draft(theirs, "theirs"),
        )
    )


class MergePlanTests(unittest.TestCase):
    """Pure planning: no store involved."""

    def test_field_merge_one_side_and_both_sides(self):
        base = draft("D", slot("S1", {"a": 1, "b": 2}))
        local = draft("D", slot("S1", {"a": 10, "b": 2}))
        theirs = draft("D", slot("S1", {"a": 1, "b": 20, "c": 3}))
        merged = plan(base, local, theirs)
        self.assertEqual(
            merged["section"][0]["slot"]["fields"], {"a": 10, "b": 20, "c": 3}
        )

    def test_field_both_same_change_is_taken(self):
        base = draft("D", slot("S1", {"a": 1}))
        both = draft("D", slot("S1", {"a": 9}))
        merged = plan(base, both, draft("D", slot("S1", {"a": 9})))
        self.assertEqual(merged["section"][0]["slot"]["fields"], {"a": 9})

    def test_field_conflict_reports_path(self):
        base = draft("D", slot("S1", {"a": 1}))
        local = draft("D", slot("S1", {"a": 2}))
        theirs = draft("D", slot("S1", {"a": 3}))
        with self.assertRaises(MergeConflict) as ctx:
            plan(base, local, theirs)
        message = str(ctx.exception)
        self.assertIn("S1", message)
        self.assertIn("'a'", message)

    def test_field_delete_vs_modify_conflicts(self):
        base = draft("D", slot("S1", {"a": 1, "b": 2}))
        local = draft("D", slot("S1", {"b": 2}))  # deleted a
        theirs = draft("D", slot("S1", {"a": 5, "b": 2}))  # modified a
        with self.assertRaises(MergeConflict):
            plan(base, local, theirs)
        # Deletion against an untouched other side is accepted.
        merged = plan(base, local, draft("D", slot("S1", {"a": 1, "b": 2})))
        self.assertEqual(merged["section"][0]["slot"]["fields"], {"b": 2})

    def test_insert_order_and_shared_anchor(self):
        base = draft("D", slot("S1"), slot("S2"), slot("S3"))
        local = draft("D", slot("L0"), slot("S1"), slot("L1"), slot("S2"), slot("S3"))
        theirs = draft("D", slot("S1"), slot("T1"), slot("S2"), slot("S3"), slot("T2"))
        merged = plan(base, local, theirs)
        # Local newcomers come first at the start and at the shared anchor.
        self.assertEqual(
            ids_of(merged), ["L0", "S1", "L1", "T1", "S2", "S3", "T2"]
        )

    def test_anchor_skips_slots_deleted_by_the_other_side(self):
        base = draft("D", slot("S1"), slot("S2"), slot("S3"))
        local = draft("D", slot("S1"), slot("S3"))  # S2 deleted
        theirs = draft("D", slot("S1"), slot("S2"), slot("T1"), slot("S3"))
        merged = plan(base, local, theirs)
        # T1 followed S2; S2 is gone, so T1 follows the surviving S1.
        self.assertEqual(ids_of(merged), ["S1", "T1", "S3"])

    def test_newcomer_survives_when_all_neighbours_are_deleted(self):
        base = draft("D", slot("S1"), slot("S2"))
        local = draft("D")  # both deleted
        theirs = draft("D", slot("S1"), slot("S2"), slot("T1"))
        merged = plan(base, local, theirs)
        self.assertEqual(ids_of(merged), ["T1"])

    def test_delete_section_vs_subordinate_change(self):
        base = draft(
            "D",
            slot("S1", {"n": 1}, attachments=[att("A1", {"p": 1})]),
            slot("S2"),
        )
        local = draft("D", slot("S2"))  # S1 deleted
        # (a) theirs changed a field of S1
        with self.assertRaises(MergeConflict):
            plan(base, local, draft("D", slot("S1", {"n": 2},
                                             attachments=[att("A1", {"p": 1})]),
                                      slot("S2")))
        # (b) theirs added an attachment under S1
        with self.assertRaises(MergeConflict):
            plan(base, local, draft("D", slot("S1", {"n": 1},
                                             attachments=[att("A1", {"p": 1}),
                                                          att("A9")]),
                                      slot("S2")))
        # (c) theirs deleted an attachment of S1
        with self.assertRaises(MergeConflict):
            plan(base, local, draft("D", slot("S1", {"n": 1}, attachments=[]),
                                      slot("S2")))
        # (d) theirs changed an attachment field of S1
        with self.assertRaises(MergeConflict):
            plan(base, local, draft("D", slot("S1", {"n": 1},
                                             attachments=[att("A1", {"p": 9})]),
                                      slot("S2")))
        # (e) theirs only inserted a neighbour section: no conflict.
        merged = plan(base, local, draft("D", slot("T1"),
                                         slot("S1", {"n": 1},
                                              attachments=[att("A1", {"p": 1})]),
                                         slot("S2")))
        self.assertEqual(ids_of(merged), ["T1", "S2"])

    def test_both_delete_and_empty_array_stays_empty(self):
        base = draft("D", slot("S1"))
        gone = draft("D")
        merged = plan(base, gone, draft("D"))
        self.assertEqual(merged["section"], [])

    def test_attachment_arrays_merge_by_slot(self):
        base = draft("D", slot("S1", attachments=[att("A1"), att("A2", {"x": 1})]))
        local = draft(
            "D", slot("S1", attachments=[att("A1"), att("LA"), att("A2", {"x": 1})])
        )
        theirs = draft(
            "D", slot("S1", attachments=[att("A1"), att("TA"), att("A2", {"x": 2})])
        )
        merged = plan(base, local, theirs)
        atts = merged["section"][0]["slot"]["attachments"]
        self.assertEqual([a["id"] for a in atts], ["A1", "LA", "TA", "A2"])
        self.assertEqual(atts[-1]["slot"], {"x": 2})

    def test_attachment_delete_vs_field_change_conflicts(self):
        base = draft("D", slot("S1", attachments=[att("A1", {"x": 1})]))
        local = draft("D", slot("S1", attachments=[]))  # A1 deleted
        theirs = draft("D", slot("S1", attachments=[att("A1", {"x": 2})]))
        with self.assertRaises(MergeConflict):
            plan(base, local, theirs)

    def test_shapes_from_non_array_base(self):
        # One side builds an array from absent; the other side did nothing.
        merged = plan(draft("D", section=False), draft("D", slot("L1")),
                      draft("D", section=False))
        self.assertEqual(ids_of(merged), ["L1"])
        # Both sides build arrays from null: all slots are new, local first.
        merged = plan(draft("D", section=None), draft("D", slot("L1")),
                      draft("D", slot("T1")))
        self.assertEqual(ids_of(merged), ["L1", "T1"])
        # One side makes it an empty array, the other leaves it null.
        merged = plan(draft("D", section=None), draft("D"),
                      draft("D", section=None))
        self.assertEqual(merged["section"], [])
        # Both changed a non-array differently and not both are arrays.
        with self.assertRaises(MergeConflict):
            plan(draft("D", section=None), draft("D"), draft("D", section=False))

    def test_array_nulled_by_one_side_is_adopted_if_other_untouched(self):
        base = draft("D", slot("S1"))
        merged = plan(base, draft("D", slot("S1")), draft("D", section=None))
        self.assertIsNone(merged["section"])
        # But if both changed and only one is an array, it conflicts.
        with self.assertRaises(MergeConflict):
            plan(base, draft("D", slot("S1", {"n": 1})),
                 draft("D", section=None))

    def test_same_new_id_on_both_sides_is_a_conflict(self):
        base = draft("D", slot("S1"))
        local = draft("D", slot("S1"), slot("X", {"v": 1}))
        theirs = draft("D", slot("S1"), slot("X", {"v": 1}))
        with self.assertRaises(MergeConflict) as ctx:
            plan(base, local, theirs)
        self.assertIn("'X'", str(ctx.exception))

    def test_reorder_of_base_slots_is_invalid(self):
        base = draft("D", slot("S1"), slot("S2"))
        local = draft("D", slot("S2"), slot("S1"))
        with self.assertRaises(ValidationError) as ctx:
            plan(base, local, draft("D", slot("S1"), slot("S2")))
        self.assertIn("reordered", str(ctx.exception))

    def test_hole_identity_change_is_invalid(self):
        base = draft("D", slot("S1", hole=True))
        local = draft("D", slot("S1", {"n": 1}))
        with self.assertRaises(ValidationError) as ctx:
            plan(base, local, base)
        self.assertIn("hole identity", str(ctx.exception))

    def test_cross_array_move_is_invalid(self):
        base = draft(
            "D",
            slot("S1", attachments=[att("A1")]),
            slot("S2", attachments=[]),
        )
        local = draft(
            "D",
            slot("S1", attachments=[]),
            slot("S2", attachments=[att("A1")]),
        )
        with self.assertRaises(ValidationError) as ctx:
            plan(base, local, base)
        self.assertIn("moved", str(ctx.exception))

    def test_base_section_id_used_as_attachment_is_invalid(self):
        base = draft("D", slot("S1"), slot("S2", attachments=[]))
        local = draft("D", slot("S2", attachments=[att("S1")]))
        with self.assertRaises(ValidationError):
            plan(base, local, base)

    def test_key_mismatch_is_invalid(self):
        with self.assertRaises(ValidationError):
            plan(draft("D", slot("S1")), draft("E", slot("S1")),
                 draft("D", slot("S1")))

    def test_planning_is_linear_for_tens_of_thousands_of_slots(self):
        count = 50000
        base = draft("D", *[slot(f"S{i}", {"i": i}) for i in range(count)])
        # Local deletes every third slot and inserts after every sixth.
        local_elements = []
        for i in range(count):
            if i % 3 == 0:
                continue
            local_elements.append(slot(f"S{i}", {"i": i}))
            if i % 6 == 1:
                local_elements.append(slot(f"L{i}"))
        # Theirs retouches every slot local keeps at i % 3 == 1 and appends.
        theirs_elements = []
        for i in range(count):
            if i % 3 == 1:
                theirs_elements.append(slot(f"S{i}", {"i": i, "t": 1}))
            else:
                theirs_elements.append(slot(f"S{i}", {"i": i}))
        theirs_elements.append(slot("T-tail"))
        start = time.monotonic()
        merged = plan(base, draft("D", *local_elements),
                      draft("D", *theirs_elements))
        elapsed = time.monotonic() - start
        expected_survivors = count - (count + 2) // 3
        expected_new = len([i for i in range(count) if i % 6 == 1 and i % 3 != 0]) + 1
        self.assertEqual(len(merged["section"]), expected_survivors + expected_new)
        self.assertEqual(merged["section"][0]["id"], "S1")
        self.assertEqual(merged["section"][-1]["id"], "T-tail")
        # O(n) planning takes well under a second per 10k slots; a quadratic
        # plan over 50k slots would take minutes.
        self.assertLess(elapsed, 20)


class ParseTests(unittest.TestCase):
    def test_duplicate_id_rejected(self):
        bad = draft("D", slot("S1"), slot("S1"))
        with self.assertRaises(ValidationError) as ctx:
            parse_draft(bad)
        self.assertIn("duplicate", str(ctx.exception))

    def test_duplicate_id_across_levels_rejected(self):
        bad = draft("D", slot("S1", attachments=[att("S1")]))
        with self.assertRaises(ValidationError):
            parse_draft(bad)

    def test_unknown_keys_rejected(self):
        with self.assertRaises(ValidationError):
            parse_draft({"key": "D", "section": [], "extra": 1})
        with self.assertRaises(ValidationError):
            parse_draft({"key": "D", "section": [{"id": "S", "slot": None, "x": 1}]})

    def test_bad_scalars_rejected(self):
        for bad_value in (True, 1.5, [1], {"nested": 1}):
            bad = draft("D", slot("S1", {"bad": bad_value}))
            with self.assertRaises(ValidationError, msg=repr(bad_value)):
                parse_draft(bad)

    def test_missing_id_or_slot_rejected(self):
        with self.assertRaises(ValidationError):
            parse_draft({"key": "D", "section": [{"slot": None}]})
        with self.assertRaises(ValidationError):
            parse_draft({"key": "D", "section": [{"id": "S"}]})

    def test_round_trip_is_canonical(self):
        doc = draft(
            "D",
            slot("S1", hole=True),
            slot("S2", {"n": 1}),
            slot("S3", {"n": 2}, attachments=None),
            slot("S4", attachments=[]),
            slot("S5", attachments=[att("A1", {"p": 1}), att("A2", hole=True)]),
        )
        self.assertEqual(draft_to_json(parse_draft(doc)), doc)


def run_cli(*args, stdin=None):
    bootstrap = (
        "import sys; sys.path.insert(0, {!r}); "
        "from inspection_store.cli import main; raise SystemExit(main())"
    ).format(ROOT)
    return subprocess.run(
        [sys.executable, "-I", "-c", bootstrap, *args],
        input=stdin,
        capture_output=True,
        text=True,
        cwd=ROOT,
    )


class MergeStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = TempStore()
        self.store = self.tmp.create()
        self._id_counters = {"section": 0, "attachment": 0}

    def tearDown(self):
        self.store.close()

    def import_with_ids(self, key, document):
        counters = self._id_counters

        def factory(kind, ordinal):
            counters[kind] += 1
            return ("S" if kind == "section" else "A") + str(counters[kind])

        self.store.import_document(key, document, id_factory=factory)
        return self.store.checkout_document(key)

    def test_checkout_preserves_shapes_and_identities(self):
        document = {
            "section": [
                {"fields": {"n": 0}, "attachments": [{"a": 1}, None]},
                None,
                {"fields": {"n": 2}},
                {"fields": {"n": 3}, "attachments": None},
                {"fields": {"n": 4}, "attachments": []},
            ]
        }
        checked_out = self.import_with_ids("D", document)
        self.assertEqual(checked_out["key"], "D")
        elements = checked_out["section"]
        self.assertEqual([e["id"] for e in elements], ["S1", "S2", "S3", "S4", "S5"])
        # Hole identity and the four array shapes survive the round trip.
        self.assertIsNone(elements[1]["slot"])
        self.assertNotIn("attachments", elements[2]["slot"])
        self.assertIsNone(elements[3]["slot"]["attachments"])
        self.assertEqual(elements[4]["slot"]["attachments"], [])
        first_atts = elements[0]["slot"]["attachments"]
        self.assertEqual([a["id"] for a in first_atts], ["A1", "A2"])
        self.assertEqual(first_atts[0]["slot"], {"a": 1})
        self.assertIsNone(first_atts[1]["slot"])
        # The checkout parses back into the identical normalized draft.
        self.assertEqual(draft_to_json(parse_draft(checked_out)), checked_out)
        # [] and [null] stay distinct.
        self.import_with_ids("EMPTY", {"section": []})
        self.import_with_ids("HOLE", {"section": [None]})
        self.assertEqual(self.store.checkout_document("EMPTY")["section"], [])
        self.assertEqual(
            len(self.store.checkout_document("HOLE")["section"]), 1
        )

    def test_checkout_reads_one_consistent_generation(self):
        self.import_with_ids("D", {"section": [{"fields": {"n": 0}}]})
        errors = []

        def writer():
            try:
                other = DeviceStore(self.tmp.path)
                try:
                    for i in range(1, 40):
                        other.apply_batch(
                            "D", [Edit.insert_section(f"W{i}", before=None, slot=None)]
                        )
                finally:
                    other.close()
            except Exception as exc:  # pragma: no cover - failure path
                errors.append(exc)

        thread = threading.Thread(target=writer)
        thread.start()
        try:
            for _ in range(40):
                checked_out = self.store.checkout_document("D")
                ids = ids_of(checked_out)
                # A consistent snapshot is S1 followed by a prefix W1..Wk.
                self.assertEqual(ids[0], "S1")
                self.assertEqual(ids[1:], [f"W{i}" for i in range(1, len(ids))])
        finally:
            thread.join(timeout=60)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])

    def test_merge_writeback_keeps_identity_and_order(self):
        base = self.import_with_ids(
            "D",
            {
                "section": [
                    {"fields": {"n": 1}, "attachments": [{"p": 1}]},
                    None,
                    {"fields": {"r": 3}},
                ]
            },
        )
        local = base  # the local crew changed nothing
        theirs = json.loads(json.dumps(base))
        theirs["section"][0]["slot"]["fields"]["n"] = 2
        theirs["section"].insert(
            1,
            {
                "id": "T1",
                "slot": {
                    "fields": {"new": 1},
                    "attachments": [{"id": "TA1", "slot": {"photo": "front"}}],
                },
            },
        )
        del theirs["section"][2]  # theirs deleted the hole S2
        stats = self.store.merge_document("D", base, local, theirs)
        self.assertEqual(stats["section_slots"], 3)
        self.assertEqual(stats["attachment_slots"], 2)

        # Existing queries show the merged shape, identities and order.
        document = self.store.load_document("D")
        self.assertEqual(
            document["section"],
            [
                {"fields": {"n": 2}, "attachments": [{"p": 1}]},
                {"fields": {"new": 1}, "attachments": [{"photo": "front"}]},
                {"fields": {"r": 3}},
            ],
        )
        self.assertEqual(self.store.slot_view("S1")["position"], 0)
        self.assertEqual(self.store.slot_view("T1")["position"], 1)
        self.assertEqual(self.store.slot_view("S3")["position"], 2)
        self.assertEqual(self.store.slot_view("TA1")["owner_section_slot_id"], "T1")
        self.assertEqual(self.store.slot_view("A1")["owner_section_slot_id"], "S1")
        self.assertEqual(self.store.array_shape("D"), "present")
        self.store.verify()

        # The relation export shows the same final order.
        out = os.path.join(self.tmp.directory, "csv")
        paths = export_csv(self.store, out, ["D"])
        import csv

        with open(paths["section_slot"], newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(
            [(r["section_slot_id"], r["position"]) for r in rows],
            [("S1", "0"), ("T1", "1"), ("S3", "2")],
        )

    def test_merge_stale_local_is_rejected_and_nothing_changes(self):
        base = self.import_with_ids(
            "D", {"section": [{"fields": {"n": 1}}, None]}
        )
        theirs = json.loads(json.dumps(base))
        theirs["section"][0]["slot"]["fields"]["n"] = 2
        # Another process records more after the local draft was taken.
        self.store.apply_batch(
            "D", [Edit.insert_section("L1", before="S2", slot=None)]
        )
        with self.assertRaises(StaleLocal) as ctx:
            self.store.merge_document("D", base, base, theirs)
        self.assertIn("section[1]", str(ctx.exception))
        # The later addition is still there; the merge did not happen.
        self.assertEqual(
            self.store.load_document("D")["section"],
            [{"fields": {"n": 1}}, None, None],
        )
        self.assertEqual(ids_of(self.store.checkout_document("D")),
                         ["S1", "L1", "S2"])

    def test_merge_twice_is_stale(self):
        base = self.import_with_ids("D", {"section": [{"fields": {"n": 1}}]})
        theirs = json.loads(json.dumps(base))
        theirs["section"].append({"id": "T1", "slot": None})
        self.store.merge_document("D", base, base, theirs)
        with self.assertRaises(StaleLocal):
            self.store.merge_document("D", base, base, theirs)

    def test_conflict_leaves_store_untouched(self):
        # Base: S1 carries one attachment A1.  Local deletes A1 (expressed
        # as a real store edit so the CAS can pass); theirs modifies A1.
        base = self.import_with_ids(
            "D", {"section": [{"fields": {}, "attachments": [{"x": 1}]}]}
        )
        self.store.apply_batch("D", [Edit.delete_attachment("S1", "A1")])
        local = self.store.checkout_document("D")
        theirs = json.loads(json.dumps(base))
        theirs["section"][0]["slot"]["attachments"][0]["slot"]["x"] = 2
        before = self.store.dump_tables()
        with self.assertRaises(MergeConflict):
            self.store.merge_document("D", base, local, theirs)
        self.assertEqual(self.store.dump_tables(), before)

    def test_invalid_draft_leaves_store_untouched(self):
        base = self.import_with_ids(
            "D", {"section": [{"fields": {}}, {"fields": {}}]}
        )
        theirs = json.loads(json.dumps(base))
        theirs["section"] = list(reversed(theirs["section"]))  # reorder
        before = self.store.dump_tables()
        with self.assertRaises(ValidationError):
            self.store.merge_document("D", base, base, theirs)
        self.assertEqual(self.store.dump_tables(), before)

    def test_merged_id_owned_by_another_device_conflicts(self):
        base = self.import_with_ids("D", {"section": [{"fields": {}}]})
        self.store.import_document(
            "OTHER", {"section": [None]},
            id_factory=lambda kind, ordinal: "CLASH",
        )
        theirs = json.loads(json.dumps(base))
        theirs["section"].append({"id": "CLASH", "slot": None})
        before = self.store.dump_tables()
        with self.assertRaises(MergeConflict) as ctx:
            self.store.merge_document("D", base, base, theirs)
        self.assertIn("CLASH", str(ctx.exception))
        self.assertIn("OTHER", str(ctx.exception))
        self.assertEqual(self.store.dump_tables(), before)

    def test_merge_touches_only_the_target_device(self):
        base = self.import_with_ids("D", {"section": [{"fields": {"n": 1}}]})
        self.import_with_ids("D2", {"section": [{"fields": {"x": 9}}, None]})
        theirs = json.loads(json.dumps(base))
        theirs["section"].append({"id": "T1", "slot": {"fields": {"n": 2}}})
        before = [
            r for r in self.store.dump_tables()["section_slot"] if r[1] == "D2"
        ]
        self.store.merge_document("D", base, base, theirs)
        after = [
            r for r in self.store.dump_tables()["section_slot"] if r[1] == "D2"
        ]
        self.assertEqual(before, after)
        self.assertEqual(self.store.load_document("D2")["section"],
                         [{"fields": {"x": 9}}, None])

    def test_merge_result_can_be_empty_array(self):
        base = self.import_with_ids("D", {"section": [{"fields": {}}]})
        local = json.loads(json.dumps(base))
        local["section"] = []  # local deleted the only slot
        self.store.apply_batch("D", [Edit.delete_section("S1")])
        stats = self.store.merge_document("D", base, local, base)
        self.assertEqual(stats["section_slots"], 0)
        self.assertEqual(self.store.array_shape("D"), "empty")
        self.assertEqual(self.store.load_document("D")["section"], [])
        self.store.verify()

    def test_merge_from_absent_section(self):
        base = self.import_with_ids("D", {})
        theirs = json.loads(json.dumps(base))
        theirs["section"] = [
            {"id": "T1", "slot": {"fields": {"n": 1}}},
            {"id": "T2", "slot": None},
        ]
        self.store.merge_document("D", base, base, theirs)
        self.assertEqual(
            self.store.load_document("D")["section"],
            [{"fields": {"n": 1}}, None],
        )
        self.store.verify()

    def test_merge_scales_to_large_drafts(self):
        count = 20000
        base = self.import_with_ids(
            "D", {"section": [{"fields": {"i": i}} for i in range(count)]}
        )
        # Local deletes every fifth slot; theirs appends one at the tail.
        local = json.loads(json.dumps(base))
        local["section"] = [
            e for i, e in enumerate(local["section"]) if i % 5 != 0
        ]
        theirs = json.loads(json.dumps(base))
        theirs["section"].append({"id": "T-tail", "slot": None})
        # Apply the local deletions to the store so the CAS passes.
        edits = [
            Edit.delete_section(f"S{i + 1}") for i in range(count) if i % 5 == 0
        ]
        self.store.apply_batch("D", edits)
        start = time.monotonic()
        stats = self.store.merge_document("D", base, local, theirs)
        elapsed = time.monotonic() - start
        self.assertEqual(stats["section_slots"], count - count // 5 + 1)
        page = self.store.page("D", count - count // 5, count - count // 5 + 1)
        self.assertEqual(page[0]["slot_id"], "T-tail")
        self.assertLess(elapsed, 30)
        self.store.verify()


class MergeCliTests(unittest.TestCase):
    def setUp(self):
        self.tmp = TempStore()
        self.store = self.tmp.path
        result = run_cli("--store", self.store, "init")
        self.assertEqual(result.returncode, 0, result.stderr)

    def write(self, name, payload):
        path = os.path.join(self.tmp.directory, name)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False)
        return path

    def checkout(self, name="base.json"):
        path = os.path.join(self.tmp.directory, name)
        result = run_cli("--store", self.store, "checkout", "D1", "-o", path)
        self.assertEqual(result.returncode, 0, result.stderr)
        with open(path, encoding="utf-8") as handle:
            return json.load(handle), path

    def test_checkout_merge_flow_and_exit_codes(self):
        document = self.write(
            "doc.json",
            {"section": [{"fields": {"n": 1}, "attachments": [{"a": 1}]}, None]},
        )
        result = run_cli("--store", self.store, "import", "D1", document)
        self.assertEqual(result.returncode, 0, result.stderr)

        base, base_path = self.checkout()
        self.assertEqual(base["key"], "D1")
        first_id = base["section"][0]["id"]

        # The returned draft: one field change, one new slot with attachment.
        theirs = json.loads(json.dumps(base))
        theirs["section"][0]["slot"]["fields"]["n"] = 2
        theirs["section"].append(
            {
                "id": "T-new",
                "slot": {
                    "fields": {"m": 5},
                    "attachments": [{"id": "T-att", "slot": {"photo": "rear"}}],
                },
            }
        )
        theirs_path = self.write("theirs.json", theirs)
        local_path = self.write("local.json", base)

        merged = run_cli(
            "--store", self.store, "merge", "D1", base_path, local_path, theirs_path
        )
        self.assertEqual(merged.returncode, 0, merged.stderr)
        self.assertEqual(json.loads(merged.stdout)["merged"], "D1")

        shown = run_cli("--store", self.store, "show", "D1", "--compact")
        self.assertEqual(
            json.loads(shown.stdout)["section"],
            [
                {"fields": {"n": 2}, "attachments": [{"a": 1}]},
                None,
                {"fields": {"m": 5}, "attachments": [{"photo": "rear"}]},
            ],
        )
        slot_view = run_cli("--store", self.store, "slot", "T-new")
        self.assertEqual(json.loads(slot_view.stdout)["position"], 2)
        owner = run_cli("--store", self.store, "slot", "T-att")
        self.assertEqual(
            json.loads(owner.stdout)["owner_section_slot_id"], "T-new"
        )
        verify = run_cli("--store", self.store, "verify")
        self.assertEqual(verify.returncode, 0, verify.stderr)

        # Merging the same drafts again is stale: the store moved on.
        again = run_cli(
            "--store", self.store, "merge", "D1", base_path, local_path, theirs_path
        )
        self.assertEqual(again.returncode, 7)
        self.assertEqual(json.loads(again.stdout)["error"], "stale_local")

    def test_merge_conflict_exit_code_and_unchanged_store(self):
        document = self.write("doc.json", {"section": [{"fields": {"n": 1}}]})
        run_cli("--store", self.store, "import", "D1", document)
        base, base_path = self.checkout()
        local = json.loads(json.dumps(base))
        local["section"][0]["slot"]["fields"]["n"] = 2
        theirs = json.loads(json.dumps(base))
        theirs["section"][0]["slot"]["fields"]["n"] = 3
        local_path = self.write("local.json", local)
        theirs_path = self.write("theirs.json", theirs)
        before = run_cli("--store", self.store, "show", "D1", "--compact").stdout
        result = run_cli(
            "--store", self.store, "merge", "D1", base_path, local_path, theirs_path
        )
        self.assertEqual(result.returncode, 6)
        self.assertEqual(json.loads(result.stdout)["error"], "merge_conflict")
        after = run_cli("--store", self.store, "show", "D1", "--compact").stdout
        self.assertEqual(before, after)

    def test_merge_invalid_exit_code(self):
        document = self.write(
            "doc.json", {"section": [{"fields": {"n": 1}}, {"fields": {"n": 2}}]}
        )
        run_cli("--store", self.store, "import", "D1", document)
        base, base_path = self.checkout()
        theirs = json.loads(json.dumps(base))
        theirs["section"] = list(reversed(theirs["section"]))
        theirs_path = self.write("theirs.json", theirs)
        local_path = self.write("local.json", base)
        result = run_cli(
            "--store", self.store, "merge", "D1", base_path, local_path, theirs_path
        )
        self.assertEqual(result.returncode, 4)
        self.assertEqual(json.loads(result.stdout)["error"], "invalid")

    def test_checkout_missing_device_exit_code(self):
        result = run_cli("--store", self.store, "checkout", "NOPE")
        self.assertEqual(result.returncode, 3)

    def test_merge_missing_device_exit_code(self):
        document = self.write("doc.json", {"section": [{"fields": {"n": 1}}]})
        run_cli("--store", self.store, "import", "D1", document)
        base, base_path = self.checkout()
        other = json.loads(json.dumps(base))
        other["key"] = "GHOST"
        other_path = self.write("other.json", other)
        result = run_cli(
            "--store", self.store, "merge", "GHOST", other_path, other_path, other_path
        )
        self.assertEqual(result.returncode, 3)


if __name__ == "__main__":
    unittest.main()
