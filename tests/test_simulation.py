"""Randomized acceptance simulation against the plain-Python reference model.

Each round starts from a random valid nested document, applies a handful of
valid edits to *both* the reference (ordinary list operations) and the store
(one atomic batch each), and compares the rebuilt document plus the three
relations' identity/shape invariants.  Invalid batches are interspersed and
must be invisible afterwards.
"""

import random
import unittest

from helpers import TempStore
import reference_model as ref
from inspection_store import BatchAborted, Edit


def random_scalar(rng):
    if rng.random() < 0.5:
        return rng.randint(-50, 50)
    return rng.choice(["a", "bb", "", "x y", "中"])


def random_fields(rng, low=0, high=3):
    return {f"k{i}": random_scalar(rng) for i in range(rng.randint(low, high))}


def random_document(rng):
    kind = rng.random()
    if kind < 0.1:
        return {}
    if kind < 0.2:
        return {"section": None}
    if kind < 0.3:
        return {"section": []}
    slots = []
    for _ in range(rng.randint(1, 6)):
        if rng.random() < 0.2:
            slots.append(None)
            continue
        slot = {"fields": random_fields(rng)}
        att = rng.random()
        if att < 0.3:
            pass
        elif att < 0.45:
            slot["attachments"] = None
        elif att < 0.6:
            slot["attachments"] = []
        else:
            slot["attachments"] = [
                None if rng.random() < 0.3 else random_fields(rng)
                for _ in range(rng.randint(1, 4))
            ]
        slots.append(slot)
    return {"section": slots}


class Simulation(unittest.TestCase):
    ROUNDS = int(__import__("os").environ.get("SIM_ROUNDS", "25"))

    def setUp(self):
        self.tmp = TempStore()
        self.store = self.tmp.create()

    def tearDown(self):
        self.store.close()

    def test_random_edits_match_reference(self):
        rng = random.Random(20261123)
        for round_number in range(self.ROUNDS):
            document = random_document(rng)
            key = f"DEV{round_number}"
            self.store.import_document(key, document)
            model = ref.build(document)
            # Adopt the ids the store actually assigned, then edit both sides
            # with shared ids from here on.
            self.adopt_store_ids(key, model)
            self.assert_documents_match(key, model)

            for step in range(8):
                edits, ref_apply = self.build_edit(rng, model, key)
                if edits is None:
                    continue
                ref_apply()
                self.store.apply_batch(key, edits)
                self.assert_documents_match(key, model)

            # A deliberately invalid batch must leave the view untouched.
            before = self.store.dump_tables()
            with self.assertRaises(BatchAborted):
                self.store.apply_batch(
                    key,
                    [
                        Edit.insert_section("BAD1", before=None, slot={"fields": {}}),
                        Edit.insert_section("BAD1", before=None, slot=None),
                    ],
                )
            self.assertEqual(before, self.store.dump_tables())
            self.assert_documents_match(key, model)
            self.store.verify()

    def adopt_store_ids(self, key, model):
        if not isinstance(model.section, list):
            return
        model.slots = {}
        for position, slot in enumerate(model.section):
            slot.section_id = self.store.page(key, position, position + 1)[0]["slot_id"]
            if isinstance(slot.attachments, list):
                slot.attachment_ids = [
                    row["slot_id"]
                    for row in self.store.page(
                        key, 0, len(slot.attachments), section_id=slot.section_id
                    )
                ]
            model.slots[slot.section_id] = slot

    # -- edit generation -------------------------------------------------
    def build_edit(self, rng, model, key):
        """Pick a valid edit, returning edit objects and a matching model action."""
        if model.section is ref.MISSING or model.section is None:
            if rng.random() < 0.5:
                return [Edit.init_section_empty()], lambda: ref.init_section_empty(model)
            return None, None

        section_ids = [slot.section_id for slot in model.section]
        choices = ["insert", "delete", "attach_insert", "attach_delete", "init_att"]
        if not section_ids:
            choices = ["insert"]
        action = rng.choice(choices)
        new_id = "N" + uuid_suffix(rng)

        if action == "insert":
            before = rng.choice(section_ids) if section_ids and rng.random() < 0.6 else None
            if rng.random() < 0.2:
                slot = None
            else:
                slot = {"fields": random_fields(rng)}
                if rng.random() < 0.5:
                    slot["attachments"] = rng.choice([[], None])
            return (
                [Edit.insert_section(new_id, before=before, slot=slot)],
                lambda: ref.insert_section(model, new_id, before, slot),
            )

        if action == "delete":
            victim = rng.choice(section_ids)
            return (
                [Edit.delete_section(victim)],
                lambda: ref.delete_section(model, victim),
            )

        # Attachment operations target a non-hole slot.
        candidates = [s for s in model.section if s.fields is not None]
        if not candidates:
            return None, None
        slot = rng.choice(candidates)
        if action == "init_att" and slot.attachments in (ref.MISSING, None):
            return (
                [Edit.init_attachments_empty(slot.section_id)],
                lambda: ref.init_attachments_empty(model, slot.section_id),
            )
        if not isinstance(slot.attachments, list):
            return None, None
        if action == "attach_insert" or not slot.attachment_ids:
            before = (
                rng.choice(slot.attachment_ids)
                if slot.attachment_ids and rng.random() < 0.6
                else None
            )
            value = None if rng.random() < 0.3 else random_fields(rng)
            return (
                [Edit.insert_attachment(slot.section_id, new_id, before=before, slot=value)],
                lambda: ref.insert_attachment(
                    model, slot.section_id, new_id, before, value
                ),
            )
        victim = rng.choice(slot.attachment_ids)
        return (
            [Edit.delete_attachment(slot.section_id, victim)],
            lambda: ref.delete_attachment(model, slot.section_id, victim),
        )

    # -- comparison ------------------------------------------------------
    def assert_documents_match(self, key, model):
        expected = ref.to_document(model)
        actual = self.store.load_document(key)
        actual.pop("key", None)
        self.assertEqual(actual, expected, msg=f"device {key}")
        self.assert_relations_match(key, model)

    def assert_relations_match(self, key, model):
        # Table row counts follow slot counts exactly (holes are rows too).
        section_count = 0 if not isinstance(model.section, list) else len(model.section)
        attachment_count = 0
        if isinstance(model.section, list):
            for slot in model.section:
                if isinstance(slot.attachments, list):
                    attachment_count += len(slot.attachments)
        tables = self.store.dump_tables()
        stored_sections = [r for r in tables["section_slot"] if r[1] == key]
        owner_ids = {r[0] for r in stored_sections}
        stored_attachments = [
            r for r in tables["attachment_slot"] if r[1] in owner_ids
        ]
        self.assertEqual(len(stored_sections), section_count)
        self.assertEqual(len(stored_attachments), attachment_count)

        # Identity and position agree with the reference.
        if isinstance(model.section, list):
            for position, slot in enumerate(model.section):
                view = self.store.slot_view(slot.section_id)
                self.assertEqual(view["kind"], "section")
                self.assertEqual(view["position"], position)
                self.assertEqual(view["is_hole"], slot.fields is None)
                if isinstance(slot.attachments, list):
                    for att_position, aid in enumerate(slot.attachment_ids):
                        aview = self.store.slot_view(aid)
                        self.assertEqual(aview["kind"], "attachment")
                        self.assertEqual(aview["position"], att_position)
                        self.assertEqual(aview["owner_section_slot_id"], slot.section_id)
                    page = self.store.page(
                        key, 0, len(slot.attachment_ids), section_id=slot.section_id
                    )
                    self.assertEqual(
                        [row["slot_id"] for row in page], slot.attachment_ids
                    )


def uuid_suffix(rng):
    return "".join(rng.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(8))


if __name__ == "__main__":
    unittest.main()
