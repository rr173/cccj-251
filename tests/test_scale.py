"""Scale tests.

These assert the two cost properties the task asks for:

* a middle insertion into a very large array writes O(log n) structural rows
  and does not rewrite the untouched suffix;
* a small page far from the start costs work proportional to the page size
  and tree height, not to the absolute start position.

They also check that exports generate the final positions.
"""

import csv
import os
import time
import unittest

from helpers import TempStore
from inspection_store import DeviceStore, Edit
from inspection_store.export import export_csv

N = int(os.environ.get("SCALE_N", "100000"))


def build_large(store: DeviceStore, key: str, n: int) -> None:
    document = {"section": [{"fields": {"i": i}} for i in range(n)]}
    store.import_document(key, document)


class ScaleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = TempStore("big.db")
        cls.store = cls.tmp.create()
        start = time.time()
        build_large(cls.store, "BIG", N)       # read-only fixture
        build_large(cls.store, "EDIT", N)      # fixture for mutating tests
        cls.import_seconds = time.time() - start

    @classmethod
    def tearDownClass(cls) -> None:
        cls.store.close()

    def test_middle_insert_does_not_rewrite_suffix(self) -> None:
        store = self.store
        sections = store.counts()["section_slot"] // 2
        self.assertEqual(sections, N)

        middle_position = N // 2
        middle_id = store.page("EDIT", middle_position, middle_position + 1)[0][
            "slot_id"
        ]
        stats = store.apply_batch(
            "EDIT",
            [
                Edit.insert_section(
                    "MIDDLE", before=middle_id, slot={"fields": {"i": -1}}
                )
            ],
        )
        # A dense pos column would rewrite the whole suffix (~N/2 rows).
        # The treap touches only a search path plus the new row.
        self.assertLess(stats["rows_written"], 100, stats)
        self.assertLess(stats["treap_updated_rows"], 100, stats)
        self.assertEqual(stats["inserted_rows"], 1)
        self.assertLess(stats["nodes_loaded"], 200, stats)
        # One row in, one row of work; the untouched suffix is not rewritten.
        self.assertEqual(stats["deleted_rows"], 0)

        self.assertEqual(store.slot_view("MIDDLE")["position"], middle_position)
        self.assertEqual(
            store.slot_view(middle_id)["position"], middle_position + 1
        )
        self.assertEqual(store.counts()["section_slot"], 2 * N + 1)
        # Positions are computed, not stored, so the last slot is cheap too.
        last = store.page("EDIT", N, N + 1)[0]
        self.assertEqual(store.slot_view(last["slot_id"])["position"], N)

        # A run of further middle inserts also stays bounded.
        worst = stats["rows_written"]
        for round_number in range(10):
            position = (N // 3) + round_number
            target = store.page("EDIT", position, position + 1)[0]["slot_id"]
            run = store.apply_batch(
                "EDIT",
                [Edit.insert_section(f"R{round_number}", before=target, slot=None)],
            )
            worst = max(worst, run["rows_written"])
        self.assertLess(worst, 120, worst)
        store.verify()

    def test_far_page_does_not_scan_from_zero(self) -> None:
        store = self.store
        near_stats = {}
        far_stats = {}
        near = store.page("BIG", 0, 10, stats=near_stats)
        far = store.page("BIG", N - 10, N, stats=far_stats)
        self.assertEqual(len(near), 10)
        self.assertEqual(len(far), 10)
        self.assertEqual(far[0]["position"], N - 10)
        # Cost depends on page size and tree height, not on `start`.
        self.assertLess(far_stats["rows_fetched"], 10 * 40 + 100, far_stats)
        self.assertLess(
            far_stats["rows_fetched"],
            near_stats["rows_fetched"] + 200,
            (near_stats, far_stats),
        )
        expected_positions = list(range(N - 10, N))
        self.assertEqual([r["position"] for r in far], expected_positions)

    def test_rank_lookup_is_logarithmic(self) -> None:
        store = self.store
        target = store.page("BIG", N - 1, N)[0]["slot_id"]
        start = time.time()
        for _ in range(200):
            view = store.slot_view(target)
        elapsed = time.time() - start
        self.assertEqual(view["position"], N - 1)
        self.assertLess(elapsed, 5.0)

    def test_export_generates_final_positions_out_of_tree(self) -> None:
        store = self.store
        output = os.path.join(self.tmp.directory, "export")
        paths = export_csv(store, output, ["BIG"])
        with open(paths["section_slot"], newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            rows = list(reader)
        self.assertEqual(len(rows), N)
        self.assertEqual([int(r["position"]) for r in rows], list(range(N)))
        self.assertEqual(rows[0]["device_section_shape"], "present")
        # No positions were written back into the live tables.
        columns = [
            c[1] for c in store.conn.execute("PRAGMA table_info(section_slot)")
        ]
        self.assertNotIn("position", columns)
        self.assertNotIn("pos", columns)
        self.assertTrue(os.path.exists(paths["device"]))
        self.assertTrue(os.path.exists(paths["attachment_slot"]))


if __name__ == "__main__":
    unittest.main()
