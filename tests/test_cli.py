"""CLI end-to-end tests: import, edit, read, page, export, bad batch."""

import json
import os
import subprocess
import sys
import unittest

from helpers import ROOT, TempStore


def run(*args, stdin=None, cwd=None):
    # -I ignores PYTHONPATH, so load the package explicitly by path.
    bootstrap = (
        "import sys; sys.path.insert(0, {!r}); "
        "from inspection_store.cli import main; raise SystemExit(main())"
    ).format(ROOT)
    result = subprocess.run(
        [sys.executable, "-I", "-c", bootstrap, *args],
        input=stdin,
        capture_output=True,
        text=True,
        cwd=cwd or ROOT,
    )
    return result


class CliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TempStore()
        self.store = self.tmp.path
        result = run("--store", self.store, "init")
        self.assertEqual(result.returncode, 0, result.stderr)

    def write(self, name, payload):
        path = os.path.join(self.tmp.directory, name)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False)
        return path

    def test_document_round_trip_and_edits(self) -> None:
        document = self.write(
            "doc.json",
            {
                "section": [
                    {"fields": {"n": 0}, "attachments": [{"a": 1}, None]},
                    None,
                    {"fields": {"n": 2}, "attachments": []},
                ]
            },
        )
        result = run("--store", self.store, "import", "D1", document)
        self.assertEqual(result.returncode, 0, result.stderr)

        show = run("--store", self.store, "show", "D1", "--compact")
        self.assertEqual(show.returncode, 0, show.stderr)
        with open(document, encoding="utf-8") as handle:
            original = json.load(handle)
        self.assertEqual(json.loads(show.stdout)["section"], original["section"])

        page = run("--store", self.store, "page", "D1", "0", "3")
        rows = json.loads(page.stdout)["rows"]
        self.assertEqual([r["position"] for r in rows], [0, 1, 2])
        first, hole, last = rows
        self.assertTrue(hole["is_hole"])
        self.assertEqual(last["attachments"], "empty")

        edits = self.write(
            "edits.json",
            {
                "edits": [
                    {
                        "op": "insert_section",
                        "id": "NEW",
                        "before": hole["slot_id"],
                        "slot": {"fields": {"n": 1}},
                    }
                ]
            },
        )
        applied = run("--store", self.store, "apply", "D1", edits)
        self.assertEqual(applied.returncode, 0, applied.stderr)
        stats = json.loads(applied.stdout)
        self.assertLess(stats["rows_written"], 100)

        slot = run("--store", self.store, "slot", "NEW")
        self.assertEqual(json.loads(slot.stdout)["position"], 1)

        shape = run(
            "--store", self.store, "shape", "D1", "--section", first["slot_id"]
        )
        self.assertEqual(json.loads(shape.stdout)["shape"], "present")

        attachment_page = run(
            "--store",
            self.store,
            "page",
            "D1",
            "0",
            "5",
            "--section",
            first["slot_id"],
            "--stats",
        )
        payload = json.loads(attachment_page.stdout)
        self.assertEqual(len(payload["rows"]), 2)
        self.assertLess(payload["read_stats"]["rows_fetched"], 500)

        output = os.path.join(self.tmp.directory, "csv")
        exported = run("--store", self.store, "export", output)
        self.assertEqual(exported.returncode, 0, exported.stderr)
        paths = json.loads(exported.stdout)["exported"]
        with open(paths["section_slot"], encoding="utf-8") as handle:
            lines = handle.read().strip().splitlines()
        self.assertEqual(lines[0].split(",")[3], "position")
        self.assertEqual(len(lines) - 1, 4)

        verify = run("--store", self.store, "verify")
        self.assertEqual(verify.returncode, 0, verify.stderr)

    def test_bad_batch_exit_code_and_no_change(self) -> None:
        document = self.write("doc.json", {"section": [{"fields": {}}]})
        run("--store", self.store, "import", "D", document)
        before = run("--store", self.store, "show", "D", "--compact").stdout
        edits = self.write(
            "bad.json",
            [
                {"op": "insert_section", "id": "X", "slot": None},
                {"op": "insert_section", "id": "X", "slot": None},
            ],
        )
        result = run("--store", self.store, "apply", "D", edits)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(json.loads(result.stdout)["error"], "batch_aborted")
        after = run("--store", self.store, "show", "D", "--compact").stdout
        self.assertEqual(before, after)

    def test_deleted_slot_lookup_fails(self) -> None:
        document = self.write(
            "doc.json", {"section": [{"fields": {}, "attachments": [{"a": 1}]}]}
        )
        run("--store", self.store, "import", "D", document)
        section = json.loads(
            run("--store", self.store, "page", "D", "0", "1").stdout
        )["rows"][0]["slot_id"]
        attachment = json.loads(
            run(
                "--store", self.store, "page", "D", "0", "1", "--section", section
            ).stdout
        )["rows"][0]["slot_id"]
        edits = self.write("del.json", [{"op": "delete_section", "id": section}])
        self.assertEqual(
            run("--store", self.store, "apply", "D", edits).returncode, 0
        )
        result = run("--store", self.store, "slot", attachment)
        self.assertEqual(result.returncode, 3)
        self.assertEqual(json.loads(result.stdout)["error"], "not_found")


if __name__ == "__main__":
    unittest.main()
