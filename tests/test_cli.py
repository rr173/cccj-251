"""CLI end-to-end tests: import, edit, read, page, export, bad batch."""

import json
import os
import sqlite3
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


class HistoryCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TempStore()
        self.store = self.tmp.path
        self.assertEqual(run("--store", self.store, "init").returncode, 0)
        self.doc = self.write(
            "doc.json",
            {"section": [{"fields": {"n": 0}, "attachments": [{"a": 1}, None]}]},
        )
        self.assertEqual(
            run("--store", self.store, "import", "D1", self.doc).returncode, 0
        )
        edits = self.write(
            "edit.json",
            [{"op": "insert_section", "id": "NEW", "slot": {"fields": {"n": 1}}}],
        )
        self.assertEqual(
            run("--store", self.store, "apply", "D1", edits).returncode, 0
        )

    def write(self, name, payload):
        path = os.path.join(self.tmp.directory, name)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False)
        return path

    def test_versions_list_and_read_and_restore(self) -> None:
        listing = run("--store", self.store, "versions", "D1")
        self.assertEqual(listing.returncode, 0, listing.stderr)
        versions = json.loads(listing.stdout)["versions"]
        self.assertEqual([v["version_no"] for v in versions], [1, 2])
        self.assertEqual([v["kind"] for v in versions], ["import", "batch"])
        for entry in versions:
            self.assertIn("created_at", entry)
            self.assertIn("content_hash", entry)

        first = run("--store", self.store, "version", "D1", "1", "--compact")
        self.assertEqual(first.returncode, 0, first.stderr)
        payload = json.loads(first.stdout)
        self.assertEqual(payload["version_no"], 1)
        self.assertEqual(
            payload["document"]["section"],
            [{"fields": {"n": 0}, "attachments": [{"a": 1}, None]}],
        )

        identified = run("--store", self.store, "version", "D1", "1", "--identified")
        draft = json.loads(identified.stdout)
        self.assertEqual(draft["key"], "D1")
        self.assertTrue(draft["section"][0]["id"])
        # The attachment hole keeps its id in the historical draft.
        self.assertIsNone(draft["section"][0]["slot"]["attachments"][1]["slot"])

        result = run("--store", self.store, "restore", "D1", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        stats = json.loads(result.stdout)
        self.assertEqual(stats["versions_added"], 1)
        self.assertEqual(stats["restored_from"], 1)

        after = json.loads(
            run("--store", self.store, "versions", "D1").stdout
        )["versions"]
        self.assertEqual([v["version_no"] for v in after], [1, 2, 3])
        self.assertEqual(after[-1]["kind"], "restore")
        current = json.loads(
            run("--store", self.store, "show", "D1", "--compact").stdout
        )
        self.assertEqual(
            current["section"],
            [{"fields": {"n": 0}, "attachments": [{"a": 1}, None]}],
        )
        # Existing query/export/verify behavior still works alongside history.
        self.assertEqual(
            run("--store", self.store, "verify").returncode, 0
        )
        outdir = os.path.join(self.tmp.directory, "csv")
        self.assertEqual(
            run("--store", self.store, "export", outdir).returncode, 0
        )

    def test_invalid_version_and_device_exit_codes(self) -> None:
        bad_version = run("--store", self.store, "version", "D1", "99")
        self.assertEqual(bad_version.returncode, 3)
        self.assertEqual(json.loads(bad_version.stdout)["error"], "not_found")

        zero = run("--store", self.store, "version", "D1", "0")
        self.assertEqual(zero.returncode, 4)
        self.assertEqual(json.loads(zero.stdout)["error"], "invalid")

        ghost = run("--store", self.store, "versions", "GHOST")
        self.assertEqual(ghost.returncode, 3)
        self.assertEqual(json.loads(ghost.stdout)["error"], "not_found")

        restore_ghost = run("--store", self.store, "restore", "GHOST", "1")
        self.assertEqual(restore_ghost.returncode, 3)

        # The failed restore left the record and history alone.
        versions = json.loads(
            run("--store", self.store, "versions", "D1").stdout
        )["versions"]
        self.assertEqual([v["version_no"] for v in versions], [1, 2])


class SyncCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TempStore()
        self.store = self.tmp.path
        self.assertEqual(run("--store", self.store, "init").returncode, 0)
        doc = self.tmp.directory
        self.document = os.path.join(doc, "doc.json")
        with open(self.document, "w", encoding="utf-8") as handle:
            json.dump(
                {"section": [{"fields": {"n": 1}, "attachments": [{"a": 0}]}]},
                handle,
            )
        self.assertEqual(
            run("--store", self.store, "import", "D1", self.document).returncode,
            0,
        )
        self.analysis = os.path.join(self.tmp.directory, "analysis.db")

    def test_sync_command_creates_and_refreshes_analysis_database(self) -> None:
        first = run("--store", self.store, "sync", self.analysis)
        self.assertEqual(first.returncode, 0, first.stderr)
        payload = json.loads(first.stdout)["synced"]
        self.assertEqual(payload["devices_total"], 1)
        self.assertEqual(payload["devices_inserted"], 1)
        self.assertTrue(os.path.exists(self.analysis))

        conn = sqlite3.connect(self.analysis)
        self.assertEqual(
            conn.execute("SELECT device_key, source_version FROM device").fetchall(),
            [("D1", 1)],
        )
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM section_slot").fetchone()[0], 1
        )
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM attachment_slot").fetchone()[0], 1
        )
        conn.close()

        # A repeat sync without source changes is a no-op.
        again = run("--store", self.store, "sync", self.analysis)
        self.assertEqual(again.returncode, 0, again.stderr)
        self.assertTrue(json.loads(again.stdout)["synced"]["already_current"])

        # A source change flows through on the next sync.
        edits = os.path.join(self.tmp.directory, "edits.json")
        with open(edits, "w", encoding="utf-8") as handle:
            json.dump(
                [{"op": "insert_section", "id": "S2", "slot": {"fields": {}}}],
                handle,
            )
        self.assertEqual(
            run("--store", self.store, "apply", "D1", edits).returncode, 0
        )
        updated = run("--store", self.store, "sync", self.analysis)
        self.assertEqual(updated.returncode, 0, updated.stderr)
        report = json.loads(updated.stdout)["synced"]
        self.assertEqual(report["devices_updated"], 1)
        conn = sqlite3.connect(self.analysis)
        self.assertEqual(
            conn.execute("SELECT source_version FROM device").fetchone()[0], 2
        )
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM section_slot").fetchone()[0], 2
        )
        conn.close()

    def test_sync_rejects_legacy_store_and_foreign_target(self) -> None:
        from inspection_store.store import _LEGACY_SCHEMA_V1

        legacy = os.path.join(self.tmp.directory, "legacy.db")
        raw = sqlite3.connect(legacy)
        raw.executescript(_LEGACY_SCHEMA_V1)
        raw.execute("INSERT INTO meta(name, value) VALUES ('schema_version', '1')")
        raw.commit()
        raw.close()
        result = run(
            "--store", legacy, "sync",
            os.path.join(self.tmp.directory, "legacy-analysis.db"),
        )
        self.assertEqual(result.returncode, 4)
        self.assertEqual(json.loads(result.stdout)["error"], "sync_failed")

        # A target already used for a different source is rejected.
        run("--store", self.store, "sync", self.analysis)
        other_store = os.path.join(self.tmp.directory, "other.db")
        self.assertEqual(run("--store", other_store, "init").returncode, 0)
        result = run("--store", other_store, "sync", self.analysis)
        self.assertEqual(result.returncode, 4)
        self.assertEqual(json.loads(result.stdout)["error"], "sync_failed")

    def test_sync_stale_publish_returns_exit_code_8(self) -> None:
        # Publish the real v1, then make the target claim an even newer
        # version so a fresh source snapshot looks older than what is
        # already published.  The CLI must refuse rather than downgrade.
        self.assertEqual(
            run("--store", self.store, "sync", self.analysis).returncode, 0
        )
        tampered = sqlite3.connect(self.analysis)
        tampered.execute(
            "UPDATE device SET source_version = 99 WHERE device_key = 'D1'"
        )
        tampered.commit()
        tampered.close()
        result = run("--store", self.store, "sync", self.analysis)
        self.assertEqual(result.returncode, 8)
        self.assertEqual(
            json.loads(result.stdout)["error"], "sync_unavailable"
        )
        # The published rows were left untouched by the rejected sync.
        conn = sqlite3.connect(self.analysis)
        self.assertEqual(
            conn.execute(
                "SELECT source_version FROM device WHERE device_key = 'D1'"
            ).fetchone()[0],
            99,
        )
        conn.close()


if __name__ == "__main__":
    unittest.main()
