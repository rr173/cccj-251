#!/usr/bin/env python3
"""Narrated acceptance demo.

Runs the scenarios from the brief against a scratch SQLite file and a scratch
export directory, printing what it checks.  Nothing is written into the
repository.  Run from anywhere:

    python3 -I scripts/demo.py [--scale 100000]
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from inspection_store import (  # noqa: E402
    BatchAborted,
    DeviceStore,
    Edit,
    StaleLocal,
    synchronize,
)
from inspection_store.export import export_csv  # noqa: E402


def heading(text: str) -> None:
    print(f"\n=== {text} ===")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scale", type=int, default=100_000)
    args = parser.parse_args()

    scratch = tempfile.mkdtemp(prefix="inspection-demo-")
    db = os.path.join(scratch, "store.db")
    store = DeviceStore(db, create=True)
    print(f"scratch store: {db}")

    heading("1. nested document with every array shape")
    document = {
        "section": [
            {"fields": {"device_part": "left"}, "attachments": [{"kind": "photo"}, None]},
            None,                                              # hole
            {"fields": {"reading": 7}},                        # attachments absent
            {"fields": {"reading": 8}, "attachments": None},   # attachments null
            {"fields": {"reading": 9}, "attachments": []},     # attachments empty
        ]
    }
    store.import_document("DEV-1", document)
    rebuilt = store.load_document("DEV-1")
    assert rebuilt["section"] == document["section"], rebuilt
    print("section shape:", store.array_shape("DEV-1"))
    rows = store.page("DEV-1", 0, 5)
    print("attachment shapes:", [r["attachments"] for r in rows])
    hole = rows[1]["slot_id"]
    print("hole has stable id and position:", hole, store.slot_view(hole)["position"])

    heading("2. distinguish [null] from []")
    store.import_document("A", {"section": []})
    store.import_document("B", {"section": [None]})
    print("[]   ->", store.array_shape("A"), store.load_document("A")["section"])
    print("[null] ->", store.array_shape("B"), store.load_document("B")["section"])

    heading("3. insert/delete with stable ids; positions move, identity does not")
    section_ids = [r["slot_id"] for r in store.page("DEV-1", 0, 5)]
    attachments_of_first = [
        r["slot_id"] for r in store.page("DEV-1", 0, 2, section_id=section_ids[0])
    ]
    stats = store.apply_batch(
        "DEV-1",
        [
            Edit.insert_section(
                "NEW-HEAD", before=section_ids[0], slot={"fields": {"reading": 0}}
            )
        ],
    )
    print("insert at head stats:", stats)
    print(
        "first section moved to position",
        store.slot_view(section_ids[0])["position"],
        "but its attachments kept position",
        [store.slot_view(a)["position"] for a in attachments_of_first],
        "and owner",
        {store.slot_view(a)["owner_section_slot_id"] for a in attachments_of_first},
    )

    heading("4. deletion cascades and lookups fail")
    store.apply_batch("DEV-1", [Edit.delete_section(section_ids[0])])
    for attachment in attachments_of_first:
        try:
            store.slot_view(attachment)
            print("ERROR: attachment survived section deletion")
            return 1
        except Exception as exc:
            print(f"lookup {attachment[:10]}... -> {type(exc).__name__}")

    heading("5. initialization rules and empty-after-delete")
    try:
        store.apply_batch("DEV-1", [Edit.init_section_empty()])
    except BatchAborted as exc:
        print("re-init present array rejected:", exc)
    store.import_document("EMPTYISH", {"section": [{"fields": {}}]})
    only = store.page("EMPTYISH", 0, 1)[0]["slot_id"]
    store.apply_batch("EMPTYISH", [Edit.delete_section(only)])
    print("delete last slot ->", store.array_shape("EMPTYISH"))

    heading("6. bad batch leaves the view untouched")
    before = store.dump_tables()
    try:
        store.apply_batch(
            "DEV-1",
            [
                Edit.insert_section("GOOD", before=None, slot={"fields": {"x": 1}}),
                Edit.insert_section("GOOD", before=None, slot=None),
            ],
        )
        print("ERROR: bad batch accepted")
        return 1
    except BatchAborted as exc:
        print("aborted:", exc)
    assert store.dump_tables() == before
    print("all three tables unchanged")

    heading("7. export generates final positions only at export time")
    out = os.path.join(scratch, "export")
    paths = export_csv(store, out)
    with open(paths["section_slot"], newline="", encoding="utf-8") as handle:
        table = list(csv.DictReader(handle))
    print("section_slot rows:", len(table), "positions:", [r["position"] for r in table])
    print("live schema columns:", [c[1] for c in store.conn.execute("PRAGMA table_info(section_slot)")])
    store.verify()
    print("verify: ok")

    heading("8. offline three-way merge of one device")
    store.import_document(
        "CREW",
        {"section": [{"fields": {"torque": 10}}, None, {"fields": {"reading": 3}}]},
    )
    base_draft = store.checkout_document("CREW")
    print("checkout keeps ids and shapes:",
          [element["id"][:8] for element in base_draft["section"]])
    # The returned crew changed one field and added a slot; the local crew
    # changed nothing, so the store still equals the local draft.
    returned = json.loads(json.dumps(base_draft))
    returned["section"][0]["slot"]["fields"]["torque"] = 12
    returned["section"].insert(
        1, {"id": "CREW-NEW-1", "slot": {"fields": {"reading": 4}}}
    )
    stats = store.merge_document("CREW", base_draft, base_draft, returned)
    print("merge stats:", stats)
    print("merged order:", [row["slot_id"] for row in store.page("CREW", 0, 10)])
    print("merged torque:", store.load_document("CREW")["section"][0]["fields"])
    try:
        store.merge_document("CREW", base_draft, base_draft, returned)
        print("ERROR: stale local draft accepted")
        return 1
    except StaleLocal as exc:
        print("re-running the same merge is stale:", exc)
    store.verify()
    print("verify: ok")

    heading("9. version history: list, read, restore")
    history_device = "HIST"
    store.import_document(
        history_device,
        {"section": [{"fields": {"v": 1}, "attachments": [{"a": 1}]}]},
    )
    store.apply_batch(
        history_device,
        [Edit.insert_section("H-NEW", slot={"fields": {"v": 2}})],
    )
    versions = store.list_versions(history_device)
    print("versions:", [(v["version_no"], v["kind"]) for v in versions])
    print("version 1 record:", store.load_version(history_device, 1)["document"])
    # A failed batch and a content-identical write both leave history alone.
    try:
        store.apply_batch(
            history_device,
            [Edit.insert_section("DUP", slot=None), Edit.insert_section("DUP", slot=None)],
        )
        print("ERROR: bad batch accepted")
        return 1
    except BatchAborted as exc:
        print("failed batch appended no versions:", exc)
    assert [v["version_no"] for v in store.list_versions(history_device)] == [1, 2]
    stats = store.restore_version(history_device, 1)
    print("restore v1 -> new version", stats["version_no"], "from", stats["restored_from"])
    assert store.load_document(history_device) == store.load_version(history_device, 1)["document"]
    assert [v["version_no"] for v in store.list_versions(history_device)] == [1, 2, 3]
    # Restoring identical content is a no-op.
    assert store.restore_version(history_device, 1)["versions_added"] == 0
    store.verify()
    print("verify: ok")

    heading(f"10. {args.scale:,}-slot middle insert")
    start = time.time()
    store.import_document(
        "BIG", {"section": [{"fields": {"i": i}} for i in range(args.scale)]}
    )
    print(f"import: {time.time() - start:.2f}s")
    middle = args.scale // 2
    target = store.page("BIG", middle, middle + 1)[0]["slot_id"]
    before_rows = dict(
        (r[0], r) for r in store.dump_tables()["section_slot"] if r[1] == "BIG"
    )
    stats = store.apply_batch(
        "BIG", [Edit.insert_section("MIDDLE", before=target, slot={"fields": {"i": -1}})]
    )
    after_rows = dict(
        (r[0], r) for r in store.dump_tables()["section_slot"] if r[1] == "BIG"
    )
    changed = [
        sid for sid in after_rows if sid in before_rows and after_rows[sid] != before_rows[sid]
    ]
    print("reported rows written:", stats["rows_written"])
    print("actually modified existing rows:", len(changed), "of", len(after_rows))
    print(f"suffix rewrite would have been ~{args.scale - middle:,} rows")
    far = store.page("BIG", args.scale, args.scale + 10, stats={})
    read_stats: dict = {}
    store.page("BIG", args.scale - 10, args.scale, stats=read_stats)
    print("far page read stats:", read_stats)
    store.verify()
    print("verify: ok")
    store.close()

    heading("11. incremental sync into a read-only analysis database")
    import sqlite3

    analysis = os.path.join(scratch, "analysis.db")
    first = synchronize(db, analysis)
    print("first sync:", {k: first[k] for k in
          ("devices_inserted", "devices_updated", "section_slots_written")})
    # A second sync with no source change reads no record bodies.
    idle = synchronize(db, analysis)
    print("idle sync is a no-op:", idle["already_current"],
          "slots written:", idle["section_slots_written"])

    # Change one device, then confirm only that device is rewritten and the
    # analysis relations expose the source version of every device.
    store = DeviceStore(db)
    store.apply_batch("HIST", [Edit.insert_section("SYNC-NEW", slot={"fields": {"v": 3}})])
    store.close()
    refreshed = synchronize(db, analysis)
    print("refresh:", {k: refreshed[k] for k in
          ("devices_updated", "devices_deleted", "section_slots_written")})
    conn = sqlite3.connect(analysis)
    versions = dict(conn.execute("SELECT device_key, source_version FROM device"))
    print("source versions in analysis DB:", versions)
    row = conn.execute(
        "SELECT position, fields_json FROM section_slot "
        "WHERE section_slot_id = 'SYNC-NEW'"
    ).fetchone()
    assert row is not None
    print("new slot materialized at position", row[0], "with", row[1])
    # The analysis tables have exactly the formal export columns plus version.
    assert [c[1] for c in conn.execute("PRAGMA table_info(device)")][-1] == \
        "source_version"
    conn.close()

    print(f"\nAll demo assertions passed. Scratch data: {scratch}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
