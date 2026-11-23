"""Command line interface for the inspection store.

All commands are offline and file-backed.  Edits are expressed as JSON and
applied as one atomic batch.  Exports are explicit and write outside the
repository (the caller chooses the directory).
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict, List, Optional

from .codec import StoreError, ValidationError
from .export import export_csv
from .store import BatchAborted, DeviceStore, Edit, NotFound


def _load_json(path: str) -> Any:
    if path == "-":
        return json.load(sys.stdin)
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _print(value: Any) -> None:
    json.dump(value, sys.stdout, ensure_ascii=False, indent=2, sort_keys=True)
    sys.stdout.write("\n")


def _open(args: argparse.Namespace, create: bool = False) -> DeviceStore:
    return DeviceStore(args.store, create=create)


def cmd_init(args: argparse.Namespace) -> int:
    store = _open(args, create=True)
    store.close()
    _print({"created": args.store})
    return 0


def cmd_import(args: argparse.Namespace) -> int:
    document = _load_json(args.document)
    store = _open(args)
    try:
        stats = store.import_document(
            args.device, document, replace=args.replace
        )
    finally:
        store.close()
    _print({"imported": args.device, **stats})
    return 0


def cmd_apply(args: argparse.Namespace) -> int:
    raw = _load_json(args.edits)
    if isinstance(raw, dict) and "edits" in raw:
        raw = raw["edits"]
    if not isinstance(raw, list):
        raise ValidationError("edits file must contain a list or {\"edits\": [...]}")
    edits = [Edit.from_json(item) for item in raw]
    store = _open(args)
    try:
        stats = store.apply_batch(args.device, edits)
    finally:
        store.close()
    _print({"device": args.device, "applied": len(edits), **stats})
    return 0


def cmd_show(args: argparse.Namespace) -> int:
    store = _open(args)
    try:
        document = store.load_document(args.device)
    finally:
        store.close()
    if args.compact:
        sys.stdout.write(json.dumps(document, ensure_ascii=False, sort_keys=True))
        sys.stdout.write("\n")
    else:
        _print(document)
    return 0


def cmd_slot(args: argparse.Namespace) -> int:
    store = _open(args)
    try:
        view = store.slot_view(args.slot_id)
    finally:
        store.close()
    _print(view)
    return 0


def cmd_shape(args: argparse.Namespace) -> int:
    store = _open(args)
    try:
        shape = store.array_shape(args.device, args.section)
    finally:
        store.close()
    _print({"shape": shape})
    return 0


def cmd_page(args: argparse.Namespace) -> int:
    store = _open(args)
    stats: Dict[str, int] = {}
    try:
        rows = store.page(
            args.device,
            args.start,
            args.stop,
            section_id=args.section,
            stats=stats,
        )
    finally:
        store.close()
    payload: Dict[str, Any] = {"rows": rows}
    if args.stats:
        payload["read_stats"] = stats
    _print(payload)
    return 0


def cmd_export(args: argparse.Namespace) -> int:
    store = _open(args)
    try:
        paths = export_csv(store, args.output, args.devices or None)
    finally:
        store.close()
    _print({"exported": paths})
    return 0


def cmd_verify(args: argparse.Namespace) -> int:
    store = _open(args)
    try:
        report = store.verify()
        counts = store.counts()
    finally:
        store.close()
    _print({"ok": True, **counts, "arrays_checked": report})
    return 0


def cmd_counts(args: argparse.Namespace) -> int:
    store = _open(args)
    try:
        counts = store.counts()
        keys = store.device_keys()
    finally:
        store.close()
    _print({"counts": counts, "devices": keys})
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m inspection_store",
        description="Offline relational store for inspection documents.",
    )
    parser.add_argument("--store", required=True, help="path to the SQLite file")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("init", help="create an empty store")
    p.set_defaults(func=cmd_init)

    p = sub.add_parser("import", help="import one device document")
    p.add_argument("device")
    p.add_argument("document", help="JSON file with {\"section\": ...}, or -")
    p.add_argument("--replace", action="store_true")
    p.set_defaults(func=cmd_import)

    p = sub.add_parser("apply", help="apply one atomic batch of edits")
    p.add_argument("device")
    p.add_argument("edits", help="JSON file with a list of edits, or -")
    p.set_defaults(func=cmd_apply)

    p = sub.add_parser("show", help="rebuild a device document")
    p.add_argument("device")
    p.add_argument("--compact", action="store_true")
    p.set_defaults(func=cmd_show)

    p = sub.add_parser("slot", help="look a slot up by its stable id")
    p.add_argument("slot_id")
    p.set_defaults(func=cmd_slot)

    p = sub.add_parser("shape", help="section or attachment array shape")
    p.add_argument("device")
    p.add_argument("--section", help="section slot id for its attachment array")
    p.set_defaults(func=cmd_shape)

    p = sub.add_parser("page", help="half-open absolute range of one array")
    p.add_argument("device")
    p.add_argument("start", type=int)
    p.add_argument("stop", type=int)
    p.add_argument("--section", help="section slot id to page its attachments")
    p.add_argument("--stats", action="store_true", help="report rows fetched")
    p.set_defaults(func=cmd_page)

    p = sub.add_parser("export", help="stream the three formal tables as CSV")
    p.add_argument("output", help="output directory (outside the repository)")
    p.add_argument("devices", nargs="*")
    p.set_defaults(func=cmd_export)

    p = sub.add_parser("verify", help="check all structural invariants")
    p.set_defaults(func=cmd_verify)

    p = sub.add_parser("counts", help="row counts and device keys")
    p.set_defaults(func=cmd_counts)
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except BatchAborted as exc:
        _print({"error": "batch_aborted", "detail": str(exc)})
        return 2
    except NotFound as exc:
        _print({"error": "not_found", "detail": str(exc)})
        return 3
    except (StoreError, ValidationError) as exc:
        _print({"error": "invalid", "detail": str(exc)})
        return 4
    except FileNotFoundError as exc:
        _print({"error": "missing_file", "detail": str(exc)})
        return 5
    except json.JSONDecodeError as exc:
        _print({"error": "bad_json", "detail": str(exc)})
        return 4


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
