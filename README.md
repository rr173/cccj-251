# Offline inspection-document store

An offline, file-backed store for the nested inspection document each device
carries, exposed as relational tables plus a query/rebuild API and CLI.

A device document is

```
{ "section": absent | null | [ slot, ... ] }
slot  = null | { "fields": {name: str|int, ...},
                 "attachments": absent | null | [ att, ... ] }
att   = null | { name: str|int, ... }
```

The section array and every attachment array each choose one of four states
independently: **absent**, **null**, **empty array**, **present array**.
Holes (`null` elements) are full members of the array: they have a stable slot
id and a position, so `[null]` and `[]` are different documents.

## What is stored

The live document is held in three tables, one row per element (holes included):

| table | holds |
| --- | --- |
| `device` | device key, section state, section root |
| `section_slot` | section elements: fields, attachment state, array links |
| `attachment_slot` | attachment elements: fields, links, owner's **stable id** |
| `device_version` | append-only per-device history: one full identified draft per committed change |

Arrays are ordered by an implicit treap kept in the slot rows
(`left_id`/`right_id`/`parent_id`/`priority`/`subtree_size`). In-order
traversal is array order and subtree sizes give zero-based ranks. There is
**no position column** in the live tables:

* middle insert/delete touches only the search path, O(log n) rows, and never
  renumbers the untouched suffix;
* a page over a half-open absolute range `[start, stop)` descends by rank, so
  its cost does not depend on `start`;
* final positions `0,1,2,…` are generated only while streaming an official
  export, so no materialized sequence can go stale.

Slot ids are stable and independent of order; ids never imply position and
positions never imply ids. `attachment_slot.section_slot_id` points at the
section's id, so reordering section slots can never rebind attachments to the
wrong object.

## Editing

Edits are grouped into batches for one device and applied in a single
transaction. Any bad reference, wrong-array reference, duplicate id, or
operation on a non-array aborts the whole batch, so the store's visible state
is exactly what it was before. A successful batch is durable and visible to a
new process.

## History and restore

Every device carries an append-only version history in `device_version`.
Opening an older database migrates it in place: each device's current record
becomes a readable **baseline** version, the three live relations keep their
shape, and all existing reads, edits, exports and merges keep working.

A new version is appended only when one operation commits a **content change**
for that device — a batch, an import (initial or `--replace`), an offline
merge write-back, or a restore. The whole batch is one change and therefore
one version. Failed operations append nothing (their transaction rolls back),
and an operation whose result is content-identical to the current record
appends nothing. Version numbers are per-device, gapless and ordered by
commit time.

Each version stores the full identified draft, so it preserves fields, the
absent / null / empty / present array shapes, holes, array order, every
stable slot id and attachment ownership. After a successful operation a fresh
process sees both the current record and its matching history version;
`verify` checks that the newest version always equals the live rows.

`restore` rebuilds the current rows from an old version through the same
identity-preserving write-back as a merge, so the record returns to that
version's content *and* stable slot identities. History never moves
backwards and no version is deleted: the restore is itself appended as a new
latest version, and every earlier version stays readable. Restoring content
identical to the current record changes nothing.

```
python -m inspection_store --store store.db versions D1
python -m inspection_store --store store.db version D1 2
python -m inspection_store --store store.db version D1 2 --identified
python -m inspection_store --store store.db restore D1 1
```

The same capabilities are available from Python: `DeviceStore.list_versions`,
`load_version`, `checkout_version` and `restore_version`. An unknown device or
version raises `NotFound`; a non-integer or non-positive version raises
`ValidationError`; in every error case neither the current record nor the
history is modified.

Supported operations (only these; turning an existing array back to
absent/null is not supported):

| edit | meaning |
| --- | --- |
| `init_section_empty` | absent/null section becomes `[]` |
| `init_attachments_empty(section)` | absent/null attachments become `[]` |
| `insert_section(id, before?, slot)` | insert a slot/hole; `before` is a stable id or omitted for the tail |
| `delete_section(id)` | remove the slot and its whole attachment array |
| `insert_attachment(section, id, before?, slot)` | insert into that section's array |
| `delete_attachment(section, id)` | remove one attachment |

A new slot may carry `[]` for attachments but not a non-empty attachment
array (initialize, then insert). Deleting the last element leaves an **empty
array**, not absent. Arrays that were absent/null must be initialized before
any slot is inserted.

## Reading

* `load_document(device)` rebuilds the nested document.
* `slot_view(slot_id)` / `slot_position(device, slot_id)` return the slot's
  current kind, owner, hole flag and computed absolute position.
* `page(device, start, stop, section_id=...)` returns a half-open absolute
  range of the section array or of one attachment array; `stats` reports rows
  fetched so the non-linear cost is observable.
* `verify()` checks every treap, ownership and shape invariant.

## CLI

```
python -m inspection_store --store store.db init
python -m inspection_store --store store.db import D1 doc.json
python -m inspection_store --store store.db apply D1 edits.json
python -m inspection_store --store store.db show D1
python -m inspection_store --store store.db checkout D1 [-o draft.json] [--compact]
python -m inspection_store --store store.db merge D1 base.json local.json theirs.json
python -m inspection_store --store store.db slot SLOT_ID
python -m inspection_store --store store.db shape D1 [--section SLOT_ID]
python -m inspection_store --store store.db page D1 1000 1010 [--section SLOT_ID] [--stats]
python -m inspection_store --store store.db export OUTDIR [DEVICE ...]
python -m inspection_store --store store.db sync ANALYSIS.db
python -m inspection_store --store store.db verify
python -m inspection_store --store store.db counts
python -m inspection_store --store store.db versions D1
python -m inspection_store --store store.db version D1 N [--identified] [--compact]
python -m inspection_store --store store.db restore D1 N
```

`edits.json` is a list (or `{"edits": [...]}`) of edit objects as in the table
above, e.g.

```json
{"edits": [
  {"op": "insert_section", "id": "S-new", "before": "S-abc",
   "slot": {"fields": {"torque": 12}}},
  {"op": "init_attachments_empty", "section": "S-new"},
  {"op": "insert_attachment", "section": "S-new", "id": "A-1",
   "slot": {"photo": "front"}}
]}
```

Exit codes: `0` ok, `2` batch aborted, `3` not found, `4` invalid input,
`5` missing file, `6` merge conflict, `7` local draft stale, `8` sync could
not publish (another sync in progress, or an older read view would overwrite
a newer published result).

## Analysis database sync

Analysts run SQL against the three formal relations without touching the live
store. The `sync` command maintains a separate, independent SQLite database
that mirrors the store's current records, and is safe to repeat against the
same source/target pair:

```
python -m inspection_store --store store.db sync analysis.db
```

The first run creates `analysis.db` from an empty target; every later run
performs an incremental refresh. The source store is opened through a
read-only (`mode=ro`) connection and is never modified, so syncs can run
alongside normal inspection work. Only a history-enabled store is accepted:
an older database is migrated in place the first time a current `DeviceStore`
opens it, after which it can be synchronized.

The analysis database exposes the same three relations the formal CSV export
writes, with identical column order and semantics (final positions
materialized, holes and the absent/null/empty/present shapes preserved), plus
the source version each device record came from:

| relation | columns |
| --- | --- |
| `device` | `device_key, section_shape, section_slot_count, source_version` |
| `section_slot` | the eight export columns (`device_key, section_slot_id, device_section_shape, position, is_hole, fields_json, attachment_shape, section_slot_count`) |
| `attachment_slot` | the seven export columns (`device_key, owner_section_slot_id, attachment_slot_id, section_position, position, is_hole, fields_json`) |

`sync` reads the latest per-device version numbers in **one source read
transaction**, so a published result always corresponds to a single committed
view: array shapes, holes, stable slot identities, attachment ownership and
final positions all come from that view. When several changes happen between
syncs, only the latest result is published — including a restore to old
content, which still mints a new version number.

The incremental rules are:

* the version list is the only full-store scan;
* the live slot rows (the record bodies) are read only for devices whose
  version differs from the published one, and historical version payloads are
  never read;
* unchanged devices are not rewritten — their analysis rows keep their
  rowids;
* a changed device's rows are replaced (children first, then the device row),
  so a slot id deleted by one device and reused by another can only appear
  under its new owner. The source API only ever adds devices, so the analysis
  database never has to delete one; a read view that somehow lacks a device a
  newer sync already published is rejected as stale rather than deleting it.

The three relations and the synchronized-version bookkeeping are swapped in
one write transaction, so a reader never sees a partially updated result. If
publishing fails, the previous result stays queryable and a plain retry
republishes everything; no progress is committed ahead of the transaction.
Two overlapping syncs cannot interleave: the second one fails fast while the
first holds the write lock, and an older source read view is never allowed to
overwrite a newer published result. The analysis database is bound to one
fixed source path and maintained solely by the tool; business-side access is
read-only SQL, and pointing it at another source is rejected rather than
mixed in.

The same entry point is available from Python:
`inspection_store.synchronize(source_path, analysis_path)`, returning counters
for inserted, updated and deleted devices and the slot rows written.

## Offline merge (checkout / merge)

Two crews can take the same device draft offline and hand back modified
copies; the store merges them for one device instead of overwriting one
copy with the other.

`checkout` writes the device's **identified draft**: the same nested shape
as `show`, but every array element — holes included — carries its stable
slot id:

```json
{"key": "D1",
 "section": [
   {"id": "s_…", "slot": null},
   {"id": "s_…", "slot": {"fields": {"torque": 10},
                           "attachments": [
                             {"id": "s_…", "slot": {"photo": "front"}}]}}]}
```

Absent, `null`, `[]` and populated arrays stay distinct at both levels, and
a slot id appears at most once in one draft.  The read behind `checkout`
is a single transaction, so a draft never mixes sections and attachments
from different moments of a concurrent edit.

`merge` plans a three-way merge from the **base** draft both crews started
from, the **local** draft (what the store must still contain) and the
**returned** draft, then writes the result back in one transaction:

* changes are judged by stable slot id, never by position or JSON key
  order;
* a field changed on one side is taken from that side, changed identically
  on both sides is taken once, changed differently is a conflict — field
  additions and removals count as changes;
* a base slot deleted on only one side stays deleted only if the other
  side left the slot and everything under it untouched (inserting
  neighbours in the parent array is fine; adding, removing or editing its
  attachments is a conflict);
* arrays merge slot by slot: each new slot follows the nearest preceding
  base slot of its own draft that survives the merge (array start when
  there is none); at one anchor the local side's newcomers come first,
  each side keeping its own order — a newcomer is never lost or
  re-parented because its neighbour was deleted, and a merged array with
  zero slots is still an empty array;
* base slots may not be moved across arrays, re-holed or reordered inside
  a branch — that is invalid input, as is a malformed draft;
* the same new id on both sides, or a merged id owned by another device in
  the store, is reported — never silently merged or renumbered;
* planning is O(slots): one pass per array with id maps, no slot pairs, so
  drafts with tens of thousands of slots merge quickly.

The write-back is compare-and-swap: if the device in the store no longer
equals the submitted local draft (another process recorded more in the
meantime), the merge fails with `stale_local` and changes nothing.  Only
the target device is rewritten, with its original identities; afterwards
`show`, `page`, `slot`, `export` and `verify` see the merged shape and
final order as usual.

## Tests

```
python3 -m unittest discover -s tests -v
python3 -I scripts/demo.py --scale 100000
```

`SCALE_N` (default 100000), `SIM_ROUNDS` (default 25) tune the scale and
randomized acceptance runs. The scale run writes its SQLite file and export
CSVs to a temp directory only; no large table is committed. `SYNC_SCALE_N`
(default 20000) sizes the two-device fixture the incremental sync scale tests
use; like the rest of the suite it creates databases only in temp
directories and the repository keeps no `.db`/`.sqlite` files.

The randomized acceptance test compares the store after every batch against
`tests/reference_model.py`, an independent model that is nothing but nested
Python lists. It checks the rebuilt document, the three relations' row counts,
every slot's identity and position, and that `[null]` and `[]` remain distinct.

Measured at 100,000 slots, inserting at the middle of the section array reports
30 rows written (27 existing rows actually change) instead of the ~50,000 a
dense position column would rewrite. A 10-slot page at position 99,990 fetches
42 rows versus 19 at position 0 — page size and tree height, not the absolute
start.

## Layout

```
inspection_store/codec.py    validation + document shape rules
inspection_store/merge.py    identified drafts + three-way merge planning
inspection_store/store.py    schema, migration, treap, batches, reads, verify,
                             checkout/merge write-back, version history + restore
inspection_store/export.py   streaming CSV export with final positions
inspection_store/sync.py     read-only incremental sync into the analysis DB
inspection_store/cli.py      command line interface
tests/                       unit, simulation, batch, scale, integrity, merge,
                             history, sync and CLI tests
scripts/demo.py              narrated end-to-end acceptance demo
```
