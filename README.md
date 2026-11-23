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

Three tables, one row per element (holes included):

| table | holds |
| --- | --- |
| `device` | device key, section state, section root |
| `section_slot` | section elements: fields, attachment state, array links |
| `attachment_slot` | attachment elements: fields, links, owner's **stable id** |

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
new process. There is no history.

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
python -m inspection_store --store store.db slot SLOT_ID
python -m inspection_store --store store.db shape D1 [--section SLOT_ID]
python -m inspection_store --store store.db page D1 1000 1010 [--section SLOT_ID] [--stats]
python -m inspection_store --store store.db export OUTDIR [DEVICE ...]
python -m inspection_store --store store.db verify
python -m inspection_store --store store.db counts
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

Exit codes: `0` ok, `2` batch aborted, `3` not found, `4` invalid input.

## Tests

```
python3 -m unittest discover -s tests -v
python3 -I scripts/demo.py --scale 100000
```

`SCALE_N` (default 100000), `SIM_ROUNDS` (default 25) tune the scale and
randomized acceptance runs. The scale run writes its SQLite file and export
CSVs to a temp directory only; no large table is committed.

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
inspection_store/store.py    schema, treap, batches, reads, verify
inspection_store/export.py   streaming CSV export with final positions
inspection_store/cli.py      command line interface
tests/                       unit, simulation, batch, scale, integrity, CLI tests
scripts/demo.py              narrated end-to-end acceptance demo
```
