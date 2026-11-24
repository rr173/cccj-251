"""Offline, order-preserving relational store for inspection documents.

A device document has one of three shapes for its inspection section:

* absent      -- the device row has no inspection section at all
* null        -- the inspection section exists but its value is null
* array       -- a list; elements are section slots.  A slot may be a hole
                 (JSON null) or carry a set of scalar string/integer fields
                 plus an optional attachment array.

The attachment array mirrors the same three-way shape *per section slot*:
absent, null, or array (with holes allowed).  There is no deeper nesting.

Storage is relational: one row per device, one per section slot and one per
attachment slot.  Array order lives in the treap structural columns
(parent/left/right/subtree size) inside each slot row; slot identity lives in
stable slot ids.  Slot ids never imply an order; that is the whole point of
having both.

The store is intentionally minimal: batch edits only (insert slot, delete
slot subtree, initialize an array to empty) and no way to turn an
already-present array back into absent/null.  Every successful content change
is appended to a per-device, append-only version history that can be read and
restored.
"""

from .codec import (
    StoreError,
    ValidationError,
    CODE_ABSENT,
    CODE_EMPTY,
    CODE_NULL,
    CODE_PRESENT,
)
from .merge import MergeConflict, StaleLocal
from .store import (
    BatchAborted,
    DeviceStore,
    DuplicateIdentity,
    Edit,
    IntegrityError,
    NotFound,
    new_slot_id,
)

__all__ = [
    "CODE_ABSENT",
    "CODE_EMPTY",
    "CODE_NULL",
    "CODE_PRESENT",
    "BatchAborted",
    "DeviceStore",
    "DuplicateIdentity",
    "Edit",
    "IntegrityError",
    "MergeConflict",
    "NotFound",
    "StaleLocal",
    "StoreError",
    "ValidationError",
    "new_slot_id",
]