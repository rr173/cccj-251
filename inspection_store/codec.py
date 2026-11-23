"""Document-level validation and serialization helpers.

The codec layer knows nothing about SQLite.  It turns a plain Python document
into a validated shape description and serializes that shape in a canonical
JSON form.  This keeps "what is a valid document" testable on its own and lets
the storage layer and the verifier agree on one definition.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple


# Shape codes used in the database.  Values matter: they are persisted.
CODE_ABSENT = 0   # no array / no section at all
CODE_NULL = 1     # explicit JSON null
CODE_EMPTY = 2    # array, zero elements
CODE_PRESENT = 3  # array, at least one element


class StoreError(Exception):
    """Base class for errors raised by the store."""


class ValidationError(StoreError):
    """A document, slot, or edit violated the schema."""


class BatchAborted(StoreError):
    """A batch was rejected; no part of it was applied."""


class _Absent:
    __slots__ = ()

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return "ABSENT"


ABSENT = _Absent()
_ABSENT = ABSENT


# ---------------------------------------------------------------------------
# Scalar validation


def _check_scalar(value: Any) -> None:
    # bool is an int subclass; it is not part of the document scalar domain.
    if isinstance(value, bool):
        raise ValidationError("booleans are not part of the scalar domain")
    if not isinstance(value, (str, int)):
        raise ValidationError(
            f"scalar must be str or int, got {type(value).__name__}"
        )


def check_slot_fields(fields: Any) -> Tuple[Tuple[str, Any], ...]:
    """Validate one slot's scalar map.  Slots may have zero fields."""
    if not isinstance(fields, dict):
        raise ValidationError(f"slot fields must be a dict, got {type(fields).__name__}")
    out: List[Tuple[str, Any]] = []
    for key, value in fields.items():
        if not isinstance(key, str):
            raise ValidationError("slot field names must be strings")
        _check_scalar(value)
        out.append((key, value))
    out.sort(key=lambda kv: kv[0])
    return tuple(out)


# ---------------------------------------------------------------------------
# Shape capture (plain nested Python -> normalized description)


@dataclass
class SlotDoc:
    """A section slot in a plain document.

    ``fields`` is a sorted tuple of (name, scalar).  ``attachments`` is
    ``ABSENT`` / ``None`` / list.  A slot with ``fields is None`` is a hole.
    """

    fields: Optional[Tuple[Tuple[str, Any], ...]]
    attachments: Any


@dataclass
class DeviceDoc:
    key: str
    section: Any  # ABSENT / None / list[SlotDoc]


def normalize_device_doc(key: Any, doc: Any) -> DeviceDoc:
    """Validate a full device document and normalize its shape.

    Accepted input::

        {"key": "D1", "section": <absent omitted> | null | [slot, ...]}

    where a slot is ``None`` (hole) or::

        {"fields": {name: str|int, ...}, "attachments": <omitted>|null|[...]}

    and an attachment element is ``None`` (hole) or a dict of str/int scalars.
    """
    if not isinstance(key, str) or not key:
        raise ValidationError("device key must be a non-empty string")
    if not isinstance(doc, dict):
        raise ValidationError("device document must be a dict")
    extra = set(doc) - {"key", "section"}
    if extra:
        raise ValidationError(f"unknown device document keys: {sorted(extra)}")
    if "key" in doc and doc["key"] != key:
        raise ValidationError("device document key disagrees with device key")

    section_raw = doc["section"] if "section" in doc else ABSENT
    if section_raw is ABSENT:
        return DeviceDoc(key=key, section=ABSENT)
    if section_raw is None:
        return DeviceDoc(key=key, section=None)
    if not isinstance(section_raw, list):
        raise ValidationError("section must be absent, null, or a list")

    slots: List[SlotDoc] = []
    for raw in section_raw:
        if raw is None:
            slots.append(SlotDoc(fields=None, attachments=ABSENT))
            continue
        if not isinstance(raw, dict):
            raise ValidationError("section slot must be null or an object")
        extra = set(raw) - {"fields", "attachments"}
        if extra:
            raise ValidationError(f"unknown slot keys: {sorted(extra)}")
        fields = check_slot_fields(raw.get("fields", {}))
        att_raw = raw["attachments"] if "attachments" in raw else ABSENT
        if att_raw is ABSENT or att_raw is None:
            atts: Any = att_raw
        elif isinstance(att_raw, list):
            atts = []
            for att in att_raw:
                if att is None:
                    atts.append(None)
                else:
                    atts.append(check_slot_fields(att))
                    # an attachment slot always has an identity of its own;
                    # its scalar set is what it is.
        else:
            raise ValidationError("attachments must be absent, null, or a list")
        slots.append(SlotDoc(fields=fields, attachments=atts))
    return DeviceDoc(key=key, section=slots)


# ---------------------------------------------------------------------------
# Canonical serialization


def dumps_value(value: Any) -> str:
    """Canonical JSON for a stored scalar/array value.  Not pretty printed."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def loads_scalar(text: str) -> Any:
    return json.loads(text)
