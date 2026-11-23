"""A tiny in-memory reference model of the document shape.

Used by the acceptance and property tests as the independent oracle: it is
just nested Python lists and dicts, updated with ordinary list operations.
It shares no code with the SQLite store.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


MISSING = object()


class RefError(Exception):
    pass


def _field(value: Any) -> Dict[str, Any]:
    return dict(value)


@dataclass
class RefSlot:
    """One section slot in the model.

    ``fields`` None means the whole slot is a hole (JSON null).
    ``attachments`` is MISSING (absent), None (null), or a list.
    """

    fields: Optional[Dict[str, Any]]
    attachments: Any = MISSING
    section_id: str = ""
    attachment_ids: List[str] = field(default_factory=list)


@dataclass
class RefDoc:
    section: Any  # MISSING / None / list[RefSlot]
    slots: Dict[str, RefSlot] = field(default_factory=dict)


def build(document: Dict[str, Any], id_factory=None) -> RefDoc:
    """Build a reference document from plain nested JSON-ish input.

    ``id_factory(kind, ordinal)`` supplies the stable ids the store will use,
    so the model and the store can be compared by identity as well as shape.
    """
    counters = {"section": 0, "attachment": 0}

    def default_id(kind: str, ordinal: int) -> str:
        counters[kind] += 1
        return ("S" if kind == "section" else "A") + str(counters[kind])

    make_id = default_id if id_factory is None else id_factory
    section = document["section"] if "section" in document else MISSING
    doc = RefDoc(section=MISSING)
    if section is MISSING:
        return doc
    if section is None:
        doc.section = None
        return doc
    slots: List[RefSlot] = []
    for ordinal, raw in enumerate(section):
        sid = make_id("section", ordinal)
        if raw is None:
            slot = RefSlot(fields=None, section_id=sid)
        else:
            atts = raw.get("attachments", MISSING)
            att_ids: List[str] = []
            if isinstance(atts, list):
                for att_ordinal, _ in enumerate(atts):
                    att_ids.append(make_id("attachment", att_ordinal))
            slot = RefSlot(
                fields=dict(raw.get("fields", {})),
                attachments=list(atts) if isinstance(atts, list) else atts,
                section_id=sid,
                attachment_ids=att_ids,
            )
        doc.slots[sid] = slot
        slots.append(slot)
    doc.section = slots
    return doc


def to_document(doc: RefDoc) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if doc.section is MISSING:
        return out
    if doc.section is None:
        out["section"] = None
        return out
    array = []
    for slot in doc.section:
        if slot.fields is None:
            array.append(None)
            continue
        item: Dict[str, Any] = {"fields": dict(slot.fields)}
        if slot.attachments is not MISSING:
            item["attachments"] = (
                None
                if slot.attachments is None
                else [None if a is None else dict(a) for a in slot.attachments]
            )
        array.append(item)
    out["section"] = array
    return out


def find_slot(doc: RefDoc, slot_id: str) -> RefSlot:
    slot = doc.slots.get(slot_id)
    if slot is None:
        raise RefError(f"unknown slot {slot_id!r}")
    return slot


def section_position(doc: RefDoc, slot_id: str) -> int:
    for index, slot in enumerate(doc.section):
        if slot.section_id == slot_id:
            return index
    raise RefError(f"unknown slot {slot_id!r}")


def insert_section(
    doc: RefDoc, slot_id: str, before: Optional[str], slot: Any
) -> None:
    if doc.section is MISSING or doc.section is None:
        raise RefError("section is not an array")
    if slot_id in doc.slots:
        raise RefError("duplicate id")
    if slot is None:
        new = RefSlot(fields=None, section_id=slot_id)
    else:
        atts = slot.get("attachments", MISSING)
        if isinstance(atts, list) and atts:
            raise RefError("a new slot may only get an empty attachment array")
        new = RefSlot(
            fields=dict(slot.get("fields", {})),
            attachments=[] if isinstance(atts, list) else atts,
            section_id=slot_id,
        )
    doc.slots[slot_id] = new
    position = (
        section_position(doc, before) if before is not None else len(doc.section)
    )
    doc.section.insert(position, new)


def delete_section(doc: RefDoc, slot_id: str) -> None:
    if doc.section is MISSING or doc.section is None:
        raise RefError("section is not an array")
    slot = find_slot(doc, slot_id)
    position = section_position(doc, slot_id)
    doc.section.pop(position)
    del doc.slots[slot_id]


def init_section_empty(doc: RefDoc) -> None:
    if doc.section not in (MISSING, None):
        raise RefError("section is already an array")
    doc.section = []


def attachments_of(doc: RefDoc, section_id: str) -> RefSlot:
    return find_slot(doc, section_id)


def init_attachments_empty(doc: RefDoc, section_id: str) -> None:
    slot = find_slot(doc, section_id)
    if slot.fields is None:
        raise RefError("hole has no attachments")
    if slot.attachments not in (MISSING, None):
        raise RefError("attachments already an array")
    slot.attachments = []
    slot.attachment_ids = []


def insert_attachment(
    doc: RefDoc,
    section_id: str,
    slot_id: str,
    before: Optional[str],
    value: Any,
) -> None:
    slot = find_slot(doc, section_id)
    if slot.fields is None:
        raise RefError("hole has no attachments")
    if not isinstance(slot.attachments, list):
        raise RefError("attachments are not an array")
    if slot_id in doc.slots or slot_id in slot.attachment_ids:
        raise RefError("duplicate id")
    if before is not None and before not in slot.attachment_ids:
        raise RefError("bad before reference")
    position = (
        slot.attachment_ids.index(before) if before is not None
        else len(slot.attachment_ids)
    )
    slot.attachments.insert(position, value)
    slot.attachment_ids.insert(position, slot_id)


def delete_attachment(doc: RefDoc, section_id: str, slot_id: str) -> None:
    slot = find_slot(doc, section_id)
    if not isinstance(slot.attachments, list):
        raise RefError("attachments are not an array")
    if slot_id not in slot.attachment_ids:
        raise RefError("unknown attachment")
    index = slot.attachment_ids.index(slot_id)
    slot.attachment_ids.pop(index)
    slot.attachments.pop(index)
