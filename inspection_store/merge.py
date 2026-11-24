"""Three-way offline merge of single-device inspection drafts.

A *draft* is the identified JSON form of one device document: the same
nested shape as :meth:`DeviceStore.load_document`, but every array element
— section slots and attachment slots, holes included — carries its stable
slot id next to its payload::

    {"key": "D1",
     "section": [
       {"id": "S1", "slot": null},
       {"id": "S2", "slot": {"fields": {"torque": 10},
                             "attachments": [
                                 {"id": "A1", "slot": {"photo": "front"}},
                                 {"id": "A2", "slot": null}]}}]}

Absent, ``null``, ``[]`` and populated arrays stay distinct at both levels,
and a slot id may appear at most once in one draft.

The merge is a three-way merge over stable identities:

* a base slot that survives in a branch keeps its array, its hole identity
  and its relative order among kept base siblings; a branch that moves,
  re-holes or reorders base slots is invalid input;
* scalar fields merge per name: one side changed it -> take that side, both
  sides changed it identically -> take it, otherwise conflict; adding and
  removing fields are changes too;
* a base slot deleted on only one side stays deleted only if the other side
  left the slot and everything under it untouched (inserting neighbours in
  the parent array is fine; touching its attachments is a conflict);
* new slots follow the nearest preceding base slot of their own draft that
  survives the merge (array start when there is none); at one anchor the
  local side's newcomers come first, each side keeping its own order, and a
  newcomer is never lost or re-parented because its neighbour was deleted;
* the same new id on both sides, or a merged id owned by another device in
  the store, is reported — never silently merged or renumbered.

Planning is pure and O(n): one pass per array with id maps.  No slot pairs
are enumerated and no parent array is rescanned per newcomer, so drafts
with tens of thousands of slots merge quickly.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

from .codec import ABSENT, StoreError, ValidationError, check_slot_fields


class MergeConflict(StoreError):
    """The two branch drafts cannot be reconciled; the store is untouched."""


class StaleLocal(StoreError):
    """The store no longer matches the submitted local draft."""


# An array position is owned by the device (its section array) or by one
# section slot (its attachment array).  Owners are compared by identity of
# the position, not by slot content.
_SECTION_OWNER = ("section",)


def _attachments_owner(section_id: str) -> Tuple[str, str]:
    return ("attachments", section_id)


def _owner_name(owner: Any) -> str:
    if owner == _SECTION_OWNER:
        return "the section array"
    return f"the attachments of section slot {owner[1]!r}"


# ---------------------------------------------------------------------------
# Normalized draft representation


@dataclass(frozen=True)
class AttDraft:
    """One attachment array element: stable id, hole flag, scalar fields."""

    slot_id: str
    hole: bool
    fields: Tuple[Tuple[str, Any], ...]


@dataclass(frozen=True)
class SlotDraft:
    """One section array element: stable id, hole flag, fields, attachments."""

    slot_id: str
    hole: bool
    fields: Tuple[Tuple[str, Any], ...]
    attachments: Any  # ABSENT | None | Tuple[AttDraft, ...]


@dataclass(frozen=True)
class Draft:
    """A whole identified device draft."""

    key: str
    section: Any  # ABSENT | None | Tuple[SlotDraft, ...]


# ---------------------------------------------------------------------------
# Parsing and serialization


def _check_id(value: Any, used: Set[str], label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValidationError(f"{label} draft: slot id must be a non-empty string")
    if value in used:
        raise ValidationError(f"{label} draft: duplicate slot id {value!r}")
    used.add(value)
    return value


def _parse_attachment_element(item: Any, used: Set[str], label: str) -> AttDraft:
    if not isinstance(item, dict):
        raise ValidationError(
            f"{label} draft: attachment elements must be objects with 'id' and 'slot'"
        )
    extra = set(item) - {"id", "slot"}
    if extra:
        raise ValidationError(
            f"{label} draft: unknown attachment element keys: {sorted(extra)}"
        )
    if "id" not in item or "slot" not in item:
        raise ValidationError(
            f"{label} draft: attachment elements need both 'id' and 'slot'"
        )
    slot_id = _check_id(item["id"], used, label)
    payload = item["slot"]
    if payload is None:
        return AttDraft(slot_id, True, ())
    if not isinstance(payload, dict):
        raise ValidationError(
            f"{label} draft: attachment payload of {slot_id!r} must be null or an object"
        )
    try:
        fields = check_slot_fields(payload)
    except ValidationError as exc:
        raise ValidationError(f"{label} draft, attachment {slot_id!r}: {exc}") from exc
    return AttDraft(slot_id, False, fields)


def _parse_section_element(item: Any, used: Set[str], label: str) -> SlotDraft:
    if not isinstance(item, dict):
        raise ValidationError(
            f"{label} draft: section elements must be objects with 'id' and 'slot'"
        )
    extra = set(item) - {"id", "slot"}
    if extra:
        raise ValidationError(
            f"{label} draft: unknown section element keys: {sorted(extra)}"
        )
    if "id" not in item or "slot" not in item:
        raise ValidationError(
            f"{label} draft: section elements need both 'id' and 'slot'"
        )
    slot_id = _check_id(item["id"], used, label)
    payload = item["slot"]
    if payload is None:
        return SlotDraft(slot_id, True, (), ABSENT)
    if not isinstance(payload, dict):
        raise ValidationError(
            f"{label} draft: slot payload of {slot_id!r} must be null or an object"
        )
    extra = set(payload) - {"fields", "attachments"}
    if extra:
        raise ValidationError(
            f"{label} draft: unknown slot keys for {slot_id!r}: {sorted(extra)}"
        )
    try:
        fields = check_slot_fields(payload.get("fields", {}))
    except ValidationError as exc:
        raise ValidationError(f"{label} draft, slot {slot_id!r}: {exc}") from exc
    att_raw = payload["attachments"] if "attachments" in payload else ABSENT
    if att_raw is ABSENT:
        attachments: Any = ABSENT
    elif att_raw is None:
        attachments = None
    elif isinstance(att_raw, list):
        attachments = tuple(
            _parse_attachment_element(element, used, label) for element in att_raw
        )
    else:
        raise ValidationError(
            f"{label} draft: attachments of {slot_id!r} must be absent, null, or an array"
        )
    return SlotDraft(slot_id, False, fields, attachments)


def parse_draft(raw: Any, label: str = "draft") -> Draft:
    """Validate an identified draft and normalize it for merging.

    ``label`` names the draft ("base" / "local" / "theirs") so an invalid
    input message says which file is at fault.
    """
    if not isinstance(raw, dict):
        raise ValidationError(f"{label} draft must be a JSON object")
    extra = set(raw) - {"key", "section"}
    if extra:
        raise ValidationError(f"{label} draft has unknown keys: {sorted(extra)}")
    key = raw.get("key")
    if not isinstance(key, str) or not key:
        raise ValidationError(f"{label} draft key must be a non-empty string")
    used: Set[str] = set()
    section_raw = raw["section"] if "section" in raw else ABSENT
    if section_raw is ABSENT:
        section: Any = ABSENT
    elif section_raw is None:
        section = None
    elif isinstance(section_raw, list):
        section = tuple(
            _parse_section_element(element, used, label) for element in section_raw
        )
    else:
        raise ValidationError(
            f"{label} draft: section must be absent, null, or an array"
        )
    return Draft(key=key, section=section)


def _attachment_element_json(att: AttDraft) -> Dict[str, Any]:
    if att.hole:
        return {"id": att.slot_id, "slot": None}
    return {"id": att.slot_id, "slot": dict(att.fields)}


def _section_element_json(slot: SlotDraft) -> Dict[str, Any]:
    if slot.hole:
        return {"id": slot.slot_id, "slot": None}
    payload: Dict[str, Any] = {"fields": dict(slot.fields)}
    if slot.attachments is ABSENT:
        pass
    elif slot.attachments is None:
        payload["attachments"] = None
    else:
        payload["attachments"] = [
            _attachment_element_json(att) for att in slot.attachments
        ]
    return {"id": slot.slot_id, "slot": payload}


def draft_to_json(draft: Draft) -> Dict[str, Any]:
    """The identified JSON form of a normalized draft."""
    out: Dict[str, Any] = {"key": draft.key}
    if draft.section is ABSENT:
        return out
    if draft.section is None:
        out["section"] = None
        return out
    out["section"] = [_section_element_json(slot) for slot in draft.section]
    return out


def draft_to_plain_document(draft: Draft) -> Dict[str, Any]:
    """The nested document form (no ids) accepted by ``import_document``."""
    if draft.section is ABSENT:
        return {}
    if draft.section is None:
        return {"section": None}
    out: List[Any] = []
    for slot in draft.section:
        if slot.hole:
            out.append(None)
            continue
        item: Dict[str, Any] = {"fields": dict(slot.fields)}
        if slot.attachments is ABSENT:
            pass
        elif slot.attachments is None:
            item["attachments"] = None
        else:
            item["attachments"] = [
                None if att.hole else dict(att.fields) for att in slot.attachments
            ]
        out.append(item)
    return {"section": out}


def draft_import_ids(draft: Draft) -> List[Tuple[str, str]]:
    """(kind, slot id) pairs in the order ``import_document`` consumes them."""
    plan: List[Tuple[str, str]] = []
    if isinstance(draft.section, tuple):
        for slot in draft.section:
            plan.append(("section", slot.slot_id))
            if isinstance(slot.attachments, tuple):
                for att in slot.attachments:
                    plan.append(("attachment", att.slot_id))
    return plan


def draft_ids(draft: Draft) -> Iterator[str]:
    """Every slot id in the draft, section ids before their attachments."""
    for _, slot_id in draft_import_ids(draft):
        yield slot_id


# ---------------------------------------------------------------------------
# Branch validation against the base draft


def _walk_arrays(draft: Draft) -> Iterator[Tuple[Any, Tuple[Any, ...]]]:
    """Yield (owner, array) for every array position that is an array."""
    if isinstance(draft.section, tuple):
        yield _SECTION_OWNER, draft.section
        for slot in draft.section:
            if isinstance(slot.attachments, tuple):
                yield _attachments_owner(slot.slot_id), slot.attachments


def _index_base(base: Draft) -> Tuple[Dict[str, Tuple[Any, bool]], Dict[Any, List[str]]]:
    """Map every base slot id to its owner and hole flag, and owners to order."""
    index: Dict[str, Tuple[Any, bool]] = {}
    order: Dict[Any, List[str]] = {}
    for owner, array in _walk_arrays(base):
        order[owner] = []
        for slot in array:
            index[slot.slot_id] = (owner, slot.hole)
            order[owner].append(slot.slot_id)
    return index, order


def _validate_branch(
    base_index: Dict[str, Tuple[Any, bool]],
    base_order: Dict[Any, List[str]],
    branch: Draft,
    label: str,
) -> Set[str]:
    """Check one branch against the base; return the ids the branch adds.

    A base slot may only disappear (deletion) or stay where it was with the
    same hole identity and the same relative order among kept siblings.
    Moving a base slot to another array, changing its hole identity, or
    reordering old slots is invalid input, not a merge decision.
    """
    new_ids: Set[str] = set()
    for owner, array in _walk_arrays(branch):
        kept: List[str] = []
        for slot in array:
            entry = base_index.get(slot.slot_id)
            if entry is None:
                new_ids.add(slot.slot_id)
                continue
            base_owner, base_hole = entry
            if base_owner != owner:
                raise ValidationError(
                    f"{label} draft: base slot {slot.slot_id!r} moved from "
                    f"{_owner_name(base_owner)} to {_owner_name(owner)}"
                )
            if base_hole != slot.hole:
                raise ValidationError(
                    f"{label} draft: base slot {slot.slot_id!r} changed its "
                    "hole identity"
                )
            kept.append(slot.slot_id)
        kept_set = set(kept)
        expected = [sid for sid in base_order.get(owner, ()) if sid in kept_set]
        if kept != expected:
            raise ValidationError(
                f"{label} draft: base slots reordered in {_owner_name(owner)}"
            )
    return new_ids


# ---------------------------------------------------------------------------
# Merge planning


def _shape_word(value: Any) -> str:
    if value is ABSENT:
        return "absent"
    if value is None:
        return "null"
    if isinstance(value, tuple):
        return "an empty array" if not value else f"an array of {len(value)}"
    return "a value"  # pragma: no cover - defensive


def _fmt_value(value: Any) -> str:
    return "absent" if value is ABSENT else repr(value)


def _merge_fields(
    base: Tuple[Tuple[str, Any], ...],
    local: Tuple[Tuple[str, Any], ...],
    theirs: Tuple[Tuple[str, Any], ...],
    path: str,
) -> Tuple[Tuple[str, Any], ...]:
    """Three-way merge of one scalar field map, per field name."""
    b = dict(base)
    l = dict(local)
    t = dict(theirs)
    out: List[Tuple[str, Any]] = []
    for name in sorted(set(b) | set(l) | set(t)):
        bv = b.get(name, ABSENT)
        lv = l.get(name, ABSENT)
        tv = t.get(name, ABSENT)
        if lv == tv:
            chosen = lv
        elif bv == lv:
            chosen = tv
        elif bv == tv:
            chosen = lv
        else:
            raise MergeConflict(
                f"{path}: field {name!r} changed both ways "
                f"(base {_fmt_value(bv)}, local {_fmt_value(lv)}, "
                f"theirs {_fmt_value(tv)})"
            )
        if chosen is not ABSENT:
            out.append((name, chosen))
    return tuple(out)


def _merge_attachment(
    base: AttDraft, local: AttDraft, theirs: AttDraft, path: str, slot_id: str
) -> AttDraft:
    if base.hole:
        # Hole identity is validated per branch, so all three are the same hole.
        return base
    fields = _merge_fields(
        base.fields, local.fields, theirs.fields, f"{path} attachment {slot_id!r}"
    )
    return AttDraft(slot_id, False, fields)


def _merge_section_slot(
    base: SlotDraft, local: SlotDraft, theirs: SlotDraft, path: str, slot_id: str
) -> SlotDraft:
    if base.hole:
        return base
    fields = _merge_fields(
        base.fields, local.fields, theirs.fields, f"section slot {slot_id!r}"
    )
    attachments = _merge_value(
        base.attachments,
        local.attachments,
        theirs.attachments,
        f"attachments of section slot {slot_id!r}",
        _merge_attachment,
    )
    return SlotDraft(slot_id, False, fields, attachments)


def _merge_array(
    base: Tuple[Any, ...],
    local: Tuple[Any, ...],
    theirs: Tuple[Any, ...],
    path: str,
    merge_slot: Any,
) -> Tuple[Any, ...]:
    """Merge two changed arrays slot by slot, in a single pass per draft."""
    base_map = {slot.slot_id: slot for slot in base}
    local_map = {slot.slot_id: slot for slot in local}
    theirs_map = {slot.slot_id: slot for slot in theirs}

    # First decide which base slots survive.  A slot deleted on only one
    # side survives deletion only when the other side left it untouched.
    survivors: List[str] = []
    for slot in base:
        slot_id = slot.slot_id
        in_local = slot_id in local_map
        in_theirs = slot_id in theirs_map
        if in_local and in_theirs:
            survivors.append(slot_id)
        elif in_local:
            if local_map[slot_id] != slot:
                raise MergeConflict(
                    f"{path}: slot {slot_id!r} was deleted by theirs but "
                    "modified by local"
                )
        elif in_theirs:
            if theirs_map[slot_id] != slot:
                raise MergeConflict(
                    f"{path}: slot {slot_id!r} was deleted by local but "
                    "modified by theirs"
                )
    survivor_set = set(survivors)

    def newcomers(branch: Tuple[Any, ...]) -> Dict[Optional[str], List[Any]]:
        """Group new slots by their nearest preceding surviving base slot."""
        groups: Dict[Optional[str], List[Any]] = {}
        anchor: Optional[str] = None
        for slot in branch:
            slot_id = slot.slot_id
            if slot_id in base_map:
                if slot_id in survivor_set:
                    anchor = slot_id
                # A deleted base slot is skipped: the anchor stays with the
                # nearest *surviving* predecessor, so the newcomer is never
                # lost or re-parented.
            else:
                groups.setdefault(anchor, []).append(slot)
        return groups

    local_new = newcomers(local)
    theirs_new = newcomers(theirs)

    out: List[Any] = []
    out.extend(local_new.get(None, ()))
    out.extend(theirs_new.get(None, ()))
    for slot_id in survivors:
        out.append(
            merge_slot(
                base_map[slot_id],
                local_map[slot_id],
                theirs_map[slot_id],
                path,
                slot_id,
            )
        )
        out.extend(local_new.get(slot_id, ()))
        out.extend(theirs_new.get(slot_id, ()))
    return tuple(out)


def _merge_value(
    base: Any, local: Any, theirs: Any, path: str, merge_slot: Any
) -> Any:
    """Merge one array position (section value or attachments value)."""
    if local == theirs:
        return local
    if base == local:
        return theirs
    if base == theirs:
        return local
    if isinstance(local, tuple) and isinstance(theirs, tuple):
        # Both sides built arrays.  When the base was not an array, every
        # slot on both sides counts as new.
        base_array = base if isinstance(base, tuple) else ()
        return _merge_array(base_array, local, theirs, path, merge_slot)
    raise MergeConflict(
        f"{path}: both sides changed it (base is {_shape_word(base)}, local "
        f"is {_shape_word(local)}, theirs is {_shape_word(theirs)}) and the "
        "results differ"
    )


def merge_drafts(base: Draft, local: Draft, theirs: Draft) -> Draft:
    """Plan the three-way merge of one device's drafts.

    Pure: raises ValidationError for invalid input and MergeConflict when
    the two branches cannot be reconciled.  Runs in O(draft size).
    """
    if base.key != local.key or base.key != theirs.key:
        raise ValidationError("all three drafts must carry the same device key")
    base_index, base_order = _index_base(base)
    new_local = _validate_branch(base_index, base_order, local, "local")
    new_theirs = _validate_branch(base_index, base_order, theirs, "theirs")
    shared = new_local & new_theirs
    if shared:
        raise MergeConflict(
            f"new slot id {sorted(shared)[0]!r} was added by both sides; it "
            "cannot be merged or renumbered silently"
        )
    section = _merge_value(
        base.section, local.section, theirs.section, "section", _merge_section_slot
    )
    return Draft(key=base.key, section=section)


# ---------------------------------------------------------------------------
# Difference location (for stale-local reports)


def _fields_difference(
    current: Tuple[Tuple[str, Any], ...], submitted: Tuple[Tuple[str, Any], ...]
) -> str:
    have = dict(current)
    want = dict(submitted)
    for name in sorted(set(have) | set(want)):
        a = have.get(name, ABSENT)
        b = want.get(name, ABSENT)
        if a != b:
            return (
                f"field {name!r}: store has {_fmt_value(a)}, local draft has "
                f"{_fmt_value(b)}"
            )
    return "fields differ"  # pragma: no cover - defensive


def _diff_slot(current: Any, submitted: Any, path: str) -> Optional[str]:
    here = f"{path} slot {current.slot_id!r}"
    if current.hole != submitted.hole:
        return f"{here}: hole identity differs"
    if current.fields != submitted.fields:
        return f"{here}: {_fields_difference(current.fields, submitted.fields)}"
    if isinstance(current, SlotDraft) and isinstance(submitted, SlotDraft):
        return _diff_value(
            current.attachments,
            submitted.attachments,
            f"attachments of section slot {current.slot_id!r}",
        )
    return None


def _diff_value(current: Any, submitted: Any, path: str) -> Optional[str]:
    current_is_array = isinstance(current, tuple)
    submitted_is_array = isinstance(submitted, tuple)
    if (
        current_is_array != submitted_is_array
        or (current is ABSENT) != (submitted is ABSENT)
        or (current is None) != (submitted is None)
    ):
        return (
            f"{path}: store has {_shape_word(current)}, local draft has "
            f"{_shape_word(submitted)}"
        )
    if not current_is_array:
        return None
    current_ids = [slot.slot_id for slot in current]
    submitted_ids = [slot.slot_id for slot in submitted]
    if current_ids != submitted_ids:
        for position, pair in enumerate(zip(current_ids, submitted_ids)):
            if pair[0] != pair[1]:
                return (
                    f"{path}[{position}]: store has slot {pair[0]!r}, local "
                    f"draft has {pair[1]!r}"
                )
        return (
            f"{path}: store has {len(current_ids)} slots, local draft has "
            f"{len(submitted_ids)}"
        )
    for current_slot, submitted_slot in zip(current, submitted):
        detail = _diff_slot(current_slot, submitted_slot, path)
        if detail is not None:
            return detail
    return None


def first_difference(current: Draft, submitted: Draft) -> str:
    """Locate the first difference between the store's draft and the local one."""
    if current.key != submitted.key:
        return (
            f"device key is {current.key!r} in the store but "
            f"{submitted.key!r} in the local draft"
        )
    detail = _diff_value(current.section, submitted.section, "section")
    return detail or "drafts differ"
