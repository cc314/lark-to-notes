"""Pure parsing for Lark IM reaction event envelopes.

Maps ``im.message.reaction.created_v1`` / ``deleted_v1`` into
:class:`NormalizedReactionEvent`, aligned with ``message_reaction_events`` in
``storage/schema.py`` (v9). ``source_id`` is caller-supplied (watched source).
``reaction_event_id`` comes from ``header.event_id`` and may be empty until
surrogate logic (lw-pzj.3.3) runs at persistence time.

Unknown envelope keys are tolerated; optional scalar fields default to empty
strings. ``payload`` keeps a compact ``header`` + ``event`` copy for replay.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class ReactionKind(StrEnum):
    """Logical reaction change kind stored as ``reaction_kind`` in SQLite."""

    ADD = "add"
    REMOVE = "remove"


_CREATED = "im.message.reaction.created_v1"
_DELETED = "im.message.reaction.deleted_v1"


@dataclass(frozen=True)
class NormalizedReactionEvent:
    """Normalized reaction sighting aligned with ``message_reaction_events``."""

    reaction_event_id: str
    source_id: str
    message_id: str
    reaction_kind: ReactionKind
    emoji_type: str
    operator_type: str
    operator_open_id: str
    operator_user_id: str
    operator_union_id: str
    action_time: str
    payload: dict[str, Any]

    def payload_json(self) -> str:
        """Serialize ``payload`` for SQLite ``payload_json``."""
        return json.dumps(self.payload, ensure_ascii=False, separators=(",", ":"))


def reaction_event_type(envelope: dict[str, Any]) -> str:
    """Return ``header.event_type`` for a decoded envelope dict."""
    header = envelope.get("header")
    if isinstance(header, dict):
        return str(header.get("event_type") or "").strip()
    return ""


def reaction_event_id_from_envelope(envelope: dict[str, Any]) -> str:
    """Return ``header.event_id`` when present (may be empty)."""
    header = envelope.get("header")
    if not isinstance(header, dict):
        return ""
    return str(header.get("event_id") or "").strip()


def _coerce_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _extract_emoji_type(event: dict[str, Any]) -> str:
    rt = event.get("reaction_type")
    if isinstance(rt, dict):
        return _coerce_str(rt.get("emoji_type"))
    return _coerce_str(event.get("emoji_type"))


def _extract_operator_ids(event: dict[str, Any]) -> tuple[str, str, str, str]:
    op_type = _coerce_str(event.get("operator_type"))
    uid = event.get("user_id")
    if not isinstance(uid, dict):
        return op_type, "", "", ""
    return (
        op_type,
        _coerce_str(uid.get("open_id")),
        _coerce_str(uid.get("user_id")),
        _coerce_str(uid.get("union_id")),
    )


def parse_reaction_envelope(
    envelope: dict[str, Any],
    *,
    source_id: str,
) -> NormalizedReactionEvent | None:
    """Parse a reaction created/deleted envelope.

    Args:
        envelope: One decoded NDJSON object from the operator event pipe.
        source_id: Watched-source identifier for this stream (required context).

    Returns:
        A :class:`NormalizedReactionEvent`, or ``None`` if the envelope is not a
        supported reaction event or lacks a ``message_id``.
    """
    et = reaction_event_type(envelope)
    if et == _CREATED:
        kind = ReactionKind.ADD
    elif et == _DELETED:
        kind = ReactionKind.REMOVE
    else:
        return None

    event = envelope.get("event")
    if not isinstance(event, dict):
        return None

    message_id = _coerce_str(event.get("message_id"))
    if not message_id:
        return None

    op_type, open_id, user_id, union_id = _extract_operator_ids(event)
    header = envelope.get("header") if isinstance(envelope.get("header"), dict) else {}
    payload: dict[str, Any] = {
        "header": dict(header) if isinstance(header, dict) else {},
        "event": dict(event),
    }

    return NormalizedReactionEvent(
        reaction_event_id=reaction_event_id_from_envelope(envelope),
        source_id=_coerce_str(source_id),
        message_id=message_id,
        reaction_kind=kind,
        emoji_type=_extract_emoji_type(event),
        operator_type=op_type,
        operator_open_id=open_id,
        operator_user_id=user_id,
        operator_union_id=union_id,
        action_time=_coerce_str(event.get("action_time")),
        payload=payload,
    )
