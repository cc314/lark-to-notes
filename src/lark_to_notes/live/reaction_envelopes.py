"""Strict, non-throwing validation for Lark IM reaction event envelopes.

Used by the live NDJSON pipeline before persistence. Callers must treat
:func:`validate_im_message_reaction_envelope` as the gate for
``im.message.reaction.created_v1`` / ``im.message.reaction.deleted_v1`` rows:
it never raises; failures return structured error codes for metrics/logging.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

_REACTION_CREATED_V1 = "im.message.reaction.created_v1"
_REACTION_DELETED_V1 = "im.message.reaction.deleted_v1"


@dataclass(frozen=True)
class ReactionEnvelopeValidation:
    """Result of validating one reaction envelope."""

    ok: bool
    errors: tuple[str, ...]
    event_type: str
    message_id: str


def im_message_reaction_event_types() -> frozenset[str]:
    """Return the Feishu/Lark reaction ``header.event_type`` values we handle."""
    return frozenset({_REACTION_CREATED_V1, _REACTION_DELETED_V1})


def is_im_message_reaction_event_type(event_type: str) -> bool:
    """Return whether *event_type* is a supported reaction envelope kind."""
    return str(event_type or "").strip() in im_message_reaction_event_types()


def _header_dict(envelope: dict[str, Any]) -> dict[str, Any] | None:
    header = envelope.get("header")
    return header if isinstance(header, dict) else None


def _header_event_type(envelope: dict[str, Any]) -> str:
    header = _header_dict(envelope)
    if header is None:
        return ""
    return str(header.get("event_type") or "").strip()


def _event_emoji_type(event: dict[str, Any]) -> str:
    rt = event.get("reaction_type")
    if isinstance(rt, dict):
        s = str(rt.get("emoji_type") or "").strip()
        if s:
            return s
    return str(event.get("emoji_type") or "").strip()


def validate_im_message_reaction_envelope(envelope: dict[str, Any]) -> ReactionEnvelopeValidation:
    """Validate a decoded reaction envelope dict.

    Rules:

    - ``header.event_type`` must be created or deleted reaction v1.
    - ``event`` must be an object with non-empty ``message_id``.
    - **created** requires a non-empty emoji key (``reaction_type.emoji_type`` or
      ``emoji_type``). **deleted** only requires ``message_id`` (emoji may be
      omitted on some tenants).
    """
    if not isinstance(envelope, dict):
        return ReactionEnvelopeValidation(False, ("envelope_not_object",), "", "")

    header = _header_dict(envelope)
    if header is None:
        return ReactionEnvelopeValidation(False, ("invalid_header",), "", "")

    et = _header_event_type(envelope)
    if not is_im_message_reaction_event_type(et):
        return ReactionEnvelopeValidation(False, ("not_reaction_event",), et, "")

    event = envelope.get("event")
    if not isinstance(event, dict):
        return ReactionEnvelopeValidation(False, ("missing_event",), et, "")

    message_id = str(event.get("message_id") or "").strip()
    if not message_id:
        return ReactionEnvelopeValidation(False, ("missing_message_id",), et, "")

    if et == _REACTION_CREATED_V1 and not _event_emoji_type(event):
        return ReactionEnvelopeValidation(False, ("missing_emoji_type",), et, message_id)

    return ReactionEnvelopeValidation(True, (), et, message_id)
