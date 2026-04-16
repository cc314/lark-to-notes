"""Compatibility API for reaction envelope checks (sync-events pipe).

Delegates to :mod:`lark_to_notes.live.reaction_envelopes` and maps error codes to
the legacy string tags expected by older call sites and tests.
"""

from __future__ import annotations

from typing import Any

_LEGACY_ERROR_TAGS: dict[str, str] = {
    "invalid_header": "missing_header_object",
    "not_reaction_event": "unsupported_reaction_event_type",
    "missing_event": "missing_event_object",
    "missing_message_id": "missing_event_message_id",
}


def reaction_envelope_validation_errors(envelope: object) -> list[str]:
    """Return human-readable error tags for envelopes that cannot be ingested."""

    if not isinstance(envelope, dict):
        return ["envelope_not_object"]

    from lark_to_notes.live.reaction_envelopes import validate_im_message_reaction_envelope

    result = validate_im_message_reaction_envelope(envelope)
    if result.ok:
        return []
    return [_LEGACY_ERROR_TAGS.get(code, code) for code in result.errors]


def reaction_envelope_is_valid(envelope: Any) -> bool:
    """Return ``True`` when :func:`reaction_envelope_validation_errors` is empty."""

    return not reaction_envelope_validation_errors(envelope)
