"""Operator-safe views of IM reaction envelopes (lw-pzj.1.2, lw-pzj.10.9).

``message_reaction_events.payload_json`` remains **full-fidelity** for replay.
These helpers build **disclosure-controlled** copies for logs, docs, and
correlation fingerprints without treating redacted text as authoritative storage.
"""

from __future__ import annotations

import copy
import hashlib
import json
from enum import StrEnum
from typing import Any, cast

# Placeholder for operator identity fields in restricted disclosure (not a credential).
REACTION_IDENTITY_PLACEHOLDER = "<reaction-identity-redacted>"


class ReactionDisclosureMode(StrEnum):
    """How much operator identity to retain in derived views."""

    FULL = "full"
    RESTRICTED = "restricted"


def redact_reaction_envelope_for_logs(
    envelope: dict[str, Any],
    *,
    mode: ReactionDisclosureMode,
) -> dict[str, Any]:
    """Return a JSON-round-trippable copy of *envelope* for operator surfaces."""

    if mode is ReactionDisclosureMode.FULL:
        return cast(dict[str, Any], json.loads(json.dumps(envelope, ensure_ascii=False)))
    redacted = copy.deepcopy(envelope)
    event = redacted.get("event")
    if isinstance(event, dict):
        uid = event.get("user_id")
        if isinstance(uid, dict):
            for key in ("open_id", "user_id", "union_id"):
                if uid.get(key):
                    uid[key] = REACTION_IDENTITY_PLACEHOLDER
    return redacted


def redact_stored_reaction_payload_json(
    payload_json: str,
    *,
    mode: ReactionDisclosureMode,
) -> str:
    """Parse SQLite ``payload_json`` and return redacted JSON text."""

    try:
        parsed = json.loads(payload_json)
    except (TypeError, ValueError):
        return payload_json
    if not isinstance(parsed, dict):
        return payload_json
    safe = redact_reaction_envelope_for_logs(parsed, mode=mode)
    return json.dumps(safe, ensure_ascii=False, separators=(",", ":"))


def reaction_redaction_stable_fingerprint(envelope: dict[str, Any]) -> str:
    """Stable SHA-256 prefix (40 hex chars) over a PII-free reaction identity tuple.

    Same logical platform event keeps the same fingerprint even when operator
    open/user/union ids differ, so restricted-mode logs can still correlate rows.
    """

    header = envelope.get("header")
    et = eid = ""
    if isinstance(header, dict):
        et = str(header.get("event_type") or "").strip()
        eid = str(header.get("event_id") or "").strip()
    event = envelope.get("event")
    message_id = emoji_type = action_time = ""
    if isinstance(event, dict):
        message_id = str(event.get("message_id") or "").strip()
        action_time = str(event.get("action_time") or "").strip()
        rt = event.get("reaction_type")
        if isinstance(rt, dict):
            emoji_type = str(rt.get("emoji_type") or "").strip()
    key = json.dumps(
        {
            "action_time": action_time,
            "emoji_type": emoji_type,
            "event_id": eid,
            "event_type": et,
            "message_id": message_id,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:40]
