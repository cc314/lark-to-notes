"""Tests for reaction envelope redaction and stable fingerprints (lw-pzj.10.9 / lw-pzj.1.2)."""

from __future__ import annotations

import json
from typing import Any

from lark_to_notes.intake.reaction_model import NormalizedReactionEvent, ReactionKind
from lark_to_notes.intake.reaction_redaction import (
    REACTION_IDENTITY_PLACEHOLDER,
    ReactionDisclosureMode,
    reaction_redaction_stable_fingerprint,
    redact_reaction_envelope_for_logs,
    redact_stored_reaction_payload_json,
)
from lark_to_notes.intake.reaction_store import insert_message_reaction_event
from lark_to_notes.storage.db import connect, init_db


def _envelope(
    *,
    event_id: str,
    open_id: str,
    message_id: str = "om_redact",
) -> dict[str, Any]:
    return {
        "header": {"event_type": "im.message.reaction.created_v1", "event_id": event_id},
        "event": {
            "message_id": message_id,
            "reaction_type": {"emoji_type": "THUMBSUP"},
            "operator_type": "user",
            "user_id": {"open_id": open_id, "user_id": "u_secret", "union_id": "union_secret"},
            "action_time": "1713096001000",
        },
    }


def test_redact_full_preserves_operator_ids() -> None:
    env = _envelope(event_id="evt-a", open_id="ou_alice")
    redacted = redact_reaction_envelope_for_logs(env, mode=ReactionDisclosureMode.FULL)
    assert redacted["event"]["user_id"]["open_id"] == "ou_alice"


def test_redact_restricted_scrubs_identity_fields() -> None:
    env = _envelope(event_id="evt-b", open_id="ou_bob")
    redacted = redact_reaction_envelope_for_logs(env, mode=ReactionDisclosureMode.RESTRICTED)
    uid = redacted["event"]["user_id"]
    assert uid["open_id"] == REACTION_IDENTITY_PLACEHOLDER
    assert uid["user_id"] == REACTION_IDENTITY_PLACEHOLDER
    assert uid["union_id"] == REACTION_IDENTITY_PLACEHOLDER
    dumped = json.dumps(redacted, ensure_ascii=False)
    assert "ou_bob" not in dumped
    assert "u_secret" not in dumped
    assert "union_secret" not in dumped


def test_redact_stored_payload_json_round_trip() -> None:
    env = _envelope(event_id="evt-c", open_id="ou_carol")
    raw = json.dumps(env, ensure_ascii=False, separators=(",", ":"))
    restricted = redact_stored_reaction_payload_json(raw, mode=ReactionDisclosureMode.RESTRICTED)
    parsed = json.loads(restricted)
    assert parsed["event"]["user_id"]["open_id"] == REACTION_IDENTITY_PLACEHOLDER


def test_stable_fingerprint_matches_for_different_operators_same_semantics() -> None:
    a = _envelope(event_id="evt-same", open_id="ou_one")
    b = _envelope(event_id="evt-same", open_id="ou_two")
    assert reaction_redaction_stable_fingerprint(a) == reaction_redaction_stable_fingerprint(b)


def test_stable_fingerprint_differs_when_event_id_differs() -> None:
    a = _envelope(event_id="evt-1", open_id="ou_x")
    b = _envelope(event_id="evt-2", open_id="ou_x")
    assert reaction_redaction_stable_fingerprint(a) != reaction_redaction_stable_fingerprint(b)


def test_sqlite_payload_retains_full_identity_until_redacted_view() -> None:
    """Storage path keeps PII; restricted redaction is opt-in at read/log time."""

    conn = connect(":memory:")
    init_db(conn)
    ev = NormalizedReactionEvent(
        reaction_event_id="evt-db",
        source_id="dm:ou_demo",
        message_id="om_z",
        reaction_kind=ReactionKind.ADD,
        emoji_type="THUMBSUP",
        operator_type="user",
        operator_open_id="ou_stored",
        operator_user_id="user_raw",
        operator_union_id="union_raw",
        action_time="1",
        payload=_envelope(event_id="evt-db", open_id="ou_stored", message_id="om_z"),
    )
    insert_message_reaction_event(conn, ev)
    row = conn.execute(
        "SELECT payload_json FROM message_reaction_events WHERE reaction_event_id = ?",
        ("evt-db",),
    ).fetchone()
    assert row is not None
    assert "ou_stored" in row["payload_json"]
    restricted = redact_stored_reaction_payload_json(
        row["payload_json"],
        mode=ReactionDisclosureMode.RESTRICTED,
    )
    assert "ou_stored" not in restricted
