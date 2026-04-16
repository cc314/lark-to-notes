"""Unit tests for :mod:`lark_to_notes.intake.reaction_model`."""

from __future__ import annotations

from lark_to_notes.intake.reaction_model import (
    NormalizedReactionEvent,
    ReactionKind,
    parse_reaction_envelope,
    reaction_event_id_from_envelope,
    reaction_event_type,
)


def _header(event_type: str, event_id: str = "ev-1") -> dict[str, str]:
    return {
        "event_id": event_id,
        "event_type": event_type,
        "create_time": "1608725989000",
    }


def test_reaction_event_type_and_id() -> None:
    env = {"header": _header("im.message.reaction.created_v1", "abc")}
    assert reaction_event_type(env) == "im.message.reaction.created_v1"
    assert reaction_event_id_from_envelope(env) == "abc"
    assert reaction_event_id_from_envelope({}) == ""


def test_parse_created_happy_path() -> None:
    env = {
        "header": _header("im.message.reaction.created_v1"),
        "event": {
            "message_id": "om_msg1",
            "reaction_type": {"emoji_type": "SMILE"},
            "operator_type": "user",
            "user_id": {
                "open_id": "ou_1",
                "user_id": "u1",
                "union_id": "on_1",
            },
            "action_time": "1627641418803",
        },
    }
    got = parse_reaction_envelope(env, source_id="dm:alice")
    assert isinstance(got, NormalizedReactionEvent)
    assert got.reaction_event_id == "ev-1"
    assert got.source_id == "dm:alice"
    assert got.message_id == "om_msg1"
    assert got.reaction_kind is ReactionKind.ADD
    assert got.emoji_type == "SMILE"
    assert got.operator_type == "user"
    assert got.operator_open_id == "ou_1"
    assert got.operator_user_id == "u1"
    assert got.operator_union_id == "on_1"
    assert got.action_time == "1627641418803"
    assert "header" in got.payload and "event" in got.payload
    assert '"SMILE"' in got.payload_json()


def test_parse_deleted_flat_emoji() -> None:
    env = {
        "header": _header("im.message.reaction.deleted_v1", "del-9"),
        "event": {
            "message_id": "om_x",
            "emoji_type": "THUMBSUP",
            "operator_type": "user",
            "user_id": {"open_id": "ou_z"},
            "action_time": "1",
        },
    }
    got = parse_reaction_envelope(env, source_id="group:g1")
    assert got is not None
    assert got.reaction_kind is ReactionKind.REMOVE
    assert got.emoji_type == "THUMBSUP"
    assert got.reaction_event_id == "del-9"


def test_parse_skips_non_reaction_and_missing_message_id() -> None:
    assert (
        parse_reaction_envelope({"header": _header("im.message.receive_v1")}, source_id="dm:a")
        is None
    )
    assert (
        parse_reaction_envelope(
            {"header": _header("im.message.reaction.created_v1"), "event": {}},
            source_id="dm:a",
        )
        is None
    )


def test_unknown_event_keys_round_trip_in_payload() -> None:
    env = {
        "header": _header("im.message.reaction.created_v1"),
        "event": {
            "message_id": "m1",
            "reaction_type": {"emoji_type": "X", "future_field": 1},
            "operator_type": "",
        },
    }
    got = parse_reaction_envelope(env, source_id="dm:x")
    assert got is not None
    ev = got.payload["event"]
    assert isinstance(ev, dict)
    assert ev.get("reaction_type", {}).get("future_field") == 1
