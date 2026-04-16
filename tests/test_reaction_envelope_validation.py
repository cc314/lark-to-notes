"""Tests for :mod:`lark_to_notes.live.reaction_envelope_validation`."""

from __future__ import annotations

from lark_to_notes.live.reaction_envelope_validation import (
    reaction_envelope_is_valid,
    reaction_envelope_validation_errors,
)


def _header(event_type: str) -> dict[str, str]:
    return {"event_type": event_type, "event_id": "e1"}


def test_valid_created_minimal() -> None:
    env = {
        "header": _header("im.message.reaction.created_v1"),
        "event": {"message_id": "om_1", "emoji_type": "OK"},
    }
    assert reaction_envelope_validation_errors(env) == []
    assert reaction_envelope_is_valid(env) is True


def test_valid_deleted_minimal() -> None:
    env = {
        "header": _header("im.message.reaction.deleted_v1"),
        "event": {"message_id": "om_1"},
    }
    assert reaction_envelope_validation_errors(env) == []


def test_rejects_non_object() -> None:
    assert reaction_envelope_validation_errors(1) == ["envelope_not_object"]


def test_rejects_wrong_event_type() -> None:
    env = {"header": _header("im.message.receive_v1"), "event": {"message_id": "x"}}
    assert "unsupported_reaction_event_type" in reaction_envelope_validation_errors(env)


def test_rejects_missing_message_id() -> None:
    env = {
        "header": _header("im.message.reaction.created_v1"),
        "event": {"emoji_type": "OK"},
    }
    assert "missing_event_message_id" in reaction_envelope_validation_errors(env)


def test_rejects_missing_event() -> None:
    env = {"header": _header("im.message.reaction.created_v1")}
    assert "missing_event_object" in reaction_envelope_validation_errors(env)
