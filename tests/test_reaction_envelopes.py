"""Tests for :mod:`lark_to_notes.live.reaction_envelopes`."""

from __future__ import annotations

from typing import Any

import pytest

from lark_to_notes.live.reaction_envelopes import (
    ReactionEnvelopeValidation,
    is_im_message_reaction_event_type,
    validate_im_message_reaction_envelope,
)


def _hdr(et: str) -> dict[str, str]:
    return {"event_type": et, "event_id": "e1"}


@pytest.mark.parametrize(
    ("env", "expect"),
    [
        (
            1,
            ReactionEnvelopeValidation(False, ("envelope_not_object",), "", ""),
        ),
        (
            {"header": "x"},
            ReactionEnvelopeValidation(False, ("invalid_header",), "", ""),
        ),
        (
            {"header": {"event_type": "im.message.receive_v1"}, "event": {}},
            ReactionEnvelopeValidation(False, ("not_reaction_event",), "im.message.receive_v1", ""),
        ),
        (
            {"header": _hdr("im.message.reaction.created_v1"), "event": "bad"},
            ReactionEnvelopeValidation(
                False, ("missing_event",), "im.message.reaction.created_v1", ""
            ),
        ),
        (
            {"header": _hdr("im.message.reaction.created_v1"), "event": {"message_id": ""}},
            ReactionEnvelopeValidation(
                False,
                ("missing_message_id",),
                "im.message.reaction.created_v1",
                "",
            ),
        ),
        (
            {
                "header": _hdr("im.message.reaction.created_v1"),
                "event": {"message_id": "m1"},
            },
            ReactionEnvelopeValidation(
                False,
                ("missing_emoji_type",),
                "im.message.reaction.created_v1",
                "m1",
            ),
        ),
    ],
)
def test_validate_reaction_envelope_failures(
    env: Any,
    expect: ReactionEnvelopeValidation,
) -> None:
    assert validate_im_message_reaction_envelope(env) == expect


def test_validate_created_happy_nested_emoji() -> None:
    env = {
        "header": _hdr("im.message.reaction.created_v1"),
        "event": {
            "message_id": "om_x",
            "reaction_type": {"emoji_type": "OK"},
            "operator_type": "user",
        },
    }
    got = validate_im_message_reaction_envelope(env)
    assert got.ok is True
    assert got.errors == ()
    assert got.message_id == "om_x"


def test_validate_created_flat_emoji() -> None:
    env = {
        "header": _hdr("im.message.reaction.created_v1"),
        "event": {"message_id": "m2", "emoji_type": "THUMBSUP"},
    }
    assert validate_im_message_reaction_envelope(env).ok is True


def test_validate_deleted_allows_missing_emoji() -> None:
    env = {
        "header": _hdr("im.message.reaction.deleted_v1"),
        "event": {"message_id": "m3"},
    }
    got = validate_im_message_reaction_envelope(env)
    assert got.ok is True
    assert got.event_type == "im.message.reaction.deleted_v1"


def test_is_im_message_reaction_event_type() -> None:
    assert is_im_message_reaction_event_type("im.message.reaction.created_v1") is True
    assert is_im_message_reaction_event_type("im.message.receive_v1") is False
