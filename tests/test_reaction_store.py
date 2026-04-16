"""Tests for :mod:`lark_to_notes.intake.reaction_store`."""

from __future__ import annotations

import sqlite3

import pytest

from lark_to_notes.intake.ledger import chat_ingest_key
from lark_to_notes.intake.reaction_model import NormalizedReactionEvent, ReactionKind
from lark_to_notes.intake.reaction_store import (
    canonical_reaction_event_id,
    insert_message_reaction_event,
    reaction_correlation_counts,
    surrogate_reaction_event_id,
)
from lark_to_notes.storage.db import connect, init_db


def _event(*, eid: str = "", emoji: str = "OK") -> NormalizedReactionEvent:
    return NormalizedReactionEvent(
        reaction_event_id=eid,
        source_id="dm:test",
        message_id="om_1",
        reaction_kind=ReactionKind.ADD,
        emoji_type=emoji,
        operator_type="user",
        operator_open_id="ou_a",
        operator_user_id="",
        operator_union_id="",
        action_time="1",
        payload={"header": {}, "event": {"message_id": "om_1"}},
    )


@pytest.fixture
def mem() -> sqlite3.Connection:
    c = connect(":memory:")
    init_db(c)
    return c


def test_surrogate_stable() -> None:
    ev = _event(eid="")
    s1 = surrogate_reaction_event_id(ev)
    s2 = surrogate_reaction_event_id(ev)
    assert s1 == s2
    assert s1.startswith("rxs_")


def test_canonical_prefers_upstream_id() -> None:
    ev = _event(eid="  feishu-id  ")
    assert canonical_reaction_event_id(ev) == "feishu-id"


def test_insert_idempotent(mem: sqlite3.Connection) -> None:
    ev = _event(eid="evdup")
    r1 = insert_message_reaction_event(mem, ev)
    assert r1.inserted is True
    assert r1.raw_message_present is False
    assert r1.chat_ingest_fingerprint == chat_ingest_key(ev.source_id, ev.message_id)
    r2 = insert_message_reaction_event(mem, ev)
    assert r2.inserted is False
    assert r2.reaction_event_id == "evdup"
    n = mem.execute("SELECT COUNT(*) AS c FROM message_reaction_events").fetchone()
    assert int(n[0]) == 1


def test_insert_surrogate_idempotent(mem: sqlite3.Connection) -> None:
    ev = _event(eid="")
    rid = canonical_reaction_event_id(ev)
    assert insert_message_reaction_event(mem, ev).inserted is True
    assert insert_message_reaction_event(mem, ev).inserted is False
    row = mem.execute(
        "SELECT reaction_event_id FROM message_reaction_events WHERE reaction_event_id = ?",
        (rid,),
    ).fetchone()
    assert row is not None


def test_raw_message_present_when_raw_exists(mem: sqlite3.Connection) -> None:
    mem.execute(
        """
        INSERT INTO raw_messages (
            message_id, source_id, source_type, chat_id, chat_type,
            sender_id, sender_name, direction, created_at, content, payload_json
        )
        VALUES (?, ?, 'dm', 'c1', 'p2p', '', '', 'incoming', '2026-01-01T00:00:00Z', '', '{}')
        """,
        ("om_1", "dm:test"),
    )
    mem.commit()
    ev = _event(eid="ev-has-raw")
    res = insert_message_reaction_event(mem, ev)
    assert res.inserted is True
    assert res.raw_message_present is True
    row = mem.execute(
        "SELECT raw_message_present, chat_ingest_fingerprint FROM message_reaction_events "
        "WHERE reaction_event_id = ?",
        ("ev-has-raw",),
    ).fetchone()
    assert int(row[0]) == 1
    assert row[1] == chat_ingest_key("dm:test", "om_1")


def test_reaction_correlation_counts_join(mem: sqlite3.Connection) -> None:
    assert reaction_correlation_counts(mem) == {
        "total": 0,
        "linked_to_raw_message": 0,
        "orphan": 0,
    }
    insert_message_reaction_event(mem, _event(eid="orph1"))
    assert reaction_correlation_counts(mem) == {
        "total": 1,
        "linked_to_raw_message": 0,
        "orphan": 1,
    }
    mem.execute(
        """
        INSERT INTO raw_messages (
            message_id, source_id, source_type, chat_id, chat_type,
            sender_id, sender_name, direction, created_at, content, payload_json
        )
        VALUES (?, ?, 'dm', 'c1', 'p2p', '', '', 'incoming', '2026-01-01T00:00:00Z', '', '{}')
        """,
        ("om_1", "dm:test"),
    )
    mem.commit()
    assert reaction_correlation_counts(mem) == {
        "total": 1,
        "linked_to_raw_message": 1,
        "orphan": 0,
    }
