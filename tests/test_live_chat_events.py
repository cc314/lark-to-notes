"""Tests for Lark chat event envelopes into the mixed chat-intake ledger."""

from __future__ import annotations

import json
import sqlite3

import pytest

from lark_to_notes.intake.ledger import chat_ingest_key, get_chat_intake_item
from lark_to_notes.intake.models import IntakePath
from lark_to_notes.live.chat_events import (
    event_type_from_envelope,
    extract_im_message_from_envelope,
    ingest_chat_event_ndjson_lines,
    ingest_receive_message_v1_envelope,
    iter_chat_event_envelopes_from_ndjson,
    payload_hash_for_chat_event,
)
from lark_to_notes.storage.db import connect, init_db


def _conn() -> sqlite3.Connection:
    c = connect(":memory:")
    init_db(c)
    return c


def _receive_v1_envelope(*, message_id: str = "om_evt_1") -> dict[str, object]:
    return {
        "header": {"event_type": "im.message.receive_v1"},
        "event": {
            "message": {
                "message_id": message_id,
                "chat_id": "ou_chat",
                "create_time": "1713096000000",
                "body": {"content": json.dumps({"text": "event body"})},
                "sender": {"id": "ou_sender", "name": "Alice"},
            }
        },
    }


def test_ingest_receive_message_v1_envelope_writes_ledger() -> None:
    conn = _conn()
    env = _receive_v1_envelope()
    item = ingest_receive_message_v1_envelope(
        conn,
        env,
        source_id="dm:ou_demo",
        worker_source_type="dm_user",
        chat_type="p2p",
        observed_at="2026-05-01T10:00:00Z",
        coalesce_window_seconds=120,
    )
    assert item is not None
    key = chat_ingest_key("dm:ou_demo", "om_evt_1")
    row = get_chat_intake_item(conn, key)
    assert row is not None
    assert row.last_intake_path is IntakePath.EVENT
    assert row.event_seen_count >= 1


def test_ingest_receive_message_v1_wrong_event_type_is_skipped() -> None:
    conn = _conn()
    env = {"header": {"event_type": "im.chat.access_event.bot_p2p_chat_enter_v1"}, "event": {}}
    assert event_type_from_envelope(env) == "im.chat.access_event.bot_p2p_chat_enter_v1"
    assert extract_im_message_from_envelope(env) is None
    item = ingest_receive_message_v1_envelope(
        conn,
        env,
        source_id="dm:ou_demo",
        worker_source_type="dm_user",
        chat_type="p2p",
    )
    assert item is None


def test_ingest_chat_event_ndjson_lines_counts_and_skips_garbage() -> None:
    conn = _conn()
    lines = [
        "",
        "not-json",
        json.dumps(_receive_v1_envelope(message_id="om_a")),
        json.dumps({"header": {"event_type": "im.message.receive_v1"}, "event": {}}),
    ]
    out = ingest_chat_event_ndjson_lines(
        conn,
        lines,
        source_id="dm:ou_demo",
        worker_source_type="dm_user",
        chat_type="p2p",
        observed_at="2026-05-01T10:00:00Z",
    )
    assert out.json_objects == 2
    assert out.chat_envelopes_ingested == 1
    assert out.reaction_rows_inserted == 0
    assert out.reaction_validation_rejects == 0


def test_ingest_chat_event_ndjson_lines_skips_invalid_reaction_envelope() -> None:
    conn = _conn()
    bad = {
        "header": {"event_type": "im.message.reaction.created_v1", "event_id": "rx-bad"},
        "event": {"message_id": "om_z"},
    }
    out = ingest_chat_event_ndjson_lines(
        conn,
        [json.dumps(bad)],
        source_id="dm:ou_demo",
        worker_source_type="dm_user",
        chat_type="p2p",
    )
    assert out.json_objects == 1
    assert out.chat_envelopes_ingested == 0
    assert out.reaction_rows_inserted == 0
    assert out.reaction_validation_rejects == 1
    assert out.last_reaction_quarantine_event_id == "rx-bad"
    assert out.last_reaction_quarantine_payload_hash == payload_hash_for_chat_event(bad)
    assert out.last_reaction_quarantine_reason_code is not None
    assert out.last_reaction_quarantine_reason_code.startswith("reaction_envelope_invalid:")


def test_ingest_chat_event_ndjson_lines_inserts_reaction_event() -> None:
    conn = _conn()
    react = {
        "header": {"event_type": "im.message.reaction.created_v1", "event_id": "rx-e2e-1"},
        "event": {
            "message_id": "om_rx_1",
            "reaction_type": {"emoji_type": "THUMBSUP"},
            "operator_type": "user",
            "user_id": {"open_id": "ou_x"},
            "action_time": "1",
        },
    }
    lines = [json.dumps(react)]
    out = ingest_chat_event_ndjson_lines(
        conn,
        lines,
        source_id="dm:ou_demo",
        worker_source_type="dm_user",
        chat_type="p2p",
    )
    assert out.json_objects == 1
    assert out.chat_envelopes_ingested == 0
    assert out.reaction_rows_inserted == 1
    assert out.reaction_validation_rejects == 0
    row = conn.execute(
        "SELECT message_id, reaction_kind FROM message_reaction_events WHERE reaction_event_id = ?",
        ("rx-e2e-1",),
    ).fetchone()
    assert row is not None
    assert row["message_id"] == "om_rx_1"
    assert row["reaction_kind"] == "add"


def test_ingest_chat_event_ndjson_lines_reaction_insert_exception_is_quarantined(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import lark_to_notes.live.chat_events as chat_events_mod

    def boom(*_a: object, **_k: object) -> None:
        raise RuntimeError("simulated db failure")

    monkeypatch.setattr(chat_events_mod, "insert_message_reaction_event", boom)
    conn = _conn()
    react = {
        "header": {"event_type": "im.message.reaction.created_v1", "event_id": "rx-exc-1"},
        "event": {
            "message_id": "om_rx_exc",
            "reaction_type": {"emoji_type": "THUMBSUP"},
            "operator_type": "user",
            "user_id": {"open_id": "ou_x"},
            "action_time": "1",
        },
    }
    out = ingest_chat_event_ndjson_lines(
        conn,
        [json.dumps(react)],
        source_id="dm:ou_demo",
        worker_source_type="dm_user",
        chat_type="p2p",
    )
    assert out.reaction_rows_inserted == 0
    assert out.reaction_insert_exceptions == 1
    assert out.last_reaction_quarantine_event_id == "rx-exc-1"
    assert out.last_reaction_quarantine_reason_code == "reaction_insert_exception:RuntimeError"


def test_ingest_chat_event_ndjson_lines_receive_v1_exception_is_quarantined(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import lark_to_notes.live.chat_events as chat_events_mod

    def boom(*_a: object, **_k: object) -> None:
        raise ValueError("simulated ledger failure")

    monkeypatch.setattr(chat_events_mod, "observe_chat_message", boom)
    conn = _conn()
    env = _receive_v1_envelope(message_id="om_exc_1")
    out = ingest_chat_event_ndjson_lines(
        conn,
        [json.dumps(env)],
        source_id="dm:ou_demo",
        worker_source_type="dm_user",
        chat_type="p2p",
    )
    assert out.json_objects == 1
    assert out.chat_envelopes_ingested == 0
    assert out.chat_receive_observation_exceptions == 1
    assert out.last_chat_quarantine_reason_code == "chat_receive_v1_exception:ValueError"


def test_iter_chat_event_envelopes_from_ndjson_yields_objects() -> None:
    lines = ["", json.dumps({"a": 1}), json.dumps({"b": 2})]
    out = list(iter_chat_event_envelopes_from_ndjson(lines))
    assert out == [{"a": 1}, {"b": 2}]
