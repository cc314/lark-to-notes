"""Tests for Lark chat event envelopes into the mixed chat-intake ledger."""

from __future__ import annotations

import json
import sqlite3

from lark_to_notes.intake.ledger import chat_ingest_key, get_chat_intake_item
from lark_to_notes.intake.models import IntakePath
from lark_to_notes.live.chat_events import (
    event_type_from_envelope,
    extract_im_message_from_envelope,
    ingest_chat_event_ndjson_lines,
    ingest_receive_message_v1_envelope,
    iter_chat_event_envelopes_from_ndjson,
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
    objs, ingested = ingest_chat_event_ndjson_lines(
        conn,
        lines,
        source_id="dm:ou_demo",
        worker_source_type="dm_user",
        chat_type="p2p",
        observed_at="2026-05-01T10:00:00Z",
    )
    assert objs == 2
    assert ingested == 1


def test_iter_chat_event_envelopes_from_ndjson_yields_objects() -> None:
    lines = ["", json.dumps({"a": 1}), json.dumps({"b": 2})]
    out = list(iter_chat_event_envelopes_from_ndjson(lines))
    assert out == [{"a": 1}, {"b": 2}]
