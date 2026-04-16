"""Tests for in-repo live chat ingestion helpers."""

from __future__ import annotations

import sqlite3

from lark_to_notes.intake.models import RawMessage
from lark_to_notes.live.chat_ingest import ingest_chat_records
from lark_to_notes.storage.db import connect, init_db


def _conn() -> sqlite3.Connection:
    c = connect(":memory:")
    init_db(c)
    return c


def _sample_record(*, message_id: str = "om_live_1") -> dict[str, object]:
    return {
        "message_id": message_id,
        "source_id": "dm:ou_demo",
        "source_type": "dm_user",
        "chat_id": "ou_chat",
        "chat_type": "p2p",
        "sender_id": "ou_sender",
        "sender_name": "Alice",
        "direction": "incoming",
        "created_at": "2026-05-01T10:00:00Z",
        "content": "hello from live ingest test",
        "payload": {"content": "hello from live ingest test"},
    }


def test_ingest_chat_records_inserts_once_idempotent() -> None:
    conn = _conn()
    rec = _sample_record()
    total, ins = ingest_chat_records(conn, [rec])
    assert total == 1
    assert ins == 1
    total2, ins2 = ingest_chat_records(conn, [rec])
    assert total2 == 1
    assert ins2 == 0


def test_ingest_chat_records_skips_malformed_without_message_id() -> None:
    conn = _conn()
    total, ins = ingest_chat_records(conn, [{"source_id": "dm:x"}])
    assert total == 0
    assert ins == 0


def test_ingest_chat_records_round_trips_raw_message_fields() -> None:
    conn = _conn()
    rec = _sample_record(message_id="om_live_round")
    ingest_chat_records(conn, [rec])
    row = conn.execute(
        "SELECT * FROM raw_messages WHERE message_id = ?", ("om_live_round",)
    ).fetchone()
    assert row is not None
    msg = RawMessage.from_jsonl_record(rec)
    assert row["message_id"] == msg.message_id
    assert row["source_id"] == msg.source_id
    assert row["content"] == msg.content
