"""End-to-end: WS-shaped NDJSON → :func:`ingest_chat_event_ndjson_lines` → drain → ``raw_messages`` + reactions."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from lark_to_notes.intake.ledger import chat_ingest_key, count_raw_messages, get_chat_intake_item
from lark_to_notes.intake.models import ChatIntakeState, IntakePath
from lark_to_notes.live.chat_events import ingest_chat_event_ndjson_lines
from lark_to_notes.runtime.executor import drain_ready_chat_intake
from lark_to_notes.runtime.retry import RetryPolicy
from lark_to_notes.storage.db import connect, init_db

_FIXTURE = Path(__file__).resolve().parent / "fixtures" / "live_ws_ndjson" / "ws_like_stream.ndjson"


def _db(tmp_path: Path) -> sqlite3.Connection:
    db_path = tmp_path / "ws_ndjson_e2e.db"
    conn = connect(db_path)
    init_db(conn)
    return conn


def test_ws_like_ndjson_fixture_ingest_and_drain_writes_raw_and_reaction(tmp_path: Path) -> None:
    """Mirrors ``sync-events`` + default drain: one receive line + one reaction line, no network."""

    text = _FIXTURE.read_text(encoding="utf-8")
    lines = [ln for ln in text.splitlines() if ln.strip()]
    conn = _db(tmp_path)
    outcome = ingest_chat_event_ndjson_lines(
        conn,
        lines,
        source_id="dm:ou_ws",
        worker_source_type="dm_user",
        chat_type="p2p",
        observed_at="2026-05-01T12:00:00Z",
        coalesce_window_seconds=0,
    )
    assert outcome.json_objects == 2
    assert outcome.chat_envelopes_ingested == 1
    assert outcome.reaction_rows_inserted == 1
    assert outcome.reaction_rows_inserted_add == 1
    assert outcome.reaction_rows_inserted_remove == 0
    assert outcome.reaction_quarantined == 0

    key = chat_ingest_key("dm:ou_ws", "om_ws_drain_1")
    before = get_chat_intake_item(conn, key)
    assert before is not None
    assert before.last_intake_path is IntakePath.EVENT
    assert before.processing_state is ChatIntakeState.PENDING

    lock_path = tmp_path / "lark-to-notes.ws_ndjson_e2e.lock"
    batch = drain_ready_chat_intake(
        conn,
        lock_path=lock_path,
        command="test-ws-ndjson-e2e",
        retry_policy=RetryPolicy(),
        sleep_fn=lambda _delay: None,
        as_of="2026-05-01T12:00:05Z",
    )
    assert batch.items_total == 1
    assert batch.items_processed == 1
    assert batch.items_failed == 0

    assert count_raw_messages(conn) == 1
    raw = conn.execute(
        "SELECT message_id, source_id FROM raw_messages WHERE message_id = ?",
        ("om_ws_drain_1",),
    ).fetchone()
    assert raw is not None
    assert raw["source_id"] == "dm:ou_ws"

    rx = conn.execute(
        "SELECT reaction_event_id, reaction_kind FROM message_reaction_events WHERE message_id = ?",
        ("om_ws_drain_1",),
    ).fetchone()
    assert rx is not None
    assert rx["reaction_event_id"] == "evt-ws-rx-1"
    assert rx["reaction_kind"] == "add"

    after = get_chat_intake_item(conn, key)
    assert after is not None
    assert after.processing_state is ChatIntakeState.PROCESSED
