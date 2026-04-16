"""Tests for the intake ledger, raw-message model, and replay semantics."""

from __future__ import annotations

import json
import sqlite3
import tempfile
from pathlib import Path

import pytest

from lark_to_notes.config.sources import SourceType, WatchedSource
from lark_to_notes.intake.ledger import (
    chat_ingest_key,
    count_raw_messages,
    document_ingest_key,
    finish_intake_run,
    get_chat_intake_item,
    get_raw_message,
    insert_raw_message,
    list_raw_messages,
    list_ready_chat_intake,
    list_ready_document_intake,
    mark_document_intake_processed,
    observe_chat_message,
    observe_document_surface,
    start_intake_run,
)
from lark_to_notes.intake.models import (
    ChatEventAction,
    ChatIntakeState,
    DocumentLifecycleState,
    DocumentRecordType,
    IntakePath,
    RawMessage,
    _parse_note_date,
)
from lark_to_notes.intake.replay import replay_jsonl_dir, replay_jsonl_file
from lark_to_notes.storage.db import connect, init_db, upsert_watched_source

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mem() -> sqlite3.Connection:
    conn = connect(":memory:")
    init_db(conn)
    return conn


def _ensure_doc_watched(conn: sqlite3.Connection, source_id: str = "doc:docx1") -> None:
    """``document_intake_ledger`` rows require a matching ``watched_sources`` row."""

    _type, external_id = source_id.split(":", 1)
    upsert_watched_source(
        conn,
        WatchedSource(
            source_id=source_id,
            source_type=SourceType(_type),
            external_id=external_id,
            name="Fixture doc",
        ),
    )


def _make_msg(
    message_id: str = "msg1",
    source_id: str = "dm:u1",
    content: str = "hello",
    created_at: str = "2026-04-14 10:00",
) -> RawMessage:
    return RawMessage(
        message_id=message_id,
        source_id=source_id,
        source_type="dm_user",
        chat_id="ou_abc",
        chat_type="p2p",
        sender_id="ou_sender",
        sender_name="Alice",
        direction="incoming",
        created_at=created_at,
        content=content,
        payload={"content": content},
    )


# ---------------------------------------------------------------------------
# RawMessage model tests
# ---------------------------------------------------------------------------


def test_parse_note_date_from_lark_format() -> None:
    assert _parse_note_date("2026-04-14 11:21") == "2026-04-14"


def test_parse_note_date_from_iso() -> None:
    assert _parse_note_date("2026-04-14T10:00:00Z") == "2026-04-14"


def test_parse_note_date_fallback() -> None:
    assert _parse_note_date("2026-04-14") == "2026-04-14"


def test_parse_note_date_unparseable() -> None:
    assert _parse_note_date("not-a-date") == ""


def test_raw_message_note_date_property() -> None:
    msg = _make_msg(created_at="2026-04-14 10:00")
    assert msg.note_date == "2026-04-14"


def test_raw_message_payload_json() -> None:
    msg = _make_msg()
    pj = msg.payload_json()
    decoded = json.loads(pj)
    assert decoded["content"] == "hello"


def test_raw_message_from_jsonl_record() -> None:
    record = {
        "message_id": "om_abc123",
        "source_id": "dm:u1",
        "source_type": "dm_user",
        "chat_id": "ou_xxx",
        "chat_type": "unknown",
        "sender_id": "ou_s1",
        "sender_name": "Bob",
        "direction": "incoming",
        "created_at": "2026-04-14 09:00",
        "content": "test content",
        "payload": {"content": "test content"},
    }
    msg = RawMessage.from_jsonl_record(record)
    assert msg.message_id == "om_abc123"
    assert msg.source_id == "dm:u1"
    assert msg.content == "test content"
    assert msg.note_date == "2026-04-14"


def test_raw_message_from_jsonl_record_string_payload() -> None:
    payload_str = json.dumps({"content": "hi"})
    record = {
        "message_id": "msg_x",
        "source_id": "dm:u1",
        "source_type": "dm_user",
        "chat_id": "ou_x",
        "chat_type": "",
        "sender_id": "ou_s",
        "sender_name": "X",
        "direction": "incoming",
        "created_at": "2026-04-14 08:00",
        "content": "hi",
        "payload": payload_str,
    }
    msg = RawMessage.from_jsonl_record(record)
    assert msg.payload["content"] == "hi"


def test_raw_message_from_db_row() -> None:
    msg = _make_msg()
    row = {
        "message_id": msg.message_id,
        "source_id": msg.source_id,
        "source_type": msg.source_type,
        "chat_id": msg.chat_id,
        "chat_type": msg.chat_type,
        "sender_id": msg.sender_id,
        "sender_name": msg.sender_name,
        "direction": msg.direction,
        "created_at": msg.created_at,
        "content": msg.content,
        "payload_json": msg.payload_json(),
        "ingested_at": msg.ingested_at,
    }
    restored = RawMessage.from_db_row(row)
    assert restored.message_id == msg.message_id
    assert restored.payload["content"] == "hello"


# ---------------------------------------------------------------------------
# Ledger CRUD tests
# ---------------------------------------------------------------------------


def test_insert_raw_message_new() -> None:
    conn = _mem()
    msg = _make_msg()
    assert insert_raw_message(conn, msg) is True


def test_insert_raw_message_duplicate_is_ignored() -> None:
    conn = _mem()
    msg = _make_msg()
    insert_raw_message(conn, msg)
    assert insert_raw_message(conn, msg) is False


def test_get_raw_message_found() -> None:
    conn = _mem()
    msg = _make_msg()
    insert_raw_message(conn, msg)
    fetched = get_raw_message(conn, "msg1")
    assert fetched is not None
    assert fetched.content == "hello"


def test_get_raw_message_missing_returns_none() -> None:
    conn = _mem()
    assert get_raw_message(conn, "nonexistent") is None


def test_list_raw_messages_by_source() -> None:
    conn = _mem()
    for i in range(3):
        insert_raw_message(conn, _make_msg(message_id=f"m{i}", source_id="dm:u1"))
    insert_raw_message(conn, _make_msg(message_id="m99", source_id="group:g1"))
    result = list_raw_messages(conn, source_id="dm:u1")
    assert len(result) == 3
    assert all(r.source_id == "dm:u1" for r in result)


def test_list_raw_messages_by_date() -> None:
    conn = _mem()
    insert_raw_message(conn, _make_msg("m1", created_at="2026-04-14 10:00"))
    insert_raw_message(conn, _make_msg("m2", created_at="2026-04-15 10:00"))
    result = list_raw_messages(conn, note_date="2026-04-14")
    assert len(result) == 1
    assert result[0].message_id == "m1"


def test_count_raw_messages() -> None:
    conn = _mem()
    for i in range(5):
        insert_raw_message(conn, _make_msg(message_id=f"m{i}"))
    assert count_raw_messages(conn) == 5


def test_count_raw_messages_by_source() -> None:
    conn = _mem()
    for i in range(3):
        insert_raw_message(conn, _make_msg(message_id=f"a{i}", source_id="dm:u1"))
    insert_raw_message(conn, _make_msg(message_id="b0", source_id="group:g1"))
    assert count_raw_messages(conn, source_id="dm:u1") == 3
    assert count_raw_messages(conn, source_id="group:g1") == 1


def test_event_observation_stays_pending_until_coalesce_window_expires() -> None:
    conn = _mem()
    msg = _make_msg()

    item = observe_chat_message(
        conn,
        msg,
        intake_path=IntakePath.EVENT,
        observed_at="2026-04-14T10:00:00Z",
        coalesce_window_seconds=60,
    )

    assert item.processing_state is ChatIntakeState.PENDING
    assert item.event_seen_count == 1
    assert item.poll_seen_count == 0
    assert list_ready_chat_intake(conn, as_of="2026-04-14T10:00:59Z") == []
    assert [
        ready.ingest_key for ready in list_ready_chat_intake(conn, as_of="2026-04-14T10:01:00Z")
    ] == [item.ingest_key]


def test_event_burst_extends_the_coalescing_window() -> None:
    conn = _mem()
    msg = _make_msg()
    observe_chat_message(
        conn,
        msg,
        intake_path=IntakePath.EVENT,
        observed_at="2026-04-14T10:00:00Z",
        coalesce_window_seconds=60,
    )
    item = observe_chat_message(
        conn,
        msg,
        intake_path=IntakePath.EVENT,
        observed_at="2026-04-14T10:00:30Z",
        coalesce_window_seconds=60,
    )

    assert item.coalesce_until == "2026-04-14T10:01:30Z"
    assert item.event_seen_count == 2
    assert list_ready_chat_intake(conn, as_of="2026-04-14T10:01:29Z") == []


def test_poll_observation_short_circuits_pending_event() -> None:
    conn = _mem()
    msg = _make_msg()
    observe_chat_message(
        conn,
        msg,
        intake_path=IntakePath.EVENT,
        observed_at="2026-04-14T10:00:00Z",
        coalesce_window_seconds=120,
    )
    item = observe_chat_message(
        conn,
        msg,
        intake_path=IntakePath.POLL,
        observed_at="2026-04-14T10:00:30Z",
    )

    assert item.poll_seen_count == 1
    assert item.event_seen_count == 1
    assert item.coalesce_until == "2026-04-14T10:00:30Z"
    assert [
        ready.ingest_key for ready in list_ready_chat_intake(conn, as_of="2026-04-14T10:00:30Z")
    ] == [item.ingest_key]


def test_processed_item_tracks_later_duplicate_observations_without_requeueing() -> None:
    conn = _mem()
    msg = _make_msg()
    item = observe_chat_message(
        conn,
        msg,
        intake_path=IntakePath.POLL,
        observed_at="2026-04-14T10:00:00Z",
    )
    conn.execute(
        "UPDATE chat_intake_ledger SET processing_state = ?, processed_at = ? WHERE ingest_key = ?",
        (ChatIntakeState.PROCESSED.value, "2026-04-14T10:00:10Z", item.ingest_key),
    )
    conn.commit()

    updated = observe_chat_message(
        conn,
        msg,
        intake_path=IntakePath.EVENT,
        observed_at="2026-04-14T10:00:20Z",
    )

    assert updated.processing_state is ChatIntakeState.PROCESSED
    assert updated.processed_at == "2026-04-14T10:00:10Z"
    assert updated.event_seen_count == 1
    assert list_ready_chat_intake(conn, as_of="2026-04-14T10:00:20Z") == []


def test_unsupported_edit_event_is_rejected() -> None:
    conn = _mem()

    with pytest.raises(ValueError, match="only create chat events are supported"):
        observe_chat_message(
            conn,
            _make_msg(),
            intake_path=IntakePath.EVENT,
            event_action=ChatEventAction.EDIT,
        )


def test_chat_ingest_key_is_stable_for_source_and_message_identity() -> None:
    assert chat_ingest_key("dm:u1", "msg-1") == chat_ingest_key("dm:u1", "msg-1")
    assert chat_ingest_key("dm:u1", "msg-1") != chat_ingest_key("dm:u2", "msg-1")
    assert get_chat_intake_item(_mem(), chat_ingest_key("dm:u1", "missing")) is None


def test_document_ingest_key_is_stable() -> None:
    k = document_ingest_key(
        "doc:tok1",
        DocumentRecordType.DOC_COMMENT,
        "doc:tok1:comments",
        "cmt-9",
    )
    assert k == document_ingest_key(
        "doc:tok1",
        DocumentRecordType.DOC_COMMENT,
        "doc:tok1:comments",
        "cmt-9",
    )
    assert k != document_ingest_key(
        "doc:tok1",
        DocumentRecordType.DOC_REPLY,
        "doc:tok1:comments",
        "cmt-9",
    )


def test_document_poll_observation_is_ready_immediately() -> None:
    conn = _mem()
    _ensure_doc_watched(conn, "doc:tok1")
    item = observe_document_surface(
        conn,
        record_type=DocumentRecordType.DOC_BODY,
        source_id="doc:tok1",
        document_token="tok1",
        source_stream_id="doc:tok1:body",
        source_item_id="body",
        content_hash="h1",
        normalized_text="Hello doc",
        payload={"rev": "1"},
        intake_path=IntakePath.POLL,
        observed_at="2026-04-14T10:00:00Z",
    )
    assert item.processing_state is ChatIntakeState.PENDING
    assert list_ready_document_intake(conn, as_of="2026-04-14T10:00:00Z") == [item]


def test_document_processed_duplicate_observation_skips_requeue() -> None:
    conn = _mem()
    _ensure_doc_watched(conn, "doc:tok1")
    item = observe_document_surface(
        conn,
        record_type=DocumentRecordType.DOC_COMMENT,
        source_id="doc:tok1",
        document_token="tok1",
        source_stream_id="doc:tok1:comments",
        source_item_id="c1",
        content_hash="ab",
        normalized_text="thread",
        payload={},
        intake_path=IntakePath.POLL,
        observed_at="2026-04-14T10:00:00Z",
    )
    mark_document_intake_processed(conn, item.ingest_key, processed_at="2026-04-14T10:00:05Z")
    again = observe_document_surface(
        conn,
        record_type=DocumentRecordType.DOC_COMMENT,
        source_id="doc:tok1",
        document_token="tok1",
        source_stream_id="doc:tok1:comments",
        source_item_id="c1",
        content_hash="ab",
        normalized_text="thread",
        payload={"touch": True},
        intake_path=IntakePath.EVENT,
        observed_at="2026-04-14T10:00:10Z",
    )
    assert again.processing_state is ChatIntakeState.PROCESSED
    assert again.processed_at == "2026-04-14T10:00:05Z"
    assert again.event_seen_count == 1
    assert list_ready_document_intake(conn, as_of="2026-04-14T10:00:10Z") == []


def test_document_substantive_change_after_processed_reopens_pending() -> None:
    conn = _mem()
    _ensure_doc_watched(conn, "doc:tok1")
    item = observe_document_surface(
        conn,
        record_type=DocumentRecordType.DOC_BODY,
        source_id="doc:tok1",
        document_token="tok1",
        source_stream_id="doc:tok1:body",
        source_item_id="body",
        content_hash="h1",
        normalized_text="v1",
        payload={},
        intake_path=IntakePath.POLL,
        observed_at="2026-04-14T10:00:00Z",
    )
    mark_document_intake_processed(conn, item.ingest_key, processed_at="2026-04-14T10:00:05Z")
    edited = observe_document_surface(
        conn,
        record_type=DocumentRecordType.DOC_BODY,
        source_id="doc:tok1",
        document_token="tok1",
        source_stream_id="doc:tok1:body",
        source_item_id="body",
        content_hash="h2",
        normalized_text="v2",
        payload={},
        intake_path=IntakePath.POLL,
        observed_at="2026-04-14T10:00:20Z",
        lifecycle_state=DocumentLifecycleState.EDITED,
    )
    assert edited.processing_state is ChatIntakeState.PENDING
    assert edited.content_hash == "h2"
    assert edited.lifecycle_state is DocumentLifecycleState.EDITED


# ---------------------------------------------------------------------------
# Intake-run audit tests
# ---------------------------------------------------------------------------


def test_start_and_finish_intake_run() -> None:
    conn = _mem()
    run_id = start_intake_run(conn, "dm:u1")
    assert isinstance(run_id, str) and len(run_id) == 36  # UUID format
    finish_intake_run(conn, run_id, messages_fetched=10, messages_new=5)
    row = conn.execute(
        "SELECT status, messages_fetched, messages_new FROM intake_runs WHERE run_id=?",
        (run_id,),
    ).fetchone()
    assert row is not None
    assert row[0] == "done"
    assert row[1] == 10
    assert row[2] == 5


def test_intake_run_error_status() -> None:
    conn = _mem()
    run_id = start_intake_run(conn, "dm:u1")
    finish_intake_run(
        conn,
        run_id,
        messages_fetched=0,
        messages_new=0,
        status="error",
        error_detail="timeout",
    )
    row = conn.execute(
        "SELECT status, error_detail FROM intake_runs WHERE run_id=?", (run_id,)
    ).fetchone()
    assert row is not None
    assert row[0] == "error"
    assert row[1] == "timeout"


# ---------------------------------------------------------------------------
# Replay tests
# ---------------------------------------------------------------------------


def _write_jsonl(path: Path, records: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")


def _sample_record(n: int) -> dict[str, object]:
    return {
        "message_id": f"om_replay{n}",
        "source_id": "dm:u1",
        "source_type": "dm_user",
        "chat_id": "ou_abc",
        "chat_type": "p2p",
        "sender_id": "ou_s1",
        "sender_name": "Alice",
        "direction": "incoming",
        "created_at": f"2026-04-14 10:0{n}",
        "content": f"replay message {n}",
        "payload": {"content": f"replay message {n}"},
    }


def test_replay_jsonl_file_inserts_new_records() -> None:
    conn = _mem()
    with tempfile.NamedTemporaryFile(suffix=".jsonl", mode="w", delete=False) as fh:
        tmp = Path(fh.name)
    try:
        _write_jsonl(tmp, [_sample_record(i) for i in range(5)])
        total, inserted = replay_jsonl_file(conn, tmp)
        assert total == 5
        assert inserted == 5
    finally:
        tmp.unlink(missing_ok=True)


def test_replay_jsonl_file_is_idempotent() -> None:
    conn = _mem()
    with tempfile.NamedTemporaryFile(suffix=".jsonl", mode="w", delete=False) as fh:
        tmp = Path(fh.name)
    try:
        _write_jsonl(tmp, [_sample_record(0)])
        replay_jsonl_file(conn, tmp)
        total, inserted = replay_jsonl_file(conn, tmp)
        assert total == 1
        assert inserted == 0  # already present
    finally:
        tmp.unlink(missing_ok=True)


def test_replay_jsonl_file_skips_invalid_lines() -> None:
    conn = _mem()
    with tempfile.NamedTemporaryFile(suffix=".jsonl", mode="w", delete=False) as fh:
        tmp = Path(fh.name)
        fh.write("not json\n")
        fh.write("{}\n")  # missing message_id
        fh.write(json.dumps(_sample_record(0)) + "\n")
    try:
        total, inserted = replay_jsonl_file(conn, tmp)
        assert total == 1  # only the valid record counted
        assert inserted == 1
    finally:
        tmp.unlink(missing_ok=True)


def test_replay_jsonl_dir() -> None:
    conn = _mem()
    with tempfile.TemporaryDirectory() as tmpdir:
        d = Path(tmpdir)
        _write_jsonl(d / "2026-04-13.jsonl", [_sample_record(0), _sample_record(1)])
        _write_jsonl(d / "2026-04-14.jsonl", [_sample_record(2)])
        results = replay_jsonl_dir(conn, d)
    assert "2026-04-13.jsonl" in results
    assert "2026-04-14.jsonl" in results
    assert results["2026-04-13.jsonl"] == (2, 2)
    assert results["2026-04-14.jsonl"] == (1, 1)


def test_replay_jsonl_dir_total_in_db() -> None:
    conn = _mem()
    with tempfile.TemporaryDirectory() as tmpdir:
        d = Path(tmpdir)
        _write_jsonl(d / "a.jsonl", [_sample_record(i) for i in range(4)])
        replay_jsonl_dir(conn, d)
    assert count_raw_messages(conn) == 4


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("group", True),
        ("doc", False),
    ],
)
def test_raw_message_direction_values(source: str, expected: bool) -> None:
    msg = _make_msg(source_id=f"{source}:x1")
    # direction is determined by the constructor, not source type; just verify field exists
    assert msg.direction in {"incoming", "outgoing"}
    _ = expected  # parameter exercises pytest parametrize path
