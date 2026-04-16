"""Integration tests for structured logging across the pipeline.

Tests in this module verify that specific pipeline functions emit the
structured log events they promise — with the expected fields — when
configured to emit JSON logs via ``configure_logging(json_logs=True)``.

All tests use real I/O and real SQLite; no mocks or monkeypatching.

Design notes
------------
* ``configure_logging(json_logs=True)`` creates a ``StreamHandler(sys.stderr)``.
  Redirect ``sys.stderr`` *before* calling it so the handler writes to the
  capture buffer.
* After each test, restore ``sys.stderr`` and call
  ``configure_logging("WARNING")`` + ``structlog.reset_defaults()`` to avoid
  polluting other test modules.
* All log event names referenced below are the first positional argument passed
  to ``logger.debug/info/warning``.
"""

from __future__ import annotations

import contextlib
import io
import json
import logging
import os
import subprocess
import sys
from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
import structlog

from lark_to_notes.logging import configure_logging

if TYPE_CHECKING:
    from lark_to_notes.intake.models import RawMessage
    from lark_to_notes.render.models import RenderItem

# ---------------------------------------------------------------------------
# Capture helper
# ---------------------------------------------------------------------------


def _run_and_capture(fn: object, *, level: str = "DEBUG") -> list[dict[str, object]]:
    """Execute *fn()* and return all structured JSON log lines it emitted."""
    buf = io.StringIO()
    old_stderr = sys.stderr
    sys.stderr = buf
    configure_logging(level, json_logs=True)
    try:
        fn()  # type: ignore[operator]
    finally:
        sys.stderr = old_stderr
        configure_logging("WARNING")
        structlog.reset_defaults()
    result: list[dict[str, object]] = []
    for raw_line in buf.getvalue().splitlines():
        if not raw_line.strip():
            continue
        with contextlib.suppress(json.JSONDecodeError):
            result.append(json.loads(raw_line))
    return result


def _events(logs: list[dict[str, object]]) -> list[object]:
    """Return just the event names from a captured log list."""
    return [rec.get("event") for rec in logs]


def _matching(logs: list[dict[str, object]], event: str) -> list[dict[str, object]]:
    """Return log records whose 'event' field matches *event* exactly."""
    return [rec for rec in logs if rec.get("event") == event]


def _containing(logs: list[dict[str, object]], substr: str) -> list[dict[str, object]]:
    """Return log records whose 'event' field contains *substr*."""
    return [rec for rec in logs if substr in str(rec.get("event", ""))]


def _raw_message(message_id: str = "om_log_ledger_1") -> RawMessage:
    from lark_to_notes.intake.models import RawMessage

    return RawMessage(
        message_id=message_id,
        source_id="dm:ou_log",
        source_type="dm_user",
        chat_id="ou_chat_log",
        chat_type="p2p",
        sender_id="ou_sender_log",
        sender_name="Logger",
        direction="incoming",
        created_at="2026-05-01T10:00:00Z",
        content="Please review the launch checklist",
        payload={"content": "Please review the launch checklist"},
        ingested_at="2026-05-01T10:00:00Z",
    )


# ---------------------------------------------------------------------------
# Autouse fixture: always restore logging state after each test
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _restore_logging_state() -> Generator[None, None, None]:
    """Guarantee logging state is clean after every test in this module."""
    root = logging.getLogger()
    saved_handlers = list(root.handlers)
    saved_level = root.level
    yield
    root.handlers = saved_handlers
    root.setLevel(saved_level)
    structlog.reset_defaults()


# ---------------------------------------------------------------------------
# Helper: minimal RenderItem
# ---------------------------------------------------------------------------


def _render_item(
    *,
    title: str = "Test task",
    fingerprint: str = "log-test-fp-001",
    task_id: str = "log-test-id-001",
    event_date: str | None = "2026-05-01",
    promotion_rec: str = "current_tasks",
    task_class: str = "task",
    confidence_band: str = "high",
    reason_code: str = "explicit_task_keyword",
    status: str = "open",
) -> RenderItem:
    from lark_to_notes.render.models import RenderItem

    return RenderItem(
        task_id=task_id,
        fingerprint=fingerprint,
        title=title,
        promotion_rec=promotion_rec,
        reason_code=reason_code,
        confidence_band=confidence_band,
        task_class=task_class,
        status=status,
        summary="",
        assignee_refs=(),
        due_at=None,
        source_message_id=None,
        event_date=event_date or "",
    )


# ---------------------------------------------------------------------------
# Test 1: intake ledger emits a structured insert log
# ---------------------------------------------------------------------------


def test_intake_ledger_emits_insert_log(tmp_path: Path) -> None:
    """insert_raw_message emits a structured event with the inserted message ID."""
    from lark_to_notes.intake.ledger import insert_raw_message
    from lark_to_notes.storage.db import connect, init_db

    conn = connect(tmp_path / "state.db")
    init_db(conn)
    message = _raw_message()

    logs = _run_and_capture(lambda: insert_raw_message(conn, message))

    matching = _matching(logs, "insert_raw_message")
    assert matching, f"expected 'insert_raw_message' event; got: {_events(logs)}"
    evt = matching[0]
    assert evt["message_id"] == "om_log_ledger_1"
    assert evt["source_id"] == "dm:ou_log"
    assert evt["inserted"] is True


# ---------------------------------------------------------------------------
# Test 2: classify_with_routing emits log on LLM escalation skip
# ---------------------------------------------------------------------------


def test_classify_emits_log_on_escalation_skip() -> None:
    """classify_with_routing logs an escalation-skip event with classifier context.

    A very long low-signal message triggers LOW confidence and escalation
    attempt.  With no provider, the router logs the skip and falls back.
    """
    from lark_to_notes.distill.models import DistillInput
    from lark_to_notes.distill.routing import classify_with_routing

    inp = DistillInput(
        message_id="om_log_test_classify",
        source_id="dm:test",
        source_type="dm_user",
        content="context context context " * 50,  # long, no clear signal → LOW
        sender_name="Alice",
        direction="incoming",
        created_at="2026-05-01T10:00:00Z",
    )

    logs = _run_and_capture(lambda: classify_with_routing(inp, llm_provider=None))

    matching = _matching(logs, "llm_escalation_skipped")
    assert matching, f"expected 'llm_escalation_skipped' event in: {_events(logs)}"
    assert "message_id" in matching[0], f"missing 'message_id' field in: {matching[0]}"
    assert matching[0]["message_id"] == "om_log_test_classify"
    assert matching[0]["confidence_band"] == "low"


# ---------------------------------------------------------------------------
# Test 3: render_raw_note emits debug log with required fields
# ---------------------------------------------------------------------------


def test_render_raw_note_emits_log_with_fields(tmp_path: Path) -> None:
    """render_raw_note emits 'render_raw_note' event with target_path and entity_id."""
    from lark_to_notes.render.raw import render_raw_note

    item = _render_item(task_id="raw-log-id-001", fingerprint="raw-log-fp-001")
    logs = _run_and_capture(lambda: render_raw_note(item, tmp_path))

    matching = _matching(logs, "render_raw_note")
    assert matching, f"expected 'render_raw_note' event; got events: {_events(logs)}"
    evt = matching[0]
    assert "target_path" in evt, f"missing 'target_path' in: {evt}"
    assert "entity_id" in evt, f"missing 'entity_id' in: {evt}"
    assert evt["entity_id"] == "raw-log-id-001"
    assert evt["outcome"] == "created"


# ---------------------------------------------------------------------------
# Test 4: render_daily_note emits debug log with date field
# ---------------------------------------------------------------------------


def test_render_daily_note_emits_log_with_date(tmp_path: Path) -> None:
    """render_daily_note emits 'render_daily_note' event with 'date' field."""
    from lark_to_notes.render.daily import render_daily_note

    item = _render_item(
        task_id="daily-log-id-001",
        fingerprint="daily-log-fp-001",
        event_date="2026-05-02",
    )
    logs = _run_and_capture(lambda: render_daily_note(item, tmp_path))

    matching = _matching(logs, "render_daily_note")
    assert matching, f"expected 'render_daily_note' event; got: {_events(logs)}"
    evt = matching[0]
    assert "date" in evt, f"missing 'date' field in: {evt}"
    assert evt["date"] == "2026-05-02"
    assert evt["outcome"] == "created"


# ---------------------------------------------------------------------------
# Test 5: render_current_tasks_item emits log with promotion_rec field
# ---------------------------------------------------------------------------


def test_render_current_tasks_emits_log_with_promotion_rec(tmp_path: Path) -> None:
    """render_current_tasks_item emits event with 'promotion_rec' field."""
    from lark_to_notes.render.current_tasks import render_current_tasks_item

    item = _render_item(
        task_id="ct-log-id-001",
        fingerprint="ct-log-fp-001",
        promotion_rec="current_tasks",
    )
    logs = _run_and_capture(lambda: render_current_tasks_item(item, tmp_path))

    matching = _matching(logs, "render_current_tasks_item")
    assert matching, f"expected 'render_current_tasks_item' event; got: {_events(logs)}"
    evt = matching[0]
    assert "promotion_rec" in evt, f"missing 'promotion_rec' field in: {evt}"
    assert evt["promotion_rec"] == "current_tasks"


# ---------------------------------------------------------------------------
# Test 6: NoteWriter.render_raw emits INFO log with outcome
# ---------------------------------------------------------------------------


def test_note_writer_emits_info_log_with_outcome(tmp_path: Path) -> None:
    """NoteWriter.render_raw emits 'note_writer_raw' INFO event with 'outcome' field."""
    from lark_to_notes.render.writer import NoteWriter

    writer = NoteWriter(vault_root=tmp_path)
    item = _render_item(task_id="writer-log-id-001", fingerprint="writer-log-fp-001")
    logs = _run_and_capture(lambda: writer.render_raw(item))

    matching = _matching(logs, "note_writer_raw")
    assert matching, f"expected 'note_writer_raw' event; got: {_events(logs)}"
    evt = matching[0]
    assert "outcome" in evt, f"missing 'outcome' field in: {evt}"
    assert "entity_id" in evt, f"missing 'entity_id' field in: {evt}"


# ---------------------------------------------------------------------------
# Test 7: execute_work_batch emits start and processed log events
# ---------------------------------------------------------------------------


def test_runtime_batch_emits_start_and_processed_events(tmp_path: Path) -> None:
    """execute_work_batch emits runtime_batch_item_start + runtime_batch_item_processed."""
    from lark_to_notes.runtime.executor import execute_work_batch
    from lark_to_notes.runtime.models import RuntimeWorkItem
    from lark_to_notes.storage.db import connect, init_db

    db_path = tmp_path / "state.db"
    conn = connect(db_path)
    init_db(conn)
    lock_path = tmp_path / ".ltn.lock"

    items = [
        RuntimeWorkItem(
            source_id="dm:log_batch_test",
            item_key="om_batch_001",
            payload={"content": "Hello"},
        ),
    ]

    def _noop_processor(_item: RuntimeWorkItem) -> None:
        pass

    logs = _run_and_capture(
        lambda: execute_work_batch(
            conn,
            command="log_test",
            items=items,
            processor=_noop_processor,
            lock_path=lock_path,
        )
    )

    event_names = _events(logs)
    assert "runtime_batch_item_start" in event_names, (
        f"missing 'runtime_batch_item_start'; got: {event_names}"
    )
    assert "runtime_batch_item_processed" in event_names, (
        f"missing 'runtime_batch_item_processed'; got: {event_names}"
    )
    # Both events must carry run_id and item_key fields.
    start_evt = next(rec for rec in logs if rec.get("event") == "runtime_batch_item_start")
    processed_evt = next(rec for rec in logs if rec.get("event") == "runtime_batch_item_processed")
    assert "run_id" in start_evt
    assert start_evt["source_id"] == "dm:log_batch_test"
    assert start_evt["item_key"] == "om_batch_001"
    assert processed_evt["source_id"] == "dm:log_batch_test"
    assert processed_evt["item_key"] == "om_batch_001"


# ---------------------------------------------------------------------------
# Test 8: reconcile_cursors emits log event for each checked source
# ---------------------------------------------------------------------------


def test_reconcile_cursor_up_to_date_emits_log(tmp_path: Path) -> None:
    """reconcile_cursors emits 'reconcile_cursor_up_to_date' when cursors match."""
    from lark_to_notes.config.sources import SourceType, WatchedSource, make_source_id
    from lark_to_notes.runtime.reconcile import SourceState, reconcile_cursors
    from lark_to_notes.storage.db import connect, init_db, upsert_watched_source

    db_path = tmp_path / "state.db"
    conn = connect(db_path)
    init_db(conn)

    source = WatchedSource(
        source_id=make_source_id(SourceType.DM, "ou_log_recon"),
        source_type=SourceType.DM,
        external_id="ou_log_recon",
        name="Log Reconcile Source",
    )
    upsert_watched_source(conn, source)
    conn.execute(
        "INSERT INTO checkpoints (source_id, last_message_id, last_message_timestamp)"
        " VALUES (?, ?, ?)",
        (source.source_id, "om_last_01", "2026-05-01T10:00:00Z"),
    )
    conn.commit()

    source_states = {
        source.source_id: SourceState(
            source_id=source.source_id,
            latest_message_id="om_last_01",
            latest_message_timestamp="2026-05-01T10:00:00Z",
        )
    }

    logs = _run_and_capture(lambda: reconcile_cursors(conn, source_states))

    matching = _matching(logs, "reconcile_cursor_up_to_date")
    assert matching, f"expected 'reconcile_cursor_up_to_date' event; got: {_events(logs)}"
    assert matching[0]["source_id"] == source.source_id


# ---------------------------------------------------------------------------
# Test 9: ledger event appears before classify, classify before render
# ---------------------------------------------------------------------------


def test_full_pipeline_ledger_before_classify_before_render_ordering(tmp_path: Path) -> None:
    """Ledger capture must occur before classify, and classify before render."""
    from lark_to_notes.distill.models import DistillInput
    from lark_to_notes.distill.routing import classify_with_routing
    from lark_to_notes.intake.ledger import insert_raw_message
    from lark_to_notes.render.raw import render_raw_note
    from lark_to_notes.storage.db import connect, init_db

    vault_root = tmp_path / "vault"
    vault_root.mkdir()
    conn = connect(tmp_path / "state.db")
    init_db(conn)
    message = _raw_message("om_order_test")

    # Use a LOW-confidence message so classify_with_routing emits at least one log event.
    inp = DistillInput(
        message_id="om_order_test",
        source_id="dm:test",
        source_type="dm_user",
        content="context context context " * 50,
        sender_name="Alice",
        direction="incoming",
        created_at="2026-05-01T10:00:00Z",
    )
    item = _render_item(
        task_id="order-test-id-001",
        fingerprint="order-test-fp-001",
        event_date="2026-05-01",
    )

    buf = io.StringIO()
    old_stderr = sys.stderr
    sys.stderr = buf
    configure_logging("DEBUG", json_logs=True)
    try:
        insert_raw_message(conn, message)
        classify_with_routing(inp, llm_provider=None)
        render_raw_note(item, vault_root)
    finally:
        sys.stderr = old_stderr
        configure_logging("WARNING")
        structlog.reset_defaults()

    logs: list[dict[str, object]] = []
    for raw_line in buf.getvalue().splitlines():
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        with contextlib.suppress(json.JSONDecodeError):
            logs.append(json.loads(raw_line))

    ledger_indices = [i for i, rec in enumerate(logs) if rec.get("event") == "insert_raw_message"]
    classify_indices = [
        i for i, rec in enumerate(logs) if rec.get("event") == "llm_escalation_skipped"
    ]
    render_indices = [i for i, rec in enumerate(logs) if rec.get("event") == "render_raw_note"]

    assert ledger_indices, "no insert_raw_message log event found"
    assert classify_indices, "no classify log event found"
    assert render_indices, "no render_raw_note log event found"
    assert min(ledger_indices) < min(classify_indices), (
        f"ledger event (index {min(ledger_indices)}) must appear before "
        f"classify event (index {min(classify_indices)})"
    )
    assert min(classify_indices) < min(render_indices), (
        f"classify event (index {min(classify_indices)}) must appear before "
        f"render event (index {min(render_indices)})"
    )


def test_verify_live_adapter_script_emits_step_events() -> None:
    """``scripts/verify_live_adapter.py`` is the offline operator smoke harness."""

    repo_root = Path(__file__).resolve().parents[1]
    script = repo_root / "scripts" / "verify_live_adapter.py"
    proc = subprocess.run(
        [sys.executable, str(script)],
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "PYTHONPATH": str(repo_root / "src")},
    )
    assert proc.returncode == 0, proc.stderr + proc.stdout
    events: list[dict[str, object]] = []
    for raw in proc.stderr.splitlines():
        raw = raw.strip()
        if raw.startswith("{"):
            with contextlib.suppress(json.JSONDecodeError):
                events.append(json.loads(raw))
    steps = {str(e.get("step", "")) for e in events}
    assert "doctor" in steps
    assert "sync_events" in steps


def test_verify_live_adapter_writes_artifact_jsonl(tmp_path: Path) -> None:
    """``--artifacts-dir`` mirrors stderr steps for post-run inspection."""

    repo_root = Path(__file__).resolve().parents[1]
    script = repo_root / "scripts" / "verify_live_adapter.py"
    art_dir = tmp_path / "artifacts"
    proc = subprocess.run(
        [
            sys.executable,
            str(script),
            "--artifacts-dir",
            str(art_dir),
        ],
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "PYTHONPATH": str(repo_root / "src")},
    )
    assert proc.returncode == 0, proc.stderr + proc.stdout
    report = art_dir / "verify_live_steps.jsonl"
    assert report.is_file()
    lines = [ln for ln in report.read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert len(lines) >= 4
    parsed = [json.loads(ln) for ln in lines]
    steps_ok = {(str(e.get("step", "")), str(e.get("status", ""))) for e in parsed}
    assert ("doctor", "ok") in steps_ok
    assert ("sync_events", "ok") in steps_ok
