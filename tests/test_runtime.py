"""Tests for the lark_to_notes.runtime package.

Coverage:
 - schema v5: runtime_runs and dead_letters tables created by init_db
 - registry: start_run, finish_run, cancel_run, get_run, list_runs,
   quarantine_item, list_dead_letters, health_report
 - retry: RetryPolicy.should_retry, delay_for, PermanentError short-circuit
 - lock: RuntimeLock acquire/release via context manager, is_held, idempotent release
 - reconcile: reconcile_cursors gap detection, repair callback, up-to-date sources,
   missing checkpoints, repair failures
 - executor: serialized batch execution, retry/quarantine orchestration,
   reconciliation run tracking
 - models: dataclass construction and field defaults
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from collections.abc import Generator
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from lark_to_notes.intake.ledger import (
    chat_ingest_key,
    count_raw_messages,
    get_chat_intake_item,
    observe_chat_message,
)
from lark_to_notes.intake.models import ChatIntakeState, IntakePath, RawMessage
from lark_to_notes.runtime.executor import (
    drain_ready_chat_intake,
    execute_reconcile_run,
    execute_work_batch,
    observe_and_drain_chat_message,
    run_background_runtime,
)
from lark_to_notes.runtime.lock import RuntimeLock
from lark_to_notes.runtime.models import (
    BatchRunResult,
    DeadLetter,
    HealthReport,
    ReconcileReport,
    ReconcileRunResult,
    RunStatus,
    RuntimeDaemonResult,
    RuntimeRun,
    RuntimeWorkItem,
)
from lark_to_notes.runtime.reconcile import SourceState, reconcile_cursors
from lark_to_notes.runtime.registry import (
    cancel_run,
    finish_run,
    get_run,
    health_report,
    list_dead_letters,
    list_runs,
    quarantine_item,
    start_run,
)
from lark_to_notes.runtime.retry import PermanentError, RetryPolicy
from lark_to_notes.storage.db import connect, init_db
from lark_to_notes.storage.schema import SCHEMA_VERSION

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def conn() -> Generator[sqlite3.Connection, None, None]:
    """Return an in-memory SQLite connection with the full schema applied."""
    c = connect(":memory:")
    init_db(c)
    yield c
    c.close()


def _chat_msg(
    message_id: str = "msg-1",
    *,
    source_id: str = "dm:u1",
    content: str = "hello",
) -> RawMessage:
    return RawMessage(
        message_id=message_id,
        source_id=source_id,
        source_type="dm_user",
        chat_id="ou_chat",
        chat_type="p2p",
        sender_id="ou_sender",
        sender_name="Alice",
        direction="incoming",
        created_at="2026-04-14 10:00",
        content=content,
        payload={"content": content},
    )


# ---------------------------------------------------------------------------
# Schema v5
# ---------------------------------------------------------------------------


class TestSchemaV5:
    def test_schema_version_is_12(self) -> None:
        assert SCHEMA_VERSION == 12

    def test_reaction_orphan_queue_table_exists(self, conn) -> None:  # type: ignore[no-untyped-def]
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='reaction_orphan_queue'"
        ).fetchone()
        assert row is not None

    def test_runtime_runs_table_exists(self, conn) -> None:  # type: ignore[no-untyped-def]
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='runtime_runs'"
        ).fetchone()
        assert row is not None

    def test_dead_letters_table_exists(self, conn) -> None:  # type: ignore[no-untyped-def]
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='dead_letters'"
        ).fetchone()
        assert row is not None

    def test_init_db_idempotent(self, conn) -> None:  # type: ignore[no-untyped-def]
        """Running init_db a second time on the same connection must not raise."""
        init_db(conn)  # second call — should be a no-op


# ---------------------------------------------------------------------------
# RetryPolicy
# ---------------------------------------------------------------------------


class TestRetryPolicy:
    def test_should_retry_within_attempts(self) -> None:
        policy = RetryPolicy(max_attempts=3)
        assert policy.should_retry(0, ValueError("transient")) is True
        assert policy.should_retry(1, ValueError("transient")) is True

    def test_should_not_retry_at_max_attempts(self) -> None:
        policy = RetryPolicy(max_attempts=3)
        assert policy.should_retry(2, ValueError("transient")) is False

    def test_permanent_error_never_retried(self) -> None:
        policy = RetryPolicy(max_attempts=10)
        assert policy.should_retry(0, PermanentError("final")) is False

    def test_delay_increases_with_attempt(self) -> None:
        policy = RetryPolicy(base_delay_s=1.0, max_delay_s=1000.0, jitter_factor=0.0)
        d0 = policy.delay_for(0)
        d1 = policy.delay_for(1)
        d2 = policy.delay_for(2)
        assert d1 > d0
        assert d2 > d1

    def test_delay_capped_at_max(self) -> None:
        policy = RetryPolicy(base_delay_s=1.0, max_delay_s=5.0, jitter_factor=0.0)
        assert policy.delay_for(20) == pytest.approx(5.0)

    def test_delay_no_negative(self) -> None:
        policy = RetryPolicy(base_delay_s=0.001, max_delay_s=1.0)
        for attempt in range(10):
            assert policy.delay_for(attempt) >= 0.0

    def test_jitter_adds_variance(self) -> None:
        policy = RetryPolicy(base_delay_s=1.0, max_delay_s=100.0, jitter_factor=1.0)
        delays = {policy.delay_for(2) for _ in range(20)}
        # With jitter_factor=1.0 we expect at least some variance
        assert len(delays) > 1

    def test_zero_jitter_is_deterministic(self) -> None:
        policy = RetryPolicy(base_delay_s=1.0, max_delay_s=100.0, jitter_factor=0.0)
        delays = {policy.delay_for(3) for _ in range(10)}
        assert len(delays) == 1


# ---------------------------------------------------------------------------
# RuntimeRun registry
# ---------------------------------------------------------------------------


class TestRuntimeRunRegistry:
    def test_start_run_creates_record(self, conn) -> None:  # type: ignore[no-untyped-def]
        run = start_run(conn, "sync")
        assert run.run_id
        assert run.command == "sync"
        assert run.status == RunStatus.RUNNING
        assert run.finished_at is None

    def test_start_run_explicit_id(self, conn) -> None:  # type: ignore[no-untyped-def]
        run = start_run(conn, "replay", run_id="test-run-001")
        assert run.run_id == "test-run-001"

    def test_get_run_returns_record(self, conn) -> None:  # type: ignore[no-untyped-def]
        run = start_run(conn, "sync")
        fetched = get_run(conn, run.run_id)
        assert fetched is not None
        assert fetched.run_id == run.run_id
        assert fetched.status == RunStatus.RUNNING

    def test_get_run_returns_none_for_missing(self, conn) -> None:  # type: ignore[no-untyped-def]
        assert get_run(conn, "nonexistent-id") is None

    def test_finish_run_completed(self, conn) -> None:  # type: ignore[no-untyped-def]
        run = start_run(conn, "sync")
        finished = finish_run(conn, run.run_id, items_processed=5)
        assert finished is not None
        assert finished.status == RunStatus.COMPLETED
        assert finished.items_processed == 5
        assert finished.finished_at is not None
        assert finished.error is None

    def test_finish_run_failed(self, conn) -> None:  # type: ignore[no-untyped-def]
        run = start_run(conn, "sync")
        finished = finish_run(conn, run.run_id, error="API timeout")
        assert finished is not None
        assert finished.status == RunStatus.FAILED
        assert finished.error == "API timeout"

    def test_finish_run_items_failed(self, conn) -> None:  # type: ignore[no-untyped-def]
        run = start_run(conn, "sync")
        finished = finish_run(conn, run.run_id, items_processed=3, items_failed=1)
        assert finished is not None
        assert finished.items_processed == 3
        assert finished.items_failed == 1

    def test_finish_run_missing_returns_none(self, conn) -> None:  # type: ignore[no-untyped-def]
        result = finish_run(conn, "no-such-run")
        assert result is None

    def test_cancel_run(self, conn) -> None:  # type: ignore[no-untyped-def]
        run = start_run(conn, "sync")
        cancelled = cancel_run(conn, run.run_id)
        assert cancelled is not None
        assert cancelled.status == RunStatus.CANCELLED

    def test_cancel_run_only_affects_running(self, conn) -> None:  # type: ignore[no-untyped-def]
        run = start_run(conn, "sync")
        finish_run(conn, run.run_id)
        # Already completed — cancel should not change status
        result = cancel_run(conn, run.run_id)
        assert result is not None
        assert result.status == RunStatus.COMPLETED

    def test_list_runs_returns_newest_first(self, conn) -> None:  # type: ignore[no-untyped-def]
        r1 = start_run(conn, "sync")
        r2 = start_run(conn, "replay")
        runs = list_runs(conn)
        # Most recent first; r2 started after r1
        ids = [r.run_id for r in runs]
        assert ids.index(r2.run_id) < ids.index(r1.run_id)

    def test_list_runs_limit(self, conn) -> None:  # type: ignore[no-untyped-def]
        for _ in range(5):
            start_run(conn, "sync")
        runs = list_runs(conn, limit=3)
        assert len(runs) == 3

    def test_list_runs_filter_by_status(self, conn) -> None:  # type: ignore[no-untyped-def]
        r1 = start_run(conn, "sync")
        r2 = start_run(conn, "sync")
        finish_run(conn, r1.run_id)
        running = list_runs(conn, status=RunStatus.RUNNING)
        run_ids = {r.run_id for r in running}
        assert r2.run_id in run_ids
        assert r1.run_id not in run_ids


# ---------------------------------------------------------------------------
# Dead-letter / quarantine registry
# ---------------------------------------------------------------------------


class TestDeadLetterRegistry:
    def test_quarantine_item_creates_record(self, conn) -> None:  # type: ignore[no-untyped-def]
        dl = quarantine_item(conn, "src-001", "disk full", raw_message_id="msg-42")
        assert dl.dl_id
        assert dl.source_id == "src-001"
        assert dl.raw_message_id == "msg-42"
        assert dl.last_error == "disk full"
        assert dl.quarantined_at

    def test_quarantine_item_attempt_count(self, conn) -> None:  # type: ignore[no-untyped-def]
        dl = quarantine_item(conn, "src-001", "error", attempt_count=3)
        assert dl.attempt_count == 3

    def test_quarantine_item_no_message_id(self, conn) -> None:  # type: ignore[no-untyped-def]
        dl = quarantine_item(conn, "src-001", "parse error")
        assert dl.raw_message_id is None

    def test_list_dead_letters_all(self, conn) -> None:  # type: ignore[no-untyped-def]
        quarantine_item(conn, "src-001", "error A")
        quarantine_item(conn, "src-002", "error B")
        items = list_dead_letters(conn)
        assert len(items) == 2

    def test_list_dead_letters_filter_by_source(self, conn) -> None:  # type: ignore[no-untyped-def]
        quarantine_item(conn, "src-001", "error A")
        quarantine_item(conn, "src-002", "error B")
        items = list_dead_letters(conn, source_id="src-001")
        assert len(items) == 1
        assert items[0].source_id == "src-001"

    def test_list_dead_letters_limit(self, conn) -> None:  # type: ignore[no-untyped-def]
        for _ in range(5):
            quarantine_item(conn, "src-001", "error")
        items = list_dead_letters(conn, limit=3)
        assert len(items) == 3


# ---------------------------------------------------------------------------
# Health report
# ---------------------------------------------------------------------------


class TestHealthReport:
    def test_empty_db_health(self, conn) -> None:  # type: ignore[no-untyped-def]
        report = health_report(conn)
        assert report.run_count_total == 0
        assert report.run_count_failed == 0
        assert report.run_count_running == 0
        assert report.dead_letter_count == 0
        assert report.queue_depth == 0
        assert report.error_rate == pytest.approx(0.0)
        assert report.last_run_at is None
        assert report.lag_seconds is None
        assert report.repeated_item_count == 0
        assert report.duplicate_event_count == 0

    def test_health_counts_runs(self, conn) -> None:  # type: ignore[no-untyped-def]
        r1 = start_run(conn, "sync")
        r2 = start_run(conn, "sync")
        finish_run(conn, r1.run_id)
        finish_run(conn, r2.run_id, error="timeout")
        report = health_report(conn)
        assert report.run_count_total == 2
        assert report.run_count_failed == 1
        assert report.run_count_running == 0

    def test_health_counts_running(self, conn) -> None:  # type: ignore[no-untyped-def]
        start_run(conn, "sync")
        report = health_report(conn)
        assert report.run_count_running == 1

    def test_health_error_rate(self, conn) -> None:  # type: ignore[no-untyped-def]
        r1 = start_run(conn, "sync")
        r2 = start_run(conn, "sync")
        # r1: 8 processed, 2 failed → total 10 items, 2 failed = 20%
        finish_run(conn, r1.run_id, items_processed=8, items_failed=2)
        finish_run(conn, r2.run_id, items_processed=10, items_failed=0)
        report = health_report(conn)
        assert report.error_rate == pytest.approx(2 / 20)  # 10%

    def test_health_dead_letter_count(self, conn) -> None:  # type: ignore[no-untyped-def]
        quarantine_item(conn, "src-001", "error A")
        quarantine_item(conn, "src-001", "error B")
        report = health_report(conn)
        assert report.dead_letter_count == 2

    def test_health_last_run_fields(self, conn) -> None:  # type: ignore[no-untyped-def]
        r = start_run(conn, "reconcile")
        finish_run(conn, r.run_id)
        report = health_report(conn)
        assert report.last_run_command == "reconcile"
        assert report.last_run_status == RunStatus.COMPLETED.value
        assert report.last_run_at is not None

    def test_health_return_type(self, conn) -> None:  # type: ignore[no-untyped-def]
        report = health_report(conn)
        assert isinstance(report, HealthReport)

    def test_health_queue_metrics(self, conn: sqlite3.Connection) -> None:
        report = health_report(
            conn,
            queued_items=(
                RuntimeWorkItem(
                    source_id="src-001",
                    item_key="item-1",
                    queued_at="2024-01-01T00:00:00Z",
                ),
                RuntimeWorkItem(
                    source_id="src-001",
                    item_key="item-2",
                    queued_at="2024-01-01T00:05:00Z",
                ),
            ),
            now=datetime(2024, 1, 1, 0, 10, tzinfo=UTC),
        )
        assert report.queue_depth == 2
        assert report.lag_seconds == pytest.approx(600.0)

    def test_health_duplicate_observation_metrics(self, conn: sqlite3.Connection) -> None:
        msg = _chat_msg("msg-health-dupe")
        observe_chat_message(
            conn,
            msg,
            intake_path=IntakePath.POLL,
            observed_at="2026-04-14T10:00:00Z",
        )
        observe_chat_message(
            conn,
            msg,
            intake_path=IntakePath.EVENT,
            observed_at="2026-04-14T10:00:10Z",
        )

        report = health_report(conn)
        assert report.repeated_item_count == 1
        assert report.duplicate_event_count == 1


# ---------------------------------------------------------------------------
# RuntimeLock
# ---------------------------------------------------------------------------


class TestRuntimeLock:
    def test_context_manager_acquire_and_release(self, tmp_path: Path) -> None:
        lock_file = tmp_path / ".test.lock"
        lock = RuntimeLock(lock_file, owner_tag="test")
        with lock:
            assert lock.is_held
        assert not lock.is_held

    def test_creates_lock_file(self, tmp_path: Path) -> None:
        lock_file = tmp_path / "sub" / ".test.lock"
        with RuntimeLock(lock_file):
            assert lock_file.exists()

    def test_release_without_acquire_is_safe(self, tmp_path: Path) -> None:
        lock = RuntimeLock(tmp_path / ".test.lock")
        lock.release()  # Should not raise

    def test_serializes_threads(self, tmp_path: Path) -> None:
        """Two threads holding the same lock should not overlap."""
        lock_file = tmp_path / ".test.lock"
        results: list[tuple[float, float]] = []
        errors: list[str] = []

        def worker(idx: int) -> None:
            lk = RuntimeLock(lock_file, owner_tag=f"worker-{idx}")
            with lk:
                start = time.monotonic()
                time.sleep(0.02)
                end = time.monotonic()
                results.append((start, end))

        t1 = threading.Thread(target=worker, args=(1,))
        t2 = threading.Thread(target=worker, args=(2,))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert not errors
        assert len(results) == 2
        # Verify the two intervals do not overlap
        (_s1, e1), (s2, _e2) = sorted(results)
        assert e1 <= s2 + 0.005  # small tolerance for timing noise


# ---------------------------------------------------------------------------
# Reconcile cursors
# ---------------------------------------------------------------------------


class TestReconcileCursors:
    def test_no_sources_no_gaps(self, conn) -> None:  # type: ignore[no-untyped-def]
        report = reconcile_cursors(conn, {})
        assert report.source_ids_checked == 0
        assert report.gaps_found == 0

    def test_up_to_date_cursor_no_gap(self, conn) -> None:  # type: ignore[no-untyped-def]
        # Insert a checkpoint
        conn.execute(
            "INSERT INTO watched_sources (source_id, source_type, external_id, name)"
            " VALUES ('s1', 'dm', 'ext-1', 'Source 1')"
        )
        conn.execute(
            "INSERT INTO checkpoints (source_id, last_message_id, last_message_timestamp,"
            " updated_at) VALUES ('s1', 'msg-100', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z')"
        )
        conn.commit()

        states = {"s1": SourceState("s1", "msg-100", "2024-01-01T00:00:00Z")}
        report = reconcile_cursors(conn, states)
        assert report.source_ids_checked == 1
        assert report.gaps_found == 0

    def test_gap_detected_when_cursor_stale(self, conn) -> None:  # type: ignore[no-untyped-def]
        conn.execute(
            "INSERT INTO watched_sources (source_id, source_type, external_id, name)"
            " VALUES ('s1', 'dm', 'ext-1', 'Source 1')"
        )
        conn.execute(
            "INSERT INTO checkpoints (source_id, last_message_id, last_message_timestamp,"
            " updated_at) VALUES ('s1', 'msg-50', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z')"
        )
        conn.commit()

        states = {"s1": SourceState("s1", "msg-100", "2024-01-02T00:00:00Z")}
        report = reconcile_cursors(conn, states)
        assert report.gaps_found == 1
        assert len(report.gap_details) == 1

    def test_gap_detected_when_no_checkpoint(self, conn) -> None:  # type: ignore[no-untyped-def]
        states = {"s-missing": SourceState("s-missing", "msg-001", "2024-01-01T00:00:00Z")}
        report = reconcile_cursors(conn, states)
        assert report.source_ids_checked == 1
        assert report.gaps_found == 1

    def test_repair_fn_called_on_gap(self, conn) -> None:  # type: ignore[no-untyped-def]
        states = {"s1": SourceState("s1", "msg-001", "2024-01-01T00:00:00Z")}
        repaired: list[tuple[str, str | None]] = []

        def repair(source_id: str, cursor: str | None) -> None:
            repaired.append((source_id, cursor))

        report = reconcile_cursors(conn, states, repair_fn=repair)
        assert report.repairs_attempted == 1
        assert report.repairs_succeeded == 1
        assert repaired == [("s1", None)]

    def test_repair_fn_receives_stored_cursor(self, conn) -> None:  # type: ignore[no-untyped-def]
        conn.execute(
            "INSERT INTO watched_sources (source_id, source_type, external_id, name)"
            " VALUES ('s1', 'dm', 'ext-1', 'Source 1')"
        )
        conn.execute(
            "INSERT INTO checkpoints (source_id, last_message_id, last_message_timestamp,"
            " updated_at) VALUES ('s1', 'msg-50', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z')"
        )
        conn.commit()

        states = {"s1": SourceState("s1", "msg-100", "2024-01-02T00:00:00Z")}
        received_cursor: list[str | None] = []

        def repair(source_id: str, cursor: str | None) -> None:
            received_cursor.append(cursor)

        reconcile_cursors(conn, states, repair_fn=repair)
        assert received_cursor == ["msg-50"]

    def test_repair_failure_counted(self, conn) -> None:  # type: ignore[no-untyped-def]
        states = {"s1": SourceState("s1", "msg-001", "2024-01-01T00:00:00Z")}

        def repair(source_id: str, cursor: str | None) -> None:
            raise RuntimeError("network error")

        report = reconcile_cursors(conn, states, repair_fn=repair)
        assert report.repairs_attempted == 1
        assert report.repairs_succeeded == 0
        assert report.repairs_failed == 1

    def test_no_repair_fn_still_reports_gap(self, conn) -> None:  # type: ignore[no-untyped-def]
        states = {"s1": SourceState("s1", "msg-001", "2024-01-01T00:00:00Z")}
        report = reconcile_cursors(conn, states, repair_fn=None)
        assert report.gaps_found == 1
        assert report.repairs_attempted == 0

    def test_multiple_sources_mixed_results(self, conn) -> None:  # type: ignore[no-untyped-def]
        # s1: up to date; s2: gap; s3: no checkpoint
        conn.execute(
            "INSERT INTO watched_sources (source_id, source_type, external_id, name)"
            " VALUES ('s1', 'dm', 'e1', 'S1')"
        )
        conn.execute(
            "INSERT INTO watched_sources (source_id, source_type, external_id, name)"
            " VALUES ('s2', 'dm', 'e2', 'S2')"
        )
        conn.execute(
            "INSERT INTO checkpoints (source_id, last_message_id, last_message_timestamp,"
            " updated_at) VALUES ('s1', 'msg-100', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z')"
        )
        conn.execute(
            "INSERT INTO checkpoints (source_id, last_message_id, last_message_timestamp,"
            " updated_at) VALUES ('s2', 'msg-50', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z')"
        )
        conn.commit()

        states = {
            "s1": SourceState("s1", "msg-100", "2024-01-01T00:00:00Z"),  # up to date
            "s2": SourceState("s2", "msg-100", "2024-01-02T00:00:00Z"),  # gap
            "s3": SourceState("s3", "msg-001", "2024-01-01T00:00:00Z"),  # no checkpoint
        }
        report = reconcile_cursors(conn, states)
        assert report.source_ids_checked == 3
        assert report.gaps_found == 2


# ---------------------------------------------------------------------------
# Runtime executor
# ---------------------------------------------------------------------------


class TestRuntimeExecutor:
    def test_execute_work_batch_records_completed_run(
        self, conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        items = (
            RuntimeWorkItem(source_id="src-001", item_key="item-1"),
            RuntimeWorkItem(source_id="src-001", item_key="item-2"),
        )
        seen: list[str] = []

        def processor(item: RuntimeWorkItem) -> None:
            seen.append(item.item_key)

        result = execute_work_batch(
            conn,
            command="sync",
            items=items,
            processor=processor,
            lock_path=tmp_path / ".ltn.lock",
            sleep_fn=lambda _delay: None,
        )

        assert seen == ["item-1", "item-2"]
        assert result.run.status == RunStatus.COMPLETED
        assert result.items_total == 2
        assert result.items_processed == 2
        assert result.items_failed == 0
        assert result.retry_count == 0
        assert result.queue_depth_peak == 2
        assert result.dead_letter_ids == ()

    def test_execute_work_batch_retries_then_succeeds(
        self, conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        item = RuntimeWorkItem(source_id="src-001", item_key="item-1")
        attempts = 0
        delays: list[float] = []

        def processor(work_item: RuntimeWorkItem) -> None:
            nonlocal attempts
            assert work_item.item_key == "item-1"
            attempts += 1
            if attempts == 1:
                raise ValueError("transient")

        result = execute_work_batch(
            conn,
            command="sync",
            items=(item,),
            processor=processor,
            lock_path=tmp_path / ".ltn.lock",
            retry_policy=RetryPolicy(
                max_attempts=3, base_delay_s=1.0, max_delay_s=1.0, jitter_factor=0.0
            ),
            sleep_fn=delays.append,
        )

        assert attempts == 2
        assert delays == [1.0]
        assert result.items_processed == 1
        assert result.items_failed == 0
        assert result.retry_count == 1
        assert list_dead_letters(conn) == []

    def test_execute_work_batch_quarantines_permanent_error(
        self, conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        item = RuntimeWorkItem(
            source_id="src-001",
            item_key="item-1",
            raw_message_id="msg-42",
        )
        delays: list[float] = []

        def processor(_work_item: RuntimeWorkItem) -> None:
            raise PermanentError("bad payload")

        result = execute_work_batch(
            conn,
            command="sync",
            items=(item,),
            processor=processor,
            lock_path=tmp_path / ".ltn.lock",
            retry_policy=RetryPolicy(max_attempts=5),
            sleep_fn=delays.append,
        )

        dead_letters = list_dead_letters(conn)
        assert delays == []
        assert result.items_processed == 0
        assert result.items_failed == 1
        assert len(result.dead_letter_ids) == 1
        assert dead_letters[0].raw_message_id == "msg-42"
        assert dead_letters[0].attempt_count == 1

    def test_execute_work_batch_quarantines_after_retry_exhaustion(
        self, conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        item = RuntimeWorkItem(source_id="src-001", item_key="item-1")
        delays: list[float] = []
        attempts = 0

        def processor(_work_item: RuntimeWorkItem) -> None:
            nonlocal attempts
            attempts += 1
            raise ValueError("still broken")

        result = execute_work_batch(
            conn,
            command="sync",
            items=(item,),
            processor=processor,
            lock_path=tmp_path / ".ltn.lock",
            retry_policy=RetryPolicy(
                max_attempts=3, base_delay_s=0.5, max_delay_s=0.5, jitter_factor=0.0
            ),
            sleep_fn=delays.append,
        )

        dead_letters = list_dead_letters(conn)
        assert attempts == 3
        assert delays == [0.5, 0.5]
        assert result.retry_count == 2
        assert dead_letters[0].attempt_count == 3

    def test_execute_work_batch_logs_queue_depth_per_item(
        self, conn: sqlite3.Connection, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.DEBUG, logger="lark_to_notes.runtime.executor")
        items = tuple(RuntimeWorkItem(source_id="src-live", item_key=f"key-{i}") for i in range(3))

        def processor(_item: RuntimeWorkItem) -> None:
            return None

        execute_work_batch(
            conn,
            command="live-stress",
            items=items,
            processor=processor,
            lock_path=tmp_path / "batch.lock",
            sleep_fn=lambda _d: None,
        )
        starts = [
            r
            for r in caplog.records
            if r.msg == "runtime_batch_item_start" and r.name == "lark_to_notes.runtime.executor"
        ]
        depths = [getattr(r, "queue_depth", None) for r in starts]
        assert depths == [3, 2, 1]

    def test_execute_work_batch_marks_run_failed_on_setup_error(
        self,
        conn: sqlite3.Connection,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def boom(self: RuntimeLock) -> None:
            raise OSError("cannot acquire")

        monkeypatch.setattr(RuntimeLock, "acquire", boom)

        with pytest.raises(OSError, match="cannot acquire"):
            execute_work_batch(
                conn,
                command="sync",
                items=(RuntimeWorkItem(source_id="src-001", item_key="item-1"),),
                processor=lambda _item: None,
                lock_path=tmp_path / ".ltn.lock",
                sleep_fn=lambda _delay: None,
            )

        runs = list_runs(conn, limit=1)
        assert runs[0].status == RunStatus.FAILED
        assert runs[0].error == "cannot acquire"

    def test_execute_work_batch_serializes_across_connections(self, tmp_path: Path) -> None:
        db_path = tmp_path / "runtime.db"
        init_conn = connect(db_path)
        init_db(init_conn)
        init_conn.close()
        lock_path = tmp_path / ".ltn.lock"
        intervals: list[tuple[float, float]] = []
        intervals_lock = threading.Lock()

        def worker(item_key: str) -> None:
            conn = connect(db_path)
            try:
                execute_work_batch(
                    conn,
                    command="sync",
                    items=(RuntimeWorkItem(source_id="src-001", item_key=item_key),),
                    processor=lambda _item: _record_interval(intervals, intervals_lock),
                    lock_path=lock_path,
                    sleep_fn=lambda _delay: None,
                )
            finally:
                conn.close()

        def _record_interval(
            recorded_intervals: list[tuple[float, float]],
            recorded_intervals_lock: threading.Lock,
        ) -> None:
            start = time.monotonic()
            time.sleep(0.02)
            end = time.monotonic()
            with recorded_intervals_lock:
                recorded_intervals.append((start, end))

        threads = [
            threading.Thread(target=worker, args=("item-1",)),
            threading.Thread(target=worker, args=("item-2",)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert len(intervals) == 2
        (_s1, e1), (s2, _e2) = sorted(intervals)
        assert e1 <= s2 + 0.005

    def test_execute_reconcile_run_records_runtime_history(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            "INSERT INTO watched_sources (source_id, source_type, external_id, name)"
            " VALUES ('s1', 'dm', 'ext-1', 'Source 1')"
        )
        conn.execute(
            "INSERT INTO checkpoints (source_id, last_message_id, last_message_timestamp,"
            " updated_at) VALUES ('s1', 'msg-50', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z')"
        )
        conn.commit()
        repaired: list[tuple[str, str | None]] = []

        result = execute_reconcile_run(
            conn,
            {"s1": SourceState("s1", "msg-100", "2024-01-02T00:00:00Z")},
            repair_fn=lambda source_id, cursor: repaired.append((source_id, cursor)),
        )

        assert repaired == [("s1", "msg-50")]
        assert result.run.command == "reconcile"
        assert result.run.status == RunStatus.COMPLETED
        assert result.run.items_processed == 1
        assert result.run.items_failed == 0
        assert result.report.gaps_found == 1

    def test_execute_reconcile_run_counts_failed_repairs(self, conn: sqlite3.Connection) -> None:
        result = execute_reconcile_run(
            conn,
            {"s1": SourceState("s1", "msg-100", "2024-01-02T00:00:00Z")},
            repair_fn=lambda _source_id, _cursor: (_ for _ in ()).throw(
                RuntimeError("repair failed")
            ),
        )

        assert result.run.status == RunStatus.COMPLETED
        assert result.run.items_processed == 0
        assert result.run.items_failed == 1
        assert result.report.repairs_failed == 1

    def test_run_background_runtime_drains_batches(
        self, conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        batches = [
            [
                RuntimeWorkItem(source_id="src-001", item_key="item-1"),
                RuntimeWorkItem(source_id="src-001", item_key="item-2"),
            ],
            [],
        ]
        seen: list[str] = []
        sleeps: list[float] = []

        def fetch_batch() -> list[RuntimeWorkItem]:
            return batches.pop(0)

        def processor(item: RuntimeWorkItem) -> None:
            seen.append(item.item_key)

        result = run_background_runtime(
            conn,
            command="sync-daemon",
            fetch_batch=fetch_batch,
            processor=processor,
            lock_path=tmp_path / ".ltn.lock",
            sleep_fn=sleeps.append,
            poll_interval_s=0.25,
            stop_when_idle=True,
        )

        assert seen == ["item-1", "item-2"]
        assert result.cycle_count == 2
        assert result.idle_cycles == 1
        assert result.items_seen == 2
        assert result.items_processed == 2
        assert result.items_failed == 0
        assert result.queue_depth_peak == 2
        assert len(result.run_ids) == 1
        assert sleeps == [0.25]

    def test_drain_ready_chat_intake_processes_event_then_poll_once(
        self, conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        msg = _chat_msg("msg-event-poll")
        observe_chat_message(
            conn,
            msg,
            intake_path=IntakePath.EVENT,
            observed_at="2026-04-14T10:00:00Z",
            coalesce_window_seconds=120,
        )
        observe_chat_message(
            conn,
            msg,
            intake_path=IntakePath.POLL,
            observed_at="2026-04-14T10:00:15Z",
        )

        result = drain_ready_chat_intake(
            conn,
            lock_path=tmp_path / ".ltn.lock",
            as_of="2026-04-14T10:00:15Z",
            sleep_fn=lambda _delay: None,
        )

        item = get_chat_intake_item(conn, chat_ingest_key(msg.source_id, msg.message_id))
        assert result.items_total == 1
        assert result.items_processed == 1
        assert count_raw_messages(conn) == 1
        assert item is not None
        assert item.processing_state is ChatIntakeState.PROCESSED

    def test_drain_ready_chat_intake_processes_poll_then_event_once(
        self, conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        msg = _chat_msg("msg-poll-event")
        observe_chat_message(
            conn,
            msg,
            intake_path=IntakePath.POLL,
            observed_at="2026-04-14T10:00:00Z",
        )
        observe_chat_message(
            conn,
            msg,
            intake_path=IntakePath.EVENT,
            observed_at="2026-04-14T10:00:20Z",
            coalesce_window_seconds=120,
        )

        result = drain_ready_chat_intake(
            conn,
            lock_path=tmp_path / ".ltn.lock",
            as_of="2026-04-14T10:00:20Z",
            sleep_fn=lambda _delay: None,
        )

        assert result.items_total == 1
        assert result.items_processed == 1
        assert count_raw_messages(conn) == 1

    def test_drain_ready_chat_intake_is_retry_safe(
        self, conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        msg = _chat_msg("msg-retry-safe")
        observe_chat_message(
            conn,
            msg,
            intake_path=IntakePath.POLL,
            observed_at="2026-04-14T10:00:00Z",
        )

        first = drain_ready_chat_intake(
            conn,
            lock_path=tmp_path / ".ltn.lock",
            as_of="2026-04-14T10:00:00Z",
            sleep_fn=lambda _delay: None,
        )
        second = drain_ready_chat_intake(
            conn,
            lock_path=tmp_path / ".ltn.lock",
            as_of="2026-04-14T10:00:10Z",
            sleep_fn=lambda _delay: None,
        )

        assert first.items_total == 1
        assert second.items_total == 0
        assert count_raw_messages(conn) == 1

    def test_observe_and_drain_chat_message_processes_poll_immediately(
        self, conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        observed, drained = observe_and_drain_chat_message(
            conn,
            message=_chat_msg("msg-bridge-poll"),
            intake_path=IntakePath.POLL,
            observed_at="2026-04-14T10:00:00Z",
            lock_path=tmp_path / ".ltn.lock",
            sleep_fn=lambda _delay: None,
        )

        assert observed.poll_seen_count == 1
        assert drained is not None
        assert drained.items_total == 1
        assert drained.items_processed == 1
        assert count_raw_messages(conn) == 1

    def test_observe_and_drain_chat_message_leaves_event_pending_until_poll_arrives(
        self, conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        msg = _chat_msg("msg-bridge-event")
        observed, drained = observe_and_drain_chat_message(
            conn,
            message=msg,
            intake_path=IntakePath.EVENT,
            observed_at="2026-04-14T10:00:00Z",
            coalesce_window_seconds=60,
            lock_path=tmp_path / ".ltn.lock",
            sleep_fn=lambda _delay: None,
        )

        assert observed.processing_state is ChatIntakeState.PENDING
        assert drained is None
        assert count_raw_messages(conn) == 0

        polled, drained = observe_and_drain_chat_message(
            conn,
            message=msg,
            intake_path=IntakePath.POLL,
            observed_at="2026-04-14T10:01:00Z",
            lock_path=tmp_path / ".ltn.lock",
            sleep_fn=lambda _delay: None,
        )

        item = get_chat_intake_item(conn, chat_ingest_key(msg.source_id, msg.message_id))
        assert polled.poll_seen_count == 1
        assert polled.event_seen_count == 1
        assert drained is not None
        assert drained.items_total == 1
        assert drained.items_processed == 1
        assert count_raw_messages(conn) == 1
        assert item is not None
        assert item.processing_state is ChatIntakeState.PROCESSED

    def test_drain_ready_chat_intake_respects_limit_for_backpressure(
        self, conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        """``limit`` bounds each drain pass for large ready queues."""

        as_of = "2026-04-14T12:00:00Z"
        for i in range(8):
            observe_chat_message(
                conn,
                _chat_msg(f"msg-batch-{i}"),
                intake_path=IntakePath.POLL,
                observed_at=as_of,
            )
        lock_path = tmp_path / "vault" / "var" / "lark-to-notes.runtime.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        first = drain_ready_chat_intake(
            conn,
            lock_path=lock_path,
            as_of=as_of,
            limit=3,
            sleep_fn=lambda _d: None,
        )
        second = drain_ready_chat_intake(
            conn,
            lock_path=lock_path,
            as_of=as_of,
            limit=3,
            sleep_fn=lambda _d: None,
        )
        third = drain_ready_chat_intake(
            conn,
            lock_path=lock_path,
            as_of=as_of,
            limit=3,
            sleep_fn=lambda _d: None,
        )
        assert first.items_processed == 3
        assert first.queue_depth_peak == 3
        assert second.items_processed == 3
        assert third.items_processed == 2
        assert count_raw_messages(conn) == 8

    def test_drain_ready_chat_quarantine_logs_structured_fields(
        self, conn: sqlite3.Connection, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.WARNING, logger="lark_to_notes.runtime.executor")
        observe_chat_message(
            conn,
            _chat_msg("msg-quarantine-rt"),
            intake_path=IntakePath.POLL,
            observed_at="2026-04-14T12:00:00Z",
        )
        with patch(
            "lark_to_notes.runtime.executor.insert_raw_message",
            side_effect=RuntimeError("simulated persistence failure"),
        ):
            result = drain_ready_chat_intake(
                conn,
                lock_path=tmp_path / "q.lock",
                as_of="2026-04-14T12:00:00Z",
                retry_policy=RetryPolicy(
                    max_attempts=1,
                    base_delay_s=0.0,
                    max_delay_s=0.0,
                    jitter_factor=0.0,
                ),
                sleep_fn=lambda _d: None,
            )
        assert result.items_failed == 1
        assert result.dead_letter_ids
        quarantine_logs = [
            r
            for r in caplog.records
            if r.msg == "runtime_batch_item_quarantined"
            and r.name == "lark_to_notes.runtime.executor"
        ]
        assert quarantine_logs
        rec = quarantine_logs[0]
        assert getattr(rec, "command", None) == "chat-intake"
        assert getattr(rec, "dead_letter_id", None)
        assert getattr(rec, "item_key", None)


# ---------------------------------------------------------------------------
# Models — basic dataclass construction
# ---------------------------------------------------------------------------


class TestModels:
    def test_runtime_run_defaults(self) -> None:
        run = RuntimeRun(
            run_id="r1",
            command="sync",
            status=RunStatus.RUNNING,
            started_at="2024-01-01T00:00:00Z",
        )
        assert run.items_processed == 0
        assert run.items_failed == 0
        assert run.finished_at is None
        assert run.error is None

    def test_dead_letter_defaults(self) -> None:
        dl = DeadLetter(
            dl_id="d1",
            source_id="s1",
            attempt_count=1,
            first_failed_at="2024-01-01T00:00:00Z",
            last_failed_at="2024-01-01T00:00:00Z",
            last_error="oops",
            quarantined_at="2024-01-01T00:00:00Z",
        )
        assert dl.raw_message_id is None

    def test_health_report_construction(self) -> None:
        report = HealthReport(
            run_count_total=5,
            run_count_failed=1,
            run_count_running=0,
            dead_letter_count=2,
            queue_depth=1,
            error_rate=0.1,
            lag_seconds=30.0,
        )
        assert report.last_run_at is None
        assert report.last_run_command is None

    def test_reconcile_report_defaults(self) -> None:
        report = ReconcileReport(
            source_ids_checked=3,
            gaps_found=1,
            repairs_attempted=1,
            repairs_succeeded=1,
            repairs_failed=0,
        )
        assert report.gap_details == ()

    def test_runtime_work_item_defaults(self) -> None:
        item = RuntimeWorkItem(source_id="src-001", item_key="item-1")
        assert item.raw_message_id is None
        assert item.queued_at is None
        assert item.payload is None

    def test_batch_run_result_construction(self) -> None:
        run = RuntimeRun(
            run_id="r1",
            command="sync",
            status=RunStatus.COMPLETED,
            started_at="2024-01-01T00:00:00Z",
            finished_at="2024-01-01T00:01:00Z",
            items_processed=2,
            items_failed=1,
        )
        result = BatchRunResult(
            run=run,
            items_total=3,
            items_processed=2,
            items_failed=1,
            retry_count=2,
            queue_depth_peak=3,
            dead_letter_ids=("dl-1",),
        )
        assert result.dead_letter_ids == ("dl-1",)

    def test_reconcile_run_result_construction(self) -> None:
        run = RuntimeRun(
            run_id="r1",
            command="reconcile",
            status=RunStatus.COMPLETED,
            started_at="2024-01-01T00:00:00Z",
        )
        report = ReconcileReport(
            source_ids_checked=1,
            gaps_found=0,
            repairs_attempted=0,
            repairs_succeeded=0,
            repairs_failed=0,
        )
        result = ReconcileRunResult(run=run, report=report)
        assert result.run.command == "reconcile"

    def test_runtime_daemon_result_construction(self) -> None:
        result = RuntimeDaemonResult(
            cycle_count=2,
            idle_cycles=1,
            items_seen=3,
            items_processed=2,
            items_failed=1,
            queue_depth_peak=2,
            run_ids=("run-1",),
        )
        assert result.run_ids == ("run-1",)


# ---------------------------------------------------------------------------
# run_background_runtime (continuous daemon loop)
# ---------------------------------------------------------------------------


class TestRunBackgroundRuntime:
    """Behavioral tests for run_background_runtime (the daemon scheduling loop).

    All tests use real SQLite, real tmp_path for lock_path, and
    sleep_fn=lambda _: None to avoid actual sleeping.
    """

    def _make_item(self, key: str = "item-1", source: str = "src-001") -> RuntimeWorkItem:
        return RuntimeWorkItem(source_id=source, item_key=key)

    def test_daemon_stops_when_idle(self, conn: sqlite3.Connection, tmp_path: Path) -> None:
        """Empty first batch with stop_when_idle=True exits after exactly 1 cycle."""

        result = run_background_runtime(
            conn,
            command="daemon",
            fetch_batch=lambda: [],
            processor=lambda _item: None,
            lock_path=tmp_path / ".ltn.lock",
            sleep_fn=lambda _: None,
            stop_when_idle=True,
        )

        assert result.cycle_count == 1
        assert result.idle_cycles == 1
        assert result.items_seen == 0
        assert result.items_processed == 0
        assert result.queue_depth_peak == 0
        assert result.run_ids == ()

    def test_daemon_runs_max_cycles(self, conn: sqlite3.Connection, tmp_path: Path) -> None:
        """max_cycles=3 with non-empty batches executes exactly 3 cycles."""
        item = self._make_item()
        cycles: list[int] = []

        def fetch() -> list[RuntimeWorkItem]:
            cycles.append(1)
            return [item]

        result = run_background_runtime(
            conn,
            command="daemon",
            fetch_batch=fetch,
            processor=lambda _item: None,
            lock_path=tmp_path / ".ltn.lock",
            sleep_fn=lambda _: None,
            max_cycles=3,
        )

        assert result.cycle_count == 3
        assert len(cycles) == 3
        assert result.idle_cycles == 0

    def test_daemon_accumulates_metrics(self, conn: sqlite3.Connection, tmp_path: Path) -> None:
        """items_seen and items_processed accumulate across cycles."""
        # cycle 1: 2 items, cycle 2: 1 item, cycle 3: empty -> stop
        batch_plan = [[self._make_item("a"), self._make_item("b")], [self._make_item("c")], []]
        idx = 0

        def fetch() -> list[RuntimeWorkItem]:
            nonlocal idx
            batch = batch_plan[idx]
            idx += 1
            return batch

        result = run_background_runtime(
            conn,
            command="daemon",
            fetch_batch=fetch,
            processor=lambda _item: None,
            lock_path=tmp_path / ".ltn.lock",
            sleep_fn=lambda _: None,
            stop_when_idle=True,
        )

        assert result.cycle_count == 3
        assert result.idle_cycles == 1
        assert result.items_seen == 3
        assert result.items_processed == 3
        assert result.items_failed == 0

    def test_daemon_poll_sleep_between_cycles(
        self, conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        """Sleep is called between non-empty cycles but NOT after the last one."""
        item = self._make_item()
        sleep_calls: list[float] = []

        result = run_background_runtime(
            conn,
            command="daemon",
            fetch_batch=lambda: [item],
            processor=lambda _item: None,
            lock_path=tmp_path / ".ltn.lock",
            sleep_fn=sleep_calls.append,
            poll_interval_s=1.5,
            max_cycles=3,
        )

        # 3 cycles: sleep after cycle 1 and 2, NOT after cycle 3 (last)
        assert result.cycle_count == 3
        assert len(sleep_calls) == 2
        assert all(s == 1.5 for s in sleep_calls)

    def test_daemon_idle_sleep_on_empty_batch(
        self, conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        """Empty batches trigger a sleep call (poll interval); non-empty do not on last cycle."""
        # Alternating: empty, non-empty, empty, non-empty (stop at max_cycles=4)
        batches = [[], [self._make_item()], [], [self._make_item()]]
        idx = 0
        sleep_calls: list[float] = []

        def fetch() -> list[RuntimeWorkItem]:
            nonlocal idx
            b = batches[idx]
            idx += 1
            return b

        result = run_background_runtime(
            conn,
            command="daemon",
            fetch_batch=fetch,
            processor=lambda _item: None,
            lock_path=tmp_path / ".ltn.lock",
            sleep_fn=sleep_calls.append,
            poll_interval_s=2.0,
            max_cycles=4,
            stop_when_idle=False,
        )

        # cycle 1 (empty): sleep; cycle 2 (non-empty, not last): sleep
        # cycle 3 (empty): sleep; cycle 4 (non-empty, IS last): NO sleep
        assert result.cycle_count == 4
        assert result.idle_cycles == 2
        assert len(sleep_calls) == 3
        assert all(s == 2.0 for s in sleep_calls)

    def test_daemon_quarantines_failed_items(
        self, conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        """Items whose processor raises are quarantined in the dead-letter store."""
        item = RuntimeWorkItem(source_id="src-001", item_key="bad-item", raw_message_id="msg-bad")

        def boom(_item: RuntimeWorkItem) -> None:
            raise PermanentError("bad payload")

        result = run_background_runtime(
            conn,
            command="daemon",
            fetch_batch=lambda: [item],
            processor=boom,
            lock_path=tmp_path / ".ltn.lock",
            sleep_fn=lambda _: None,
            max_cycles=1,
        )

        dead = list_dead_letters(conn)
        assert result.items_failed == 1
        assert result.items_processed == 0
        assert len(dead) == 1
        assert dead[0].raw_message_id == "msg-bad"

    def test_daemon_queue_depth_peak_across_cycles(
        self, conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        """queue_depth_peak tracks the largest batch seen across all cycles."""
        batches = [
            [self._make_item(f"i{i}") for i in range(5)],  # 5 items
            [self._make_item(f"j{i}") for i in range(2)],  # 2 items
        ]
        idx = 0

        def fetch() -> list[RuntimeWorkItem]:
            nonlocal idx
            b = batches[idx]
            idx += 1
            return b

        result = run_background_runtime(
            conn,
            command="daemon",
            fetch_batch=fetch,
            processor=lambda _item: None,
            lock_path=tmp_path / ".ltn.lock",
            sleep_fn=lambda _: None,
            max_cycles=2,
        )

        assert result.queue_depth_peak == 5

    def test_daemon_run_ids_populated(self, conn: sqlite3.Connection, tmp_path: Path) -> None:
        """Each non-empty cycle appends a valid UUID run_id to run_ids."""
        import uuid

        item = self._make_item()

        result = run_background_runtime(
            conn,
            command="daemon",
            fetch_batch=lambda: [item],
            processor=lambda _item: None,
            lock_path=tmp_path / ".ltn.lock",
            sleep_fn=lambda _: None,
            max_cycles=3,
        )

        assert len(result.run_ids) == 3
        for run_id in result.run_ids:
            uuid.UUID(run_id)  # raises ValueError if not a valid UUID

    def test_daemon_stop_when_idle_false_max_cycles(
        self, conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        """stop_when_idle=False with all-empty batches still stops at max_cycles."""
        result = run_background_runtime(
            conn,
            command="daemon",
            fetch_batch=lambda: [],
            processor=lambda _item: None,
            lock_path=tmp_path / ".ltn.lock",
            sleep_fn=lambda _: None,
            max_cycles=2,
            stop_when_idle=False,
        )

        assert result.cycle_count == 2
        assert result.idle_cycles == 2
        assert result.items_seen == 0
