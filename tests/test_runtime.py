"""Tests for the lark_to_notes.runtime package.

Coverage:
 - schema v4: runtime_runs and dead_letters tables created by init_db
 - registry: start_run, finish_run, cancel_run, get_run, list_runs,
   quarantine_item, list_dead_letters, health_report
 - retry: RetryPolicy.should_retry, delay_for, PermanentError short-circuit
 - lock: RuntimeLock acquire/release via context manager, is_held, idempotent release
 - reconcile: reconcile_cursors gap detection, repair callback, up-to-date sources,
   missing checkpoints, repair failures
 - models: dataclass construction and field defaults
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest

from lark_to_notes.runtime.lock import RuntimeLock
from lark_to_notes.runtime.models import (
    DeadLetter,
    HealthReport,
    ReconcileReport,
    RunStatus,
    RuntimeRun,
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
def conn():  # type: ignore[return]
    """Return an in-memory SQLite connection with the full schema applied."""
    c = connect(":memory:")
    init_db(c)
    yield c
    c.close()


# ---------------------------------------------------------------------------
# Schema v4
# ---------------------------------------------------------------------------


class TestSchemaV4:
    def test_schema_version_is_4(self) -> None:
        assert SCHEMA_VERSION == 4

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
        assert report.error_rate == pytest.approx(0.0)
        assert report.last_run_at is None

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
            error_rate=0.1,
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
