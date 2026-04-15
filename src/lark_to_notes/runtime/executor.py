"""High-level runtime orchestration for batch work and reconciliation.

This module composes the lower-level runtime primitives so callers can run a
single-writer batch with retries and quarantine semantics, or execute a
reconciliation pass while recording run history.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from lark_to_notes.runtime.lock import RuntimeLock
from lark_to_notes.runtime.models import (
    BatchRunResult,
    ReconcileRunResult,
    RuntimeDaemonResult,
    RuntimeRun,
    RuntimeWorkItem,
)
from lark_to_notes.runtime.reconcile import SourceState, reconcile_cursors
from lark_to_notes.runtime.registry import finish_run, quarantine_item, start_run
from lark_to_notes.runtime.retry import RetryPolicy

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable, Iterable
    from pathlib import Path

logger = logging.getLogger(__name__)


def execute_work_batch(
    conn: sqlite3.Connection,
    *,
    command: str,
    items: Iterable[RuntimeWorkItem],
    processor: Callable[[RuntimeWorkItem], None],
    lock_path: Path,
    retry_policy: RetryPolicy | None = None,
    sleep_fn: Callable[[float], None] | None = None,
    run_id: str | None = None,
) -> BatchRunResult:
    """Run *items* through *processor* with lock, retries, and quarantine.

    The entire batch runs under a :class:`RuntimeLock` so note writes stay
    single-writer across processes. Transient failures are retried according to
    *retry_policy*; exhausted or permanent failures are moved into the
    dead-letter store.
    """

    work_items = tuple(items)
    policy = retry_policy or RetryPolicy()
    sleeper = sleep_fn or time.sleep
    run = start_run(conn, command, run_id=run_id)
    items_processed = 0
    items_failed = 0
    retry_count = 0
    dead_letter_ids: list[str] = []
    queue_depth_peak = len(work_items)

    try:
        with RuntimeLock(lock_path, owner_tag=f"{command}:{run.run_id}"):
            for index, item in enumerate(work_items):
                queue_depth = len(work_items) - index
                logger.debug(
                    "runtime_batch_item_start",
                    extra={
                        "run_id": run.run_id,
                        "command": command,
                        "source_id": item.source_id,
                        "item_key": item.item_key,
                        "queue_depth": queue_depth,
                    },
                )
                attempt = 0
                while True:
                    try:
                        processor(item)
                        items_processed += 1
                        logger.debug(
                            "runtime_batch_item_processed",
                            extra={
                                "run_id": run.run_id,
                                "command": command,
                                "source_id": item.source_id,
                                "item_key": item.item_key,
                                "attempt_count": attempt + 1,
                            },
                        )
                        break
                    except Exception as exc:
                        if policy.should_retry(attempt, exc):
                            delay = policy.delay_for(attempt)
                            retry_count += 1
                            logger.info(
                                "runtime_batch_item_retry",
                                extra={
                                    "run_id": run.run_id,
                                    "command": command,
                                    "source_id": item.source_id,
                                    "item_key": item.item_key,
                                    "attempt_count": attempt + 1,
                                    "delay_s": delay,
                                    "error": str(exc),
                                },
                            )
                            sleeper(delay)
                            attempt += 1
                            continue

                        dead_letter = quarantine_item(
                            conn,
                            item.source_id,
                            str(exc),
                            raw_message_id=item.raw_message_id,
                            attempt_count=attempt + 1,
                        )
                        items_failed += 1
                        dead_letter_ids.append(dead_letter.dl_id)
                        logger.warning(
                            "runtime_batch_item_quarantined",
                            extra={
                                "run_id": run.run_id,
                                "command": command,
                                "source_id": item.source_id,
                                "item_key": item.item_key,
                                "attempt_count": attempt + 1,
                                "dead_letter_id": dead_letter.dl_id,
                                "error": str(exc),
                            },
                        )
                        break
    except Exception as exc:
        failed_run = _finish_or_raise_failed_run(
            conn,
            run,
            items_processed=items_processed,
            items_failed=items_failed,
            error=str(exc),
        )
        logger.exception(
            "runtime_batch_failed",
            extra={
                "run_id": failed_run.run_id,
                "command": command,
                "items_processed": items_processed,
                "items_failed": items_failed,
            },
        )
        raise

    completed_run = _finish_completed_run(
        conn,
        run,
        items_processed=items_processed,
        items_failed=items_failed,
    )
    return BatchRunResult(
        run=completed_run,
        items_total=len(work_items),
        items_processed=items_processed,
        items_failed=items_failed,
        retry_count=retry_count,
        queue_depth_peak=queue_depth_peak,
        dead_letter_ids=tuple(dead_letter_ids),
    )


def execute_reconcile_run(
    conn: sqlite3.Connection,
    source_states: dict[str, SourceState],
    *,
    repair_fn: Callable[[str, str | None], None] | None = None,
    command: str = "reconcile",
    run_id: str | None = None,
) -> ReconcileRunResult:
    """Run cursor reconciliation and persist the pass in runtime history."""

    run = start_run(conn, command, run_id=run_id)
    try:
        report = reconcile_cursors(conn, source_states, repair_fn=repair_fn)
    except Exception as exc:
        failed_run = _finish_or_raise_failed_run(conn, run, error=str(exc))
        logger.exception(
            "runtime_reconcile_failed",
            extra={"run_id": failed_run.run_id, "command": command},
        )
        raise

    completed_run = _finish_completed_run(
        conn,
        run,
        items_processed=max(report.source_ids_checked - report.repairs_failed, 0),
        items_failed=report.repairs_failed,
    )
    logger.info(
        "runtime_reconcile_completed",
        extra={
            "run_id": completed_run.run_id,
            "command": command,
            "sources_checked": report.source_ids_checked,
            "gaps_found": report.gaps_found,
            "repairs_attempted": report.repairs_attempted,
            "repairs_failed": report.repairs_failed,
        },
    )
    return ReconcileRunResult(run=completed_run, report=report)


def run_background_runtime(
    conn: sqlite3.Connection,
    *,
    command: str,
    fetch_batch: Callable[[], Iterable[RuntimeWorkItem]],
    processor: Callable[[RuntimeWorkItem], None],
    lock_path: Path,
    retry_policy: RetryPolicy | None = None,
    sleep_fn: Callable[[float], None] | None = None,
    poll_interval_s: float = 5.0,
    max_cycles: int | None = None,
    stop_when_idle: bool = False,
) -> RuntimeDaemonResult:
    """Continuously fetch and process batches until the stop condition is met."""

    sleeper = sleep_fn or time.sleep
    cycle_count = 0
    idle_cycles = 0
    items_seen = 0
    items_processed = 0
    items_failed = 0
    queue_depth_peak = 0
    run_ids: list[str] = []

    while max_cycles is None or cycle_count < max_cycles:
        cycle_count += 1
        batch = tuple(fetch_batch())
        queue_depth_peak = max(queue_depth_peak, len(batch))
        if not batch:
            idle_cycles += 1
            if stop_when_idle:
                break
            sleeper(poll_interval_s)
            continue

        result = execute_work_batch(
            conn,
            command=command,
            items=batch,
            processor=processor,
            lock_path=lock_path,
            retry_policy=retry_policy,
            sleep_fn=sleeper,
        )
        run_ids.append(result.run.run_id)
        items_seen += result.items_total
        items_processed += result.items_processed
        items_failed += result.items_failed

        if max_cycles is None or cycle_count < max_cycles:
            sleeper(poll_interval_s)

    return RuntimeDaemonResult(
        cycle_count=cycle_count,
        idle_cycles=idle_cycles,
        items_seen=items_seen,
        items_processed=items_processed,
        items_failed=items_failed,
        queue_depth_peak=queue_depth_peak,
        run_ids=tuple(run_ids),
    )


def _finish_completed_run(
    conn: sqlite3.Connection,
    run: RuntimeRun,
    *,
    items_processed: int,
    items_failed: int,
) -> RuntimeRun:
    finished = finish_run(
        conn,
        run.run_id,
        items_processed=items_processed,
        items_failed=items_failed,
    )
    return finished or run


def _finish_or_raise_failed_run(
    conn: sqlite3.Connection,
    run: RuntimeRun,
    *,
    items_processed: int = 0,
    items_failed: int = 0,
    error: str,
) -> RuntimeRun:
    finished = finish_run(
        conn,
        run.run_id,
        items_processed=items_processed,
        items_failed=items_failed,
        error=error,
    )
    return finished or run
