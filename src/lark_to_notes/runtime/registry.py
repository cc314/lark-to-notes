"""SQLite CRUD for the runtime operations layer.

All functions accept a :class:`sqlite3.Connection` as their first argument
so callers control transaction boundaries (matching the pattern in
``storage/db.py`` and ``intake/ledger.py``).
"""

from __future__ import annotations

import sqlite3
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from lark_to_notes.runtime.models import (
    DeadLetter,
    HealthReport,
    RunStatus,
    RuntimeRun,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

    from lark_to_notes.runtime.models import RuntimeWorkItem


def _utcnow_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _new_id() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# RuntimeRun CRUD
# ---------------------------------------------------------------------------


def start_run(
    conn: sqlite3.Connection,
    command: str,
    *,
    run_id: str | None = None,
) -> RuntimeRun:
    """Record the start of a new runtime run and return its record.

    Args:
        conn:    An open database connection.
        command: Human-readable command name (e.g. ``"sync"``).
        run_id:  Optional explicit run ID (defaults to a new UUID).

    Returns:
        The newly created :class:`RuntimeRun` with status ``RUNNING``.
    """
    rid = run_id or _new_id()
    started = _utcnow_iso()
    conn.execute(
        """
        INSERT INTO runtime_runs
            (run_id, command, status, started_at,
             items_processed, items_failed)
        VALUES (?, ?, ?, ?, 0, 0)
        """,
        (rid, command, RunStatus.RUNNING.value, started),
    )
    conn.commit()
    return RuntimeRun(
        run_id=rid,
        command=command,
        status=RunStatus.RUNNING,
        started_at=started,
    )


def finish_run(
    conn: sqlite3.Connection,
    run_id: str,
    *,
    items_processed: int = 0,
    items_failed: int = 0,
    error: str | None = None,
) -> RuntimeRun | None:
    """Mark a runtime run as completed or failed.

    Args:
        conn:             An open database connection.
        run_id:           The run to finish.
        items_processed:  Count of items successfully processed.
        items_failed:     Count of items that raised recoverable errors.
        error:            If non-``None``, the run is marked FAILED; otherwise
                          COMPLETED.

    Returns:
        The updated :class:`RuntimeRun`, or ``None`` if *run_id* was not found.
    """
    status = RunStatus.FAILED if error else RunStatus.COMPLETED
    finished = _utcnow_iso()
    conn.execute(
        """
        UPDATE runtime_runs
        SET status = ?, finished_at = ?,
            items_processed = ?, items_failed = ?,
            error = ?
        WHERE run_id = ?
        """,
        (status.value, finished, items_processed, items_failed, error, run_id),
    )
    conn.commit()
    return get_run(conn, run_id)


def cancel_run(conn: sqlite3.Connection, run_id: str) -> RuntimeRun | None:
    """Mark a runtime run as cancelled.

    Args:
        conn:   An open database connection.
        run_id: The run to cancel.

    Returns:
        The updated :class:`RuntimeRun`, or ``None`` if not found.
    """
    conn.execute(
        """
        UPDATE runtime_runs
        SET status = ?, finished_at = ?
        WHERE run_id = ? AND status = ?
        """,
        (RunStatus.CANCELLED.value, _utcnow_iso(), run_id, RunStatus.RUNNING.value),
    )
    conn.commit()
    return get_run(conn, run_id)


def get_run(conn: sqlite3.Connection, run_id: str) -> RuntimeRun | None:
    """Fetch a single :class:`RuntimeRun` by ID.

    Args:
        conn:   An open database connection.
        run_id: The run ID to look up.

    Returns:
        The :class:`RuntimeRun`, or ``None`` if not found.
    """
    row = conn.execute("SELECT * FROM runtime_runs WHERE run_id = ?", (run_id,)).fetchone()
    if row is None:
        return None
    return _run_from_row(row)


def list_runs(
    conn: sqlite3.Connection,
    *,
    limit: int = 50,
    status: RunStatus | None = None,
) -> list[RuntimeRun]:
    """Return recent runtime runs, newest first.

    Args:
        conn:   An open database connection.
        limit:  Maximum rows to return (default 50).
        status: When provided, filter to runs with this status.

    Returns:
        List of :class:`RuntimeRun` in descending ``started_at`` order.
    """
    if status is not None:
        rows = conn.execute(
            "SELECT * FROM runtime_runs WHERE status = ? ORDER BY started_at DESC LIMIT ?",
            (status.value, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM runtime_runs ORDER BY started_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [_run_from_row(r) for r in rows]


def _run_from_row(row: sqlite3.Row) -> RuntimeRun:
    d = dict(row)
    return RuntimeRun(
        run_id=d["run_id"],
        command=d["command"],
        status=RunStatus(d["status"]),
        started_at=d["started_at"],
        finished_at=d.get("finished_at"),
        items_processed=d.get("items_processed", 0),
        items_failed=d.get("items_failed", 0),
        error=d.get("error"),
    )


# ---------------------------------------------------------------------------
# Dead-letter / quarantine CRUD
# ---------------------------------------------------------------------------


def quarantine_item(
    conn: sqlite3.Connection,
    source_id: str,
    last_error: str,
    *,
    raw_message_id: str | None = None,
    attempt_count: int = 1,
) -> DeadLetter:
    """Move an item to the dead-letter queue.

    Args:
        conn:            An open database connection.
        source_id:       Watched-source identifier.
        last_error:      Error message from the final failure.
        raw_message_id:  Original raw message ID, if available.
        attempt_count:   Total number of processing attempts made.

    Returns:
        The new :class:`DeadLetter` record.
    """
    dl_id = _new_id()
    now = _utcnow_iso()
    conn.execute(
        """
        INSERT INTO dead_letters
            (dl_id, source_id, raw_message_id, attempt_count,
             first_failed_at, last_failed_at, last_error, quarantined_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (dl_id, source_id, raw_message_id, attempt_count, now, now, last_error, now),
    )
    conn.commit()
    return DeadLetter(
        dl_id=dl_id,
        source_id=source_id,
        raw_message_id=raw_message_id,
        attempt_count=attempt_count,
        first_failed_at=now,
        last_failed_at=now,
        last_error=last_error,
        quarantined_at=now,
    )


def list_dead_letters(
    conn: sqlite3.Connection,
    *,
    source_id: str | None = None,
    limit: int = 100,
) -> list[DeadLetter]:
    """Return quarantined items, newest first.

    Args:
        conn:      An open database connection.
        source_id: When provided, filter to a specific source.
        limit:     Maximum rows to return (default 100).

    Returns:
        List of :class:`DeadLetter` in descending ``quarantined_at`` order.
    """
    if source_id is not None:
        rows = conn.execute(
            """SELECT * FROM dead_letters WHERE source_id = ?
               ORDER BY quarantined_at DESC LIMIT ?""",
            (source_id, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM dead_letters ORDER BY quarantined_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [_dl_from_row(r) for r in rows]


def _dl_from_row(row: sqlite3.Row) -> DeadLetter:
    d = dict(row)
    return DeadLetter(
        dl_id=d["dl_id"],
        source_id=d["source_id"],
        raw_message_id=d.get("raw_message_id"),
        attempt_count=d.get("attempt_count", 0),
        first_failed_at=d["first_failed_at"],
        last_failed_at=d["last_failed_at"],
        last_error=d["last_error"],
        quarantined_at=d["quarantined_at"],
    )


# ---------------------------------------------------------------------------
# Health report
# ---------------------------------------------------------------------------


def health_report(
    conn: sqlite3.Connection,
    *,
    queued_items: Iterable[RuntimeWorkItem] | None = None,
    now: datetime | None = None,
) -> HealthReport:
    """Compute a point-in-time health snapshot.

    Args:
        conn: An open database connection.
        queued_items: Optional current queue contents for live lag/depth
            metrics.
        now: Optional clock override used for deterministic lag calculations.

    Returns:
        A :class:`HealthReport` reflecting the current runtime state.
    """
    # Run counts
    row_total = conn.execute("SELECT COUNT(*) FROM runtime_runs").fetchone()
    run_count_total: int = row_total[0] if row_total else 0

    row_failed = conn.execute(
        "SELECT COUNT(*) FROM runtime_runs WHERE status = ?",
        (RunStatus.FAILED.value,),
    ).fetchone()
    run_count_failed: int = row_failed[0] if row_failed else 0

    row_running = conn.execute(
        "SELECT COUNT(*) FROM runtime_runs WHERE status = ?",
        (RunStatus.RUNNING.value,),
    ).fetchone()
    run_count_running: int = row_running[0] if row_running else 0

    # Dead-letter count
    row_dl = conn.execute("SELECT COUNT(*) FROM dead_letters").fetchone()
    dead_letter_count: int = row_dl[0] if row_dl else 0

    # Error rate across completed + failed runs
    row_totals = conn.execute(
        """
        SELECT COALESCE(SUM(items_processed), 0),
               COALESCE(SUM(items_failed), 0)
        FROM runtime_runs
        WHERE status IN (?, ?)
        """,
        (RunStatus.COMPLETED.value, RunStatus.FAILED.value),
    ).fetchone()
    if row_totals:
        total_proc: int = row_totals[0]
        total_fail: int = row_totals[1]
        denom = total_proc + total_fail
        error_rate = total_fail / denom if denom > 0 else 0.0
    else:
        error_rate = 0.0

    # Most recent run
    row_last = conn.execute(
        """
        SELECT command, status, started_at
        FROM runtime_runs
        ORDER BY started_at DESC
        LIMIT 1
        """
    ).fetchone()

    last_run_at: str | None = None
    last_run_command: str | None = None
    last_run_status: str | None = None
    if row_last:
        last_run_command = row_last[0]
        last_run_status = row_last[1]
        last_run_at = row_last[2]

    queue = tuple(queued_items or ())
    queue_depth = len(queue)
    lag_seconds = _lag_seconds(queue, now=now)

    return HealthReport(
        run_count_total=run_count_total,
        run_count_failed=run_count_failed,
        run_count_running=run_count_running,
        dead_letter_count=dead_letter_count,
        queue_depth=queue_depth,
        error_rate=error_rate,
        last_run_at=last_run_at,
        last_run_command=last_run_command,
        last_run_status=last_run_status,
        lag_seconds=lag_seconds,
    )


def _lag_seconds(
    queued_items: tuple[RuntimeWorkItem, ...],
    *,
    now: datetime | None = None,
) -> float | None:
    queued_times = [
        parsed
        for item in queued_items
        if item.queued_at is not None
        for parsed in [_parse_queued_at(item.queued_at)]
        if parsed is not None
    ]
    if not queued_times:
        return None
    reference = now or datetime.now(UTC)
    return max((reference - min(queued_times)).total_seconds(), 0.0)


def _parse_queued_at(value: str) -> datetime | None:
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except ValueError:
        return None
