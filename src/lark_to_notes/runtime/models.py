"""Data models for the runtime operations layer.

These are pure dataclasses that capture runtime state without coupling to
SQLite column types.  Registry functions (in ``registry.py``) handle the
persistence contract.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class RunStatus(StrEnum):
    """Lifecycle state of a :class:`RuntimeRun`."""

    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True)
class RuntimeRun:
    """A single operator-initiated or background runtime session.

    Attributes:
        run_id:           UUID identifying this run.
        command:          Human-readable command name (e.g. ``"sync"``,
                          ``"reconcile"``, ``"replay"``).
        status:           Current lifecycle status.
        started_at:       ISO-8601 UTC timestamp when the run began.
        finished_at:      ISO-8601 UTC timestamp when the run ended, or
                          ``None`` while still running.
        items_processed:  Total items successfully processed.
        items_failed:     Total items that raised recoverable errors.
        error:            Terminal error message if status is FAILED.
    """

    run_id: str
    command: str
    status: RunStatus
    started_at: str
    finished_at: str | None = None
    items_processed: int = 0
    items_failed: int = 0
    error: str | None = None


@dataclass(frozen=True, slots=True)
class DeadLetter:
    """A permanently-failed item that has been quarantined.

    Attributes:
        dl_id:           UUID for this dead-letter record.
        source_id:       Watched-source identifier.
        raw_message_id:  Original raw message ID (may be ``None`` for
                         synthetic items).
        attempt_count:   How many times processing was attempted.
        first_failed_at: ISO-8601 UTC timestamp of the first failure.
        last_failed_at:  ISO-8601 UTC timestamp of the most recent failure.
        last_error:      Most recent error message.
        quarantined_at:  ISO-8601 UTC timestamp when the item was moved to
                         the dead-letter queue.
    """

    dl_id: str
    source_id: str
    attempt_count: int
    first_failed_at: str
    last_failed_at: str
    last_error: str
    quarantined_at: str
    raw_message_id: str | None = None


@dataclass(frozen=True, slots=True)
class HealthReport:
    """Point-in-time health snapshot of the runtime.

    Attributes:
        run_count_total:     Total runtime runs recorded.
        run_count_failed:    Runs that ended with FAILED status.
        run_count_running:   Runs currently in RUNNING state (should be 0
                             or 1 under the single-writer model).
        dead_letter_count:   Items currently quarantined.
        queue_depth:         Current number of queued work items waiting to be
                             processed.
        last_run_at:         ISO-8601 UTC timestamp of the most recent run,
                              or ``None`` if no runs have been recorded.
        last_run_command:    Command name of the most recent run.
        last_run_status:     Status of the most recent run.
        lag_seconds:         Age in seconds of the oldest queued work item, or
                             ``None`` when queue timing is unavailable.
        error_rate:          Fraction of completed runs that failed
                              (``items_failed / (items_processed + items_failed)``
                              across all completed runs, clamped to ``[0, 1]``).
    """

    run_count_total: int
    run_count_failed: int
    run_count_running: int
    dead_letter_count: int
    queue_depth: int
    error_rate: float
    last_run_at: str | None = None
    last_run_command: str | None = None
    last_run_status: str | None = None
    lag_seconds: float | None = None


@dataclass(frozen=True, slots=True)
class ReconcileReport:
    """Result of a cursor-reconciliation pass.

    Attributes:
        source_ids_checked:   Number of sources examined.
        gaps_found:           Number of sources where stored cursor lags the
                              source state.
        repairs_attempted:    Number of repair actions initiated.
        repairs_succeeded:    Number of repairs that completed without error.
        repairs_failed:       Number of repairs that raised errors.
        gap_details:          Per-source gap descriptions for diagnostics.
    """

    source_ids_checked: int
    gaps_found: int
    repairs_attempted: int
    repairs_succeeded: int
    repairs_failed: int
    gap_details: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class RuntimeWorkItem:
    """A unit of runtime-managed work.

    Attributes:
        source_id:       Stable watched-source identifier used for quarantine
                         and diagnostics.
        item_key:        Stable identifier for the work item within the batch.
        raw_message_id:  Optional raw-message identifier when the work item
                         comes from the intake ledger.
        queued_at:       Optional ISO-8601 UTC timestamp of when the item
                         entered the queue, used for lag metrics.
        payload:         Optional opaque value passed through to the processor.
    """

    source_id: str
    item_key: str
    raw_message_id: str | None = None
    queued_at: str | None = None
    payload: object | None = None


@dataclass(frozen=True, slots=True)
class BatchRunResult:
    """Summary of a serialized batch run.

    Attributes:
        run:               Final persisted runtime-run record.
        items_total:       Number of work items seen in the batch.
        items_processed:   Number of items completed successfully.
        items_failed:      Number of items that ended up quarantined.
        retry_count:       Total retry attempts consumed across the batch.
        queue_depth_peak:  Largest outstanding queue depth observed.
        dead_letter_ids:   Dead-letter IDs created during the run.
    """

    run: RuntimeRun
    items_total: int
    items_processed: int
    items_failed: int
    retry_count: int
    queue_depth_peak: int
    dead_letter_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ReconcileRunResult:
    """Result of a reconciliation pass that was recorded as a runtime run."""

    run: RuntimeRun
    report: ReconcileReport


@dataclass(frozen=True, slots=True)
class RuntimeDaemonResult:
    """Summary of a continuous background runtime loop."""

    cycle_count: int
    idle_cycles: int
    items_seen: int
    items_processed: int
    items_failed: int
    queue_depth_peak: int
    run_ids: tuple[str, ...] = ()
