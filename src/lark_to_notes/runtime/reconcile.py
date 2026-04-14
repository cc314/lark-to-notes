"""Cursor-reconciliation for detecting and repairing intake drift.

The reconciler compares the stored checkpoint for each watched source against
the *source state* provided by the caller.  When a gap is detected (i.e. the
stored cursor lags behind the source), it calls the supplied repair callback
so the higher-level orchestrator can re-fetch the missed window.

This module is intentionally pure with respect to Lark API calls — it takes
``source_state`` as a dict so callers can inject it from tests or from actual
API responses without coupling the reconciler to the network layer.

Usage::

    from lark_to_notes.runtime.reconcile import reconcile_cursors, SourceState

    source_states = {
        "src-001": SourceState(
            source_id="src-001",
            latest_message_id="msg-999",
            latest_message_timestamp="2024-06-01T12:00:00Z",
        ),
    }

    def repair(source_id: str, from_cursor: str | None) -> None:
        # kick off a partial re-fetch from from_cursor
        ...

    report = reconcile_cursors(conn, source_states, repair_fn=repair)
    print(report)
"""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass

from lark_to_notes.runtime.models import ReconcileReport

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SourceState:
    """Current state of a single watched source as reported by the API.

    Attributes:
        source_id:                Stable source identifier (must match
                                  ``watched_sources.source_id``).
        latest_message_id:        Most recent message ID seen in the source.
        latest_message_timestamp: ISO-8601 UTC timestamp of that message.
    """

    source_id: str
    latest_message_id: str
    latest_message_timestamp: str


def reconcile_cursors(
    conn: sqlite3.Connection,
    source_states: dict[str, SourceState],
    *,
    repair_fn: Callable[[str, str | None], None] | None = None,
) -> ReconcileReport:
    """Compare stored checkpoints against *source_states* and repair gaps.

    For each source in *source_states*:

    1. Load the stored checkpoint from the ``checkpoints`` table.
    2. Compare ``last_message_id`` against
       :attr:`SourceState.latest_message_id`.
    3. When they differ (gap detected), call ``repair_fn(source_id,
       stored_cursor)`` so the caller can schedule a partial re-fetch.

    Args:
        conn:          An open database connection.
        source_states: Mapping of ``source_id`` → :class:`SourceState`
                       representing the live API view of each source.
        repair_fn:     Optional callback invoked for each gap found.
                       Receives ``(source_id, stored_last_message_id)``
                       where ``stored_last_message_id`` may be ``None``
                       if no checkpoint exists.

    Returns:
        A :class:`ReconcileReport` summarising the findings.
    """
    sources_checked = 0
    gaps_found = 0
    repairs_attempted = 0
    repairs_succeeded = 0
    repairs_failed = 0
    gap_details: list[str] = []

    for source_id, state in source_states.items():
        sources_checked += 1

        row = conn.execute(
            "SELECT last_message_id FROM checkpoints WHERE source_id = ?",
            (source_id,),
        ).fetchone()

        stored_cursor: str | None = row["last_message_id"] if row else None

        if stored_cursor == state.latest_message_id:
            logger.debug(
                "reconcile_cursor_up_to_date",
                extra={"source_id": source_id, "cursor": stored_cursor},
            )
            continue

        # Gap detected
        gaps_found += 1
        detail = (
            f"source={source_id} stored={stored_cursor!r} "
            f"latest={state.latest_message_id!r}"
        )
        gap_details.append(detail)
        logger.info(
            "reconcile_gap_detected",
            extra={
                "source_id": source_id,
                "stored_cursor": stored_cursor,
                "latest_message_id": state.latest_message_id,
            },
        )

        if repair_fn is None:
            continue

        repairs_attempted += 1
        try:
            repair_fn(source_id, stored_cursor)
            repairs_succeeded += 1
            logger.info(
                "reconcile_repair_succeeded",
                extra={"source_id": source_id, "stored_cursor": stored_cursor},
            )
        except Exception as exc:
            repairs_failed += 1
            logger.warning(
                "reconcile_repair_failed",
                extra={
                    "source_id": source_id,
                    "stored_cursor": stored_cursor,
                    "error": str(exc),
                },
            )

    return ReconcileReport(
        source_ids_checked=sources_checked,
        gaps_found=gaps_found,
        repairs_attempted=repairs_attempted,
        repairs_succeeded=repairs_succeeded,
        repairs_failed=repairs_failed,
        gap_details=tuple(gap_details),
    )
