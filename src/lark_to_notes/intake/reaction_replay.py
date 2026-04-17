"""Scoped replay for reaction-derived ledger state (lw-pzj.13.1).

Rebuilds ``raw_message_present`` / orphan-queue linkage from existing
``message_reaction_events`` + ``raw_messages`` without inserting or updating
``raw_messages`` rows.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

from lark_to_notes.intake.ledger import _link_pending_reactions_for_raw_pair

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable, Iterator

REPLAY_STAGE_REACTION_ORPHAN_RELINK = "reaction_orphan_relink"


@dataclass(frozen=True)
class ReplayOrphanReactionsSummary:
    """Aggregate counters for :func:`replay_orphan_reactions`."""

    pairs_processed: int
    rows_attached: int
    pairs_idempotent_noop: int
    duration_ms: float


def _iter_relinkable_pairs(
    conn: sqlite3.Connection,
    *,
    source_ids: frozenset[str] | None,
) -> Iterator[tuple[str, str]]:
    """Yield ``(source_id, message_id)`` with orphan reactions and a raw row."""

    sql = """
        SELECT DISTINCT e.source_id AS source_id, e.message_id AS message_id
        FROM message_reaction_events e
        WHERE e.raw_message_present = 0
          AND EXISTS (
              SELECT 1 FROM raw_messages m
              WHERE m.message_id = e.message_id AND m.source_id = e.source_id
          )
    """
    params: tuple[str, ...] = ()
    if source_ids:
        placeholders = ",".join("?" * len(source_ids))
        sql += f" AND e.source_id IN ({placeholders})"
        params = tuple(sorted(source_ids))
    sql += " ORDER BY e.source_id, e.message_id"
    cur = conn.execute(sql, params)
    for row in cur:
        yield str(row["source_id"]), str(row["message_id"])


def _pair_progress_meta(
    conn: sqlite3.Connection,
    *,
    source_id: str,
    message_id: str,
) -> tuple[str, str, str]:
    """Return ``(cursor_high_water, governance_version, policy_version)``."""

    row = conn.execute(
        """
        SELECT reaction_event_id, governance_version, policy_version
        FROM message_reaction_events
        WHERE source_id = ? AND message_id = ?
        ORDER BY reaction_event_id DESC
        LIMIT 1
        """,
        (source_id, message_id),
    ).fetchone()
    if row is None:
        return "", "", ""
    return (
        str(row["reaction_event_id"] or ""),
        str(row["governance_version"] or ""),
        str(row["policy_version"] or ""),
    )


def replay_orphan_reactions(
    conn: sqlite3.Connection,
    *,
    source_ids: frozenset[str] | None = None,
    progress: Callable[[dict[str, object]], None] | None = None,
) -> ReplayOrphanReactionsSummary:
    """Re-run orphan→raw linkage for pairs that already have ``raw_messages``.

    Emits optional NDJSON progress objects (one per pair) matching lw-pzj.13.1:
    ``stage``, ``source_id``, ``message_id``, ``rows_processed``,
    ``rows_skipped_idempotent``, ``cursor_high_water``, ``governance_version``,
    ``policy_version``, ``duration_ms``.

    Args:
        conn: Open SQLite connection.
        source_ids: When set, only process these ``source_id`` values.
        progress: Optional sink for progress dicts (caller serializes, e.g. JSON).

    Returns:
        Summary counters for the completed run.
    """
    t0 = time.perf_counter()
    pairs_processed = 0
    rows_attached = 0
    pairs_idempotent_noop = 0

    for source_id, message_id in _iter_relinkable_pairs(conn, source_ids=source_ids):
        pairs_processed += 1
        cursor_hw, gv, pv = _pair_progress_meta(conn, source_id=source_id, message_id=message_id)
        t_pair = time.perf_counter()
        batch = uuid.uuid4().hex
        attached = _link_pending_reactions_for_raw_pair(
            conn,
            source_id=source_id,
            message_id=message_id,
            orphan_batch_id=batch,
        )
        conn.commit()
        elapsed_ms = round((time.perf_counter() - t_pair) * 1000.0, 3)
        rows_attached += attached
        if attached == 0:
            pairs_idempotent_noop += 1
        if progress is not None:
            progress(
                {
                    "stage": REPLAY_STAGE_REACTION_ORPHAN_RELINK,
                    "source_id": source_id,
                    "message_id": message_id,
                    "rows_processed": attached,
                    "rows_skipped_idempotent": 1 if attached == 0 else 0,
                    "cursor_high_water": cursor_hw,
                    "governance_version": gv,
                    "policy_version": pv,
                    "duration_ms": elapsed_ms,
                },
            )

    return ReplayOrphanReactionsSummary(
        pairs_processed=pairs_processed,
        rows_attached=rows_attached,
        pairs_idempotent_noop=pairs_idempotent_noop,
        duration_ms=round((time.perf_counter() - t0) * 1000.0, 3),
    )


def format_replay_progress_line(payload: dict[str, object]) -> str:
    """Serialize a progress dict as one NDJSON line."""

    return json.dumps(payload, ensure_ascii=False)
