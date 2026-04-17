"""Explicit rows when reaction intake hits governance caps (lw-pzj.12.1)."""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    import sqlite3

ReactionPipelineDoctorStatus = Literal[
    "healthy",
    "degraded_partial_history",
    "blocked_missing_scope",
    "blocked_missing_api",
    "quarantine_elevated",
]


def reaction_intake_deferral_id(
    *,
    run_id: str,
    source_id: str,
    cursor_event_id: str,
    cursor_payload_hash: str,
    reason_code: str,
) -> str:
    """Deterministic primary key so replays do not multiply deferral rows."""

    raw = json.dumps(
        {
            "run_id": run_id,
            "source_id": source_id,
            "cursor_event_id": cursor_event_id,
            "cursor_payload_hash": cursor_payload_hash,
            "reason_code": reason_code,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:40]
    return f"rdef_{digest}"


def insert_reaction_intake_deferral(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    source_id: str,
    cursor_event_id: str,
    cursor_payload_hash: str,
    reason_code: str,
    governance_version: str,
    policy_version: str,
    payload_extra: dict[str, Any] | None = None,
) -> bool:
    """Persist one cap deferral; returns whether a new row was inserted."""

    rid = reaction_intake_deferral_id(
        run_id=run_id,
        source_id=source_id,
        cursor_event_id=cursor_event_id,
        cursor_payload_hash=cursor_payload_hash,
        reason_code=reason_code,
    )
    payload = json.dumps(
        payload_extra or {},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO reaction_intake_deferrals (
            deferral_id, run_id, source_id, cursor_event_id, cursor_payload_hash,
            reason_code, governance_version, policy_version, payload_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            rid,
            run_id,
            source_id,
            cursor_event_id,
            cursor_payload_hash,
            reason_code,
            governance_version,
            policy_version,
            payload,
        ),
    )
    conn.commit()
    return cur.rowcount == 1


def count_reaction_intake_deferrals(conn: sqlite3.Connection, *, run_id: str) -> int:
    """Count deferrals recorded for a runtime / intake run (tests and doctor)."""

    row = conn.execute(
        "SELECT COUNT(*) AS c FROM reaction_intake_deferrals WHERE run_id = ?",
        (run_id,),
    ).fetchone()
    return int(row["c"] if row is not None else 0)


def reaction_intake_deferral_metrics(conn: sqlite3.Connection) -> dict[str, Any]:
    """Aggregate cap-deferral rows for ``doctor --json`` (lw-pzj.9.1)."""

    row = conn.execute(
        """
        SELECT COUNT(*) AS n,
               MAX(deferred_at) AS last_deferred_at
        FROM reaction_intake_deferrals
        """
    ).fetchone()
    total = int(row["n"] if row is not None else 0)
    last_at = row["last_deferred_at"] if row is not None else None
    by_reason_rows = conn.execute(
        """
        SELECT reason_code, COUNT(*) AS c
        FROM reaction_intake_deferrals
        GROUP BY reason_code
        ORDER BY c DESC, reason_code ASC
        """
    ).fetchall()
    by_reason = {str(r["reason_code"]): int(r["c"]) for r in by_reason_rows}
    return {
        "deferral_row_count": total,
        "last_deferred_at": last_at,
        "by_reason_code": by_reason,
    }


def classify_reaction_pipeline_doctor_status(
    *,
    dead_letter_count: int,
    error_rate: float,
    deferral_row_count: int,
    orphan_queue_depth: int,
    correlation_orphan_rows: int,
    blocked_missing_scope: bool = False,
    blocked_missing_api: bool = False,
) -> ReactionPipelineDoctorStatus:
    """Single mutually-exclusive triage label for doctor JSON (lw-pzj.9.1).

    Precedence is highest-severity first. ``blocked_*`` hints are reserved for
    explicit capability probes (``lw-pzj.14``); they default false until wired.
    """

    if blocked_missing_scope:
        return "blocked_missing_scope"
    if blocked_missing_api:
        return "blocked_missing_api"
    if dead_letter_count > 0 or error_rate >= 0.25:
        return "quarantine_elevated"
    if deferral_row_count > 0 or orphan_queue_depth > 0 or correlation_orphan_rows > 0:
        return "degraded_partial_history"
    return "healthy"
