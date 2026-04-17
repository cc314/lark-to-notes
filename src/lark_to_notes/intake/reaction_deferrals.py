"""Explicit rows when reaction intake hits governance caps (lw-pzj.12.1)."""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import sqlite3


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
