"""SQLite persistence helpers for structured feedback events."""

from __future__ import annotations

import json
import sqlite3
import uuid

from lark_to_notes.feedback.models import FeedbackEntry, FeedbackEventRecord, FeedbackTargetType


def insert_event(
    conn: sqlite3.Connection,
    entry: FeedbackEntry,
    *,
    artifact_path: str = "",
) -> str:
    """Persist *entry* to the ``feedback_events`` table.

    Uses a deterministic UUIDv5 keyed by artifact path, target, and payload
    so re-importing the same artifact remains idempotent.

    Args:
        conn: Open SQLite connection.
        entry: The structured feedback entry to store.
        artifact_path: Optional path to the source YAML sidecar.

    Returns:
        The stable ``feedback_id`` for this entry.
    """
    payload_json = json.dumps(entry.directive.to_payload(), sort_keys=True)
    feedback_id = _feedback_event_id(
        target_type=entry.target_type.value,
        target_id=entry.target_id,
        payload_json=payload_json,
        artifact_path=artifact_path,
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO feedback_events (
            feedback_id, target_type, target_id, action, payload_json,
            comment, actor_ref, artifact_path
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            feedback_id,
            entry.target_type.value,
            entry.target_id,
            entry.directive.action.value,
            payload_json,
            entry.directive.comment,
            entry.directive.actor_ref,
            artifact_path,
        ),
    )
    return feedback_id


def get_event(conn: sqlite3.Connection, feedback_id: str) -> FeedbackEventRecord | None:
    """Return the stored feedback event with *feedback_id*, or ``None``."""
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT * FROM feedback_events WHERE feedback_id = ?",
        (feedback_id,),
    ).fetchone()
    return FeedbackEventRecord.from_db_row(row) if row else None


def list_events(
    conn: sqlite3.Connection,
    *,
    target_type: str | None = None,
    target_id: str | None = None,
    action: str | None = None,
    limit: int = 100,
) -> list[FeedbackEventRecord]:
    """Return stored feedback events matching the given filters."""
    if target_type is not None:
        FeedbackTargetType(target_type)

    sql = """
        SELECT *
        FROM feedback_events
        WHERE (? IS NULL OR target_type = ?)
          AND (? IS NULL OR target_id = ?)
          AND (? IS NULL OR action = ?)
        ORDER BY created_at DESC
        LIMIT ?
    """
    params = (
        target_type,
        target_type,
        target_id,
        target_id,
        action,
        action,
        limit,
    )

    conn.row_factory = sqlite3.Row
    rows = conn.execute(sql, params).fetchall()
    return [FeedbackEventRecord.from_db_row(row) for row in rows]


def has_manual_override(conn: sqlite3.Connection, task_id: str) -> bool:
    """Return ``True`` if any stored feedback event targets *task_id*."""
    row = conn.execute(
        "SELECT 1 FROM feedback_events WHERE target_type = 'task' AND target_id = ? LIMIT 1",
        (task_id,),
    ).fetchone()
    return row is not None


def _feedback_event_id(
    *,
    target_type: str,
    target_id: str,
    payload_json: str,
    artifact_path: str,
) -> str:
    seed = json.dumps(
        {
            "target_type": target_type,
            "target_id": target_id,
            "payload_json": payload_json,
            "artifact_path": artifact_path,
        },
        sort_keys=True,
    )
    return str(uuid.uuid5(uuid.NAMESPACE_URL, seed))
