"""SQLite CRUD for the task registry and evidence tables.

Idempotency contract
--------------------
* :func:`upsert_task` is idempotent by ``fingerprint``: if a task with
  the same fingerprint already exists, the existing ``task_id`` is
  returned and **no fields are changed**.  A new evidence row is always
  added so repeated evidence accumulates without duplicating the task.
* :func:`add_evidence` inserts a new row unconditionally; callers are
  responsible for de-duplicating if needed.
* :func:`update_task_status` is a no-op when the task is already in a
  terminal state and ``force=False``.

All functions accept a :class:`sqlite3.Connection` so the caller
controls transaction boundaries.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime

from lark_to_notes.tasks.models import TaskEvidence, TaskRecord, TaskStatus

# Status string used when a new task has low confidence
_NEEDS_REVIEW_STATUS = TaskStatus.NEEDS_REVIEW.value
_OPEN_STATUS = TaskStatus.OPEN.value


def upsert_task(
    conn: sqlite3.Connection,
    *,
    fingerprint: str,
    title: str,
    task_class: str,
    confidence_band: str,
    summary: str = "",
    reason_code: str = "",
    promotion_rec: str = "review",
    assignee_refs: list[str] | None = None,
    due_at: str | None = None,
    created_from_raw_record_id: str | None = None,
) -> tuple[str, bool]:
    """Insert a new task or return the existing one for this fingerprint.

    Args:
        conn:                       Open SQLite connection.
        fingerprint:                16-char hex fingerprint (see
                                    :mod:`~lark_to_notes.tasks.fingerprint`).
        title:                      Short human-readable title.
        task_class:                 Classification string value.
        confidence_band:            Confidence string value.
        summary:                    Optional longer description.
        reason_code:                Machine-readable classification reason.
        promotion_rec:              Recommended promotion destination.
        assignee_refs:              Detected assignee references.
        due_at:                     Optional due-date string.
        created_from_raw_record_id: ``message_id`` of the triggering raw
                                    record.

    Returns:
        A ``(task_id, was_created)`` tuple.  *was_created* is ``True``
        when a new row was inserted, ``False`` when an existing row was
        found by fingerprint.
    """
    existing = get_task_by_fingerprint(conn, fingerprint)
    if existing is not None:
        return existing.task_id, False

    task_id = str(uuid.uuid4())

    # Low-confidence tasks go straight to needs_review
    if confidence_band == "low" or task_class == "needs_review":
        initial_status = _NEEDS_REVIEW_STATUS
    else:
        initial_status = _OPEN_STATUS

    conn.execute(
        """
        INSERT INTO tasks (
            task_id, fingerprint, title, status, task_class,
            confidence_band, summary, reason_code, promotion_rec,
            assignee_refs, due_at, created_from_raw_record_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            task_id,
            fingerprint,
            title,
            initial_status,
            task_class,
            confidence_band,
            summary,
            reason_code,
            promotion_rec,
            json.dumps(assignee_refs or []),
            due_at,
            created_from_raw_record_id,
        ),
    )
    return task_id, True


def get_task(conn: sqlite3.Connection, task_id: str) -> TaskRecord | None:
    """Return the task with *task_id*, or ``None`` if not found.

    Args:
        conn:    Open SQLite connection.
        task_id: UUID string identifying the task.

    Returns:
        A :class:`TaskRecord` or ``None``.
    """
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
    ).fetchone()
    return TaskRecord.from_db_row(row) if row else None


def get_task_by_fingerprint(
    conn: sqlite3.Connection, fingerprint: str
) -> TaskRecord | None:
    """Return the task matching *fingerprint*, or ``None``.

    Args:
        conn:        Open SQLite connection.
        fingerprint: 16-char hex fingerprint string.

    Returns:
        A :class:`TaskRecord` or ``None``.
    """
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT * FROM tasks WHERE fingerprint = ?", (fingerprint,)
    ).fetchone()
    return TaskRecord.from_db_row(row) if row else None


def list_tasks(
    conn: sqlite3.Connection,
    *,
    status: str | None = None,
    limit: int = 200,
) -> list[TaskRecord]:
    """Return tasks, optionally filtered by *status*.

    Args:
        conn:   Open SQLite connection.
        status: Optional :class:`TaskStatus` string value to filter by.
        limit:  Maximum number of rows to return (default: 200).

    Returns:
        A list of :class:`TaskRecord` objects ordered by
        ``last_updated_at`` descending.
    """
    conn.row_factory = sqlite3.Row
    if status is not None:
        rows = conn.execute(
            "SELECT * FROM tasks WHERE status = ? ORDER BY last_updated_at DESC LIMIT ?",
            (status, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM tasks ORDER BY last_updated_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [TaskRecord.from_db_row(r) for r in rows]


def update_task_status(
    conn: sqlite3.Connection,
    task_id: str,
    new_status: str,
    *,
    force: bool = False,
) -> bool:
    """Transition *task_id* to *new_status*.

    Terminal states (:meth:`TaskStatus.terminal_states`) are sticky: the
    transition is silently skipped unless *force* is ``True``.

    Args:
        conn:       Open SQLite connection.
        task_id:    UUID of the task to update.
        new_status: Target :class:`TaskStatus` string value.
        force:      When ``True``, override even terminal states.

    Returns:
        ``True`` if a row was updated, ``False`` otherwise (not found,
        or skipped due to terminal state).
    """
    # Validate the new_status value
    TaskStatus(new_status)  # raises ValueError if invalid

    if not force:
        conn.row_factory = sqlite3.Row
        current = conn.execute(
            "SELECT status FROM tasks WHERE task_id = ?", (task_id,)
        ).fetchone()
        if current is None:
            return False
        if TaskStatus(current["status"]).is_terminal:
            return False

    now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    result = conn.execute(
        "UPDATE tasks SET status = ?, last_updated_at = ? WHERE task_id = ?",
        (new_status, now, task_id),
    )
    return result.rowcount > 0


def add_evidence(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    raw_record_id: str | None = None,
    source_item_id: str = "",
    excerpt: str = "",
    confidence_delta: float = 0.0,
    evidence_role: str = "primary",
) -> str:
    """Attach a new evidence row to an existing task.

    Args:
        conn:             Open SQLite connection.
        task_id:          UUID of the task to add evidence to.
        raw_record_id:    Optional FK into ``raw_messages``.
        source_item_id:   Source-level item identifier.
        excerpt:          Short text excerpt.
        confidence_delta: Evidence strength adjustment (positive or
                          negative float, typically in ``[-1.0, 1.0]``).
        evidence_role:    One of ``"primary"``, ``"corroboration"``, or
                          ``"repetition"``.

    Returns:
        The new ``evidence_id`` UUID string.

    Raises:
        sqlite3.IntegrityError: If *task_id* does not exist.
    """
    evidence_id = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO task_evidence (
            evidence_id, task_id, raw_record_id, source_item_id,
            excerpt, confidence_delta, evidence_role
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            evidence_id,
            task_id,
            raw_record_id,
            source_item_id,
            excerpt,
            confidence_delta,
            evidence_role,
        ),
    )
    return evidence_id


def list_evidence(
    conn: sqlite3.Connection,
    task_id: str,
) -> list[TaskEvidence]:
    """Return all evidence rows for *task_id*, oldest first.

    Args:
        conn:    Open SQLite connection.
        task_id: UUID of the task.

    Returns:
        A list of :class:`TaskEvidence` objects.
    """
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM task_evidence WHERE task_id = ? ORDER BY created_at ASC",
        (task_id,),
    ).fetchall()
    return [TaskEvidence.from_db_row(r) for r in rows]
