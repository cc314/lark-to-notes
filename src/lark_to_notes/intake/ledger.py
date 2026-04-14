"""Intake ledger: idempotent raw-message capture and intake-run audit.

The ledger writes new :class:`~lark_to_notes.intake.models.RawMessage`
records into the SQLite ``raw_messages`` table using ``INSERT OR IGNORE``
so re-processing the same message ID is always a no-op.

It also maintains the ``intake_runs`` audit table so every ingest session
is observable and diagnosable.
"""

from __future__ import annotations

import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any

from lark_to_notes.intake.models import RawMessage


def _utcnow_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Raw message persistence
# ---------------------------------------------------------------------------


def insert_raw_message(conn: sqlite3.Connection, message: RawMessage) -> bool:
    """Write a raw message to the ledger.

    The insert is ignored if the ``message_id`` already exists, making
    the operation fully idempotent.

    Args:
        conn: An open database connection.
        message: The raw message to persist.

    Returns:
        ``True`` if the row was inserted (new message), ``False`` if it
        was already present.
    """
    cursor = conn.execute(
        """
        INSERT OR IGNORE INTO raw_messages
            (message_id, source_id, source_type, chat_id, chat_type,
             sender_id, sender_name, direction, created_at, content,
             payload_json, ingested_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            message.message_id,
            message.source_id,
            message.source_type,
            message.chat_id,
            message.chat_type,
            message.sender_id,
            message.sender_name,
            message.direction,
            message.created_at,
            message.content,
            message.payload_json(),
            message.ingested_at,
        ),
    )
    conn.commit()
    return cursor.rowcount > 0


def get_raw_message(conn: sqlite3.Connection, message_id: str) -> RawMessage | None:
    """Fetch a single raw message by its Lark message ID.

    Args:
        conn: An open database connection.
        message_id: The Lark message identifier to look up.

    Returns:
        A :class:`~lark_to_notes.intake.models.RawMessage` if found,
        otherwise ``None``.
    """
    row = conn.execute("SELECT * FROM raw_messages WHERE message_id = ?", (message_id,)).fetchone()
    if row is None:
        return None
    return RawMessage.from_db_row(dict(row))


def list_raw_messages(
    conn: sqlite3.Connection,
    *,
    source_id: str | None = None,
    note_date: str | None = None,
    limit: int = 500,
) -> list[RawMessage]:
    """Query raw messages with optional filters.

    Args:
        conn: An open database connection.
        source_id: If given, restrict to messages from this source.
        note_date: If given (``YYYY-MM-DD``), restrict to messages whose
            ``created_at`` starts with that prefix.
        limit: Maximum number of rows to return.  Defaults to 500.

    Returns:
        A list of :class:`~lark_to_notes.intake.models.RawMessage`
        instances ordered by ``created_at`` ascending.
    """
    clauses: list[str] = []
    params: list[Any] = []

    if source_id is not None:
        clauses.append("source_id = ?")
        params.append(source_id)
    if note_date is not None:
        clauses.append("created_at LIKE ?")
        params.append(f"{note_date}%")

    sql = "SELECT * FROM raw_messages"
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY created_at ASC LIMIT ?"
    params.append(limit)

    rows = conn.execute(sql, params).fetchall()
    return [RawMessage.from_db_row(dict(row)) for row in rows]


def count_raw_messages(conn: sqlite3.Connection, source_id: str | None = None) -> int:
    """Return the total number of raw messages, optionally for one source.

    Args:
        conn: An open database connection.
        source_id: If given, count only messages from this source.

    Returns:
        An integer count.
    """
    if source_id is not None:
        row = conn.execute(
            "SELECT COUNT(*) FROM raw_messages WHERE source_id = ?", (source_id,)
        ).fetchone()
    else:
        row = conn.execute("SELECT COUNT(*) FROM raw_messages").fetchone()
    return int(row[0]) if row else 0


# ---------------------------------------------------------------------------
# Intake-run audit
# ---------------------------------------------------------------------------


def start_intake_run(conn: sqlite3.Connection, source_id: str) -> str:
    """Create a new intake-run record and return its ``run_id``.

    Args:
        conn: An open database connection.
        source_id: The watched-source identifier for this run.

    Returns:
        A UUID string identifying the new run.
    """
    run_id = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO intake_runs (run_id, source_id, started_at, status)
        VALUES (?, ?, ?, 'running')
        """,
        (run_id, source_id, _utcnow_iso()),
    )
    conn.commit()
    return run_id


def finish_intake_run(
    conn: sqlite3.Connection,
    run_id: str,
    *,
    messages_fetched: int,
    messages_new: int,
    status: str = "done",
    error_detail: str | None = None,
) -> None:
    """Mark an intake run as finished.

    Args:
        conn: An open database connection.
        run_id: The run identifier returned by :func:`start_intake_run`.
        messages_fetched: Total messages retrieved from the API.
        messages_new: Messages that were actually inserted (not duplicates).
        status: Final status — ``"done"`` or ``"error"``.
        error_detail: Optional error description when *status* is
            ``"error"``.
    """
    conn.execute(
        """
        UPDATE intake_runs SET
            finished_at      = ?,
            messages_fetched = ?,
            messages_new     = ?,
            status           = ?,
            error_detail     = ?
        WHERE run_id = ?
        """,
        (
            _utcnow_iso(),
            messages_fetched,
            messages_new,
            status,
            error_detail,
            run_id,
        ),
    )
    conn.commit()
