"""Intake ledger: idempotent raw-message capture and intake-run audit.

The ledger writes new :class:`~lark_to_notes.intake.models.RawMessage`
records into the SQLite ``raw_messages`` table using ``INSERT OR IGNORE``
so re-processing the same message ID is always a no-op.

It also maintains the ``intake_runs`` audit table so every ingest session
is observable and diagnosable.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from lark_to_notes.intake.models import (
    ChatEventAction,
    ChatIntakeItem,
    ChatIntakeState,
    DocumentIntakeItem,
    DocumentLifecycleState,
    DocumentRecordType,
    IntakePath,
    RawMessage,
)

logger = logging.getLogger(__name__)


def _utcnow_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Raw message persistence
# ---------------------------------------------------------------------------


def _link_pending_reactions_for_raw_pair(
    conn: sqlite3.Connection,
    *,
    source_id: str,
    message_id: str,
    orphan_batch_id: str,
) -> int:
    """Flip ``raw_message_present`` and dequeue orphans for one chat pair.

    Reactions may be ingested before chat intake drains the parent message into
    ``raw_messages`` (plan: Message reaction ingestion §4, lw-pzj.15).

    This path only **updates** existing ``message_reaction_events`` rows and
    deletes from ``reaction_orphan_queue`` — it never inserts duplicate reaction
    identities. Emits structured ``reaction_orphan_reconciled`` log (lw-pzj.15.2).
    Caller commits the transaction.

    Returns:
        Count of ``message_reaction_events`` rows updated from
        ``raw_message_present = 0`` to ``1`` for this pair.
    """

    t0 = time.perf_counter()
    cur = conn.execute(
        """
        UPDATE message_reaction_events
        SET raw_message_present = 1
        WHERE source_id = ? AND message_id = ?
          AND raw_message_present = 0
          AND EXISTS (
              SELECT 1 FROM raw_messages AS m
              WHERE m.message_id = ? AND m.source_id = ?
          )
        """,
        (source_id, message_id, message_id, source_id),
    )
    attached = int(cur.rowcount) if cur.rowcount is not None and cur.rowcount >= 0 else 0
    conn.execute(
        """
        DELETE FROM reaction_orphan_queue
        WHERE reaction_event_id IN (
            SELECT reaction_event_id FROM message_reaction_events
            WHERE source_id = ? AND message_id = ? AND raw_message_present = 1
        )
        """,
        (source_id, message_id),
    )
    pair_row = conn.execute(
        """
        SELECT COUNT(*) AS c FROM reaction_orphan_queue
        WHERE source_id = ? AND message_id = ?
        """,
        (source_id, message_id),
    ).fetchone()
    total_row = conn.execute("SELECT COUNT(*) AS c FROM reaction_orphan_queue").fetchone()
    still_pair = int(pair_row["c"] if pair_row is not None else 0)
    still_global = int(total_row["c"] if total_row is not None else 0)
    elapsed_s = time.perf_counter() - t0
    if attached > 0:
        conn.execute(
            """
            INSERT INTO reaction_reconcile_observations (
                observed_at, orphan_batch_id, source_id, message_id,
                attached_count, elapsed_ms
            )
            VALUES (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), ?, ?, ?, ?, ?)
            """,
            (
                orphan_batch_id,
                source_id,
                message_id,
                attached,
                round(elapsed_s * 1000.0, 6),
            ),
        )
    logger.info(
        "reaction_orphan_reconciled",
        extra={
            "orphan_batch_id": orphan_batch_id,
            "message_id": message_id,
            "source_id": source_id,
            "attached_count": attached,
            "still_orphan": still_global,
            "still_orphan_pair": still_pair,
            "elapsed_ms": round(elapsed_s * 1000.0, 3),
        },
    )
    return attached


def insert_raw_message(conn: sqlite3.Connection, message: RawMessage) -> bool:
    """Write a raw message to the ledger.

    The insert is ignored if the ``message_id`` already exists, making
    the operation fully idempotent.

    After each call (whether the row was new or ignored), any
    ``message_reaction_events`` rows for the same ``(source_id, message_id)``
    that were waiting on ``raw_messages`` have ``raw_message_present`` updated
    to ``1`` in the **same** transaction as the insert attempt, then the
    connection commits once. Structured ``reaction_orphan_reconciled`` logs
    record attach latency and orphan depth (lw-pzj.15.2).

    Args:
        conn: An open database connection.
        message: The raw message to persist.

    Returns:
        ``True`` if the row was inserted (new message), ``False`` if it
        was already present.
    """
    orphan_batch_id = uuid.uuid4().hex
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
    inserted = cursor.rowcount > 0
    _ = _link_pending_reactions_for_raw_pair(
        conn,
        source_id=message.source_id,
        message_id=message.message_id,
        orphan_batch_id=orphan_batch_id,
    )
    conn.commit()
    logger.debug(
        "insert_raw_message",
        extra={
            "message_id": message.message_id,
            "source_id": message.source_id,
            "source_type": message.source_type,
            "inserted": inserted,
        },
    )
    return inserted


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


def list_raw_messages_recent(conn: sqlite3.Connection, *, limit: int = 200) -> list[RawMessage]:
    """Return the most recently ingested raw messages (``rowid`` order)."""

    rows = conn.execute(
        "SELECT * FROM raw_messages ORDER BY rowid DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [RawMessage.from_db_row(dict(row)) for row in rows]


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


def chat_ingest_key(source_id: str, message_id: str) -> str:
    """Return the canonical mixed-intake key for one chat message."""

    raw = f"{source_id}\x00{message_id}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def document_ingest_key(
    source_id: str,
    record_type: DocumentRecordType,
    source_stream_id: str,
    source_item_id: str,
) -> str:
    """Return the canonical intake key for one document-side surface item."""

    raw = f"{source_id}\x00{record_type.value}\x00{source_stream_id}\x00{source_item_id}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def observe_chat_message(
    conn: sqlite3.Connection,
    message: RawMessage,
    *,
    intake_path: IntakePath,
    observed_at: str | None = None,
    coalesce_window_seconds: int = 60,
    event_action: ChatEventAction = ChatEventAction.CREATE,
) -> ChatIntakeItem:
    """Observe a chat message through the mixed poll/event intake ledger.

    Polling and event-driven paths write to the same canonical row keyed by
    ``source_id`` and ``message_id``. Event observations remain pending until
    their coalescing window expires, while a later poll observation can make a
    pending row immediately ready for downstream processing.
    """

    if intake_path is IntakePath.EVENT and event_action is not ChatEventAction.CREATE:
        raise ValueError(
            "only create chat events are supported; edit/delete events need a revision-aware path"
        )

    ingest_key = chat_ingest_key(message.source_id, message.message_id)
    seen_at = observed_at or _utcnow_iso()
    existing = get_chat_intake_item(conn, ingest_key)

    if existing is None:
        poll_seen_count = 1 if intake_path is IntakePath.POLL else 0
        event_seen_count = 1 if intake_path is IntakePath.EVENT else 0
        coalesce_until = (
            None
            if intake_path is IntakePath.POLL
            else _add_seconds(seen_at, coalesce_window_seconds)
        )
        conn.execute(
            """
            INSERT INTO chat_intake_ledger (
                ingest_key, message_id, source_id, source_type, chat_id, chat_type,
                sender_id, sender_name, direction, created_at, content, payload_json,
                first_seen_at, last_seen_at, first_intake_path, last_intake_path,
                poll_seen_count, event_seen_count, coalesce_until, processing_state,
                processed_at, last_error
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ingest_key,
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
                seen_at,
                seen_at,
                intake_path.value,
                intake_path.value,
                poll_seen_count,
                event_seen_count,
                coalesce_until,
                ChatIntakeState.PENDING.value,
                None,
                "",
            ),
        )
        conn.commit()
        return get_chat_intake_item(conn, ingest_key) or ChatIntakeItem(
            ingest_key=ingest_key,
            message_id=message.message_id,
            source_id=message.source_id,
            source_type=message.source_type,
            chat_id=message.chat_id,
            chat_type=message.chat_type,
            sender_id=message.sender_id,
            sender_name=message.sender_name,
            direction=message.direction,
            created_at=message.created_at,
            content=message.content,
            payload=message.payload,
            first_seen_at=seen_at,
            last_seen_at=seen_at,
            first_intake_path=intake_path,
            last_intake_path=intake_path,
            poll_seen_count=poll_seen_count,
            event_seen_count=event_seen_count,
            coalesce_until=coalesce_until,
        )

    coalesce_until = existing.coalesce_until
    if existing.processing_state is ChatIntakeState.PENDING:
        if intake_path is IntakePath.POLL:
            coalesce_until = seen_at
        elif existing.poll_seen_count == 0:
            next_window = _add_seconds(seen_at, coalesce_window_seconds)
            coalesce_until = _max_timestamp(existing.coalesce_until, next_window)

    conn.execute(
        """
        UPDATE chat_intake_ledger SET
            source_type = ?,
            chat_id = ?,
            chat_type = ?,
            sender_id = ?,
            sender_name = ?,
            direction = ?,
            created_at = ?,
            content = ?,
            payload_json = ?,
            last_seen_at = ?,
            last_intake_path = ?,
            poll_seen_count = ?,
            event_seen_count = ?,
            coalesce_until = ?
        WHERE ingest_key = ?
        """,
        (
            message.source_type,
            message.chat_id,
            message.chat_type,
            message.sender_id,
            message.sender_name,
            message.direction,
            message.created_at,
            message.content,
            message.payload_json(),
            seen_at,
            intake_path.value,
            existing.poll_seen_count + int(intake_path is IntakePath.POLL),
            existing.event_seen_count + int(intake_path is IntakePath.EVENT),
            coalesce_until,
            ingest_key,
        ),
    )
    conn.commit()
    updated = get_chat_intake_item(conn, ingest_key)
    if updated is None:
        raise RuntimeError(f"chat intake row disappeared for ingest_key={ingest_key}")
    return updated


def get_chat_intake_item(conn: sqlite3.Connection, ingest_key: str) -> ChatIntakeItem | None:
    """Fetch one mixed-intake ledger row by its ingest key."""

    row = conn.execute(
        "SELECT * FROM chat_intake_ledger WHERE ingest_key = ?",
        (ingest_key,),
    ).fetchone()
    if row is None:
        return None
    return ChatIntakeItem.from_db_row(dict(row))


def list_ready_chat_intake(
    conn: sqlite3.Connection,
    *,
    as_of: str | None = None,
    limit: int = 100,
) -> list[ChatIntakeItem]:
    """Return pending chat-intake rows whose coalescing window has expired."""

    ready_at = as_of or _utcnow_iso()
    rows = conn.execute(
        """
        SELECT *
        FROM chat_intake_ledger
        WHERE processing_state = ?
          AND (coalesce_until IS NULL OR coalesce_until <= ?)
        ORDER BY first_seen_at ASC
        LIMIT ?
        """,
        (ChatIntakeState.PENDING.value, ready_at, limit),
    ).fetchall()
    return [ChatIntakeItem.from_db_row(dict(row)) for row in rows]


def mark_chat_intake_processed(
    conn: sqlite3.Connection,
    ingest_key: str,
    *,
    processed_at: str | None = None,
) -> None:
    """Mark a chat-intake row as processed after raw capture succeeds."""

    finished_at = processed_at or _utcnow_iso()
    conn.execute(
        """
        UPDATE chat_intake_ledger
        SET processing_state = ?, processed_at = ?, last_error = ''
        WHERE ingest_key = ?
        """,
        (ChatIntakeState.PROCESSED.value, finished_at, ingest_key),
    )
    conn.commit()


def chat_intake_ledger_counts(
    conn: sqlite3.Connection,
    *,
    now_iso: str | None = None,
) -> dict[str, int]:
    """Return aggregate counts for mixed poll/event chat-intake rows.

    *pending_ready* rows are eligible for drain (coalescing window elapsed).
    *pending_coalescing* rows are still waiting inside ``coalesce_until``.
    """

    now = now_iso or _utcnow_iso()
    row = conn.execute(
        """
        SELECT
            COALESCE(SUM(CASE
                WHEN processing_state = ? AND (coalesce_until IS NULL OR coalesce_until <= ?)
                THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE
                WHEN processing_state = ? AND coalesce_until IS NOT NULL AND coalesce_until > ?
                THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN processing_state = ? THEN 1 ELSE 0 END), 0)
        FROM chat_intake_ledger
        """,
        (
            ChatIntakeState.PENDING.value,
            now,
            ChatIntakeState.PENDING.value,
            now,
            ChatIntakeState.PROCESSED.value,
        ),
    ).fetchone()
    return {
        "pending_ready": int(row[0]) if row else 0,
        "pending_coalescing": int(row[1]) if row else 0,
        "processed": int(row[2]) if row else 0,
    }


def observe_document_surface(
    conn: sqlite3.Connection,
    *,
    record_type: DocumentRecordType,
    source_id: str,
    document_token: str,
    source_stream_id: str,
    source_item_id: str,
    content_hash: str,
    normalized_text: str,
    payload: dict[str, Any],
    intake_path: IntakePath,
    parent_item_id: str = "",
    revision_id: str = "",
    lifecycle_state: DocumentLifecycleState = DocumentLifecycleState.ACTIVE,
    canonical_link: str = "",
    observed_at: str | None = None,
    coalesce_window_seconds: int = 60,
) -> DocumentIntakeItem:
    """Observe a revision-bearing document surface through the mixed poll/event ledger."""

    ingest_key = document_ingest_key(source_id, record_type, source_stream_id, source_item_id)
    seen_at = observed_at or _utcnow_iso()
    payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    existing = get_document_intake_item(conn, ingest_key)

    if existing is None:
        poll_seen_count = 1 if intake_path is IntakePath.POLL else 0
        event_seen_count = 1 if intake_path is IntakePath.EVENT else 0
        coalesce_until = (
            None
            if intake_path is IntakePath.POLL
            else _add_seconds(seen_at, coalesce_window_seconds)
        )
        conn.execute(
            """
            INSERT INTO document_intake_ledger (
                ingest_key, record_type, source_id, document_token, source_stream_id,
                source_item_id, parent_item_id, revision_id, lifecycle_state,
                content_hash, normalized_text, payload_json, canonical_link,
                first_seen_at, last_seen_at, first_intake_path, last_intake_path,
                poll_seen_count, event_seen_count, coalesce_until, processing_state,
                processed_at, last_error
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ingest_key,
                record_type.value,
                source_id,
                document_token,
                source_stream_id,
                source_item_id,
                parent_item_id,
                revision_id,
                lifecycle_state.value,
                content_hash,
                normalized_text,
                payload_json,
                canonical_link,
                seen_at,
                seen_at,
                intake_path.value,
                intake_path.value,
                poll_seen_count,
                event_seen_count,
                coalesce_until,
                ChatIntakeState.PENDING.value,
                None,
                "",
            ),
        )
        conn.commit()
        return get_document_intake_item(conn, ingest_key) or DocumentIntakeItem(
            ingest_key=ingest_key,
            record_type=record_type,
            source_id=source_id,
            document_token=document_token,
            source_stream_id=source_stream_id,
            source_item_id=source_item_id,
            parent_item_id=parent_item_id,
            revision_id=revision_id,
            lifecycle_state=lifecycle_state,
            content_hash=content_hash,
            normalized_text=normalized_text,
            payload=payload,
            canonical_link=canonical_link,
            first_seen_at=seen_at,
            last_seen_at=seen_at,
            first_intake_path=intake_path,
            last_intake_path=intake_path,
            poll_seen_count=poll_seen_count,
            event_seen_count=event_seen_count,
            coalesce_until=coalesce_until,
        )

    substantive = (
        existing.content_hash != content_hash
        or existing.revision_id != revision_id
        or existing.lifecycle_state != lifecycle_state
        or existing.normalized_text != normalized_text
    )

    if existing.processing_state is ChatIntakeState.PROCESSED and not substantive:
        conn.execute(
            """
            UPDATE document_intake_ledger SET
                payload_json = ?,
                last_seen_at = ?,
                last_intake_path = ?,
                poll_seen_count = ?,
                event_seen_count = ?
            WHERE ingest_key = ?
            """,
            (
                payload_json,
                seen_at,
                intake_path.value,
                existing.poll_seen_count + int(intake_path is IntakePath.POLL),
                existing.event_seen_count + int(intake_path is IntakePath.EVENT),
                ingest_key,
            ),
        )
        conn.commit()
        updated = get_document_intake_item(conn, ingest_key)
        if updated is None:
            raise RuntimeError(f"document intake row disappeared for ingest_key={ingest_key}")
        return updated

    coalesce_until = existing.coalesce_until
    processing_state = ChatIntakeState.PENDING.value
    processed_at: str | None = None

    if existing.processing_state is ChatIntakeState.PENDING:
        if intake_path is IntakePath.POLL:
            coalesce_until = seen_at
        elif existing.poll_seen_count == 0:
            next_window = _add_seconds(seen_at, coalesce_window_seconds)
            coalesce_until = _max_timestamp(existing.coalesce_until, next_window)
    elif substantive:
        if intake_path is IntakePath.POLL:
            coalesce_until = seen_at
        else:
            coalesce_until = _add_seconds(seen_at, coalesce_window_seconds)

    conn.execute(
        """
        UPDATE document_intake_ledger SET
            parent_item_id = ?,
            revision_id = ?,
            lifecycle_state = ?,
            content_hash = ?,
            normalized_text = ?,
            payload_json = ?,
            canonical_link = ?,
            last_seen_at = ?,
            last_intake_path = ?,
            poll_seen_count = ?,
            event_seen_count = ?,
            coalesce_until = ?,
            processing_state = ?,
            processed_at = ?,
            last_error = ''
        WHERE ingest_key = ?
        """,
        (
            parent_item_id,
            revision_id,
            lifecycle_state.value,
            content_hash,
            normalized_text,
            payload_json,
            canonical_link,
            seen_at,
            intake_path.value,
            existing.poll_seen_count + int(intake_path is IntakePath.POLL),
            existing.event_seen_count + int(intake_path is IntakePath.EVENT),
            coalesce_until,
            processing_state,
            processed_at,
            ingest_key,
        ),
    )
    conn.commit()
    updated = get_document_intake_item(conn, ingest_key)
    if updated is None:
        raise RuntimeError(f"document intake row disappeared for ingest_key={ingest_key}")
    return updated


def get_document_intake_item(
    conn: sqlite3.Connection, ingest_key: str
) -> DocumentIntakeItem | None:
    """Fetch one document-intake ledger row by its ingest key."""

    row = conn.execute(
        "SELECT * FROM document_intake_ledger WHERE ingest_key = ?",
        (ingest_key,),
    ).fetchone()
    if row is None:
        return None
    return DocumentIntakeItem.from_db_row(dict(row))


def list_ready_document_intake(
    conn: sqlite3.Connection,
    *,
    as_of: str | None = None,
    limit: int = 100,
) -> list[DocumentIntakeItem]:
    """Return pending document-intake rows whose coalescing window has expired."""

    ready_at = as_of or _utcnow_iso()
    rows = conn.execute(
        """
        SELECT *
        FROM document_intake_ledger
        WHERE processing_state = ?
          AND (coalesce_until IS NULL OR coalesce_until <= ?)
        ORDER BY first_seen_at ASC
        LIMIT ?
        """,
        (ChatIntakeState.PENDING.value, ready_at, limit),
    ).fetchall()
    return [DocumentIntakeItem.from_db_row(dict(row)) for row in rows]


def mark_document_intake_processed(
    conn: sqlite3.Connection,
    ingest_key: str,
    *,
    processed_at: str | None = None,
) -> None:
    """Mark a document-intake row as processed after raw capture succeeds."""

    finished_at = processed_at or _utcnow_iso()
    conn.execute(
        """
        UPDATE document_intake_ledger
        SET processing_state = ?, processed_at = ?, last_error = ''
        WHERE ingest_key = ?
        """,
        (ChatIntakeState.PROCESSED.value, finished_at, ingest_key),
    )
    conn.commit()


def _parse_iso_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _add_seconds(value: str, seconds: int) -> str:
    return (_parse_iso_timestamp(value) + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _max_timestamp(left: str | None, right: str | None) -> str | None:
    if left is None:
        return right
    if right is None:
        return left
    return left if _parse_iso_timestamp(left) >= _parse_iso_timestamp(right) else right


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
