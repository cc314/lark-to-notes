"""Append-only persistence for normalized IM reaction events.

Maps :class:`~lark_to_notes.intake.reaction_model.NormalizedReactionEvent` rows
into ``message_reaction_events``. Each call uses one insert + ``commit``, like
:mod:`lark_to_notes.intake.ledger`. Deterministic surrogate ids cover missing
``header.event_id`` so ``INSERT OR IGNORE`` replays stay idempotent.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from typing import Literal

from lark_to_notes.intake.ledger import chat_ingest_key
from lark_to_notes.intake.reaction_model import NormalizedReactionEvent

ReactionIntakePath = Literal["event", "poll", "backfill"]


@dataclass(frozen=True)
class ReactionInsertResult:
    """Outcome of one reaction insert attempt."""

    reaction_event_id: str
    inserted: bool
    chat_ingest_fingerprint: str
    raw_message_present: bool


def reaction_correlation_counts(conn: sqlite3.Connection) -> dict[str, int]:
    """Count reaction rows versus rows in ``raw_messages`` for the same chat pair.

    **Linked** means a ``raw_messages`` row exists with the same ``message_id``
    and ``source_id`` as the reaction row (matches :func:`insert_message_reaction_event`
    presence detection). **Orphan** is ``total - linked`` (reconcile / backfill may
    change linkage over time even if ``raw_message_present`` was snapshotted at insert).
    """

    total_row = conn.execute("SELECT COUNT(*) AS c FROM message_reaction_events").fetchone()
    total = int(total_row["c"] if total_row is not None else 0)
    linked_row = conn.execute(
        """
        SELECT COUNT(*) AS c FROM message_reaction_events r
        WHERE EXISTS (
            SELECT 1 FROM raw_messages m
            WHERE m.message_id = r.message_id AND m.source_id = r.source_id
        )
        """
    ).fetchone()
    linked = int(linked_row["c"] if linked_row is not None else 0)
    return {
        "total": total,
        "linked_to_raw_message": linked,
        "orphan": max(0, total - linked),
    }


def surrogate_reaction_event_id(event: NormalizedReactionEvent) -> str:
    """Build a deterministic surrogate primary key when ``event_id`` is absent.

    The digest inputs are restricted to fields that should repeat across
    redeliveries of the same logical platform event; volatile header noise is
    intentionally omitted.
    """

    key = json.dumps(
        {
            "source_id": event.source_id,
            "message_id": event.message_id,
            "kind": event.reaction_kind.value,
            "emoji_type": event.emoji_type,
            "operator_type": event.operator_type,
            "operator_open_id": event.operator_open_id,
            "operator_user_id": event.operator_user_id,
            "operator_union_id": event.operator_union_id,
            "action_time": event.action_time,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:40]
    return f"rxs_{digest}"


def canonical_reaction_event_id(event: NormalizedReactionEvent) -> str:
    """Return upstream ``event_id`` when set, otherwise :func:`surrogate_reaction_event_id`."""
    head = event.reaction_event_id.strip()
    return head if head else surrogate_reaction_event_id(event)


def insert_message_reaction_event(
    conn: sqlite3.Connection,
    event: NormalizedReactionEvent,
    *,
    intake_path: ReactionIntakePath = "event",
) -> ReactionInsertResult:
    """Insert one reaction row when its primary key is new.

    Args:
        conn: Open SQLite connection (``sqlite3.Row`` factory recommended).
        event: Parsed reaction envelope.
        intake_path: Which intake path produced the row.

    Returns:
        :class:`ReactionInsertResult` with the effective id and whether a new
        row was stored.
    """
    rid = canonical_reaction_event_id(event)
    ingest_fp = chat_ingest_key(event.source_id, event.message_id)
    raw_hit = conn.execute(
        "SELECT 1 FROM raw_messages WHERE message_id = ? AND source_id = ? LIMIT 1",
        (event.message_id, event.source_id),
    ).fetchone()
    raw_present = 1 if raw_hit is not None else 0
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO message_reaction_events (
            reaction_event_id, source_id, message_id, reaction_kind, emoji_type,
            operator_type, operator_open_id, operator_user_id, operator_union_id,
            action_time, intake_path, payload_json, governance_version, policy_version,
            chat_ingest_fingerprint, raw_message_present
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', '', ?, ?)
        """,
        (
            rid,
            event.source_id,
            event.message_id,
            event.reaction_kind.value,
            event.emoji_type,
            event.operator_type,
            event.operator_open_id,
            event.operator_user_id,
            event.operator_union_id,
            event.action_time,
            intake_path,
            event.payload_json(),
            ingest_fp,
            raw_present,
        ),
    )
    conn.commit()
    return ReactionInsertResult(
        reaction_event_id=rid,
        inserted=cur.rowcount == 1,
        chat_ingest_fingerprint=ingest_fp,
        raw_message_present=raw_present == 1,
    )
