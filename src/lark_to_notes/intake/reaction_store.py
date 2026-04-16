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

from lark_to_notes.intake.reaction_model import NormalizedReactionEvent

ReactionIntakePath = Literal["event", "poll", "backfill"]


@dataclass(frozen=True)
class ReactionInsertResult:
    """Outcome of one reaction insert attempt."""

    reaction_event_id: str
    inserted: bool


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
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO message_reaction_events (
            reaction_event_id, source_id, message_id, reaction_kind, emoji_type,
            operator_type, operator_open_id, operator_user_id, operator_union_id,
            action_time, intake_path, payload_json, governance_version, policy_version
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', '')
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
        ),
    )
    conn.commit()
    return ReactionInsertResult(reaction_event_id=rid, inserted=cur.rowcount == 1)
