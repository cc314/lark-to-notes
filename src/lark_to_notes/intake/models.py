"""Raw message model for the intake ledger."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


def _utcnow_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_note_date(created_at: str) -> str:
    """Extract a ``YYYY-MM-DD`` date string from a ``created_at`` value.

    Handles both ISO 8601 (``"2026-04-14T11:21:00Z"``) and the
    ``"YYYY-MM-DD HH:MM"`` format used in the raw JSONL logs.

    Args:
        created_at: A date/time string from a raw message record.

    Returns:
        A ``YYYY-MM-DD`` string, or ``""`` if parsing fails.
    """
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(created_at[:19], fmt[: len(created_at[:19])]).strftime(
                "%Y-%m-%d"
            )
        except ValueError:
            continue
    # Fallback: take the date prefix directly if it looks like YYYY-MM-DD.
    if len(created_at) >= 10 and created_at[4] == "-" and created_at[7] == "-":
        return created_at[:10]
    return ""


@dataclass(frozen=True)
class RawMessage:
    """A single message record as captured from a Lark surface.

    This is the canonical record type for the intake ledger.  Instances
    are serialised into both the SQLite ``raw_messages`` table and the
    append-only daily JSONL files under ``<vault_root>/raw/lark-worker/``.

    Attributes:
        message_id: Stable Lark message identifier.
        source_id: Watched-source identifier (``"{type}:{external_id}"``).
        source_type: Lark surface type string (e.g. ``"dm_user"``).
        chat_id: Lark chat or doc identifier.
        chat_type: Chat subtype from Lark (``"p2p"``, ``"group"``, etc.).
        sender_id: Lark open-ID of the sender.
        sender_name: Display name of the sender.
        direction: ``"incoming"`` or ``"outgoing"``.
        created_at: Original Lark timestamp (preserved as-is for fidelity).
        content: Normalised plain-text content of the message.
        payload: The raw Lark API payload dict.
        ingested_at: ISO 8601 UTC timestamp of when the record was ingested.
    """

    message_id: str
    source_id: str
    source_type: str
    chat_id: str
    chat_type: str
    sender_id: str
    sender_name: str
    direction: str
    created_at: str
    content: str
    payload: dict[str, Any]
    ingested_at: str = field(default_factory=_utcnow_iso)

    @property
    def note_date(self) -> str:
        """Return the ``YYYY-MM-DD`` date for vault daily-note routing."""
        return _parse_note_date(self.created_at)

    def payload_json(self) -> str:
        """Serialise the payload dict to a compact JSON string."""
        return json.dumps(self.payload, ensure_ascii=False, separators=(",", ":"))

    @classmethod
    def from_jsonl_record(cls, record: dict[str, Any]) -> RawMessage:
        """Build a :class:`RawMessage` from a raw JSONL log record.

        Args:
            record: A dict decoded from one line of a raw JSONL log file.

        Returns:
            A :class:`RawMessage` instance.
        """
        payload = record.get("payload", {})
        if isinstance(payload, str):
            payload = json.loads(payload)
        return cls(
            message_id=record["message_id"],
            source_id=record.get("source_id", ""),
            source_type=record.get("source_type", ""),
            chat_id=record.get("chat_id", ""),
            chat_type=record.get("chat_type", ""),
            sender_id=record.get("sender_id", ""),
            sender_name=record.get("sender_name", ""),
            direction=record.get("direction", "incoming"),
            created_at=record.get("created_at", ""),
            content=record.get("content", ""),
            payload=payload,
        )

    @classmethod
    def from_db_row(cls, row: dict[str, Any]) -> RawMessage:
        """Build a :class:`RawMessage` from a SQLite row dict.

        Args:
            row: A dict produced from a ``sqlite3.Row`` object.

        Returns:
            A :class:`RawMessage` instance.
        """
        raw_payload = row.get("payload_json", "{}")
        payload: dict[str, Any] = json.loads(raw_payload) if raw_payload else {}
        return cls(
            message_id=row["message_id"],
            source_id=row.get("source_id", ""),
            source_type=row.get("source_type", ""),
            chat_id=row.get("chat_id", ""),
            chat_type=row.get("chat_type", ""),
            sender_id=row.get("sender_id", ""),
            sender_name=row.get("sender_name", ""),
            direction=row.get("direction", "incoming"),
            created_at=row.get("created_at", ""),
            content=row.get("content", ""),
            payload=payload,
            ingested_at=row.get("ingested_at", ""),
        )


class IntakePath(StrEnum):
    """How a chat message was observed by the live intake pipeline."""

    POLL = "poll"
    EVENT = "event"


class ChatEventAction(StrEnum):
    """Supported live chat event actions."""

    CREATE = "create"
    EDIT = "edit"
    DELETE = "delete"


class ChatIntakeState(StrEnum):
    """Lifecycle state for one canonical chat-intake ledger row."""

    PENDING = "pending"
    PROCESSED = "processed"


class DocumentRecordType(StrEnum):
    """Which revision-bearing document surface a ledger row represents."""

    DOC_BODY = "doc_body"
    DOC_COMMENT = "doc_comment"
    DOC_REPLY = "doc_reply"


class DocumentLifecycleState(StrEnum):
    """Mutable document-side lifecycle (distinct from chat message deletion)."""

    ACTIVE = "active"
    EDITED = "edited"
    DELETED = "deleted"
    SUPERSEDED = "superseded"


@dataclass(frozen=True)
class DocumentIntakeItem:
    """One canonical row in ``document_intake_ledger`` for doc/comment/reply capture."""

    ingest_key: str
    record_type: DocumentRecordType
    source_id: str
    document_token: str
    source_stream_id: str
    source_item_id: str
    parent_item_id: str
    revision_id: str
    lifecycle_state: DocumentLifecycleState
    content_hash: str
    normalized_text: str
    payload: dict[str, Any]
    canonical_link: str
    first_seen_at: str
    last_seen_at: str
    first_intake_path: IntakePath
    last_intake_path: IntakePath
    poll_seen_count: int
    event_seen_count: int
    processing_state: ChatIntakeState = ChatIntakeState.PENDING
    coalesce_until: str | None = None
    processed_at: str | None = None
    last_error: str = ""

    @classmethod
    def from_db_row(cls, row: dict[str, Any]) -> DocumentIntakeItem:
        raw_payload = row.get("payload_json", "{}")
        payload: dict[str, Any] = json.loads(raw_payload) if raw_payload else {}
        return cls(
            ingest_key=row["ingest_key"],
            record_type=DocumentRecordType(row["record_type"]),
            source_id=row["source_id"],
            document_token=row.get("document_token", ""),
            source_stream_id=row["source_stream_id"],
            source_item_id=row["source_item_id"],
            parent_item_id=row.get("parent_item_id", ""),
            revision_id=row.get("revision_id", ""),
            lifecycle_state=DocumentLifecycleState(
                row.get("lifecycle_state", DocumentLifecycleState.ACTIVE)
            ),
            content_hash=row.get("content_hash", ""),
            normalized_text=row.get("normalized_text", ""),
            payload=payload,
            canonical_link=row.get("canonical_link", ""),
            first_seen_at=row["first_seen_at"],
            last_seen_at=row["last_seen_at"],
            first_intake_path=IntakePath(row["first_intake_path"]),
            last_intake_path=IntakePath(row["last_intake_path"]),
            poll_seen_count=int(row.get("poll_seen_count", 0)),
            event_seen_count=int(row.get("event_seen_count", 0)),
            processing_state=ChatIntakeState(row.get("processing_state", ChatIntakeState.PENDING)),
            coalesce_until=row.get("coalesce_until"),
            processed_at=row.get("processed_at"),
            last_error=row.get("last_error", ""),
        )


@dataclass(frozen=True)
class ChatIntakeItem:
    """A canonical chat-intake ledger row for mixed poll/event observation."""

    ingest_key: str
    message_id: str
    source_id: str
    source_type: str
    chat_id: str
    chat_type: str
    sender_id: str
    sender_name: str
    direction: str
    created_at: str
    content: str
    payload: dict[str, Any]
    first_seen_at: str
    last_seen_at: str
    first_intake_path: IntakePath
    last_intake_path: IntakePath
    poll_seen_count: int
    event_seen_count: int
    processing_state: ChatIntakeState = ChatIntakeState.PENDING
    coalesce_until: str | None = None
    processed_at: str | None = None
    last_error: str = ""

    @classmethod
    def from_db_row(cls, row: dict[str, Any]) -> ChatIntakeItem:
        """Build a :class:`ChatIntakeItem` from a SQLite row dict."""

        raw_payload = row.get("payload_json", "{}")
        payload: dict[str, Any] = json.loads(raw_payload) if raw_payload else {}
        return cls(
            ingest_key=row["ingest_key"],
            message_id=row["message_id"],
            source_id=row["source_id"],
            source_type=row["source_type"],
            chat_id=row["chat_id"],
            chat_type=row.get("chat_type", ""),
            sender_id=row.get("sender_id", ""),
            sender_name=row.get("sender_name", ""),
            direction=row.get("direction", "incoming"),
            created_at=row.get("created_at", ""),
            content=row.get("content", ""),
            payload=payload,
            first_seen_at=row["first_seen_at"],
            last_seen_at=row["last_seen_at"],
            first_intake_path=IntakePath(row["first_intake_path"]),
            last_intake_path=IntakePath(row["last_intake_path"]),
            poll_seen_count=int(row.get("poll_seen_count", 0)),
            event_seen_count=int(row.get("event_seen_count", 0)),
            processing_state=ChatIntakeState(row.get("processing_state", ChatIntakeState.PENDING)),
            coalesce_until=row.get("coalesce_until"),
            processed_at=row.get("processed_at"),
            last_error=row.get("last_error", ""),
        )

    def to_raw_message(self) -> RawMessage:
        """Project the ledger row into the canonical raw-message shape."""

        return RawMessage(
            message_id=self.message_id,
            source_id=self.source_id,
            source_type=self.source_type,
            chat_id=self.chat_id,
            chat_type=self.chat_type,
            sender_id=self.sender_id,
            sender_name=self.sender_name,
            direction=self.direction,
            created_at=self.created_at,
            content=self.content,
            payload=self.payload,
            ingested_at=self.last_seen_at,
        )
