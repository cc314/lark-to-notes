"""Source model: SourceType enum, WatchedSource, and Checkpoint."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


class SourceType(StrEnum):
    """Lark surface types that can be watched."""

    DM = "dm"
    GROUP = "group"
    DOC = "doc"


def make_source_id(source_type: SourceType, external_id: str) -> str:
    """Build a stable, deterministic source identifier.

    Args:
        source_type: The surface type of the source.
        external_id: The Lark-native identifier for the source.

    Returns:
        A colon-delimited string ``"{type}:{external_id}"``.
    """
    return f"{source_type}:{external_id}"


def _utcnow_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True)
class WatchedSource:
    """A Lark surface that the system is configured to watch.

    Attributes:
        source_id: Stable identifier (``"{source_type}:{external_id}"``).
        source_type: Surface type — dm, group, or doc.
        external_id: Lark-native ID for the surface.
        name: Human-readable display name.
        enabled: Whether polling is active for this source.
        config: Type-specific settings (e.g. fetch window, label overrides).
        created_at: ISO 8601 UTC timestamp of first registration.
        updated_at: ISO 8601 UTC timestamp of last modification.
    """

    source_id: str
    source_type: SourceType
    external_id: str
    name: str
    enabled: bool = True
    config: dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=_utcnow_iso)
    updated_at: str = field(default_factory=_utcnow_iso)

    def config_json(self) -> str:
        """Serialise config to a JSON string for storage."""
        return json.dumps(self.config, ensure_ascii=False, sort_keys=True)

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> WatchedSource:
        """Deserialise a database row into a :class:`WatchedSource`.

        Args:
            row: A mapping produced by ``sqlite3.Row`` or similar.

        Returns:
            A populated :class:`WatchedSource` instance.
        """
        config: dict[str, Any] = json.loads(row.get("config_json") or "{}")
        return cls(
            source_id=row["source_id"],
            source_type=SourceType(row["source_type"]),
            external_id=row["external_id"],
            name=row["name"],
            enabled=bool(row["enabled"]),
            config=config,
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )


@dataclass(frozen=True)
class Checkpoint:
    """Pagination state for a single watched source.

    Attributes:
        source_id: Foreign key into ``watched_sources``.
        last_message_id: Lark message ID of the most-recently ingested item.
        last_message_timestamp: ISO 8601 UTC of the most-recently ingested item.
        page_token: Opaque continuation token from the Lark API, if any.
        updated_at: ISO 8601 UTC of the last checkpoint write.
    """

    source_id: str
    last_message_id: str | None = None
    last_message_timestamp: str | None = None
    page_token: str | None = None
    updated_at: str = field(default_factory=_utcnow_iso)

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> Checkpoint:
        """Deserialise a database row into a :class:`Checkpoint`.

        Args:
            row: A mapping produced by ``sqlite3.Row`` or similar.

        Returns:
            A populated :class:`Checkpoint` instance.
        """
        return cls(
            source_id=row["source_id"],
            last_message_id=row.get("last_message_id"),
            last_message_timestamp=row.get("last_message_timestamp"),
            page_token=row.get("page_token"),
            updated_at=row["updated_at"],
        )


@dataclass(frozen=True)
class ReactionBackfillCheckpoint:
    """Resume cursor for :func:`lark_to_notes.intake.reaction_backfill.execute_reaction_backfill`.

    ``watermark_*`` is the last ``raw_messages`` row **fully** drained through
    ``im.reactions.list`` pagination. ``inflight_*`` captures a mid-message
    ``page_token`` so a restarted process can continue without skipping pages.
    """

    source_id: str
    watermark_created_at: str | None
    watermark_message_id: str | None
    inflight_message_id: str | None = None
    inflight_created_at: str | None = None
    inflight_page_token: str | None = None
    batches_completed: int = 0
    api_calls: int = 0
    rows_inserted: int = 0
    last_error: str | None = None
    updated_at: str = field(default_factory=_utcnow_iso)

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> ReactionBackfillCheckpoint:
        """Build from a ``reaction_backfill_checkpoints`` row."""

        return cls(
            source_id=row["source_id"],
            watermark_created_at=row.get("watermark_created_at"),
            watermark_message_id=row.get("watermark_message_id"),
            inflight_message_id=row.get("inflight_message_id"),
            inflight_created_at=row.get("inflight_created_at"),
            inflight_page_token=row.get("inflight_page_token"),
            batches_completed=int(row.get("batches_completed") or 0),
            api_calls=int(row.get("api_calls") or 0),
            rows_inserted=int(row.get("rows_inserted") or 0),
            last_error=row.get("last_error"),
            updated_at=row["updated_at"],
        )
