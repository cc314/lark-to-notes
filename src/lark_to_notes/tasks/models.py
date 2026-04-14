"""Data models for the task registry.

These are pure data objects.  All database interaction lives in
:mod:`lark_to_notes.tasks.registry`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import sqlite3


class TaskStatus(StrEnum):
    """Lifecycle states for a task in the registry."""

    OPEN = "open"
    """Confirmed or high-confidence task; awaiting action."""

    NEEDS_REVIEW = "needs_review"
    """Lower-confidence or ambiguous item; requires human triage."""

    SNOOZED = "snoozed"
    """Temporarily deferred; will resurface later."""

    DISMISSED = "dismissed"
    """Manually rejected as noise or not actionable."""

    COMPLETED = "completed"
    """Work is done; closed by the operator."""

    MERGED = "merged"
    """Consolidated into another task; this record is superseded."""

    SUPERSEDED = "superseded"
    """Replaced by a newer interpretation of the same evidence."""

    @classmethod
    def terminal_states(cls) -> frozenset[TaskStatus]:
        """Return states from which no further lifecycle transition is expected."""
        return frozenset({cls.DISMISSED, cls.COMPLETED, cls.MERGED, cls.SUPERSEDED})

    @property
    def is_terminal(self) -> bool:
        """Return True if this state is terminal."""
        return self in self.terminal_states()


@dataclass(frozen=True, slots=True)
class TaskRecord:
    """A single task in the registry.

    All string fields that correspond to enum values (``status``,
    ``task_class``, etc.) are stored as plain strings so this class has
    no import dependency on the :mod:`distill` package.

    Attributes:
        task_id:                    UUID string.
        fingerprint:                16-char hex fingerprint (see
                                    :mod:`~lark_to_notes.tasks.fingerprint`).
        title:                      Short human-readable title.
        status:                     Current lifecycle state string.
        task_class:                 Classification string (``"task"``,
                                    ``"follow_up"``, ``"context"``,
                                    ``"needs_review"``).
        confidence_band:            Confidence string (``"high"``,
                                    ``"medium"``, ``"low"``).
        summary:                    Optional longer description.
        reason_code:                Machine-readable reason for
                                    classification.
        promotion_rec:              Recommended promotion destination
                                    string.
        assignee_refs:              Tuple of detected assignee references.
        due_at:                     Optional due-date string.
        manual_override_state:      JSON string capturing any operator
                                    override, or ``None``.
        created_from_raw_record_id: ``message_id`` of the raw record
                                    that first triggered this task.
        created_at:                 ISO timestamp when the row was
                                    inserted.
        last_updated_at:            ISO timestamp of last row change.
    """

    task_id: str
    fingerprint: str
    title: str
    status: str
    task_class: str
    confidence_band: str
    summary: str
    reason_code: str
    promotion_rec: str
    assignee_refs: tuple[str, ...]
    due_at: str | None
    manual_override_state: str | None
    created_from_raw_record_id: str | None
    created_at: str
    last_updated_at: str

    @classmethod
    def from_db_row(cls, row: sqlite3.Row) -> TaskRecord:
        """Construct a :class:`TaskRecord` from a :class:`sqlite3.Row`.

        Args:
            row: A row from the ``tasks`` table.

        Returns:
            A populated :class:`TaskRecord`.
        """
        raw_refs = row["assignee_refs"] or "[]"
        try:
            refs: list[str] = json.loads(raw_refs)
        except (json.JSONDecodeError, TypeError):
            refs = []
        return cls(
            task_id=row["task_id"],
            fingerprint=row["fingerprint"],
            title=row["title"],
            status=row["status"],
            task_class=row["task_class"],
            confidence_band=row["confidence_band"],
            summary=row["summary"] or "",
            reason_code=row["reason_code"] or "",
            promotion_rec=row["promotion_rec"] or "review",
            assignee_refs=tuple(str(r) for r in refs),
            due_at=row["due_at"],
            manual_override_state=row["manual_override_state"],
            created_from_raw_record_id=row["created_from_raw_record_id"],
            created_at=row["created_at"],
            last_updated_at=row["last_updated_at"],
        )

    @property
    def task_status(self) -> TaskStatus:
        """Return ``status`` as a :class:`TaskStatus` enum value.

        Raises:
            ValueError: If the stored string is not a valid
                        :class:`TaskStatus`.
        """
        return TaskStatus(self.status)


@dataclass(frozen=True, slots=True)
class TaskEvidence:
    """One piece of source evidence attached to a task.

    Evidence accumulates as repeated asks or related messages are
    processed.  Attaching evidence is preferred over creating duplicate
    tasks when the fingerprint matches.

    Attributes:
        evidence_id:      UUID string.
        task_id:          FK into the ``tasks`` table.
        raw_record_id:    Optional FK into ``raw_messages``.
        source_item_id:   Source-level item identifier.
        excerpt:          Short text excerpt driving the evidence link.
        confidence_delta: How much this evidence adjusts confidence
                          (positive = reinforces, negative = weakens).
        evidence_role:    ``"primary"``, ``"corroboration"``, or
                          ``"repetition"``.
        created_at:       ISO timestamp of evidence insertion.
    """

    evidence_id: str
    task_id: str
    raw_record_id: str | None
    source_item_id: str
    excerpt: str
    confidence_delta: float
    evidence_role: str
    created_at: str

    @classmethod
    def from_db_row(cls, row: sqlite3.Row) -> TaskEvidence:
        """Construct a :class:`TaskEvidence` from a :class:`sqlite3.Row`."""
        return cls(
            evidence_id=row["evidence_id"],
            task_id=row["task_id"],
            raw_record_id=row["raw_record_id"],
            source_item_id=row["source_item_id"] or "",
            excerpt=row["excerpt"] or "",
            confidence_delta=float(row["confidence_delta"] or 0.0),
            evidence_role=row["evidence_role"] or "primary",
            created_at=row["created_at"],
        )
