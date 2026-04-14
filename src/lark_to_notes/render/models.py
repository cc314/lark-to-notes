"""Data models for the render layer.

These models describe what to render and where, and capture the outcome
of a render operation so callers can log, test, and audit results.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path


class RenderSurface(StrEnum):
    """Which vault surface receives the rendered output."""

    RAW = "raw"
    """A new note in ``raw/`` for source provenance."""

    DAILY = "daily"
    """The daily note for the date of the source event."""

    CURRENT_TASKS = "current_tasks"
    """The durable open-work list at ``area/current tasks/index.md``."""


class RenderOutcome(StrEnum):
    """High-level result of a single render operation."""

    CREATED = "created"
    """A new note file was created."""

    UPDATED = "updated"
    """An existing note was updated (block replaced or appended)."""

    SKIPPED = "skipped"
    """The render was a no-op (content unchanged or item not eligible)."""

    FAILED = "failed"
    """The render failed with an error (see :attr:`RenderResult.error`)."""


@dataclass(frozen=True, slots=True)
class RenderItem:
    """A single item to render into the vault.

    Attributes:
        task_id:      UUID of the :class:`~lark_to_notes.tasks.models.TaskRecord`.
        fingerprint:  16-char hex fingerprint.
        title:        Short task title for bullet / heading text.
        summary:      Optional longer description.
        promotion_rec: Recommended promotion destination string
                       (``"current_tasks"``, ``"daily_only"``, ``"review"``).
        reason_code:  Classification reason code.
        confidence_band: Confidence string (``"high"``, ``"medium"``, ``"low"``).
        task_class:   Classification string.
        status:       Current task status string.
        assignee_refs: Detected assignee references.
        source_note_path: Vault path to the raw provenance note, if available.
        daily_note_path:  Vault path to the relevant daily note, if available.
        due_at:       Optional due-date string.
        source_message_id: Original raw message ID for provenance backlink.
        event_date:   Date string (``YYYY-MM-DD``) of the originating event.
    """

    task_id: str
    fingerprint: str
    title: str
    promotion_rec: str
    reason_code: str
    confidence_band: str
    task_class: str
    status: str
    summary: str = ""
    assignee_refs: tuple[str, ...] = ()
    source_note_path: str | None = None
    daily_note_path: str | None = None
    due_at: str | None = None
    source_message_id: str | None = None
    event_date: str = ""


@dataclass(frozen=True, slots=True)
class RenderResult:
    """The outcome of one render call on one surface.

    Attributes:
        surface:    Which surface was targeted.
        outcome:    High-level result code.
        target_path: Absolute path of the note that was created or modified.
        block_id:   Stable block ID that was written (may be empty for new files).
        entity_id:  Task or item ID that was rendered.
        error:      Error message if :attr:`outcome` is ``FAILED``, else ``None``.
    """

    surface: RenderSurface
    outcome: RenderOutcome
    target_path: str
    block_id: str
    entity_id: str
    error: str | None = None


@dataclass(frozen=True, slots=True)
class RenderTarget:
    """A resolved vault path ready for writing.

    Attributes:
        path:    Absolute :class:`~pathlib.Path` of the target note.
        surface: Which surface this target belongs to.
        existed: Whether the note file already existed before the render.
    """

    path: Path
    surface: RenderSurface
    existed: bool
