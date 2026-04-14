"""Serialized note writer — the safe entry point for all vault writes.

All vault mutations from the render layer must go through this module.
The writer uses a simple in-process lock (:class:`threading.Lock`) to
prevent concurrent writes to the same note.  This matches the plan's
requirement that the note writer remain serialized.

For multi-process safety, an advisory file lock should be layered on top
(that belongs to lw-nss.10 runtime operations).  This module provides the
intra-process guarantee.

Usage::

    from pathlib import Path
    from lark_to_notes.render.writer import NoteWriter

    writer = NoteWriter(vault_root=Path("/path/to/vault"))
    results = writer.render_pipeline(item)
"""

from __future__ import annotations

import dataclasses
import logging
import threading
from pathlib import Path

from lark_to_notes.render.current_tasks import render_current_tasks_item
from lark_to_notes.render.daily import render_daily_note
from lark_to_notes.render.models import RenderItem, RenderOutcome, RenderResult, RenderSurface
from lark_to_notes.render.raw import render_raw_note

logger = logging.getLogger(__name__)


class NoteWriter:
    """Thread-safe facade for all vault-write operations.

    Attributes:
        vault_root: Absolute path to the vault root directory.
    """

    def __init__(self, vault_root: Path) -> None:
        self.vault_root = vault_root
        self._lock = threading.Lock()

    def render_raw(self, item: RenderItem) -> RenderResult:
        """Write or update the raw provenance note for *item*.

        Args:
            item: The item to render.

        Returns:
            A :class:`~lark_to_notes.render.models.RenderResult`.
        """
        with self._lock:
            result = render_raw_note(item, self.vault_root)
        logger.info(
            "note_writer_raw",
            extra={
                "outcome": result.outcome,
                "target_path": result.target_path,
                "entity_id": result.entity_id,
            },
        )
        return result

    def render_daily(self, item: RenderItem, *, date: str | None = None) -> RenderResult:
        """Write or update the daily-note entry for *item*.

        Args:
            item: The item to render.
            date: Override for the daily note date (``YYYY-MM-DD``).

        Returns:
            A :class:`~lark_to_notes.render.models.RenderResult`.
        """
        with self._lock:
            result = render_daily_note(item, self.vault_root, date=date)
        logger.info(
            "note_writer_daily",
            extra={
                "outcome": result.outcome,
                "target_path": result.target_path,
                "entity_id": result.entity_id,
            },
        )
        return result

    def render_current_tasks(self, item: RenderItem) -> RenderResult:
        """Write or update the Current Tasks entry for *item*.

        Only items with ``promotion_rec == "current_tasks"`` produce a
        non-skipped result.

        Args:
            item: The item to render.

        Returns:
            A :class:`~lark_to_notes.render.models.RenderResult`.
        """
        with self._lock:
            result = render_current_tasks_item(item, self.vault_root)
        logger.info(
            "note_writer_ct",
            extra={
                "outcome": result.outcome,
                "target_path": result.target_path,
                "entity_id": result.entity_id,
                "promotion_rec": item.promotion_rec,
            },
        )
        return result

    def render_pipeline(self, item: RenderItem) -> list[RenderResult]:
        """Run the full render pipeline for *item*.

        Stages:
        1. Raw provenance note (always)
        2. Daily note entry (always when a date is available)
        3. Current Tasks entry (only when ``promotion_rec == "current_tasks"``)

        Each stage runs under the write lock individually, yielding the lock
        between stages to prevent starvation.

        Args:
            item: The item to render through all applicable surfaces.

        Returns:
            One :class:`~lark_to_notes.render.models.RenderResult` per
            surface that was attempted.
        """
        results: list[RenderResult] = []

        # Stage 1: raw
        raw_result = self.render_raw(item)
        results.append(raw_result)

        # Propagate the raw note path for backlinks only when write succeeded
        if raw_result.target_path and raw_result.outcome != RenderOutcome.FAILED:
            item = dataclasses.replace(item, source_note_path=raw_result.target_path)

        # Stage 2: daily
        if item.event_date:
            daily_result = self.render_daily(item)
            results.append(daily_result)
            # Propagate daily note path for CT backlinks only when write succeeded
            if daily_result.target_path and daily_result.outcome != RenderOutcome.FAILED:
                item = dataclasses.replace(item, daily_note_path=daily_result.target_path)

        # Stage 3: current tasks (items with current_tasks promotion_rec only)
        if item.promotion_rec == "current_tasks":
            ct_result = self.render_current_tasks(item)
            results.append(ct_result)

        logger.debug(
            "render_pipeline_complete",
            extra={
                "entity_id": item.task_id,
                "stages": len(results),
                "outcomes": [r.outcome for r in results],
            },
        )
        return results

    def render_surface(self, item: RenderItem, surface: RenderSurface) -> RenderResult:
        """Render *item* onto a single *surface*.

        A convenience method for callers that need to write to a specific
        surface without running the full pipeline.

        Args:
            item:    The item to render.
            surface: Which surface to write to.

        Returns:
            A :class:`~lark_to_notes.render.models.RenderResult`.
        """
        if surface == RenderSurface.RAW:
            return self.render_raw(item)
        if surface == RenderSurface.DAILY:
            return self.render_daily(item)
        return self.render_current_tasks(item)
