"""Daily-note renderer.

Appends or updates a machine-owned task bullet in the relevant daily note
at ``daily/YYYY-MM-DD.md``.  The note is created with minimal frontmatter
if it does not exist.

Each task gets one bullet inside a machine-owned block.  The block is keyed
by the task fingerprint so repeated renders update the same bullet rather
than appending duplicates.

Section structure inside the daily note::

    ## Work items (ltn-managed)

    <!-- ltn:begin id="ltn-daily-<fingerprint>" -->
    - [ ] <title> `#<reason_code>` [[<raw_note_name>]]
    <!-- ltn:end id="ltn-daily-<fingerprint>" -->

"""

from __future__ import annotations

import logging
from pathlib import Path

from lark_to_notes.render.blocks import MalformedBlockError, replace_block, wrap_block
from lark_to_notes.render.models import (
    RenderItem,
    RenderOutcome,
    RenderResult,
    RenderSurface,
)

logger = logging.getLogger(__name__)

_DAILY_SECTION_HEADER = "## Work items (ltn-managed)\n"
_DAILY_BLOCK_ID_PREFIX = "ltn-daily-"

# Status checkbox by task status
_STATUS_CHECKBOX: dict[str, str] = {
    "open": "[ ]",
    "needs_review": "[?]",
    "snoozed": "[~]",
    "completed": "[x]",
    "dismissed": "[-]",
    "merged": "[>]",
    "superseded": "[>]",
}


def _daily_block_id(item: RenderItem) -> str:
    return f"{_DAILY_BLOCK_ID_PREFIX}{item.fingerprint}"


def _daily_frontmatter(date: str) -> str:
    return f"---\ntype: daily\ndate: {date}\ntags:\n  - daily\n---\n"


def _bullet_content(item: RenderItem) -> str:
    """Build the single-line bullet for this item."""
    checkbox = _STATUS_CHECKBOX.get(item.status, "[ ]")
    title = item.title

    parts = [f"- {checkbox} {title}"]

    if item.reason_code:
        parts.append(f"`#{item.reason_code}`")

    if item.source_note_path:
        note_stem = Path(item.source_note_path).stem
        parts.append(f"[[{note_stem}]]")

    if item.assignee_refs:
        parts.append("→ " + ", ".join(item.assignee_refs))

    if item.due_at:
        parts.append(f"📅 {item.due_at}")

    return " ".join(parts)


def render_daily_note(
    item: RenderItem,
    vault_root: Path,
    *,
    date: str | None = None,
) -> RenderResult:
    """Create or update the daily note entry for *item*.

    Args:
        item:       The item to render.
        vault_root: Absolute path to the vault root directory.
        date:       The date string ``YYYY-MM-DD`` for the daily note.
                    Defaults to :attr:`~RenderItem.event_date`.

    Returns:
        A :class:`~lark_to_notes.render.models.RenderResult` describing
        the outcome.
    """
    effective_date = date or item.event_date
    if not effective_date:
        return RenderResult(
            surface=RenderSurface.DAILY,
            outcome=RenderOutcome.FAILED,
            target_path="",
            block_id="",
            entity_id=item.task_id,
            error="no date available for daily note",
        )

    daily_dir = vault_root / "daily"
    daily_dir.mkdir(parents=True, exist_ok=True)

    note_path = daily_dir / f"{effective_date}.md"
    block_id = _daily_block_id(item)

    try:
        if note_path.exists():
            existing = note_path.read_text(encoding="utf-8")
            # Ensure the section header exists before the block
            if _DAILY_SECTION_HEADER not in existing:
                existing = existing.rstrip("\n") + "\n\n" + _DAILY_SECTION_HEADER
            updated = replace_block(existing, block_id, _bullet_content(item))
            if updated == existing:
                result = RenderResult(
                    surface=RenderSurface.DAILY,
                    outcome=RenderOutcome.SKIPPED,
                    target_path=str(note_path),
                    block_id=block_id,
                    entity_id=item.task_id,
                )
                logger.debug(
                    "render_daily_note",
                    extra={
                        "target_path": str(note_path),
                        "block_id": block_id,
                        "entity_id": item.task_id,
                        "date": effective_date,
                        "outcome": result.outcome,
                    },
                )
                return result
            note_path.write_text(updated, encoding="utf-8")
            outcome = RenderOutcome.UPDATED
        else:
            # New daily note: frontmatter + section + first block
            initial = (
                _daily_frontmatter(effective_date)
                + "\n"
                + _DAILY_SECTION_HEADER
                + "\n"
                + wrap_block(block_id, _bullet_content(item))
            )
            note_path.write_text(initial, encoding="utf-8")
            outcome = RenderOutcome.CREATED

    except MalformedBlockError as exc:
        logger.warning(
            "render_daily_note_malformed_block",
            extra={"block_id": block_id, "entity_id": item.task_id, "detail": exc.detail},
        )
        return RenderResult(
            surface=RenderSurface.DAILY,
            outcome=RenderOutcome.FAILED,
            target_path=str(note_path),
            block_id=block_id,
            entity_id=item.task_id,
            error=str(exc),
        )
    except OSError as exc:
        logger.exception(
            "render_daily_note_io_error",
            extra={"target_path": str(note_path), "entity_id": item.task_id},
        )
        return RenderResult(
            surface=RenderSurface.DAILY,
            outcome=RenderOutcome.FAILED,
            target_path=str(note_path),
            block_id=block_id,
            entity_id=item.task_id,
            error=str(exc),
        )

    result = RenderResult(
        surface=RenderSurface.DAILY,
        outcome=outcome,
        target_path=str(note_path),
        block_id=block_id,
        entity_id=item.task_id,
    )
    logger.debug(
        "render_daily_note",
        extra={
            "target_path": str(note_path),
            "block_id": block_id,
            "entity_id": item.task_id,
            "date": effective_date,
            "outcome": result.outcome,
        },
    )
    return result
