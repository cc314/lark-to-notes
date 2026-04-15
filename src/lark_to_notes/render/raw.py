"""Raw-note renderer.

Produces provenance notes under ``raw/`` for each ingested source item.
These notes are never overwritten on rerender; instead the existing block
content is updated while user-authored prose around the block is preserved.

Each raw note contains:

* YAML frontmatter (``type``, ``source``, ``author``, ``published``, ``tags``)
* A machine-owned block with a summary excerpt and backlink to the daily note
* Optionally, the full message payload as a fenced code block

The note path follows the pattern::

    raw/<YYYY-MM-DD>-<short_slug>.md
"""

from __future__ import annotations

import logging
import re
import textwrap
from pathlib import Path

from lark_to_notes.render.blocks import MalformedBlockError, replace_block, wrap_block
from lark_to_notes.render.models import (
    RenderItem,
    RenderOutcome,
    RenderResult,
    RenderSurface,
)

logger = logging.getLogger(__name__)

# Block ID used in each raw note for the machine-managed summary section
_RAW_BLOCK_ID_PREFIX = "ltn-raw-"


def _slugify(text: str, max_len: int = 40) -> str:
    """Turn *text* into a short URL-safe slug."""
    slug = re.sub(r"[^\w\s-]", "", text.lower())
    slug = re.sub(r"[\s_-]+", "-", slug).strip("-")
    return slug[:max_len]


def _raw_block_id(item: RenderItem) -> str:
    return f"{_RAW_BLOCK_ID_PREFIX}{item.fingerprint}"


def _frontmatter(item: RenderItem) -> str:
    """Build YAML frontmatter for the raw note."""
    tags = ["raw", "ltn-generated"]
    if item.confidence_band:
        tags.append(f"confidence-{item.confidence_band}")
    tags_yaml = "\n".join(f"  - {t}" for t in tags)
    published = item.event_date or ""
    return f"---\ntype: raw\nsource: lark\npublished: {published}\ntags:\n{tags_yaml}\n---\n"


def _body_block(item: RenderItem) -> str:
    """Build the machine-managed block content for the raw note."""
    lines: list[str] = []

    # Heading
    lines.append(f"## {item.title}\n")

    if item.summary:
        lines.append(textwrap.fill(item.summary, width=100))
        lines.append("")

    # Metadata table
    lines.append("| field | value |")
    lines.append("| ----- | ----- |")
    lines.append(f"| task_id | `{item.task_id}` |")
    lines.append(f"| fingerprint | `{item.fingerprint}` |")
    lines.append(f"| confidence | {item.confidence_band} |")
    lines.append(f"| reason | `{item.reason_code}` |")
    lines.append(f"| promotion | {item.promotion_rec} |")
    if item.assignee_refs:
        lines.append(f"| assignees | {', '.join(item.assignee_refs)} |")
    if item.due_at:
        lines.append(f"| due | {item.due_at} |")
    if item.source_message_id:
        lines.append(f"| source_message_id | `{item.source_message_id}` |")
    lines.append("")

    # Backlink to daily note
    if item.daily_note_path:
        # Convert absolute or vault-relative path to an Obsidian wikilink
        note_name = Path(item.daily_note_path).stem
        lines.append(f"**Daily note:** [[{note_name}]]\n")

    return "\n".join(lines)


def render_raw_note(
    item: RenderItem,
    vault_root: Path,
) -> RenderResult:
    """Create or update the raw provenance note for *item*.

    Args:
        item:       The item to render.
        vault_root: Absolute path to the vault root directory.

    Returns:
        A :class:`~lark_to_notes.render.models.RenderResult` describing
        the outcome.
    """
    raw_dir = vault_root / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    date_prefix = item.event_date or "unknown"
    slug = _slugify(item.title)
    note_name = f"{date_prefix}-{slug}.md" if slug else f"{date_prefix}-{item.fingerprint}.md"
    note_path = raw_dir / note_name
    block_id = _raw_block_id(item)

    try:
        if note_path.exists():
            existing = note_path.read_text(encoding="utf-8")
            updated = replace_block(existing, block_id, _body_block(item))
            if updated == existing:
                result = RenderResult(
                    surface=RenderSurface.RAW,
                    outcome=RenderOutcome.SKIPPED,
                    target_path=str(note_path),
                    block_id=block_id,
                    entity_id=item.task_id,
                )
                logger.debug(
                    "render_raw_note",
                    extra={
                        "target_path": str(note_path),
                        "block_id": block_id,
                        "entity_id": item.task_id,
                        "surface": RenderSurface.RAW,
                        "outcome": result.outcome,
                    },
                )
                return result
            note_path.write_text(updated, encoding="utf-8")
            outcome = RenderOutcome.UPDATED
        else:
            # New file: frontmatter + initial wrapped block
            content = _frontmatter(item) + "\n" + wrap_block(block_id, _body_block(item))
            note_path.write_text(content, encoding="utf-8")
            outcome = RenderOutcome.CREATED
    except MalformedBlockError as exc:
        logger.warning(
            "render_raw_note_malformed_block",
            extra={"block_id": block_id, "entity_id": item.task_id, "detail": exc.detail},
        )
        return RenderResult(
            surface=RenderSurface.RAW,
            outcome=RenderOutcome.FAILED,
            target_path=str(note_path),
            block_id=block_id,
            entity_id=item.task_id,
            error=str(exc),
        )
    except OSError as exc:
        logger.exception(
            "render_raw_note_io_error",
            extra={"target_path": str(note_path), "entity_id": item.task_id},
        )
        return RenderResult(
            surface=RenderSurface.RAW,
            outcome=RenderOutcome.FAILED,
            target_path=str(note_path),
            block_id=block_id,
            entity_id=item.task_id,
            error=str(exc),
        )

    result = RenderResult(
        surface=RenderSurface.RAW,
        outcome=outcome,
        target_path=str(note_path),
        block_id=block_id,
        entity_id=item.task_id,
    )
    logger.debug(
        "render_raw_note",
        extra={
            "target_path": str(note_path),
            "block_id": block_id,
            "entity_id": item.task_id,
            "surface": RenderSurface.RAW,
            "outcome": result.outcome,
        },
    )
    return result
