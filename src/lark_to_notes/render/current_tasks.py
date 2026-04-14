"""Current-Tasks renderer.

Maintains the durable open-work list at ``area/current tasks/index.md``.
Only tasks with ``promotion_rec == "current_tasks"`` and a non-terminal
status are included.  Each task occupies one machine-owned block so
re-renders update in place without touching user text outside the blocks.

Section structure::

    ## Open tasks (ltn-managed)

    <!-- ltn:begin id="ltn-ct-<fingerprint>" -->
    - [ ] <title> `#<reason_code>` [[<raw_note_name>]] [[<daily_note_name>]]
    <!-- ltn:end id="ltn-ct-<fingerprint>" -->

Demotion: when a task is completed, dismissed, merged, or superseded, its
block content is replaced with a struck-through entry so the history remains
but the item is visually de-emphasised.  Blocks for superseded items are
removed on the *next* full render pass when :func:`render_current_tasks` is
called with the full item list.
"""

from __future__ import annotations

import logging
from pathlib import Path

from lark_to_notes.render.blocks import (
    MalformedBlockError,
    list_block_ids,
    make_begin_marker,
    make_end_marker,
    replace_block,
    wrap_block,
)
from lark_to_notes.render.models import (
    RenderItem,
    RenderOutcome,
    RenderResult,
    RenderSurface,
)

logger = logging.getLogger(__name__)

_CT_PATH = Path("area") / "current tasks" / "index.md"
_CT_SECTION_HEADER = "## Open tasks (ltn-managed)\n"
_CT_BLOCK_ID_PREFIX = "ltn-ct-"

_TERMINAL_STATUSES = frozenset({"completed", "dismissed", "merged", "superseded"})

# Checkboxes shared with daily renderer
_STATUS_CHECKBOX: dict[str, str] = {
    "open": "[ ]",
    "needs_review": "[?]",
    "snoozed": "[~]",
    "completed": "[x]",
    "dismissed": "[-]",
    "merged": "[>]",
    "superseded": "[>]",
}


def _ct_block_id(item: RenderItem) -> str:
    return f"{_CT_BLOCK_ID_PREFIX}{item.fingerprint}"


def _ct_frontmatter() -> str:
    return (
        "---\n"
        "type: area\n"
        "tags:\n"
        "  - current-tasks\n"
        "  - ltn-managed\n"
        "---\n"
    )


def _bullet_content(item: RenderItem) -> str:
    """Build the task bullet for the Current Tasks list."""
    checkbox = _STATUS_CHECKBOX.get(item.status, "[ ]")

    # Struck-through for terminal items
    title = f"~~{item.title}~~" if item.status in _TERMINAL_STATUSES else item.title

    parts = [f"- {checkbox} {title}"]

    if item.reason_code:
        parts.append(f"`#{item.reason_code}`")

    if item.source_note_path:
        note_stem = Path(item.source_note_path).stem
        parts.append(f"[[{note_stem}]]")

    if item.daily_note_path:
        daily_stem = Path(item.daily_note_path).stem
        parts.append(f"[[{daily_stem}]]")

    if item.assignee_refs:
        parts.append("→ " + ", ".join(item.assignee_refs))

    if item.due_at:
        parts.append(f"📅 {item.due_at}")

    return " ".join(parts)


def render_current_tasks_item(
    item: RenderItem,
    vault_root: Path,
) -> RenderResult:
    """Create or update one item's block in the Current Tasks note.

    This function is safe to call repeatedly; it updates only the
    machine-owned block for *item* and preserves all surrounding text.

    Args:
        item:       The item to render.
        vault_root: Absolute path to the vault root directory.

    Returns:
        A :class:`~lark_to_notes.render.models.RenderResult`.
    """
    ct_path = vault_root / _CT_PATH
    ct_path.parent.mkdir(parents=True, exist_ok=True)

    block_id = _ct_block_id(item)

    logger.debug(
        "render_current_tasks_item",
        extra={
            "target_path": str(ct_path),
            "block_id": block_id,
            "entity_id": item.task_id,
            "status": item.status,
            "promotion_rec": item.promotion_rec,
        },
    )

    try:
        if ct_path.exists():
            existing = ct_path.read_text(encoding="utf-8")
            if _CT_SECTION_HEADER not in existing:
                existing = existing.rstrip("\n") + "\n\n" + _CT_SECTION_HEADER
            updated = replace_block(existing, block_id, _bullet_content(item))
            if updated == existing:
                return RenderResult(
                    surface=RenderSurface.CURRENT_TASKS,
                    outcome=RenderOutcome.SKIPPED,
                    target_path=str(ct_path),
                    block_id=block_id,
                    entity_id=item.task_id,
                )
            ct_path.write_text(updated, encoding="utf-8")
            outcome = RenderOutcome.UPDATED
        else:
            initial = (
                _ct_frontmatter()
                + "\n# Current Tasks\n\n"
                + _CT_SECTION_HEADER
                + "\n"
                + wrap_block(block_id, _bullet_content(item))
            )
            ct_path.write_text(initial, encoding="utf-8")
            outcome = RenderOutcome.CREATED

    except MalformedBlockError as exc:
        logger.warning(
            "render_ct_malformed_block",
            extra={"block_id": block_id, "entity_id": item.task_id, "detail": exc.detail},
        )
        return RenderResult(
            surface=RenderSurface.CURRENT_TASKS,
            outcome=RenderOutcome.FAILED,
            target_path=str(ct_path),
            block_id=block_id,
            entity_id=item.task_id,
            error=str(exc),
        )
    except OSError as exc:
        logger.exception(
            "render_ct_io_error",
            extra={"target_path": str(ct_path), "entity_id": item.task_id},
        )
        return RenderResult(
            surface=RenderSurface.CURRENT_TASKS,
            outcome=RenderOutcome.FAILED,
            target_path=str(ct_path),
            block_id=block_id,
            entity_id=item.task_id,
            error=str(exc),
        )

    return RenderResult(
        surface=RenderSurface.CURRENT_TASKS,
        outcome=outcome,
        target_path=str(ct_path),
        block_id=block_id,
        entity_id=item.task_id,
    )


def render_current_tasks(
    items: list[RenderItem],
    vault_root: Path,
) -> list[RenderResult]:
    """Render all *items* into the Current Tasks note as a batch.

    This is the primary entry point for full-list renders.  Only items
    with ``promotion_rec == "current_tasks"`` are written; others are
    skipped (their blocks are left in place, which is safe — they can be
    cleaned up by calling :func:`remove_demoted_blocks` separately).

    Args:
        items:      Full list of items to consider.
        vault_root: Absolute path to the vault root directory.

    Returns:
        One :class:`~lark_to_notes.render.models.RenderResult` per item.
    """
    results: list[RenderResult] = []
    for item in items:
        if item.promotion_rec != "current_tasks":
            results.append(
                RenderResult(
                    surface=RenderSurface.CURRENT_TASKS,
                    outcome=RenderOutcome.SKIPPED,
                    target_path=str(vault_root / _CT_PATH),
                    block_id=_ct_block_id(item),
                    entity_id=item.task_id,
                )
            )
            continue
        results.append(render_current_tasks_item(item, vault_root))
    return results


def remove_demoted_blocks(
    keep_fingerprints: set[str],
    vault_root: Path,
) -> list[str]:
    """Remove machine-owned blocks for fingerprints no longer in *keep_fingerprints*.

    Scans the Current Tasks note and removes any ``ltn-ct-<fingerprint>``
    block whose fingerprint is not in *keep_fingerprints*.

    Args:
        keep_fingerprints: Set of fingerprint strings that should be kept.
        vault_root:        Absolute path to the vault root directory.

    Returns:
        List of block IDs that were removed.
    """
    ct_path = vault_root / _CT_PATH
    if not ct_path.exists():
        return []

    text = ct_path.read_text(encoding="utf-8")
    block_ids = list_block_ids(text)
    removed: list[str] = []

    for block_id in block_ids:
        if not block_id.startswith(_CT_BLOCK_ID_PREFIX):
            continue
        fingerprint = block_id[len(_CT_BLOCK_ID_PREFIX):]
        if fingerprint in keep_fingerprints:
            continue
        # Remove the block entirely by replacing with empty content and then
        # stripping the resulting empty wrapped block.
        text = _remove_single_block(text, block_id)
        removed.append(block_id)
        logger.debug(
            "removed_demoted_block",
            extra={"block_id": block_id, "fingerprint": fingerprint},
        )

    if removed:
        ct_path.write_text(text, encoding="utf-8")

    return removed


def _remove_single_block(text: str, block_id: str) -> str:
    """Remove a complete machine-owned block (markers + content) from *text*."""
    begin_marker = make_begin_marker(block_id)
    end_marker = make_end_marker(block_id)

    begin_pos = text.find(begin_marker)
    end_pos = text.find(end_marker)

    if begin_pos == -1 or end_pos == -1 or end_pos < begin_pos:
        return text

    # Consume any preceding blank line so removal doesn't leave double blanks
    prefix = text[:begin_pos]
    if prefix.endswith("\n\n"):
        prefix = prefix[:-1]

    end_of_block = end_pos + len(end_marker)
    suffix = text[end_of_block:]
    return prefix + suffix
