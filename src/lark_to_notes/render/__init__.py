"""Vault-safe note rendering and promotion flow."""

from __future__ import annotations

from lark_to_notes.render.blocks import (
    MalformedBlockError,
    extract_block,
    list_block_ids,
    make_begin_marker,
    make_end_marker,
    replace_block,
    wrap_block,
)
from lark_to_notes.render.current_tasks import (
    remove_demoted_blocks,
    render_current_tasks,
    render_current_tasks_item,
)
from lark_to_notes.render.daily import render_daily_note
from lark_to_notes.render.models import (
    RenderItem,
    RenderOutcome,
    RenderResult,
    RenderSurface,
    RenderTarget,
)
from lark_to_notes.render.raw import render_raw_note
from lark_to_notes.render.reaction_vault import (
    VAULT_REACTION_SCHEMA_VERSION,
    format_reaction_summary_markdown,
    reaction_block_id,
    reaction_primary_heading,
)
from lark_to_notes.render.writer import NoteWriter

__all__ = [
    "VAULT_REACTION_SCHEMA_VERSION",
    "MalformedBlockError",
    "NoteWriter",
    "RenderItem",
    "RenderOutcome",
    "RenderResult",
    "RenderSurface",
    "RenderTarget",
    "extract_block",
    "format_reaction_summary_markdown",
    "list_block_ids",
    "make_begin_marker",
    "make_end_marker",
    "reaction_block_id",
    "reaction_primary_heading",
    "remove_demoted_blocks",
    "render_current_tasks",
    "render_current_tasks_item",
    "render_daily_note",
    "render_raw_note",
    "replace_block",
    "wrap_block",
]
