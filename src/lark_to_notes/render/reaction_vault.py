"""Machine-owned **IM reaction** blocks for vault ``raw/`` notes.

Normative block schema (lw-pzj.7.1)
-----------------------------------

Each per-message reaction projection is one machine-owned HTML-comment
envelope (see :mod:`lark_to_notes.render.blocks`) whose ``block_id`` is
:func:`reaction_block_id`.

Inside the envelope, Markdown is structured for Obsidian back-links and for
downstream distill evidence (lw-pzj.8.*):

1. **Primary heading** — ``## IM reactions ^{block_id}`` so operators and
   distillers can link to a **stable block reference** that matches the
   ``ltn:begin`` / ``ltn:end`` id.
2. **``### Summary``** — identity fields, optional governance tuple, and the
   effective-count table (sorted keys, deterministic rows).
3. **``### Timeline``** — ordered reaction events (append-only ledger
   projection). When no rows are projected yet, a single italic placeholder
   line keeps the heading stable for diffs.

Schema version is recorded in-summary as ``vault_reaction_schema_version``;
bump it only when this structure changes incompatibly.
"""

from __future__ import annotations

import hashlib

from lark_to_notes.render.blocks import wrap_block

_RX_BLOCK_PREFIX = "ltn-rx-"
VAULT_REACTION_SCHEMA_VERSION = "1"


def reaction_block_id(source_id: str, message_id: str) -> str:
    """Return stable machine block id for ``(source_id, message_id)``."""

    key = f"{source_id.strip()}\n{message_id.strip()}".encode()
    digest = hashlib.sha256(key).hexdigest()[:16]
    return f"{_RX_BLOCK_PREFIX}{digest}"


def reaction_primary_heading(block_id: str) -> str:
    """Return the H2 line with Obsidian block id (back-link target)."""

    return f"## IM reactions ^{block_id}"


def format_reaction_summary_markdown(
    *,
    source_id: str,
    message_id: str,
    effective_counts: dict[tuple[str, str], int],
    governance_version: str = "",
    policy_version: str = "",
) -> str:
    """Format a wrapped machine block summarizing effective reaction counts.

    *effective_counts* maps ``(emoji_type, operator_key)`` to a non-negative
    integer count (see :mod:`lark_to_notes.intake.reaction_effective`).
    """

    block_id = reaction_block_id(source_id, message_id)
    lines: list[str] = [
        reaction_primary_heading(block_id),
        "",
        "### Summary",
        "",
        f"- **vault_reaction_schema_version:** `{VAULT_REACTION_SCHEMA_VERSION}`",
        f"- **source_id:** `{source_id}`",
        f"- **message_id:** `{message_id}`",
        "",
    ]
    gv = governance_version.strip()
    pv = policy_version.strip()
    if gv or pv:
        lines.append(f"- **governance_version:** `{gv}`")
        lines.append(f"- **policy_version:** `{pv}`")
        lines.append("")

    lines.extend(
        [
            "| emoji_type | operator | count |",
            "| --- | --- | ---:|",
        ]
    )
    for (emoji, op), cnt in sorted(effective_counts.items(), key=lambda x: (x[0][0], x[0][1])):
        lines.append(f"| `{emoji}` | `{op}` | {int(cnt)} |")
    lines.extend(
        [
            "",
            "### Timeline",
            "",
            "_No ledger rows projected in this render._",
            "",
        ]
    )
    body = "\n".join(lines)
    return wrap_block(block_id, body)
