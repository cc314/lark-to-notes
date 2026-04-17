"""Machine-owned **IM reaction summary** blocks for vault ``raw/`` notes.

Stable block IDs and deterministic table row ordering support golden-file
tests and idempotent rerenders (plan: Message reaction ingestion, vault projection).
"""

from __future__ import annotations

import hashlib

from lark_to_notes.render.blocks import wrap_block

_RX_BLOCK_PREFIX = "ltn-rx-"


def reaction_block_id(source_id: str, message_id: str) -> str:
    """Return stable machine block id for ``(source_id, message_id)``."""

    key = f"{source_id.strip()}\n{message_id.strip()}".encode()
    digest = hashlib.sha256(key).hexdigest()[:16]
    return f"{_RX_BLOCK_PREFIX}{digest}"


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
        "## IM reactions",
        "",
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
    lines.append("")
    body = "\n".join(lines)
    return wrap_block(block_id, body)
