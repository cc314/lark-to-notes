"""Machine-owned **IM reaction** blocks for vault ``raw/`` notes.

Normative block schema (lw-pzj.7.1, lw-pzj.7.3)
-----------------------------------------------

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
3. **``### Provenance``** — ``effective_reaction_set_fingerprint`` (ledger
   materialization), ``vault_projection_fingerprint`` (hash of schema + ids +
   governance + effective fingerprint + optional durable event pointers), and
   optional ``last_ledger_event_id`` / ``last_ingested_at`` lines for replay
   traceability. Missing pointers use a fixed italic placeholder so rerenders
   stay byte-stable when SQLite has not supplied linkage yet.
4. **``### Timeline``** — ordered reaction events (append-only ledger
   projection). When no rows are projected yet, a single italic placeholder
   line keeps the heading stable for diffs.

Schema version is recorded in-summary as ``vault_reaction_schema_version``;
bump it only when this structure changes incompatibly.
"""

from __future__ import annotations

import hashlib
import json

from lark_to_notes.intake.reaction_effective import effective_reaction_set_fingerprint
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


def reaction_vault_projection_fingerprint(
    *,
    source_id: str,
    message_id: str,
    effective_counts: dict[tuple[str, str], int],
    governance_version: str = "",
    policy_version: str = "",
    last_ledger_event_id: str = "",
    last_ingested_at: str = "",
) -> str:
    """Return SHA-256 hex digest of canonical projection inputs (lw-pzj.7.3).

    Same logical ledger snapshot and linkage metadata always yield the same
    digest, independent of ``dict`` insertion order for *effective_counts*.
    """

    eff_fp = effective_reaction_set_fingerprint(effective_counts)
    payload = json.dumps(
        {
            "effective_reaction_set_fingerprint": eff_fp,
            "governance_version": governance_version.strip(),
            "last_ingested_at": last_ingested_at.strip(),
            "last_ledger_event_id": last_ledger_event_id.strip(),
            "message_id": message_id,
            "policy_version": policy_version.strip(),
            "source_id": source_id,
            "vault_reaction_schema_version": VAULT_REACTION_SCHEMA_VERSION,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def format_reaction_summary_markdown(
    *,
    source_id: str,
    message_id: str,
    effective_counts: dict[tuple[str, str], int],
    governance_version: str = "",
    policy_version: str = "",
    last_ledger_event_id: str = "",
    last_ingested_at: str = "",
) -> str:
    """Format a wrapped machine block summarizing effective reaction counts.

    *effective_counts* maps ``(emoji_type, operator_key)`` to a non-negative
    integer count (see :mod:`lark_to_notes.intake.reaction_effective`).
    """

    block_id = reaction_block_id(source_id, message_id)
    gv = governance_version.strip()
    pv = policy_version.strip()
    le = last_ledger_event_id.strip()
    li = last_ingested_at.strip()
    eff_fp = effective_reaction_set_fingerprint(effective_counts)
    proj_fp = reaction_vault_projection_fingerprint(
        source_id=source_id,
        message_id=message_id,
        effective_counts=effective_counts,
        governance_version=governance_version,
        policy_version=policy_version,
        last_ledger_event_id=last_ledger_event_id,
        last_ingested_at=last_ingested_at,
    )
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
            "### Provenance",
            "",
            f"- **effective_reaction_set_fingerprint:** `{eff_fp}`",
            f"- **vault_projection_fingerprint:** `{proj_fp}`",
        ]
    )
    if le:
        lines.append(f"- **last_ledger_event_id:** `{le}`")
    else:
        lines.append("- **last_ledger_event_id:** _Not linked in this render._")
    if li:
        lines.append(f"- **last_ingested_at:** `{li}`")
    else:
        lines.append("- **last_ingested_at:** _Not linked in this render._")
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
