"""Vault reaction machine blocks vs SQLite ledger drift (lw-pzj.13.3).

Detects ``vault_projection_fingerprint`` mismatches by replaying ordered
``message_reaction_events`` rows into
:func:`~lark_to_notes.intake.reaction_effective.materialize_effective_counts`
and recomputing
:func:`~lark_to_notes.render.reaction_vault.reaction_vault_projection_fingerprint`.

Optional repair rewrites only the machine-owned envelope via
:func:`~lark_to_notes.render.blocks.replace_block` using fresh inner Markdown
from :func:`~lark_to_notes.render.reaction_vault.format_reaction_summary_markdown`
(``wrap=False``).
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from lark_to_notes.intake.reaction_effective import materialize_effective_counts
from lark_to_notes.intake.reaction_model import ReactionKind
from lark_to_notes.render.blocks import extract_block, list_block_ids, replace_block
from lark_to_notes.render.reaction_vault import (
    format_reaction_summary_markdown,
    reaction_vault_projection_fingerprint,
)

if TYPE_CHECKING:
    import sqlite3

_RX_BLOCK_PREFIX = "ltn-rx-"

_SRC_RE = re.compile(r"^\s*-\s*\*\*source_id:\*\*\s*`([^`]+)`", re.MULTILINE)
_MID_RE = re.compile(r"^\s*-\s*\*\*message_id:\*\*\s*`([^`]+)`", re.MULTILINE)
_VAULT_FP_RE = re.compile(
    r"^\s*-\s*\*\*vault_projection_fingerprint:\*\*\s*`([0-9a-f]{64})`",
    re.MULTILINE,
)


def list_reaction_block_ids(note_text: str) -> list[str]:
    """Return machine-owned reaction block ids (``ltn-rx-…``) in document order."""

    return [b for b in list_block_ids(note_text) if b.startswith(_RX_BLOCK_PREFIX)]


def parse_reaction_block_identity(body: str) -> tuple[str, str] | None:
    """Parse ``(source_id, message_id)`` from a reaction block's Summary section."""

    ms = _SRC_RE.search(body)
    mm = _MID_RE.search(body)
    if ms is None or mm is None:
        return None
    return ms.group(1).strip(), mm.group(1).strip()


def parse_vault_projection_fingerprint(body: str) -> str | None:
    """Return digest from ``### Provenance`` or ``None`` when absent."""

    m = _VAULT_FP_RE.search(body)
    if m is None:
        return None
    return m.group(1).strip().lower()


def ledger_reaction_projection_bundle(
    conn: sqlite3.Connection,
    *,
    source_id: str,
    message_id: str,
) -> tuple[dict[tuple[str, str], int], str, str, str, str]:
    """Return counts + linkage fields derived from the SQLite ledger."""

    rows = conn.execute(
        """
        SELECT reaction_kind, emoji_type, operator_open_id, operator_user_id,
               operator_union_id, reaction_event_id, first_seen_at,
               governance_version, policy_version
        FROM message_reaction_events
        WHERE source_id = ? AND message_id = ?
        ORDER BY action_time ASC, reaction_event_id ASC
        """,
        (source_id, message_id),
    ).fetchall()
    steps: list[tuple[ReactionKind, str, str]] = []
    last_rid = ""
    last_seen = ""
    last_gv = ""
    last_pv = ""
    for row in rows:
        kind = ReactionKind(str(row["reaction_kind"]))
        emoji = str(row["emoji_type"] or "").strip()
        oo = str(row["operator_open_id"] or "").strip()
        ou = str(row["operator_user_id"] or "").strip()
        oj = str(row["operator_union_id"] or "").strip()
        op_key = oo or ou or oj
        steps.append((kind, emoji, op_key))
        last_rid = str(row["reaction_event_id"] or "")
        last_seen = str(row["first_seen_at"] or "")
        last_gv = str(row["governance_version"] or "")
        last_pv = str(row["policy_version"] or "")
    counts = materialize_effective_counts(steps)
    return counts, last_gv, last_pv, last_rid, last_seen


def analyze_reaction_vault_block(
    conn: sqlite3.Connection,
    *,
    note_text: str,
    block_id: str,
) -> dict[str, object]:
    """Compare one reaction machine block to the ledger for ``(source_id, message_id)``."""

    body = extract_block(note_text, block_id)
    if body is None:
        return {
            "block_id": block_id,
            "drift": True,
            "reasons": ["missing_machine_block"],
            "vault_projection_fingerprint": None,
            "ledger_projection_fingerprint": None,
            "source_id": None,
            "message_id": None,
        }

    ident = parse_reaction_block_identity(body)
    if ident is None:
        return {
            "block_id": block_id,
            "drift": True,
            "reasons": ["unparseable_block_identity"],
            "vault_projection_fingerprint": parse_vault_projection_fingerprint(body),
            "ledger_projection_fingerprint": None,
            "source_id": None,
            "message_id": None,
        }

    sid, mid = ident
    vault_fp = parse_vault_projection_fingerprint(body)
    if vault_fp is None:
        return {
            "block_id": block_id,
            "drift": True,
            "reasons": ["missing_vault_projection_fingerprint"],
            "vault_projection_fingerprint": None,
            "ledger_projection_fingerprint": None,
            "source_id": sid,
            "message_id": mid,
        }

    counts, gv, pv, last_rid, last_seen = ledger_reaction_projection_bundle(
        conn,
        source_id=sid,
        message_id=mid,
    )
    ledger_fp = reaction_vault_projection_fingerprint(
        source_id=sid,
        message_id=mid,
        effective_counts=counts,
        governance_version=gv,
        policy_version=pv,
        last_ledger_event_id=last_rid,
        last_ingested_at=last_seen,
    ).lower()
    drift = vault_fp != ledger_fp
    reasons: list[str] = []
    if drift:
        reasons.append("vault_projection_fingerprint_mismatch")
    return {
        "block_id": block_id,
        "drift": drift,
        "reasons": reasons,
        "vault_projection_fingerprint": vault_fp,
        "ledger_projection_fingerprint": ledger_fp,
        "source_id": sid,
        "message_id": mid,
    }


def scan_vault_note_reaction_drift(
    conn: sqlite3.Connection,
    *,
    note_text: str,
) -> dict[str, Any]:
    """Scan all ``ltn-rx-`` blocks in *note_text*."""

    blocks: list[dict[str, Any]] = []
    for bid in list_reaction_block_ids(note_text):
        blocks.append(analyze_reaction_vault_block(conn, note_text=note_text, block_id=bid))
    drift_count = sum(1 for b in blocks if bool(b.get("drift")))
    return {"blocks": blocks, "drift_count": drift_count, "block_count": len(blocks)}


def repair_vault_note_reaction_blocks(
    conn: sqlite3.Connection,
    *,
    note_text: str,
) -> tuple[str, int]:
    """Rewrite drifted reaction blocks from the ledger; return ``(new_text, repaired)``."""

    current = note_text
    repaired = 0
    for bid in list_reaction_block_ids(current):
        info = analyze_reaction_vault_block(conn, note_text=current, block_id=bid)
        if not info.get("drift"):
            continue
        sid = info.get("source_id")
        mid = info.get("message_id")
        if not isinstance(sid, str) or not isinstance(mid, str):
            continue
        counts, gv, pv, last_rid, last_seen = ledger_reaction_projection_bundle(
            conn,
            source_id=sid,
            message_id=mid,
        )
        inner = format_reaction_summary_markdown(
            source_id=sid,
            message_id=mid,
            effective_counts=counts,
            governance_version=gv,
            policy_version=pv,
            last_ledger_event_id=last_rid,
            last_ingested_at=last_seen,
            wrap=False,
        )
        current = replace_block(current, bid, inner)
        repaired += 1
    return current, repaired
