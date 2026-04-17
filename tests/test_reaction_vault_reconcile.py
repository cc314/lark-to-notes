"""Tests for :mod:`lark_to_notes.render.reaction_vault_reconcile` (lw-pzj.13.3)."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, cast

import pytest

from lark_to_notes.cli import run
from lark_to_notes.intake.reaction_model import NormalizedReactionEvent, ReactionKind
from lark_to_notes.intake.reaction_store import insert_message_reaction_event
from lark_to_notes.render.reaction_vault import format_reaction_summary_markdown, reaction_block_id
from lark_to_notes.render.reaction_vault_reconcile import (
    repair_vault_note_reaction_blocks,
    scan_vault_note_reaction_drift,
)
from lark_to_notes.storage.db import connect, init_db


def _ev(*, eid: str, message_id: str = "om_1") -> NormalizedReactionEvent:
    return NormalizedReactionEvent(
        reaction_event_id=eid,
        source_id="dm:test",
        message_id=message_id,
        reaction_kind=ReactionKind.ADD,
        emoji_type="OK",
        operator_type="user",
        operator_open_id="ou_a",
        operator_user_id="",
        operator_union_id="",
        action_time="2026-01-01T00:00:00Z",
        payload={"header": {"event_id": eid}, "event": {"message_id": message_id}},
    )


@pytest.fixture
def mem() -> sqlite3.Connection:
    c = connect(":memory:")
    init_db(c)
    return c


def test_scan_reports_no_drift_when_vault_matches_ledger(mem: sqlite3.Connection) -> None:
    insert_message_reaction_event(
        mem, _ev(eid="rx-vr-1"), governance_version="g", policy_version="p"
    )
    row = mem.execute(
        "SELECT reaction_event_id, first_seen_at FROM message_reaction_events WHERE message_id = ?",
        ("om_1",),
    ).fetchone()
    assert row is not None
    rid = str(row["reaction_event_id"])
    seen = str(row["first_seen_at"])
    block = format_reaction_summary_markdown(
        source_id="dm:test",
        message_id="om_1",
        effective_counts={("OK", "ou_a"): 1},
        governance_version="g",
        policy_version="p",
        last_ledger_event_id=rid,
        last_ingested_at=seen,
    )
    note = f"Context\n\n{block}"
    rep = scan_vault_note_reaction_drift(mem, note_text=note)
    assert rep["drift_count"] == 0
    assert rep["block_count"] == 1
    blocks = cast(list[dict[str, Any]], rep["blocks"])
    assert blocks[0]["drift"] is False


def _tamper_vault_projection_line(block: str) -> str:
    lines = block.splitlines()
    out_lines: list[str] = []
    for line in lines:
        if "**vault_projection_fingerprint:**" in line and "`" in line:
            prefix, _, rest = line.partition("`")
            _digest, _, suffix = rest.partition("`")
            out_lines.append(prefix + "`" + "0" * 64 + "`" + suffix)
        else:
            out_lines.append(line)
    return "\n".join(out_lines)


def test_scan_detects_tampered_vault_projection_fingerprint(mem: sqlite3.Connection) -> None:
    insert_message_reaction_event(
        mem, _ev(eid="rx-vr-2"), governance_version="g", policy_version="p"
    )
    row = mem.execute(
        "SELECT reaction_event_id, first_seen_at FROM message_reaction_events WHERE message_id = ?",
        ("om_1",),
    ).fetchone()
    assert row is not None
    rid = str(row["reaction_event_id"])
    seen = str(row["first_seen_at"])
    block = format_reaction_summary_markdown(
        source_id="dm:test",
        message_id="om_1",
        effective_counts={("OK", "ou_a"): 1},
        governance_version="g",
        policy_version="p",
        last_ledger_event_id=rid,
        last_ingested_at=seen,
    )
    tampered = _tamper_vault_projection_line(block)
    note = f"X\n\n{tampered}"
    rep = scan_vault_note_reaction_drift(mem, note_text=note)
    assert rep["drift_count"] == 1
    blocks = cast(list[dict[str, Any]], rep["blocks"])
    assert blocks[0]["reasons"] == ["vault_projection_fingerprint_mismatch"]


def test_repair_rewrites_block_from_ledger(mem: sqlite3.Connection) -> None:
    insert_message_reaction_event(
        mem, _ev(eid="rx-vr-3"), governance_version="g", policy_version="p"
    )
    row = mem.execute(
        "SELECT reaction_event_id, first_seen_at FROM message_reaction_events WHERE message_id = ?",
        ("om_1",),
    ).fetchone()
    assert row is not None
    rid = str(row["reaction_event_id"])
    seen = str(row["first_seen_at"])
    block = format_reaction_summary_markdown(
        source_id="dm:test",
        message_id="om_1",
        effective_counts={("OK", "ou_a"): 1},
        governance_version="g",
        policy_version="p",
        last_ledger_event_id=rid,
        last_ingested_at=seen,
    )
    tampered = _tamper_vault_projection_line(block)
    note = "\n\n".join(["# note", tampered])
    new_text, n = repair_vault_note_reaction_blocks(mem, note_text=note)
    assert n == 1
    rep = scan_vault_note_reaction_drift(mem, note_text=new_text)
    assert rep["drift_count"] == 0


def test_format_reaction_summary_wrap_false_omits_envelope() -> None:
    inner = format_reaction_summary_markdown(
        source_id="dm:test",
        message_id="om_1",
        effective_counts={},
        wrap=False,
    )
    assert "<!-- ltn:begin" not in inner
    bid = reaction_block_id("dm:test", "om_1")
    assert inner.startswith(f"## IM reactions ^{bid}")


def test_cli_vault_reconcile_reactions_json(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """CLI wires DB + file scan (uses on-disk DB file)."""

    db_path = tmp_path / "v.db"
    conn = connect(db_path)
    init_db(conn)
    insert_message_reaction_event(
        conn, _ev(eid="rx-cli-vr"), governance_version="g", policy_version="p"
    )
    row = conn.execute(
        "SELECT reaction_event_id, first_seen_at FROM message_reaction_events WHERE message_id = ?",
        ("om_1",),
    ).fetchone()
    assert row is not None
    rid = str(row["reaction_event_id"])
    seen = str(row["first_seen_at"])
    conn.commit()
    conn.close()

    block = format_reaction_summary_markdown(
        source_id="dm:test",
        message_id="om_1",
        effective_counts={("OK", "ou_a"): 1},
        governance_version="g",
        policy_version="p",
        last_ledger_event_id=rid,
        last_ingested_at=seen,
    )
    md_path = tmp_path / "n.md"
    md_path.write_text(block, encoding="utf-8")

    assert (
        run(
            ["vault-reconcile-reactions", "--db", str(db_path), str(md_path), "--json"],
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["drift_count"] == 0
