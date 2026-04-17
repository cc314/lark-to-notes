"""Golden markdown for IM reaction vault summaries (lw-pzj.10.4).

Scrub policy lives beside the fixture:
``tests/fixtures/golden/reaction_vault_summary.scrub.txt``.
"""

from __future__ import annotations

from pathlib import Path

from lark_to_notes.render.blocks import extract_block, make_begin_marker, replace_block
from lark_to_notes.render.reaction_vault import (
    VAULT_REACTION_SCHEMA_VERSION,
    format_reaction_summary_markdown,
    reaction_block_id,
    reaction_primary_heading,
    reaction_vault_projection_fingerprint,
)

_FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "golden"
_EXPECTED = _FIXTURE_DIR / "reaction_vault_summary.expected.md"


def test_reaction_block_id_is_stable_for_source_message_pair() -> None:
    assert reaction_block_id("dm:test", "om_golden_rx") == "ltn-rx-e9a864045634b89f"
    assert reaction_block_id("dm:other", "om_golden_rx") != reaction_block_id(
        "dm:test", "om_golden_rx"
    )


def test_reaction_primary_heading_embeds_obsidian_block_anchor() -> None:
    bid = reaction_block_id("dm:test", "om_golden_rx")
    assert reaction_primary_heading(bid) == f"## IM reactions ^{bid}"
    assert VAULT_REACTION_SCHEMA_VERSION == "1"


def test_reaction_summary_matches_golden_fixture() -> None:
    effective = {("THUMBSUP", "ou_alice"): 2, ("SMILE", "ou_bob"): 1}
    rendered = format_reaction_summary_markdown(
        source_id="dm:test",
        message_id="om_golden_rx",
        effective_counts=effective,
        governance_version="1",
        policy_version="test-pol",
    )
    expected = _EXPECTED.read_text(encoding="utf-8")
    assert rendered == expected


def test_reaction_summary_is_idempotent_and_order_invariant() -> None:
    a = {("THUMBSUP", "ou_alice"): 2, ("SMILE", "ou_bob"): 1}
    b = {("SMILE", "ou_bob"): 1, ("THUMBSUP", "ou_alice"): 2}
    r1 = format_reaction_summary_markdown(
        source_id="dm:x",
        message_id="om_y",
        effective_counts=a,
    )
    r2 = format_reaction_summary_markdown(
        source_id="dm:x",
        message_id="om_y",
        effective_counts=b,
    )
    assert r1 == r2
    assert r1 == format_reaction_summary_markdown(
        source_id="dm:x",
        message_id="om_y",
        effective_counts=a,
    )


def test_reaction_vault_projection_fingerprint_includes_ledger_linkage() -> None:
    base = {("A", "op"): 1}
    fp0 = reaction_vault_projection_fingerprint(
        source_id="s",
        message_id="m",
        effective_counts=base,
    )
    fp1 = reaction_vault_projection_fingerprint(
        source_id="s",
        message_id="m",
        effective_counts=base,
        last_ledger_event_id="ev_1",
    )
    assert fp0 != fp1


def test_reaction_block_rerender_replaces_inner_table_only() -> None:
    """Machine block replacement updates counts without duplicating markers."""

    bid = reaction_block_id("dm:x", "om_y")
    assert bid == "ltn-rx-a266ce6533f1811b"
    note = (
        "User line before.\n\n"
        + format_reaction_summary_markdown(
            source_id="dm:x",
            message_id="om_y",
            effective_counts={("A", "op1"): 1},
            governance_version="",
            policy_version="",
        )
        + "\nUser line after.\n"
    )
    v2 = format_reaction_summary_markdown(
        source_id="dm:x",
        message_id="om_y",
        effective_counts={("A", "op1"): 3},
        governance_version="",
        policy_version="",
    )
    new_inner = extract_block(v2, bid)
    assert new_inner is not None
    updated = replace_block(note, bid, new_inner)
    assert updated.count(make_begin_marker(bid)) == 1
    assert "User line before." in updated
    assert "User line after." in updated
    assert "| `A` | `op1` | 3 |" in updated
    assert "| `A` | `op1` | 1 |" not in updated
