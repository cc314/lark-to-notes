"""Golden-style tests for :mod:`lark_to_notes.distill.reaction_reclassify` (lw-pzj.13.2)."""

from __future__ import annotations

import json

import pytest

from lark_to_notes.cli import run
from lark_to_notes.distill.reaction_reclassify import (
    GOVERNANCE_ONLY_BUMP_DISTILL_STAGES,
    POLICY_BUMP_DISTILL_STAGES,
    STAGE_REACTION_DISTILL_OVERLAY,
    STAGE_REACTION_SIGNAL_EVIDENCE,
    STAGE_TASK_CLASSIFICATION,
    reaction_distill_stages_invalidated,
    reaction_reclassify_invalidation_report,
)


def test_policy_bump_invalidates_overlay_signal_and_task_stages() -> None:
    got = reaction_distill_stages_invalidated(
        policy_version_changed=True,
        governance_version_changed=False,
    )
    assert got == POLICY_BUMP_DISTILL_STAGES
    assert got == frozenset(
        {
            STAGE_REACTION_DISTILL_OVERLAY,
            STAGE_REACTION_SIGNAL_EVIDENCE,
            STAGE_TASK_CLASSIFICATION,
        },
    )


def test_governance_only_bump_invalidates_no_distill_stages() -> None:
    got = reaction_distill_stages_invalidated(
        policy_version_changed=False,
        governance_version_changed=True,
    )
    assert got == GOVERNANCE_ONLY_BUMP_DISTILL_STAGES
    assert got == frozenset()


def test_both_bumps_follows_policy_precedence() -> None:
    got = reaction_distill_stages_invalidated(
        policy_version_changed=True,
        governance_version_changed=True,
    )
    assert got == POLICY_BUMP_DISTILL_STAGES


def test_invalidation_report_is_deterministic_json_roundtrip() -> None:
    a = reaction_reclassify_invalidation_report()
    b = reaction_reclassify_invalidation_report()
    assert a == b
    s = json.dumps(a, sort_keys=True)
    assert json.loads(s) == a
    names = [row["name"] for row in a["scenarios"]]
    assert names == [
        "policy_bump",
        "governance_only_bump",
        "both_bumps",
        "unchanged",
    ]
    policy_row = next(x for x in a["scenarios"] if x["name"] == "policy_bump")
    assert policy_row["stages"] == sorted(POLICY_BUMP_DISTILL_STAGES)
    gov_row = next(x for x in a["scenarios"] if x["name"] == "governance_only_bump")
    assert gov_row["stages"] == []
    unchanged = next(x for x in a["scenarios"] if x["name"] == "unchanged")
    assert unchanged["stages"] == []


def test_cli_reaction_reclassify_map_json(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = run(["reaction-reclassify-map", "--json"])
    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["bead"] == "lw-pzj.13.2"
    assert payload["policy_bump_distill_stages"] == sorted(POLICY_BUMP_DISTILL_STAGES)
