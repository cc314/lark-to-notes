"""Reaction distill **reclassify** invalidation map (lw-pzj.13.2).

Defines which **distill-only** derived stages must rerun when intake
``policy_version`` or ``governance_version`` tuples change, without rewriting
immutable ``message_reaction_events`` rows or raw message bodies (plan Replay
§3).

**Supersession:** rerunning :data:`STAGE_TASK_CLASSIFICATION` must not silently
reinterpret the same human-facing task identity. Callers that persist tasks
via :func:`lark_to_notes.tasks.registry.upsert_task` should either incorporate
``policy_version`` (and ruleset version) into the fingerprint salt or mark prior
tasks ``superseded`` when classification materially changes — that persistence
path is intentionally out of scope for this normative map.
"""

from __future__ import annotations

from typing import Any

# Stable stage identifiers for logs, CLI JSON, and tests.
STAGE_REACTION_DISTILL_OVERLAY = "reaction_distill_overlay"
STAGE_REACTION_SIGNAL_EVIDENCE = "reaction_signal_evidence"
STAGE_TASK_CLASSIFICATION = "task_classification"

# When ``policy_version`` changes, rules/heuristics that interpret effective
# reaction counts or emit ``reaction_signal`` evidence may change. Task rows
# derived from those outcomes must be recomputed under explicit supersession.
POLICY_BUMP_DISTILL_STAGES: frozenset[str] = frozenset(
    {
        STAGE_REACTION_DISTILL_OVERLAY,
        STAGE_REACTION_SIGNAL_EVIDENCE,
        STAGE_TASK_CLASSIFICATION,
    },
)

# ``governance_version`` stamps caps / intake gates. Historical effective
# reaction sets already materialized from immutable events do not change when
# governance alone bumps, so **distill-only** stages are not invalidated here.
# (Future intake/replay beads may define separate governance-driven stages.)
GOVERNANCE_ONLY_BUMP_DISTILL_STAGES: frozenset[str] = frozenset()


def reaction_distill_stages_invalidated(
    *,
    policy_version_changed: bool,
    governance_version_changed: bool,
) -> frozenset[str]:
    """Return distill stage ids that must rerun for the given tuple changes."""

    if policy_version_changed:
        return POLICY_BUMP_DISTILL_STAGES
    if governance_version_changed:
        return GOVERNANCE_ONLY_BUMP_DISTILL_STAGES
    return frozenset()


def reaction_reclassify_invalidation_report() -> dict[str, Any]:
    """Return a deterministic JSON-serializable invalidation summary (lw-pzj.13.2)."""

    scenarios: list[dict[str, Any]] = []
    for name, pv, gv in (
        ("policy_bump", True, False),
        ("governance_only_bump", False, True),
        ("both_bumps", True, True),
        ("unchanged", False, False),
    ):
        stages = reaction_distill_stages_invalidated(
            policy_version_changed=pv,
            governance_version_changed=gv,
        )
        scenarios.append(
            {
                "name": name,
                "policy_version_changed": pv,
                "governance_version_changed": gv,
                "stages": sorted(stages),
            },
        )

    return {
        "bead": "lw-pzj.13.2",
        "policy_bump_distill_stages": sorted(POLICY_BUMP_DISTILL_STAGES),
        "governance_only_bump_distill_stages": sorted(GOVERNANCE_ONLY_BUMP_DISTILL_STAGES),
        "scenarios": scenarios,
        "supersession_contract": (
            "Do not upsert a materially new classification at the same fingerprint "
            "without an explicit superseded row or fingerprint salt that pins "
            "policy_version / ruleset.version."
        ),
    }
