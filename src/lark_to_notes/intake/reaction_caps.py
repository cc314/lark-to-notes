"""Per-run / per-source caps for IM reaction event intake (lw-pzj.12.1).

Caps gate **new** reaction rows after structural validation and parse.
Envelopes whose canonical ``reaction_event_id`` already exists skip the cap
(benign replays; lw-pzj.12.4); remaining inserts are bounded per run / per source.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# Bump when cap semantics or defaults change (operator-facing governance hook).
REACTION_INTAKE_GOVERNANCE_VERSION = "1"
# Default policy stamp for new reaction intake rows (empty until operators pin policy).
REACTION_INTAKE_POLICY_VERSION = ""


@dataclass(frozen=True)
class ReactionIntakeCaps:
    """Governance knobs for reaction NDJSON / event intake."""

    max_reaction_envelopes_per_run: int = 0
    max_reaction_envelopes_per_source_per_run: int = 0
    governance_version: str = REACTION_INTAKE_GOVERNANCE_VERSION
    policy_version: str = REACTION_INTAKE_POLICY_VERSION

    @property
    def limits_active(self) -> bool:
        run_on = self.max_reaction_envelopes_per_run > 0
        src_on = self.max_reaction_envelopes_per_source_per_run > 0
        return bool(run_on or src_on)


@dataclass
class ReactionIntakeCapState:
    """Mutable counters for one ingest call (or shared across a runtime run)."""

    run_total: int = 0
    by_source: dict[str, int] = field(default_factory=dict)


def reaction_cap_block_reason(
    caps: ReactionIntakeCaps,
    state: ReactionIntakeCapState,
    *,
    source_id: str,
) -> str | None:
    """Return a stable ``reason_code`` when the next validated envelope must defer."""

    if not caps.limits_active:
        return None
    cap_run = caps.max_reaction_envelopes_per_run
    if cap_run > 0 and state.run_total >= cap_run:
        return "reaction_cap_per_run_exceeded"
    src = state.by_source.get(source_id, 0)
    cap_src = caps.max_reaction_envelopes_per_source_per_run
    if cap_src > 0 and src >= cap_src:
        return "reaction_cap_per_source_exceeded"
    return None


def reaction_cap_consume_slot(state: ReactionIntakeCapState, *, source_id: str) -> None:
    """Record one validated reaction envelope that will proceed to parse/insert."""

    state.run_total += 1
    state.by_source[source_id] = state.by_source.get(source_id, 0) + 1


def reaction_cap_release_slot(state: ReactionIntakeCapState, *, source_id: str) -> None:
    """Undo :func:`reaction_cap_consume_slot` when parse/insert fails after the gate."""

    if state.run_total > 0:
        state.run_total -= 1
    prev = state.by_source.get(source_id, 0)
    if prev <= 1:
        state.by_source.pop(source_id, None)
    else:
        state.by_source[source_id] = prev - 1
