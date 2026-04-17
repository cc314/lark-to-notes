"""Versioned **reaction → task_signal** rulesets (lw-pzj.8.1).

Historical rows must pin a :attr:`ReactionRuleset.version` so policy bumps never
silently reinterpret stored evidence.  New versions ship with a mandatory
*migration_note* string for operator-facing changelogs.

The default ruleset stays **conservative**: emoji-only promotion stays off unless
an operator explicitly selects a named aggressive ruleset in configuration
(wiring lands in later CLI / config beads).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ReactionRuleset:
    """Immutable parameters for reaction-derived distill heuristics.

    Attributes:
        version: Stable identifier stored beside distilled artifacts.
        migration_note: Human-readable summary of what changed vs prior versions.
        allow_emoji_only_promotion: When ``False``, emoji reactions alone never
            auto-promote to task surfaces without corroborating text signals.
        min_effective_total_count_for_task_signal: Minimum summed effective
            reaction counts (across keys) before emitting a non-review signal.
    """

    version: str
    migration_note: str
    allow_emoji_only_promotion: bool = False
    min_effective_total_count_for_task_signal: int = 2


class UnknownReactionRulesetError(LookupError):
    """Raised when a requested ruleset version is not registered."""

    def __init__(self, version: str) -> None:
        super().__init__(f"unknown reaction ruleset version: {version!r}")
        self.version = version


REACTION_RULESET_REGISTRY: dict[str, ReactionRuleset] = {
    "1": ReactionRuleset(
        version="1",
        migration_note=(
            "Initial conservative baseline: emoji-only hints disabled; "
            "require summed effective reaction counts ≥ 2 before a task_signal."
        ),
        allow_emoji_only_promotion=False,
        min_effective_total_count_for_task_signal=2,
    ),
    "2026-04-aggressive": ReactionRuleset(
        version="2026-04-aggressive",
        migration_note=(
            "Opt-in experimental ruleset: allows emoji-only promotion and lowers "
            "the effective-count floor to 1. Operators must pin explicitly."
        ),
        allow_emoji_only_promotion=True,
        min_effective_total_count_for_task_signal=1,
    ),
}

DEFAULT_REACTION_RULESET_VERSION = "1"


def default_reaction_ruleset() -> ReactionRuleset:
    """Return the conservative default ruleset."""

    return REACTION_RULESET_REGISTRY[DEFAULT_REACTION_RULESET_VERSION]


def get_reaction_ruleset(version: str) -> ReactionRuleset:
    """Return the ruleset for *version* or raise :class:`UnknownReactionRulesetError`."""

    key = version.strip()
    try:
        return REACTION_RULESET_REGISTRY[key]
    except KeyError as exc:
        raise UnknownReactionRulesetError(key) from exc


def reaction_ruleset_versions() -> tuple[str, ...]:
    """Return registered versions in lexicographic order for stable UIs."""

    return tuple(sorted(REACTION_RULESET_REGISTRY.keys()))
