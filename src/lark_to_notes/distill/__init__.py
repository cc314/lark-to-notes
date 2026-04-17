"""Deterministic heuristic distillation of raw Lark records into action items."""

from __future__ import annotations

from lark_to_notes.distill.heuristics import HeuristicClassifier, default_classifier
from lark_to_notes.distill.models import (
    ClassifierResult,
    ConfidenceBand,
    DistillInput,
    PromotionRec,
    TaskClass,
)
from lark_to_notes.distill.reaction_distill import (
    apply_reaction_distill_overlay,
    reaction_distill_overlay,
)
from lark_to_notes.distill.reaction_rules import (
    DEFAULT_REACTION_RULESET_VERSION,
    ReactionRuleset,
    UnknownReactionRulesetError,
    default_reaction_ruleset,
    get_reaction_ruleset,
    reaction_ruleset_versions,
)
from lark_to_notes.distill.reaction_signal import (
    ReactionSignalEvidence,
    build_reaction_signal_evidence,
    reaction_signal_id,
)
from lark_to_notes.distill.routing import LLMProvider, classify_with_routing

__all__ = [
    "DEFAULT_REACTION_RULESET_VERSION",
    "ClassifierResult",
    "ConfidenceBand",
    "DistillInput",
    "HeuristicClassifier",
    "LLMProvider",
    "PromotionRec",
    "ReactionRuleset",
    "ReactionSignalEvidence",
    "TaskClass",
    "UnknownReactionRulesetError",
    "apply_reaction_distill_overlay",
    "build_reaction_signal_evidence",
    "classify_with_routing",
    "default_classifier",
    "default_reaction_ruleset",
    "get_reaction_ruleset",
    "reaction_distill_overlay",
    "reaction_ruleset_versions",
    "reaction_signal_id",
]
