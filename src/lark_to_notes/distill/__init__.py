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
from lark_to_notes.distill.routing import LLMProvider, classify_with_routing

__all__ = [
    "ClassifierResult",
    "ConfidenceBand",
    "DistillInput",
    "HeuristicClassifier",
    "LLMProvider",
    "PromotionRec",
    "TaskClass",
    "classify_with_routing",
    "default_classifier",
]
