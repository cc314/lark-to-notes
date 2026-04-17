"""Reaction-aware distill overlay (lw-pzj.8.3).

Maps effective reaction counts + a :class:`~lark_to_notes.distill.models.ClassifierResult`
from text heuristics into an optional **replacement** classification when reaction
signals exist. Conservative defaults keep emoji-only threads in ``needs_review``;
opt-in rulesets (see :mod:`lark_to_notes.distill.reaction_rules`) may emit a bounded
``follow_up`` signal while still routing ambiguous cases to review.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from lark_to_notes.distill.models import (
    ClassifierResult,
    ConfidenceBand,
    PromotionRec,
    TaskClass,
)

if TYPE_CHECKING:
    from lark_to_notes.distill.reaction_rules import ReactionRuleset


def _effective_total(counts: dict[tuple[str, str], int]) -> int:
    return int(sum(int(v) for v in counts.values() if int(v) > 0))


def _active_keys(counts: dict[tuple[str, str], int]) -> int:
    return sum(1 for v in counts.values() if int(v) > 0)


def _multi_reactor_noise(counts: dict[tuple[str, str], int], *, total: int) -> bool:
    keys = _active_keys(counts)
    if keys >= 7:
        return True
    return keys >= 5 and total >= keys * 2


def _strong_high_task(text: ClassifierResult) -> bool:
    return text.task_class == TaskClass.TASK and text.confidence_band == ConfidenceBand.HIGH


def _weak_for_emoji_policy(text: ClassifierResult) -> bool:
    """True when text alone should not block emoji-only policy gates."""

    return not (
        _strong_high_task(text)
        or (
            text.task_class == TaskClass.FOLLOW_UP
            and text.confidence_band == ConfidenceBand.HIGH
        )
    )


def reaction_distill_overlay(
    *,
    effective_counts: dict[tuple[str, str], int],
    ruleset: ReactionRuleset,
    text_result: ClassifierResult,
) -> ClassifierResult | None:
    """Return a reaction-driven classification overlay, or ``None`` to keep *text_result*.

    When non-``None``, callers should treat the return value as the effective distill
    outcome for this message (reaction pipeline won over text-only heuristics for
    this evaluation).

    Machine ``reason_code`` values emitted here use stable prefixes
    ``reaction_review_`` (review lane) and ``reaction_signal_`` (allowed promotion).
    """

    total = _effective_total(effective_counts)
    if total <= 0:
        return None

    if _multi_reactor_noise(effective_counts, total=total):
        return ClassifierResult(
            task_class=TaskClass.NEEDS_REVIEW,
            confidence_band=ConfidenceBand.MEDIUM,
            promotion_rec=PromotionRec.REVIEW,
            reason_code="reaction_review_multi_reactor_noise",
            matched_signal="reactions",
            excerpt=text_result.excerpt[:120] if text_result.excerpt else "",
        )

    if total < ruleset.min_effective_total_count_for_task_signal:
        return ClassifierResult(
            task_class=TaskClass.NEEDS_REVIEW,
            confidence_band=ConfidenceBand.MEDIUM,
            promotion_rec=PromotionRec.REVIEW,
            reason_code="reaction_review_engagement_below_min",
            matched_signal="reactions",
            excerpt=text_result.excerpt[:120] if text_result.excerpt else "",
        )

    if not _weak_for_emoji_policy(text_result):
        return None

    if not ruleset.allow_emoji_only_promotion:
        return ClassifierResult(
            task_class=TaskClass.NEEDS_REVIEW,
            confidence_band=ConfidenceBand.MEDIUM,
            promotion_rec=PromotionRec.REVIEW,
            reason_code="reaction_review_emoji_only_policy_blocked",
            matched_signal="reactions",
            excerpt=text_result.excerpt[:120] if text_result.excerpt else "",
        )

    keys = _active_keys(effective_counts)
    if total == ruleset.min_effective_total_count_for_task_signal and keys >= 2:
        return ClassifierResult(
            task_class=TaskClass.NEEDS_REVIEW,
            confidence_band=ConfidenceBand.MEDIUM,
            promotion_rec=PromotionRec.REVIEW,
            reason_code="reaction_review_borderline_shared_floor",
            matched_signal="reactions",
            excerpt=text_result.excerpt[:120] if text_result.excerpt else "",
        )

    return ClassifierResult(
        task_class=TaskClass.FOLLOW_UP,
        confidence_band=ConfidenceBand.MEDIUM,
        promotion_rec=PromotionRec.DAILY_ONLY,
        reason_code="reaction_signal_engagement_follow_up",
        matched_signal="reactions",
        excerpt=text_result.excerpt[:120] if text_result.excerpt else "",
    )


def apply_reaction_distill_overlay(
    text_result: ClassifierResult,
    *,
    effective_counts: dict[tuple[str, str], int],
    ruleset: ReactionRuleset,
) -> ClassifierResult:
    """Return *text_result* or the reaction overlay when one applies."""

    layered = reaction_distill_overlay(
        effective_counts=effective_counts,
        ruleset=ruleset,
        text_result=text_result,
    )
    return text_result if layered is None else layered
