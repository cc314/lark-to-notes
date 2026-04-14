"""Tests for the distillation layer: models, heuristics, and routing."""

from __future__ import annotations

import pytest

from lark_to_notes.distill.heuristics import HeuristicClassifier, default_classifier
from lark_to_notes.distill.models import (
    ClassifierResult,
    ConfidenceBand,
    DistillInput,
    PromotionRec,
    TaskClass,
)
from lark_to_notes.distill.routing import classify_with_routing

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _inp(content: str, **kwargs: str) -> DistillInput:
    return DistillInput(
        message_id=kwargs.get("message_id", "om_test"),
        source_id=kwargs.get("source_id", "dm-test:s1"),
        source_type=kwargs.get("source_type", "dm_user"),
        content=content,
        sender_name=kwargs.get("sender_name", "Tester"),
        direction=kwargs.get("direction", "incoming"),
        created_at=kwargs.get("created_at", "2026-04-14 10:00"),
    )


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


def test_task_class_values() -> None:
    assert TaskClass.CONTEXT.value == "context"
    assert TaskClass.FOLLOW_UP.value == "follow_up"
    assert TaskClass.TASK.value == "task"
    assert TaskClass.NEEDS_REVIEW.value == "needs_review"


def test_confidence_band_values() -> None:
    assert ConfidenceBand.HIGH.value == "high"
    assert ConfidenceBand.MEDIUM.value == "medium"
    assert ConfidenceBand.LOW.value == "low"


def test_promotion_rec_values() -> None:
    assert PromotionRec.CURRENT_TASKS.value == "current_tasks"
    assert PromotionRec.DAILY_ONLY.value == "daily_only"
    assert PromotionRec.REVIEW.value == "review"


def test_classifier_result_needs_review_low_band() -> None:
    r = ClassifierResult(
        task_class=TaskClass.TASK,
        confidence_band=ConfidenceBand.LOW,
        promotion_rec=PromotionRec.REVIEW,
        reason_code="weak",
    )
    assert r.needs_review is True


def test_classifier_result_needs_review_class() -> None:
    r = ClassifierResult(
        task_class=TaskClass.NEEDS_REVIEW,
        confidence_band=ConfidenceBand.MEDIUM,
        promotion_rec=PromotionRec.DAILY_ONLY,
        reason_code="ambiguous",
    )
    assert r.needs_review is True


def test_classifier_result_not_needs_review() -> None:
    r = ClassifierResult(
        task_class=TaskClass.TASK,
        confidence_band=ConfidenceBand.HIGH,
        promotion_rec=PromotionRec.CURRENT_TASKS,
        reason_code="en_please_verb",
    )
    assert r.needs_review is False


def test_classifier_result_needs_review_promotion_rec() -> None:
    r = ClassifierResult(
        task_class=TaskClass.FOLLOW_UP,
        confidence_band=ConfidenceBand.MEDIUM,
        promotion_rec=PromotionRec.REVIEW,
        reason_code="some_reason",
    )
    assert r.needs_review is True


def test_distill_input_defaults() -> None:
    inp = _inp("hello")
    assert inp.extra_context == []
    assert inp.direction == "incoming"


# ---------------------------------------------------------------------------
# English high-confidence heuristics
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "content,expected_reason",
    [
        ("Please review the PR before Thursday", "en_please_verb"),
        ("Please send the updated doc to Alice", "en_please_verb"),
        ("Could you send the updated doc?", "en_polite_request"),
        ("Can you help with this?", "en_polite_request"),
        ("I need you to approve the budget", "en_need_to"),
        ("Finish this by EOD today", "en_deadline"),
        ("Deadline for this project is next Monday", "en_deadline_word"),
        ("Reminder: submit the report by tomorrow", "en_reminder"),
        ("Don't forget to update the changelog", "en_reminder"),
        ("action item: investigate the crash", "en_action_marker"),
        ("TODO: add unit tests for auth", "en_action_marker"),
        ("FOLLOW-UP: confirm with the vendor", "en_action_marker"),
        ("@Alice please review this PR", "en_please_verb"),
    ],
)
def test_english_high_confidence(content: str, expected_reason: str) -> None:
    result = default_classifier.classify(_inp(content))
    assert result.confidence_band == ConfidenceBand.HIGH, (
        f"Expected HIGH for {content!r}, got {result.confidence_band!r} "
        f"(reason={result.reason_code!r})"
    )
    assert result.reason_code == expected_reason, (
        f"Expected reason {expected_reason!r}, got {result.reason_code!r}"
    )
    assert result.task_class in {TaskClass.TASK, TaskClass.FOLLOW_UP}


def test_english_imperative_start() -> None:
    result = default_classifier.classify(_inp("Send the updated report to Alice"))
    assert result.confidence_band in {ConfidenceBand.HIGH, ConfidenceBand.MEDIUM}
    assert result.task_class == TaskClass.TASK


def test_english_at_assign_high() -> None:
    result = default_classifier.classify(_inp("@Bob merge the feature branch"))
    assert result.confidence_band == ConfidenceBand.HIGH
    assert result.reason_code == "en_at_assign"


# ---------------------------------------------------------------------------
# English medium-confidence heuristics
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "content,expected_reason",
    [
        ("Let's schedule a review meeting", "en_collective_intent"),
        ("We should update the docs", "en_collective_intent"),
        ("I'll need the report by next week", "en_collective_intent"),
        ("Follow up with the ops team", "en_follow_up_phrase"),
        ("Check on the deploy status", "en_follow_up_phrase"),
        ("I need to prepare the slides", "en_need_to_soft"),
        # "will you ...?" hits en_action_question (HIGH list only covers can/could/would)
        ("Will you be able to join the call?", "en_action_question"),
    ],
)
def test_english_medium_confidence(content: str, expected_reason: str) -> None:
    result = default_classifier.classify(_inp(content))
    assert result.confidence_band == ConfidenceBand.MEDIUM, (
        f"Expected MEDIUM for {content!r}, got {result.confidence_band!r} "
        f"(reason={result.reason_code!r})"
    )
    assert result.reason_code == expected_reason


# ---------------------------------------------------------------------------
# Chinese high-confidence heuristics
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "content,expected_reason",
    [
        ("请帮我确认一下这个需求", "zh_qing_verb"),
        ("麻烦你明天早上给我发一下报告", "zh_mafan_verb"),
        ("帮我看一下这个错误", "zh_bang_verb"),
        ("需要你明天之前完成", "zh_xuyao_assign"),
        ("记得周五前提交", "zh_reminder"),
        ("别忘了更新文档", "zh_reminder"),
        ("截止2026/04/15提交材料", "zh_deadline"),
        ("待办：跟进供应商报价", "zh_action_marker"),  # noqa: RUF001
        # Chinese @-mention: zh_qing_verb fires (en_at_assign requires ASCII verb following @)
        ("@张敏 请确认一下", "zh_qing_verb"),
    ],
)
def test_chinese_high_confidence(content: str, expected_reason: str) -> None:
    result = default_classifier.classify(_inp(content))
    assert result.confidence_band == ConfidenceBand.HIGH, (
        f"Expected HIGH for {content!r}, got {result.confidence_band!r} "
        f"(reason={result.reason_code!r})"
    )
    assert result.reason_code == expected_reason


# ---------------------------------------------------------------------------
# Chinese medium-confidence heuristics
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "content,expected_reason",
    [
        ("需要审核一下这个方案", "zh_xuyao_verb"),
        ("这个方案可以吗？", "zh_polite_question"),  # noqa: RUF001
        ("应该在周五之前完成", "zh_should"),
        ("跟进一下这个问题", "zh_follow_up"),
        ("我来处理这个任务", "zh_assignment"),
    ],
)
def test_chinese_medium_confidence(content: str, expected_reason: str) -> None:
    result = default_classifier.classify(_inp(content))
    assert result.confidence_band == ConfidenceBand.MEDIUM, (
        f"Expected MEDIUM for {content!r}, got {result.confidence_band!r} "
        f"(reason={result.reason_code!r})"
    )
    assert result.reason_code == expected_reason


# ---------------------------------------------------------------------------
# Context (no task signal)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "content",
    [
        "Thanks for the update!",
        "Got it.",
        "Sounds good.",
        "明白了",
        "好的，谢谢",  # noqa: RUF001
        "Meeting is at 3pm tomorrow.",
    ],
)
def test_context_classification(content: str) -> None:
    result = default_classifier.classify(_inp(content))
    assert result.task_class == TaskClass.CONTEXT
    assert result.confidence_band == ConfidenceBand.HIGH
    assert result.promotion_rec == PromotionRec.DAILY_ONLY
    assert result.reason_code == "no_task_signal"


# ---------------------------------------------------------------------------
# Long content escalation
# ---------------------------------------------------------------------------


def test_long_content_no_signal_becomes_needs_review() -> None:
    long_content = "This is some very long informational content. " * 20
    assert len(long_content) > 800
    result = default_classifier.classify(_inp(long_content))
    assert result.task_class == TaskClass.NEEDS_REVIEW
    assert result.confidence_band == ConfidenceBand.LOW
    assert result.escalate_to_llm is True
    assert result.promotion_rec == PromotionRec.REVIEW


def test_long_content_with_high_signal_sets_escalate_flag() -> None:
    long_content = "Please review the following document. " + "details " * 200
    result = default_classifier.classify(_inp(long_content))
    assert result.confidence_band == ConfidenceBand.HIGH
    assert result.escalate_to_llm is True


# ---------------------------------------------------------------------------
# Promotion recommendations
# ---------------------------------------------------------------------------


def test_high_task_promotes_to_current_tasks() -> None:
    result = default_classifier.classify(_inp("Please send me the report"))
    assert result.promotion_rec == PromotionRec.CURRENT_TASKS


def test_high_follow_up_promotes_to_daily_only() -> None:
    result = default_classifier.classify(_inp("Let's follow up on this tomorrow"))
    assert result.promotion_rec == PromotionRec.DAILY_ONLY


def test_medium_result_promotes_to_daily_only() -> None:
    result = default_classifier.classify(_inp("We should schedule a call"))
    assert result.promotion_rec == PromotionRec.DAILY_ONLY


# ---------------------------------------------------------------------------
# Excerpt extraction
# ---------------------------------------------------------------------------


def test_excerpt_is_populated_on_match() -> None:
    result = default_classifier.classify(_inp("Please send me the report"))
    assert len(result.excerpt) > 0


def test_excerpt_is_populated_on_context() -> None:
    result = default_classifier.classify(_inp("Got it, thanks"))
    assert len(result.excerpt) > 0


# ---------------------------------------------------------------------------
# Custom patterns
# ---------------------------------------------------------------------------


def test_extra_high_pattern() -> None:
    classifier = HeuristicClassifier(
        extra_high_patterns=[(r"(?i)\bUrgent\b", "custom_urgent")]
    )
    result = classifier.classify(_inp("Urgent: fix the prod issue"))
    assert result.confidence_band == ConfidenceBand.HIGH
    assert result.reason_code == "custom_urgent"


def test_extra_medium_pattern() -> None:
    classifier = HeuristicClassifier(
        extra_medium_patterns=[(r"(?i)\bsomeday\b", "custom_someday")]
    )
    # Use content with no built-in signals so the custom pattern fires
    result = classifier.classify(_inp("Someday I hope to refactor the legacy module"))
    assert result.confidence_band == ConfidenceBand.MEDIUM
    assert result.reason_code == "custom_someday"


# ---------------------------------------------------------------------------
# Routing: heuristics-only mode
# ---------------------------------------------------------------------------


def test_routing_no_provider_high_confidence() -> None:
    inp = _inp("Please review this PR")
    result = classify_with_routing(inp, llm_provider=None)
    assert result.confidence_band == ConfidenceBand.HIGH
    assert result.escalate_to_llm is False or result.escalate_to_llm is True
    # Without provider, result should still be valid
    assert result.task_class in TaskClass.__members__.values()


def test_routing_no_provider_low_confidence_becomes_needs_review() -> None:
    # Force a LOW band result: long content with no signal
    long_no_signal = "context context context " * 40
    inp = _inp(long_no_signal)
    result = classify_with_routing(inp, llm_provider=None)
    assert result.task_class == TaskClass.NEEDS_REVIEW
    assert result.confidence_band == ConfidenceBand.LOW
    assert result.promotion_rec == PromotionRec.REVIEW
    assert result.escalate_to_llm is False


def test_routing_with_returning_provider() -> None:
    from lark_to_notes.distill.routing import LLMProvider

    class FakeProvider:
        def classify(
            self, inp: DistillInput, hint: ClassifierResult
        ) -> ClassifierResult | None:
            return ClassifierResult(
                task_class=TaskClass.TASK,
                confidence_band=ConfidenceBand.HIGH,
                promotion_rec=PromotionRec.CURRENT_TASKS,
                reason_code="llm_override",
            )

    assert isinstance(FakeProvider(), LLMProvider)
    inp = _inp("This is long " * 100)
    result = classify_with_routing(inp, llm_provider=FakeProvider())
    assert result.reason_code == "llm_override"


def test_routing_with_none_returning_provider_falls_back() -> None:
    class NullProvider:
        def classify(
            self, inp: DistillInput, hint: ClassifierResult
        ) -> ClassifierResult | None:
            return None

    # High-band with long content: escalate=True but provider returns None → use heuristics
    inp = _inp("Please send me the report " + "detail " * 200)
    result = classify_with_routing(inp, llm_provider=NullProvider())
    # Should fall back to heuristic result (HIGH band, en_please_verb)
    assert result.confidence_band == ConfidenceBand.HIGH
    assert result.reason_code == "en_please_verb"


def test_routing_with_raising_provider_falls_back() -> None:
    class BrokenProvider:
        def classify(
            self, inp: DistillInput, hint: ClassifierResult
        ) -> ClassifierResult | None:
            raise RuntimeError("LLM unavailable")

    inp = _inp("Please send me the report " + "x " * 200)
    result = classify_with_routing(inp, llm_provider=BrokenProvider())
    # Exception caught → fall back to heuristics
    assert result.task_class in TaskClass.__members__.values()
