"""Deterministic heuristic classifier for Lark messages.

Rules are evaluated in priority order.  The first matching rule wins.
LLM escalation is signalled but never executed here; that is the
:mod:`routing` layer's responsibility.

Supported languages: English, Simplified Chinese, mixed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from lark_to_notes.distill.models import (
    ClassifierResult,
    ConfidenceBand,
    DistillInput,
    PromotionRec,
    TaskClass,
)

# ---------------------------------------------------------------------------
# Pattern definitions
# ---------------------------------------------------------------------------

# High-confidence English task phrases (imperative + request forms)
_EN_HIGH_PATTERNS: list[tuple[str, str]] = [
    # explicit action-item markers (colon required to avoid matching bare "follow up")
    (r"(?i)\b(?:action\s*item|TODO|FIXME|FOLLOWUP|FOLLOW[-\s]UP)\s*:", "en_action_marker"),
    # reminder cues — checked BEFORE polite-request patterns to capture "Reminder: ..."
    (r"(?i)\b(?:remind(?:er)?|don'?t\s+forget|remember\s+to)\b", "en_reminder"),
    # direct request: please <verb>
    (r"(?i)\bplease\s+\w+", "en_please_verb"),
    # can/could/would you + verb (direct polite requests)
    (r"(?i)\b(?:can|could|would)\s+you\s+\w+", "en_polite_request"),
    # need you to / need me to
    (r"(?i)\bneed\s+(?:you|me|us|someone)\s+to\b", "en_need_to"),
    # deadline cues
    (
        r"(?i)\bby\s+(?:eod|eow|end\s+of\s+(?:day|week|month)"
        r"|(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday))\b",
        "en_deadline",
    ),
    (r"(?i)\bdeadline\b", "en_deadline_word"),
    # assignment with @ — ASCII name only, to avoid matching CJK content after @
    (r"@[A-Za-z]\w*\s+[A-Za-z]", "en_at_assign"),
]

# Medium-confidence English patterns
_EN_MEDIUM_PATTERNS: list[tuple[str, str]] = [
    # "let's", "we should", "I'll need"
    (r"(?i)\b(?:let'?s|we\s+should|i'?ll\s+need|we\s+need\s+to)\b", "en_collective_intent"),
    # "follow up on" / "check on" / "look into"
    (r"(?i)\b(?:follow\s*up|check\s+on|look\s+into|reach\s+out)\b", "en_follow_up_phrase"),
    # "need to" (without assignee)
    (r"(?i)\bneed\s+to\b", "en_need_to_soft"),
    # short polite questions ending in "?": "Can you ...?" / "Will you ...?"
    (r"(?i)\b(?:can|will|shall)\s+you\b.*\?", "en_action_question"),
    # imperative at start of sentence — long verb list, must use noqa
    (
        r"(?i)^(?:please\s+)?\b(?:send|check|confirm|review|update|prepare|schedule|arrange"
        r"|complete|finish|submit|approve|deploy|fix|test|add|remove|create|delete|make|get"
        r"|set|run|call|write|read|verify|ensure|provide|share|notify|contact|ping|block"
        r"|unblock|merge|release|revert|rebase|tag|bump|close|open|assign|escalate|resolve"
        r"|investigate|analyze|analyse|draft|revise|coordinate|document|record|file|log"
        r"|track|monitor|enable|disable|configure|install|upgrade|downgrade|migrate"
        r"|rollback)\b",
        "en_imperative_start",
    ),
]

# High-confidence Chinese task phrases
_ZH_HIGH_PATTERNS: list[tuple[str, str]] = [
    # 请 + verb (please do X)
    (r"请\s*[\u4e00-\u9fff]", "zh_qing_verb"),
    # 麻烦 + verb (would you mind)
    (r"麻烦\s*[\u4e00-\u9fff]", "zh_mafan_verb"),
    # 帮(我/他/她) + verb
    (r"帮\s*(?:我|他|她|一下|个忙)?\s*[\u4e00-\u9fff]", "zh_bang_verb"),
    # 需要你/我/我们 + verb
    (r"需要(?:你|我|我们|他们)\s*[\u4e00-\u9fff]", "zh_xuyao_assign"),
    # 记得 / 别忘了 (remember / don't forget)
    (r"(?:记得|别忘了|不要忘)", "zh_reminder"),
    # explicit deadline markers — fullwidth commas/periods are intentional (Chinese punctuation)
    (
        r"(?:截止|deadline|ddl|DDL|DL)[^，。\n]{0,20}"  # noqa: RUF001
        r"(?:\d{1,2}[月/]\d{1,2}[日号]?|\d{4}[-/]\d{1,2}[-/]\d{1,2})",
        "zh_deadline",
    ),
    # action item markers
    (r"(?:待办|待处理|待确认|action\s*item|TODO)", "zh_action_marker"),
    # @名字 cue
    (r"@[\u4e00-\u9fff\w]+\s*[\u4e00-\u9fff]", "zh_at_assign"),
]

# Medium-confidence Chinese patterns
_ZH_MEDIUM_PATTERNS: list[tuple[str, str]] = [
    # 需要 + verb (need to do)
    (r"需要\s*[\u4e00-\u9fff]", "zh_xuyao_verb"),
    # follow-up question expecting response — fullwidth "?" is intentional
    (r"(?:能否|可以吗|好吗|行吗|怎么样|如何)\s*[？?]?", "zh_polite_question"),  # noqa: RUF001
    # should / ought to
    (r"(?:应该|应当|最好|建议)\s*[\u4e00-\u9fff]", "zh_should"),
    # 跟进 / 确认一下 (follow up / confirm)
    (r"(?:跟进|确认一下|核实|检查一下|看一下)", "zh_follow_up"),
    # 我来 / 你来 (I'll handle / you handle)
    (r"(?:我来|你来|我去|你去)\s*[\u4e00-\u9fff]", "zh_assignment"),
]


@dataclass(frozen=True, slots=True)
class _CompiledRule:
    pattern: re.Pattern[str]
    signal_name: str
    band: ConfidenceBand
    task_class: TaskClass


def _compile_rules(
    patterns: list[tuple[str, str]],
    band: ConfidenceBand,
    task_class: TaskClass,
) -> list[_CompiledRule]:
    return [
        _CompiledRule(
            pattern=re.compile(pat, re.MULTILINE),
            signal_name=sig,
            band=band,
            task_class=task_class,
        )
        for pat, sig in patterns
    ]


@dataclass
class HeuristicClassifier:
    """Deterministic, configurable heuristic classifier.

    Rules are checked in priority order: high-confidence first, then
    medium.  The first match wins.  If nothing matches, the message
    is classified as :attr:`TaskClass.CONTEXT` with
    :attr:`ConfidenceBand.HIGH`.

    Args:
        extra_high_patterns: Additional ``(regex, signal_name)`` pairs
            at high-confidence level.
        extra_medium_patterns: Additional ``(regex, signal_name)`` pairs
            at medium-confidence level.
        min_content_length_for_llm: Content longer than this (in chars)
            will have ``escalate_to_llm=True`` even on medium hits.
    """

    extra_high_patterns: list[tuple[str, str]] = field(default_factory=list)
    extra_medium_patterns: list[tuple[str, str]] = field(default_factory=list)
    min_content_length_for_llm: int = 800

    def __post_init__(self) -> None:
        self._high_rules: list[_CompiledRule] = _compile_rules(
            _EN_HIGH_PATTERNS + _ZH_HIGH_PATTERNS + self.extra_high_patterns,
            ConfidenceBand.HIGH,
            TaskClass.TASK,
        )
        self._medium_rules: list[_CompiledRule] = _compile_rules(
            _EN_MEDIUM_PATTERNS + _ZH_MEDIUM_PATTERNS + self.extra_medium_patterns,
            ConfidenceBand.MEDIUM,
            TaskClass.TASK,
        )
        # follow-up phrases get classified as FOLLOW_UP rather than TASK
        self._follow_up_signals: frozenset[str] = frozenset(
            {
                "en_follow_up_phrase",
                "zh_follow_up",
                "en_collective_intent",
            }
        )

    def classify(self, inp: DistillInput) -> ClassifierResult:
        """Classify a single :class:`DistillInput` deterministically.

        Args:
            inp: The message to classify.

        Returns:
            A :class:`ClassifierResult` with all fields populated.
        """
        content = inp.content.strip()
        long_content = len(content) > self.min_content_length_for_llm

        # High-confidence rules first
        for rule in self._high_rules:
            m = rule.pattern.search(content)
            if m:
                excerpt = _excerpt(content, m.start(), m.end())
                task_class = (
                    TaskClass.FOLLOW_UP
                    if rule.signal_name in self._follow_up_signals
                    else TaskClass.TASK
                )
                promotion = _promotion_for(task_class, ConfidenceBand.HIGH)
                return ClassifierResult(
                    task_class=task_class,
                    confidence_band=ConfidenceBand.HIGH,
                    promotion_rec=promotion,
                    reason_code=rule.signal_name,
                    matched_signal=m.group(0)[:80],
                    escalate_to_llm=long_content,
                    excerpt=excerpt,
                )

        # Medium-confidence rules
        for rule in self._medium_rules:
            m = rule.pattern.search(content)
            if m:
                excerpt = _excerpt(content, m.start(), m.end())
                task_class = (
                    TaskClass.FOLLOW_UP
                    if rule.signal_name in self._follow_up_signals
                    else TaskClass.TASK
                )
                escalate = long_content
                promotion = _promotion_for(task_class, ConfidenceBand.MEDIUM)
                return ClassifierResult(
                    task_class=task_class,
                    confidence_band=ConfidenceBand.MEDIUM,
                    promotion_rec=promotion,
                    reason_code=rule.signal_name,
                    matched_signal=m.group(0)[:80],
                    escalate_to_llm=escalate,
                    excerpt=excerpt,
                )

        # No match — classify as context
        if long_content:
            return ClassifierResult(
                task_class=TaskClass.NEEDS_REVIEW,
                confidence_band=ConfidenceBand.LOW,
                promotion_rec=PromotionRec.REVIEW,
                reason_code="long_content_no_signal",
                escalate_to_llm=True,
                excerpt=content[:120],
            )

        return ClassifierResult(
            task_class=TaskClass.CONTEXT,
            confidence_band=ConfidenceBand.HIGH,
            promotion_rec=PromotionRec.DAILY_ONLY,
            reason_code="no_task_signal",
            excerpt=content[:120],
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _excerpt(text: str, start: int, end: int, window: int = 60) -> str:
    left = max(0, start - window)
    right = min(len(text), end + window)
    return text[left:right].strip()


def _promotion_for(task_class: TaskClass, band: ConfidenceBand) -> PromotionRec:
    if task_class == TaskClass.CONTEXT:
        return PromotionRec.DAILY_ONLY
    if band == ConfidenceBand.HIGH and task_class == TaskClass.TASK:
        return PromotionRec.CURRENT_TASKS
    if band == ConfidenceBand.HIGH and task_class == TaskClass.FOLLOW_UP:
        return PromotionRec.DAILY_ONLY
    if band == ConfidenceBand.MEDIUM:
        return PromotionRec.DAILY_ONLY
    return PromotionRec.REVIEW


# Module-level default classifier instance for convenience
default_classifier = HeuristicClassifier()
