"""Data models for the distillation layer.

Each raw message is classified into a :class:`TaskClass`, assigned a
:class:`ConfidenceBand`, and given a :class:`PromotionRec` that governs
where (or whether) a candidate task is surfaced to the operator.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class TaskClass(StrEnum):
    """Broad classification of a candidate action item."""

    CONTEXT = "context"
    """Informational content; no action required."""

    FOLLOW_UP = "follow_up"
    """Implicit follow-up cue; soft action expected."""

    TASK = "task"
    """Explicit task, request, or action item."""

    NEEDS_REVIEW = "needs_review"
    """Ambiguous or uncertain; requires human review before promotion."""


class ConfidenceBand(StrEnum):
    """Classifier confidence in the emitted :class:`TaskClass`."""

    HIGH = "high"
    """Strong deterministic signal; safe to promote without review."""

    MEDIUM = "medium"
    """Heuristic match with moderate confidence; optional review."""

    LOW = "low"
    """Weak signal; escalate to LLM or route to review lane."""


class PromotionRec(StrEnum):
    """Where a candidate task should be surfaced."""

    CURRENT_TASKS = "current_tasks"
    """High-confidence task; promote directly to Current Tasks."""

    DAILY_ONLY = "daily_only"
    """Capture in the daily note only; do not promote."""

    REVIEW = "review"
    """Route to the ``needs_review`` lane for human triage."""


@dataclass(frozen=True, slots=True)
class ClassifierResult:
    """The output of one classification pass on a single message.

    Attributes:
        task_class:      Broad classification of the message content.
        confidence_band: How confident the classifier is.
        promotion_rec:   Recommended promotion destination.
        reason_code:     Short machine-readable code explaining the decision.
        matched_signal:  The specific pattern or keyword that triggered
                         classification, if any.
        escalate_to_llm: Whether this result should be re-evaluated by an
                         LLM before being accepted.
        excerpt:         A short excerpt of the text that drove classification.
    """

    task_class: TaskClass
    confidence_band: ConfidenceBand
    promotion_rec: PromotionRec
    reason_code: str
    matched_signal: str | None = None
    escalate_to_llm: bool = False
    excerpt: str = ""

    @property
    def needs_review(self) -> bool:
        """Return True if this result should land in the review lane."""
        return (
            self.task_class == TaskClass.NEEDS_REVIEW
            or self.confidence_band == ConfidenceBand.LOW
            or self.promotion_rec == PromotionRec.REVIEW
        )


@dataclass(frozen=True, slots=True)
class DistillInput:
    """Minimal fields required to classify one raw message.

    Attributes:
        message_id:  Stable message identifier (Lark ``om_…`` ID).
        source_id:   Watched-source identifier the message belongs to.
        source_type: Raw ``source_type`` value from the Lark payload.
        content:     Full text of the message.
        sender_name: Display name of the sender.
        direction:   ``"incoming"`` or ``"outgoing"``.
        created_at:  Original timestamp string from the raw record.
        extra_context: Optional surrounding context for multi-turn window.
    """

    message_id: str
    source_id: str
    source_type: str
    content: str
    sender_name: str
    direction: str
    created_at: str
    extra_context: list[str] = field(default_factory=list)
