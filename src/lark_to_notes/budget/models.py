"""Data models for the LLM budget and performance-control layer.

This module defines the types used throughout the budget package:
routing decisions, fallback reasons, per-call usage records, configurable
policy, budget snapshots, content-cache keys, and feedback-derived quality
metrics.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class ProviderRoute(StrEnum):
    """Routing decision produced by the budget enforcer.

    Attributes:
        HEURISTICS_ONLY: Do not call the LLM; use heuristic result only.
        LLM_ESCALATE:    Call the LLM provider as requested.
        CACHE_HIT:       A cached LLM result is available; skip the provider.
    """

    HEURISTICS_ONLY = "heuristics_only"
    LLM_ESCALATE = "llm_escalate"
    CACHE_HIT = "cache_hit"


class FallbackReason(StrEnum):
    """Why an LLM escalation was suppressed.

    Attributes:
        NO_PROVIDER:       No LLM provider is configured.
        BUDGET_EXHAUSTED:  A cap (run or day) has been reached.
        PROVIDER_ERROR:    The provider returned an error or None.
        LATENCY_SPIKE:     Observed latency exceeded the configured threshold.
        CONTENT_CACHED:    A cached result satisfied the request.
        NOT_APPLICABLE:    No fallback occurred (escalation succeeded or was
                           not attempted).
    """

    NO_PROVIDER = "no_provider"
    BUDGET_EXHAUSTED = "budget_exhausted"
    PROVIDER_ERROR = "provider_error"
    LATENCY_SPIKE = "latency_spike"
    CONTENT_CACHED = "content_cached"
    NOT_APPLICABLE = "not_applicable"


@dataclass(frozen=True, slots=True)
class UsageRecord:
    """Audit record for a single LLM call or heuristics-only fallback.

    Attributes:
        call_id:          Stable UUID for this record.
        provider:         Provider identifier (e.g. ``"copilot"``), or
                          ``"heuristics"`` when no LLM was invoked.
        model:            Model identifier, or ``""`` for heuristics-only.
        prompt_tokens:    Tokens in the prompt, or 0 for heuristics-only.
        completion_tokens: Tokens in the completion, or 0 for heuristics-only.
        duration_ms:      Wall-clock time of the call in milliseconds.
        cached:           Whether the result came from the content cache.
        fallback:         Whether the call fell back to heuristics.
        fallback_reason:  Reason code if ``fallback`` is True.
        run_id:           Runtime run that triggered this call.
        source_id:        Watched-source the message belongs to.
        created_at:       ISO 8601 UTC timestamp.
    """

    call_id: str
    provider: str
    model: str
    prompt_tokens: int
    completion_tokens: int
    duration_ms: int
    cached: bool
    fallback: bool
    fallback_reason: FallbackReason
    run_id: str
    source_id: str
    created_at: str


@dataclass(frozen=True, slots=True)
class BudgetPolicy:
    """Configurable LLM budget and performance policy.

    All caps are enforced by :class:`~lark_to_notes.budget.policy.BudgetEnforcer`.
    Set a cap to ``0`` to disable it (no limit).

    Attributes:
        max_llm_calls_per_run:  Maximum LLM calls in a single runtime run.
        max_llm_calls_per_day:  Maximum LLM calls across all runs in a UTC day.
        max_tokens_per_run:     Maximum total tokens (prompt + completion) per run.
        max_tokens_per_day:     Maximum total tokens per UTC day.
        max_latency_ms:         Maximum acceptable call duration before a latency
                                spike is recorded; 0 disables threshold.
        cache_ttl_seconds:      How long content-cache entries remain valid.
                                0 means never cache.
        max_chunk_size_chars:   Maximum characters per text chunk when splitting
                                large documents.  0 disables chunking.
        batch_size:             Number of items to process per batch.
                                0 means unbounded.
    """

    max_llm_calls_per_run: int = 0
    max_llm_calls_per_day: int = 0
    max_tokens_per_run: int = 0
    max_tokens_per_day: int = 0
    max_latency_ms: int = 0
    cache_ttl_seconds: int = 3600
    max_chunk_size_chars: int = 8000
    batch_size: int = 50


@dataclass(frozen=True, slots=True)
class BudgetSnapshot:
    """Aggregated budget usage for a run or a UTC day.

    Attributes:
        scope:              ``"run:<run_id>"`` or ``"day:<YYYY-MM-DD>"``.
        call_count:         Total usage records in scope.
        prompt_tokens_sum:  Total prompt tokens consumed.
        completion_tokens_sum: Total completion tokens consumed.
        cached_count:       Records where ``cached=True``.
        fallback_count:     Records where ``fallback=True``.
        duration_ms_total:  Sum of all ``duration_ms`` values.
        p95_latency_ms:     95th-percentile latency in milliseconds, or
                            ``None`` if fewer than two records exist.
        cache_hit_rate:     ``cached_count / call_count`` (or 0.0 if no calls).
    """

    scope: str
    call_count: int
    prompt_tokens_sum: int
    completion_tokens_sum: int
    cached_count: int
    fallback_count: int
    duration_ms_total: int
    p95_latency_ms: float | None
    cache_hit_rate: float

    @property
    def total_tokens(self) -> int:
        """Sum of prompt and completion tokens."""
        return self.prompt_tokens_sum + self.completion_tokens_sum

    @property
    def net_llm_call_count(self) -> int:
        """Actual LLM calls (excluding cached hits and fallbacks).

        Only records with ``cached=False`` and ``fallback=False`` represent
        genuine LLM API calls that consume quota.
        """
        return max(0, self.call_count - self.fallback_count - self.cached_count)


@dataclass(frozen=True, slots=True)
class ContentHashKey:
    """Cache lookup key for a single LLM classification request.

    Attributes:
        content_hash:  SHA-256 hex digest of the normalised input text.
        model:         Model identifier the result was produced by.
    """

    content_hash: str
    model: str

    def cache_key(self) -> str:
        """Return a single string suitable for use as a DB primary key."""
        return f"{self.content_hash}:{self.model}"


@dataclass(frozen=True, slots=True)
class QualityMetrics:
    """Feedback-derived quality signals for tuning.

    Computed by aggregating :class:`~lark_to_notes.feedback.models.FeedbackAction`
    counts from the ``feedback_events`` table.

    Attributes:
        total_events:    Total feedback events in scope.
        confirm_count:   Events with action ``confirm``.
        dismiss_count:   Events with action ``dismiss``.
        duplicate_count: Events with action ``merge`` (duplicate surfaced).
        wrong_class_count: Events with action ``wrong_class``.
        missed_task_count: Events with action ``missed_task``.
        snooze_count:    Events with action ``snooze``.
        dismiss_rate:    ``dismiss_count / total_events`` (0.0 if no events).
        confirm_rate:    ``confirm_count / total_events`` (0.0 if no events).
        duplicate_rate:  ``duplicate_count / total_events`` (0.0 if no events).
        review_rate:     ``(wrong_class_count + missed_task_count) / total_events``
                         (0.0 if no events).
    """

    total_events: int
    confirm_count: int
    dismiss_count: int
    duplicate_count: int
    wrong_class_count: int
    missed_task_count: int
    snooze_count: int
    dismiss_rate: float = field(default=0.0)
    confirm_rate: float = field(default=0.0)
    duplicate_rate: float = field(default=0.0)
    review_rate: float = field(default=0.0)

    @classmethod
    def from_counts(
        cls,
        *,
        confirm: int = 0,
        dismiss: int = 0,
        merge: int = 0,
        wrong_class: int = 0,
        missed_task: int = 0,
        snooze: int = 0,
    ) -> QualityMetrics:
        """Build a :class:`QualityMetrics` instance from raw action counts.

        Args:
            confirm:     Count of ``confirm`` events.
            dismiss:     Count of ``dismiss`` events.
            merge:       Count of ``merge`` (duplicate) events.
            wrong_class: Count of ``wrong_class`` events.
            missed_task: Count of ``missed_task`` events.
            snooze:      Count of ``snooze`` events.

        Returns:
            A fully populated :class:`QualityMetrics` with rates computed.
        """
        total = confirm + dismiss + merge + wrong_class + missed_task + snooze
        if total == 0:
            return cls(
                total_events=0,
                confirm_count=0,
                dismiss_count=0,
                duplicate_count=0,
                wrong_class_count=0,
                missed_task_count=0,
                snooze_count=0,
            )
        return cls(
            total_events=total,
            confirm_count=confirm,
            dismiss_count=dismiss,
            duplicate_count=merge,
            wrong_class_count=wrong_class,
            missed_task_count=missed_task,
            snooze_count=snooze,
            dismiss_rate=round(dismiss / total, 4),
            confirm_rate=round(confirm / total, 4),
            duplicate_rate=round(merge / total, 4),
            review_rate=round((wrong_class + missed_task) / total, 4),
        )
