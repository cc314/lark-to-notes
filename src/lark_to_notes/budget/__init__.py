"""LLM budget and performance-control layer for lark-to-notes.

Public API:

- :class:`~lark_to_notes.budget.models.BudgetPolicy` — configurable caps
- :class:`~lark_to_notes.budget.models.BudgetSnapshot` — per-run/day stats
- :class:`~lark_to_notes.budget.models.ContentHashKey` — cache lookup key
- :class:`~lark_to_notes.budget.models.FallbackReason` — why LLM was skipped
- :class:`~lark_to_notes.budget.models.ProviderRoute` — routing decision
- :class:`~lark_to_notes.budget.models.QualityMetrics` — feedback-derived
- :class:`~lark_to_notes.budget.models.UsageRecord` — per-call audit row
- :class:`~lark_to_notes.budget.policy.BudgetEnforcer` — decision engine
- :func:`~lark_to_notes.budget.chunking.chunk_text` — large-doc splitter
- :func:`~lark_to_notes.budget.chunking.coalesce_batch` — batch partitioner
- :class:`~lark_to_notes.budget.chunking.ContentHasher` — stable hash
- :func:`~lark_to_notes.budget.store.get_content_cache` — cache fetch
- :func:`~lark_to_notes.budget.store.get_day_budget_snapshot` — day stats
- :func:`~lark_to_notes.budget.store.get_run_budget_snapshot` — run stats
- :func:`~lark_to_notes.budget.store.put_content_cache` — cache write
- :func:`~lark_to_notes.budget.store.record_usage` — usage write
- :func:`~lark_to_notes.budget.store.rollup_quality_metrics` — quality stats
"""

from __future__ import annotations

from lark_to_notes.budget.chunking import ContentHasher, chunk_text, coalesce_batch
from lark_to_notes.budget.models import (
    BudgetPolicy,
    BudgetSnapshot,
    ContentHashKey,
    FallbackReason,
    ProviderRoute,
    QualityMetrics,
    UsageRecord,
)
from lark_to_notes.budget.policy import BudgetEnforcer
from lark_to_notes.budget.store import (
    get_content_cache,
    get_day_budget_snapshot,
    get_run_budget_snapshot,
    put_content_cache,
    record_usage,
    rollup_quality_metrics,
)

__all__ = [
    "BudgetEnforcer",
    "BudgetPolicy",
    "BudgetSnapshot",
    "ContentHashKey",
    "ContentHasher",
    "FallbackReason",
    "ProviderRoute",
    "QualityMetrics",
    "UsageRecord",
    "chunk_text",
    "coalesce_batch",
    "get_content_cache",
    "get_day_budget_snapshot",
    "get_run_budget_snapshot",
    "put_content_cache",
    "record_usage",
    "rollup_quality_metrics",
]
