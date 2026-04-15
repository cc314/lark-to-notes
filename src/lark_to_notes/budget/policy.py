"""Budget enforcement: routing decisions and LLM-cap tracking.

:class:`BudgetEnforcer` is the single decision point that other layers
consult before making an LLM call.  It checks, in order:

1. Is a cached result available?  → ``CACHE_HIT``
2. Has the per-run cap been reached?  → ``HEURISTICS_ONLY``
3. Has the per-day cap been reached?  → ``HEURISTICS_ONLY``
4. Otherwise → ``LLM_ESCALATE``

Caps set to ``0`` in the :class:`~lark_to_notes.budget.models.BudgetPolicy`
are treated as disabled (no limit enforced).
"""

from __future__ import annotations

import logging
from dataclasses import asdict
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from lark_to_notes.budget.models import (
    BudgetPolicy,
    BudgetSnapshot,
    FallbackReason,
    ProviderRoute,
    QualityMetrics,
    UsageRecord,
)
from lark_to_notes.budget.store import (
    get_content_cache,
    get_day_budget_snapshot,
    get_run_budget_snapshot,
    put_content_cache,
    record_usage,
    rollup_quality_metrics,
)

if TYPE_CHECKING:
    import sqlite3

logger = logging.getLogger(__name__)


class BudgetEnforcer:
    """Stateless routing-decision helper backed by a live SQLite connection.

    Args:
        conn:   Open SQLite connection to the local store.
        policy: Budget policy with cap thresholds and cache settings.
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        policy: BudgetPolicy | None = None,
    ) -> None:
        self._conn = conn
        self._policy = policy or BudgetPolicy()

    @property
    def policy(self) -> BudgetPolicy:
        """The active budget policy."""
        return self._policy

    def should_escalate(
        self,
        *,
        run_id: str,
        cache_key: str | None = None,
    ) -> tuple[ProviderRoute, FallbackReason]:
        """Decide whether to call the LLM or stay in heuristics-only mode.

        Checks the content cache first, then per-run and per-day token/call
        caps in that order.

        Args:
            run_id:    Runtime run identifier used for per-run cap checks.
            cache_key: Optional content-hash key to check the result cache.
                       ``None`` means skip the cache check.

        Returns:
            A tuple of ``(ProviderRoute, FallbackReason)``.  When the
            route is :attr:`~ProviderRoute.LLM_ESCALATE` the fallback
            reason is :attr:`~FallbackReason.NOT_APPLICABLE`.
        """
        # 1. Content cache
        if cache_key is not None and self._policy.cache_ttl_seconds > 0:
            cached = get_content_cache(self._conn, cache_key)
            if cached is not None:
                logger.debug(
                    "budget_route_cache_hit",
                    extra={"run_id": run_id, "cache_key": cache_key},
                )
                return ProviderRoute.CACHE_HIT, FallbackReason.CONTENT_CACHED

        today = datetime.now(UTC).strftime("%Y-%m-%d")

        # 2. Per-run caps
        if self._policy.max_llm_calls_per_run > 0 or self._policy.max_tokens_per_run > 0:
            run_snap = get_run_budget_snapshot(self._conn, run_id)
            reason = _check_snap_against_policy(run_snap, self._policy, scope="run")
            if reason is not None:
                logger.info(
                    "budget_route_heuristics_only",
                    extra={"run_id": run_id, "scope": "run", "reason": reason},
                )
                return ProviderRoute.HEURISTICS_ONLY, reason

        # 3. Per-day caps
        if self._policy.max_llm_calls_per_day > 0 or self._policy.max_tokens_per_day > 0:
            day_snap = get_day_budget_snapshot(self._conn, today)
            reason = _check_snap_against_policy(day_snap, self._policy, scope="day")
            if reason is not None:
                logger.info(
                    "budget_route_heuristics_only",
                    extra={"run_id": run_id, "scope": "day", "today": today, "reason": reason},
                )
                return ProviderRoute.HEURISTICS_ONLY, reason

        logger.debug("budget_route_llm_escalate", extra={"run_id": run_id})
        return ProviderRoute.LLM_ESCALATE, FallbackReason.NOT_APPLICABLE

    def get_quality_metrics(self) -> QualityMetrics:
        """Return aggregated feedback quality metrics from the local store.

        Returns:
            A :class:`~lark_to_notes.budget.models.QualityMetrics` instance
            summarising all feedback events in the database.
        """
        return rollup_quality_metrics(self._conn)

    def get_run_snapshot(self, run_id: str) -> BudgetSnapshot:
        """Return the budget snapshot for a specific runtime run.

        Args:
            run_id: Runtime run identifier.

        Returns:
            Aggregated :class:`~lark_to_notes.budget.models.BudgetSnapshot`
            scoped to the run.
        """
        return get_run_budget_snapshot(self._conn, run_id)

    def get_day_snapshot(self, date_str: str) -> BudgetSnapshot:
        """Return the budget snapshot for a specific UTC date.

        Args:
            date_str: UTC date in ``YYYY-MM-DD`` format.

        Returns:
            Aggregated :class:`~lark_to_notes.budget.models.BudgetSnapshot`
            scoped to the date.
        """
        return get_day_budget_snapshot(self._conn, date_str)

    def get_cached_result(self, cache_key: str) -> str | None:
        """Return a cached classification payload if it is still valid."""
        return get_content_cache(self._conn, cache_key)

    def put_cached_result(self, cache_key: str, result_json: str) -> None:
        """Persist a cached classification payload when caching is enabled."""
        if self._policy.cache_ttl_seconds <= 0:
            return
        put_content_cache(
            self._conn,
            cache_key,
            result_json,
            ttl_seconds=self._policy.cache_ttl_seconds,
        )

    def record_usage(self, record: UsageRecord) -> None:
        """Persist one usage record for routing and operator inspection."""
        payload = asdict(record)
        payload["fallback_reason"] = str(record.fallback_reason)
        record_usage(self._conn, payload)


def _check_snap_against_policy(
    snap: BudgetSnapshot,
    policy: BudgetPolicy,
    scope: str,
) -> FallbackReason | None:
    """Return :attr:`~FallbackReason.BUDGET_EXHAUSTED` if a cap is exceeded.

    Args:
        snap:   Budget snapshot to evaluate.
        policy: Policy thresholds to compare against.
        scope:  ``"run"`` or ``"day"`` for log context.

    Returns:
        :attr:`~FallbackReason.BUDGET_EXHAUSTED` if any active cap is
        exhausted; ``None`` otherwise.
    """
    call_cap = policy.max_llm_calls_per_run if scope == "run" else policy.max_llm_calls_per_day
    token_cap = policy.max_tokens_per_run if scope == "run" else policy.max_tokens_per_day

    if call_cap > 0 and snap.net_llm_call_count >= call_cap:
        logger.debug(
            "budget_cap_exceeded",
            extra={
                "scope": scope,
                "cap_type": "calls",
                "net_llm_calls": snap.net_llm_call_count,
                "cap": call_cap,
            },
        )
        return FallbackReason.BUDGET_EXHAUSTED

    if token_cap > 0 and snap.total_tokens >= token_cap:
        logger.debug(
            "budget_cap_exceeded",
            extra={
                "scope": scope,
                "cap_type": "tokens",
                "total": snap.total_tokens,
                "cap": token_cap,
            },
        )
        return FallbackReason.BUDGET_EXHAUSTED

    return None
