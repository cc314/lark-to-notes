"""SQLite persistence for LLM budget tracking and content-result caching.

Provides:
- :func:`record_usage` — append a :class:`~lark_to_notes.budget.models.UsageRecord`
- :func:`get_run_budget_snapshot` — aggregate stats for a single runtime run
- :func:`get_day_budget_snapshot` — aggregate stats across a UTC calendar day
- :func:`put_content_cache` — store an LLM result keyed by content hash
- :func:`get_content_cache` — retrieve a cached result or ``None`` if stale
- :func:`rollup_quality_metrics` — aggregate feedback-event action counts
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from lark_to_notes.budget.models import BudgetSnapshot, QualityMetrics

if TYPE_CHECKING:
    import sqlite3

logger = logging.getLogger(__name__)


def record_usage(conn: sqlite3.Connection, record_row: dict[str, object]) -> None:
    """Insert a usage record into ``llm_usage_records``.

    The function is idempotent: duplicate ``call_id`` values are silently
    ignored via ``INSERT OR IGNORE``.

    Args:
        conn:       Open SQLite connection.
        record_row: Mapping with keys matching the ``llm_usage_records``
                    columns.  See schema V6 for the full column list.
    """
    conn.execute(
        """
        INSERT OR IGNORE INTO llm_usage_records (
            call_id, provider, model,
            prompt_tokens, completion_tokens, duration_ms,
            cached, fallback, fallback_reason,
            run_id, source_id, created_at
        ) VALUES (
            :call_id, :provider, :model,
            :prompt_tokens, :completion_tokens, :duration_ms,
            :cached, :fallback, :fallback_reason,
            :run_id, :source_id, :created_at
        )
        """,
        record_row,
    )
    logger.debug(
        "budget_usage_recorded",
        extra={
            "call_id": record_row.get("call_id"),
            "provider": record_row.get("provider"),
            "fallback": record_row.get("fallback"),
            "cached": record_row.get("cached"),
            "run_id": record_row.get("run_id"),
        },
    )


def get_run_budget_snapshot(
    conn: sqlite3.Connection,
    run_id: str,
) -> BudgetSnapshot:
    """Return aggregated budget stats for a single runtime run.

    Args:
        conn:   Open SQLite connection.
        run_id: Runtime run identifier to aggregate.

    Returns:
        A :class:`~lark_to_notes.budget.models.BudgetSnapshot` scoped to
        the given run.
    """
    return _aggregate_snapshot(conn, scope=f"run:{run_id}", where="run_id = ?", params=(run_id,))


def get_day_budget_snapshot(
    conn: sqlite3.Connection,
    date_str: str,
) -> BudgetSnapshot:
    """Return aggregated budget stats across a UTC calendar day.

    Args:
        conn:     Open SQLite connection.
        date_str: UTC date in ``YYYY-MM-DD`` format.

    Returns:
        A :class:`~lark_to_notes.budget.models.BudgetSnapshot` scoped to
        the given date.
    """
    return _aggregate_snapshot(
        conn,
        scope=f"day:{date_str}",
        where="substr(created_at, 1, 10) = ?",
        params=(date_str,),
    )


def _aggregate_snapshot(
    conn: sqlite3.Connection,
    *,
    scope: str,
    where: str,
    params: tuple[object, ...],
) -> BudgetSnapshot:
    """Internal helper: run the aggregation query and build a snapshot.

    p95 latency is computed by sorting all ``duration_ms`` values and
    selecting the value at the 95th percentile position.
    """
    row = conn.execute(
        f"""
        SELECT
            COUNT(*)                          AS call_count,
            COALESCE(SUM(prompt_tokens), 0)   AS prompt_tokens_sum,
            COALESCE(SUM(completion_tokens), 0) AS completion_tokens_sum,
            COALESCE(SUM(CASE WHEN cached=1 THEN 1 ELSE 0 END), 0) AS cached_count,
            COALESCE(SUM(CASE WHEN fallback=1 THEN 1 ELSE 0 END), 0) AS fallback_count,
            COALESCE(SUM(duration_ms), 0)     AS duration_ms_total
        FROM llm_usage_records
        WHERE {where}
        """,  # noqa: S608
        params,
    ).fetchone()

    call_count = row[0]
    prompt_tokens_sum = row[1]
    completion_tokens_sum = row[2]
    cached_count = row[3]
    fallback_count = row[4]
    duration_ms_total = row[5]

    # Compute p95 latency via row-number approach
    p95_latency_ms: float | None = None
    if call_count >= 2:
        p95_index = max(0, int(call_count * 0.95) - 1)
        p95_row = conn.execute(
            f"""
            SELECT duration_ms
            FROM llm_usage_records
            WHERE {where}
            ORDER BY duration_ms
            LIMIT 1 OFFSET ?
            """,  # noqa: S608
            (*params, p95_index),
        ).fetchone()
        if p95_row is not None:
            p95_latency_ms = float(p95_row[0])

    cache_hit_rate = cached_count / call_count if call_count > 0 else 0.0

    return BudgetSnapshot(
        scope=scope,
        call_count=call_count,
        prompt_tokens_sum=prompt_tokens_sum,
        completion_tokens_sum=completion_tokens_sum,
        cached_count=cached_count,
        fallback_count=fallback_count,
        duration_ms_total=duration_ms_total,
        p95_latency_ms=p95_latency_ms,
        cache_hit_rate=round(cache_hit_rate, 4),
    )


def put_content_cache(
    conn: sqlite3.Connection,
    cache_key: str,
    result_json: str,
    ttl_seconds: int,
) -> None:
    """Store or refresh a content-cache entry.

    Args:
        conn:        Open SQLite connection.
        cache_key:   Stable cache key from
                     :meth:`~lark_to_notes.budget.models.ContentHashKey.cache_key`.
        result_json: JSON-serialised LLM result to store.
        ttl_seconds: Seconds from now until the entry expires.
    """
    conn.execute(
        """
        INSERT OR REPLACE INTO content_cache (cache_key, result_json, expires_at)
        VALUES (
            ?,
            ?,
            strftime('%Y-%m-%dT%H:%M:%SZ', datetime('now', '+' || ? || ' seconds'))
        )
        """,
        (cache_key, result_json, ttl_seconds),
    )
    logger.debug("content_cache_put", extra={"cache_key": cache_key, "ttl_seconds": ttl_seconds})


def get_content_cache(
    conn: sqlite3.Connection,
    cache_key: str,
) -> str | None:
    """Fetch a cached LLM result if it has not expired.

    Args:
        conn:      Open SQLite connection.
        cache_key: Stable cache key to look up.

    Returns:
        The stored JSON string, or ``None`` if the entry is missing or stale.
    """
    row = conn.execute(
        """
        SELECT result_json
        FROM content_cache
        WHERE cache_key = ?
          AND expires_at > strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
        """,
        (cache_key,),
    ).fetchone()
    if row is None:
        logger.debug("content_cache_miss", extra={"cache_key": cache_key})
        return None
    logger.debug("content_cache_hit", extra={"cache_key": cache_key})
    return str(row[0])


def rollup_quality_metrics(conn: sqlite3.Connection) -> QualityMetrics:
    """Aggregate feedback-event action counts into quality-metric rates.

    Queries the ``feedback_events`` table and computes the per-action
    counts used as tuning signals.

    Args:
        conn: Open SQLite connection (must include the V5 schema).

    Returns:
        A :class:`~lark_to_notes.budget.models.QualityMetrics` instance
        with rates derived from the total event count.
    """
    rows = conn.execute(
        """
        SELECT action, COUNT(*) AS cnt
        FROM feedback_events
        GROUP BY action
        """
    ).fetchall()

    counts: dict[str, int] = {str(r[0]): int(r[1]) for r in rows}
    return QualityMetrics.from_counts(
        confirm=counts.get("confirm", 0),
        dismiss=counts.get("dismiss", 0),
        merge=counts.get("merge", 0),
        wrong_class=counts.get("wrong_class", 0),
        missed_task=counts.get("missed_task", 0),
        snooze=counts.get("snooze", 0),
    )
