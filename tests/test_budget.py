"""Tests for the lark_to_notes.budget package.

Coverage:
- models: BudgetPolicy defaults, BudgetSnapshot.total_tokens, QualityMetrics.from_counts,
  ContentHashKey.cache_key, ProviderRoute/FallbackReason enum values, UsageRecord
- store: record_usage (idempotent), run snapshot aggregation, day snapshot aggregation,
  p95 latency, cache_hit_rate, content_cache put/get/expiry, rollup_quality_metrics
- policy: BudgetEnforcer routes (cache_hit, llm_escalate, heuristics_only per-run/day call cap,
  heuristics_only per-run/day token cap)
- chunking: chunk_text basic, short text, newline-break preference, zero max_chars,
  coalesce_batch basic, empty list, zero batch_size, ContentHasher stable hash,
  ContentHasher normalisation
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from lark_to_notes.budget import (
    BudgetEnforcer,
    BudgetPolicy,
    BudgetSnapshot,
    ContentHasher,
    ContentHashKey,
    FallbackReason,
    ProviderRoute,
    QualityMetrics,
    UsageRecord,
    chunk_text,
    coalesce_batch,
    get_content_cache,
    get_day_budget_snapshot,
    get_run_budget_snapshot,
    put_content_cache,
    record_usage,
    rollup_quality_metrics,
)
from lark_to_notes.storage.db import init_db

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def conn(tmp_path: Path) -> sqlite3.Connection:
    """Open an in-memory-equivalent DB with the full schema applied."""
    db_path = tmp_path / "test.db"
    c = sqlite3.connect(str(db_path))
    c.row_factory = sqlite3.Row
    init_db(c)
    return c


def _make_usage(
    call_id: str,
    *,
    run_id: str = "run-1",
    provider: str = "copilot",
    model: str = "gpt-4o",
    prompt_tokens: int = 100,
    completion_tokens: int = 50,
    duration_ms: int = 200,
    cached: bool = False,
    fallback: bool = False,
    fallback_reason: FallbackReason = FallbackReason.NOT_APPLICABLE,
    source_id: str = "dm:test",
    created_at: str = "2026-04-14T09:00:00Z",
) -> dict[str, object]:
    return {
        "call_id": call_id,
        "provider": provider,
        "model": model,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "duration_ms": duration_ms,
        "cached": 1 if cached else 0,
        "fallback": 1 if fallback else 0,
        "fallback_reason": fallback_reason,
        "run_id": run_id,
        "source_id": source_id,
        "created_at": created_at,
    }


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


def test_budget_policy_defaults() -> None:
    p = BudgetPolicy()
    assert p.max_llm_calls_per_run == 0
    assert p.max_llm_calls_per_day == 0
    assert p.max_tokens_per_run == 0
    assert p.max_tokens_per_day == 0
    assert p.cache_ttl_seconds == 3600
    assert p.max_chunk_size_chars == 8000
    assert p.batch_size == 50


def test_budget_snapshot_total_tokens() -> None:
    snap = BudgetSnapshot(
        scope="run:r1",
        call_count=2,
        prompt_tokens_sum=300,
        completion_tokens_sum=150,
        cached_count=0,
        fallback_count=0,
        duration_ms_total=400,
        p95_latency_ms=None,
        cache_hit_rate=0.0,
    )
    assert snap.total_tokens == 450


def test_quality_metrics_from_counts_all_zero() -> None:
    m = QualityMetrics.from_counts()
    assert m.total_events == 0
    assert m.dismiss_rate == 0.0
    assert m.confirm_rate == 0.0
    assert m.duplicate_rate == 0.0
    assert m.review_rate == 0.0


def test_quality_metrics_from_counts_rates() -> None:
    m = QualityMetrics.from_counts(confirm=4, dismiss=2, merge=2, wrong_class=1, missed_task=1)
    assert m.total_events == 10
    assert m.confirm_count == 4
    assert m.dismiss_count == 2
    assert m.duplicate_count == 2
    assert m.wrong_class_count == 1
    assert m.missed_task_count == 1
    assert m.confirm_rate == pytest.approx(0.4)
    assert m.dismiss_rate == pytest.approx(0.2)
    assert m.duplicate_rate == pytest.approx(0.2)
    assert m.review_rate == pytest.approx(0.2)


def test_content_hash_key_cache_key() -> None:
    key = ContentHashKey(content_hash="abc123", model="gpt-4o")
    assert key.cache_key() == "abc123:gpt-4o"


def test_usage_record_fields() -> None:
    rec = UsageRecord(
        call_id="c1",
        provider="copilot",
        model="gpt-4o",
        prompt_tokens=10,
        completion_tokens=5,
        duration_ms=100,
        cached=False,
        fallback=False,
        fallback_reason=FallbackReason.NOT_APPLICABLE,
        run_id="r1",
        source_id="dm:x",
        created_at="2026-04-14T09:00:00Z",
    )
    assert rec.provider == "copilot"
    assert rec.fallback is False


def test_provider_route_enum_values() -> None:
    assert ProviderRoute.HEURISTICS_ONLY == "heuristics_only"
    assert ProviderRoute.LLM_ESCALATE == "llm_escalate"
    assert ProviderRoute.CACHE_HIT == "cache_hit"


def test_fallback_reason_enum_values() -> None:
    assert FallbackReason.BUDGET_EXHAUSTED == "budget_exhausted"
    assert FallbackReason.CONTENT_CACHED == "content_cached"
    assert FallbackReason.NOT_APPLICABLE == "not_applicable"


# ---------------------------------------------------------------------------
# Store tests
# ---------------------------------------------------------------------------


def test_record_usage_stored(conn: sqlite3.Connection) -> None:
    row = _make_usage("c1")
    record_usage(conn, row)
    result = conn.execute(
        "SELECT call_id, prompt_tokens, completion_tokens FROM llm_usage_records"
    ).fetchone()
    assert result["call_id"] == "c1"
    assert result["prompt_tokens"] == 100
    assert result["completion_tokens"] == 50


def test_record_usage_idempotent(conn: sqlite3.Connection) -> None:
    row = _make_usage("c-idem")
    record_usage(conn, row)
    record_usage(conn, row)  # should not raise or duplicate
    count = conn.execute(
        "SELECT COUNT(*) FROM llm_usage_records WHERE call_id='c-idem'"
    ).fetchone()[0]
    assert count == 1


def test_run_budget_snapshot_empty(conn: sqlite3.Connection) -> None:
    snap = get_run_budget_snapshot(conn, "nonexistent")
    assert snap.call_count == 0
    assert snap.total_tokens == 0
    assert snap.cache_hit_rate == 0.0
    assert snap.p95_latency_ms is None


def test_run_budget_snapshot_sums(conn: sqlite3.Connection) -> None:
    for i in range(3):
        record_usage(
            conn,
            _make_usage(
                f"c{i}",
                run_id="run-sum",
                prompt_tokens=100,
                completion_tokens=50,
                duration_ms=100 * (i + 1),
            ),
        )
    snap = get_run_budget_snapshot(conn, "run-sum")
    assert snap.call_count == 3
    assert snap.prompt_tokens_sum == 300
    assert snap.completion_tokens_sum == 150
    assert snap.total_tokens == 450
    assert snap.duration_ms_total == 600


def test_run_budget_snapshot_cache_hit_rate(conn: sqlite3.Connection) -> None:
    record_usage(conn, _make_usage("c-hit", run_id="run-cache", cached=True))
    record_usage(conn, _make_usage("c-miss", run_id="run-cache", cached=False))
    snap = get_run_budget_snapshot(conn, "run-cache")
    assert snap.cache_hit_rate == pytest.approx(0.5)
    assert snap.cached_count == 1


def test_day_budget_snapshot_sums(conn: sqlite3.Connection) -> None:
    for i in range(4):
        record_usage(
            conn,
            _make_usage(
                f"day-c{i}",
                run_id=f"run-{i}",
                created_at="2026-04-14T10:00:00Z",
                prompt_tokens=50,
                completion_tokens=25,
            ),
        )
    # Different day — should NOT be included
    record_usage(
        conn,
        _make_usage("other-day", run_id="other", created_at="2026-04-15T10:00:00Z"),
    )
    snap = get_day_budget_snapshot(conn, "2026-04-14")
    assert snap.call_count == 4
    assert snap.prompt_tokens_sum == 200
    assert snap.scope == "day:2026-04-14"


def test_p95_latency_computed(conn: sqlite3.Connection) -> None:
    durations = [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]
    for i, d in enumerate(durations):
        record_usage(
            conn,
            _make_usage(f"lat-{i}", run_id="run-lat", duration_ms=d),
        )
    snap = get_run_budget_snapshot(conn, "run-lat")
    # 10 records → p95 index = int(10 * 0.95) - 1 = 8 → value at sorted pos 8 = 900
    assert snap.p95_latency_ms == pytest.approx(900.0)


def test_p95_latency_none_for_single_record(conn: sqlite3.Connection) -> None:
    record_usage(conn, _make_usage("single", run_id="run-single", duration_ms=500))
    snap = get_run_budget_snapshot(conn, "run-single")
    assert snap.p95_latency_ms is None


def test_content_cache_put_and_get(conn: sqlite3.Connection) -> None:
    put_content_cache(conn, "key1", '{"class": "action"}', ttl_seconds=3600)
    result = get_content_cache(conn, "key1")
    assert result == '{"class": "action"}'


def test_content_cache_miss(conn: sqlite3.Connection) -> None:
    result = get_content_cache(conn, "nonexistent")
    assert result is None


def test_content_cache_expired(conn: sqlite3.Connection) -> None:
    # Insert with 0-second TTL so it's immediately expired
    put_content_cache(conn, "expired-key", '{"class": "stale"}', ttl_seconds=0)
    result = get_content_cache(conn, "expired-key")
    # Should be None since expires_at <= now
    assert result is None


def test_content_cache_replace(conn: sqlite3.Connection) -> None:
    put_content_cache(conn, "rep-key", '{"v": 1}', ttl_seconds=3600)
    put_content_cache(conn, "rep-key", '{"v": 2}', ttl_seconds=3600)
    result = get_content_cache(conn, "rep-key")
    assert result == '{"v": 2}'


def test_rollup_quality_metrics_empty(conn: sqlite3.Connection) -> None:
    metrics = rollup_quality_metrics(conn)
    assert metrics.total_events == 0
    assert metrics.dismiss_rate == 0.0


def test_rollup_quality_metrics_with_events(conn: sqlite3.Connection) -> None:
    # Insert feedback_events directly
    import uuid

    events = [
        ("confirm", "task", "t1"),
        ("confirm", "task", "t2"),
        ("dismiss", "task", "t3"),
        ("merge", "task", "t4"),
        ("wrong_class", "task", "t5"),
    ]
    for action, target_type, target_id in events:
        conn.execute(
            """
            INSERT INTO feedback_events (
                feedback_id, target_type, target_id, action, artifact_path
            ) VALUES (?, ?, ?, ?, '')
            """,
            (str(uuid.uuid4()), target_type, target_id, action),
        )
    conn.commit()
    metrics = rollup_quality_metrics(conn)
    assert metrics.total_events == 5
    assert metrics.confirm_count == 2
    assert metrics.dismiss_count == 1
    assert metrics.duplicate_count == 1
    assert metrics.wrong_class_count == 1
    assert metrics.confirm_rate == pytest.approx(0.4)
    assert metrics.dismiss_rate == pytest.approx(0.2)
    assert metrics.review_rate == pytest.approx(0.2)


# ---------------------------------------------------------------------------
# BudgetEnforcer (policy) tests
# ---------------------------------------------------------------------------


def test_enforcer_routes_llm_escalate_when_no_caps(conn: sqlite3.Connection) -> None:
    enforcer = BudgetEnforcer(conn, BudgetPolicy())
    route, reason = enforcer.should_escalate(run_id="r1")
    assert route == ProviderRoute.LLM_ESCALATE
    assert reason == FallbackReason.NOT_APPLICABLE


def test_enforcer_routes_cache_hit(conn: sqlite3.Connection) -> None:
    put_content_cache(conn, "k1:gpt-4o", '{"ok": true}', ttl_seconds=3600)
    enforcer = BudgetEnforcer(conn, BudgetPolicy(cache_ttl_seconds=3600))
    route, reason = enforcer.should_escalate(run_id="r1", cache_key="k1:gpt-4o")
    assert route == ProviderRoute.CACHE_HIT
    assert reason == FallbackReason.CONTENT_CACHED


def test_enforcer_no_cache_when_ttl_zero(conn: sqlite3.Connection) -> None:
    put_content_cache(conn, "k2:gpt-4o", '{"ok": true}', ttl_seconds=3600)
    enforcer = BudgetEnforcer(conn, BudgetPolicy(cache_ttl_seconds=0))
    route, _ = enforcer.should_escalate(run_id="r1", cache_key="k2:gpt-4o")
    # ttl=0 disables cache → should not return cache hit
    assert route != ProviderRoute.CACHE_HIT


def test_enforcer_run_call_cap_exhausted(conn: sqlite3.Connection) -> None:
    policy = BudgetPolicy(max_llm_calls_per_run=2)
    enforcer = BudgetEnforcer(conn, policy)
    for i in range(2):
        record_usage(conn, _make_usage(f"cap-c{i}", run_id="run-cap"))
    route, reason = enforcer.should_escalate(run_id="run-cap")
    assert route == ProviderRoute.HEURISTICS_ONLY
    assert reason == FallbackReason.BUDGET_EXHAUSTED


def test_enforcer_run_call_cap_not_yet_exhausted(conn: sqlite3.Connection) -> None:
    policy = BudgetPolicy(max_llm_calls_per_run=5)
    enforcer = BudgetEnforcer(conn, policy)
    record_usage(conn, _make_usage("cap-c0", run_id="run-ok"))
    route, _ = enforcer.should_escalate(run_id="run-ok")
    assert route == ProviderRoute.LLM_ESCALATE


def test_enforcer_run_token_cap_exhausted(conn: sqlite3.Connection) -> None:
    policy = BudgetPolicy(max_tokens_per_run=100)
    enforcer = BudgetEnforcer(conn, policy)
    record_usage(
        conn,
        _make_usage("tok-c0", run_id="run-tok", prompt_tokens=80, completion_tokens=30),
    )
    route, reason = enforcer.should_escalate(run_id="run-tok")
    assert route == ProviderRoute.HEURISTICS_ONLY
    assert reason == FallbackReason.BUDGET_EXHAUSTED


def test_enforcer_day_call_cap_exhausted(conn: sqlite3.Connection) -> None:
    from datetime import UTC, datetime

    today = datetime.now(UTC).strftime("%Y-%m-%d")
    today_ts = f"{today}T08:00:00Z"
    policy = BudgetPolicy(max_llm_calls_per_day=2)
    enforcer = BudgetEnforcer(conn, policy)
    for i in range(2):
        record_usage(
            conn,
            _make_usage(f"day-cap-{i}", run_id=f"run-d{i}", created_at=today_ts),
        )
    route, reason = enforcer.should_escalate(run_id="run-new")
    assert route == ProviderRoute.HEURISTICS_ONLY
    assert reason == FallbackReason.BUDGET_EXHAUSTED


def test_enforcer_cache_hits_dont_consume_call_cap(conn: sqlite3.Connection) -> None:
    """Cache hits and fallbacks must not count against the LLM call cap."""
    policy = BudgetPolicy(max_llm_calls_per_run=2)
    enforcer = BudgetEnforcer(conn, policy)
    # Record 2 cache hits — these should NOT consume the cap
    for i in range(2):
        record_usage(conn, _make_usage(f"ch-{i}", run_id="run-ch", cached=True))
    route, _ = enforcer.should_escalate(run_id="run-ch")
    assert route == ProviderRoute.LLM_ESCALATE  # cap not consumed by cache hits


def test_enforcer_fallbacks_dont_consume_call_cap(conn: sqlite3.Connection) -> None:
    """Fallback records (heuristics-only) must not count against the LLM call cap."""
    policy = BudgetPolicy(max_llm_calls_per_run=2)
    enforcer = BudgetEnforcer(conn, policy)
    for i in range(2):
        record_usage(
            conn,
            _make_usage(
                f"fb-{i}",
                run_id="run-fb",
                fallback=True,
                fallback_reason=FallbackReason.BUDGET_EXHAUSTED,
            ),
        )
    route, _ = enforcer.should_escalate(run_id="run-fb")
    assert route == ProviderRoute.LLM_ESCALATE  # cap not consumed by fallbacks


def test_enforcer_get_quality_metrics(conn: sqlite3.Connection) -> None:
    enforcer = BudgetEnforcer(conn, BudgetPolicy())
    metrics = enforcer.get_quality_metrics()
    assert metrics.total_events == 0


def test_enforcer_get_run_snapshot(conn: sqlite3.Connection) -> None:
    record_usage(conn, _make_usage("snap-c1", run_id="run-snap"))
    enforcer = BudgetEnforcer(conn, BudgetPolicy())
    snap = enforcer.get_run_snapshot("run-snap")
    assert snap.call_count == 1
    assert snap.scope == "run:run-snap"


def test_enforcer_get_day_snapshot(conn: sqlite3.Connection) -> None:
    record_usage(
        conn,
        _make_usage("day-snap-c1", run_id="r1", created_at="2026-04-14T09:00:00Z"),
    )
    enforcer = BudgetEnforcer(conn, BudgetPolicy())
    snap = enforcer.get_day_snapshot("2026-04-14")
    assert snap.call_count == 1
    assert snap.scope == "day:2026-04-14"


# ---------------------------------------------------------------------------
# Chunking tests
# ---------------------------------------------------------------------------


def test_chunk_text_short_returns_single() -> None:
    chunks = chunk_text("Hello world", max_chars=100)
    assert chunks == ["Hello world"]


def test_chunk_text_basic_split() -> None:
    text = "A" * 100
    chunks = chunk_text(text, max_chars=30)
    assert len(chunks) == 4  # 30+30+30+10
    for c in chunks:
        assert len(c) <= 30


def test_chunk_text_prefers_newline_break() -> None:
    text = "line one\nline two\nline three\nfour five six seven eight nine"
    chunks = chunk_text(text, max_chars=20)
    # Each chunk should respect the newline preference
    for c in chunks:
        assert len(c) <= 20


def test_chunk_text_zero_max_returns_whole() -> None:
    text = "A" * 200
    chunks = chunk_text(text, max_chars=0)
    assert chunks == [text]


def test_chunk_text_empty() -> None:
    chunks = chunk_text("", max_chars=50)
    assert chunks == [""]


def test_coalesce_batch_basic() -> None:
    items = list(range(10))
    batches = coalesce_batch(items, batch_size=3)
    assert len(batches) == 4
    assert batches[0] == [0, 1, 2]
    assert batches[-1] == [9]


def test_coalesce_batch_empty() -> None:
    batches = coalesce_batch([], batch_size=5)
    assert batches == [[]]


def test_coalesce_batch_zero_size() -> None:
    items = list(range(5))
    batches = coalesce_batch(items, batch_size=0)
    assert batches == [items]


def test_coalesce_batch_exact_multiple() -> None:
    items = list(range(9))
    batches = coalesce_batch(items, batch_size=3)
    assert len(batches) == 3
    assert all(len(b) == 3 for b in batches)


def test_content_hasher_stable() -> None:
    h = ContentHasher()
    assert h.hash("hello world") == h.hash("hello world")


def test_content_hasher_different_inputs() -> None:
    h = ContentHasher()
    assert h.hash("foo") != h.hash("bar")


def test_content_hasher_normalisation() -> None:
    h = ContentHasher()
    # Leading/trailing whitespace stripped before hashing
    assert h.hash("  hello  ") == h.hash("hello")


def test_content_hasher_unicode_normalisation() -> None:
    h = ContentHasher()
    # NFC normalisation: composed vs decomposed "é"
    composed = "\u00e9"  # é (precomposed)
    decomposed = "e\u0301"  # e + combining accent
    assert h.hash(composed) == h.hash(decomposed)
