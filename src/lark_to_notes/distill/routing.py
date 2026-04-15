"""LLM routing and heuristics-only fallback for the distillation layer.

In V1 no LLM provider implementation is shipped.  The :class:`LLMProvider`
protocol defines the interface so the rest of the system treats routing as
a black box and callers can inject a real provider later without changing
any call sites.

Routing policy:

* If the heuristic result does **not** request escalation, return it as-is.
* If escalation is requested **and** a provider is available, call the
  provider.  Use the provider result if it returns one; otherwise fall back.
* If escalation is requested but **no** provider is available, demote a
  low-confidence result to ``needs_review`` so it is never silently
  discarded.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Protocol, runtime_checkable
from uuid import uuid4

from lark_to_notes.budget import (
    BudgetEnforcer,
    ContentHasher,
    ContentHashKey,
    FallbackReason,
    ProviderRoute,
    UsageRecord,
)
from lark_to_notes.distill.heuristics import HeuristicClassifier, default_classifier
from lark_to_notes.distill.models import (
    ClassifierResult,
    ConfidenceBand,
    DistillInput,
    PromotionRec,
    TaskClass,
)

logger = logging.getLogger(__name__)


@runtime_checkable
class LLMProvider(Protocol):
    """Minimal interface for an optional LLM classifier backend.

    Implementations should return ``None`` when the provider is
    unavailable, over budget, or encounters a transient error.  The
    routing layer will fall back to the heuristic result in that case.
    """

    def classify(
        self,
        inp: DistillInput,
        hint: ClassifierResult,
    ) -> ClassifierResult | None:
        """Classify *inp* assisted by *hint* from the heuristic pass.

        Args:
            inp:  The message to classify.
            hint: Heuristic result for context and routing guidance.

        Returns:
            A :class:`ClassifierResult` or ``None`` if the provider
            cannot produce a reliable answer.
        """
        ...  # pragma: no cover


def classify_with_routing(
    inp: DistillInput,
    *,
    classifier: HeuristicClassifier | None = None,
    llm_provider: LLMProvider | None = None,
    budget_enforcer: BudgetEnforcer | None = None,
    run_id: str | None = None,
) -> ClassifierResult:
    """Classify *inp* using heuristics, optionally escalating to an LLM.

    This function is the single entry point for the distillation layer.
    Callers never need to decide themselves whether to use heuristics or
    an LLM; the routing policy is encoded here.

    Args:
        inp:          The message to classify.
        classifier:   Heuristic classifier to use.  Defaults to the
                      module-level :data:`default_classifier`.
        llm_provider: Optional LLM backend.  When ``None`` the system
                      runs in heuristics-only mode.
        budget_enforcer: Optional budget and cache policy enforcer.
        run_id:         Runtime run identifier used for budget tracking.

    Returns:
        The final :class:`ClassifierResult` after routing.
    """
    if classifier is None:
        classifier = default_classifier

    heuristic_result = classifier.classify(inp)

    # Fast path: no escalation requested — return as-is
    if not heuristic_result.escalate_to_llm:
        return heuristic_result

    provider_name = _provider_name(llm_provider)
    provider_model = _provider_model(llm_provider)
    cache_key = None
    if budget_enforcer is not None and run_id is not None and llm_provider is not None:
        cache_key = ContentHashKey(
            content_hash=ContentHasher().hash(inp.content),
            model=provider_model,
        ).cache_key()
        route, reason = budget_enforcer.should_escalate(run_id=run_id, cache_key=cache_key)
        if route == ProviderRoute.CACHE_HIT:
            cached_payload = budget_enforcer.get_cached_result(cache_key)
            if cached_payload is not None:
                budget_enforcer.record_usage(
                    _usage_record(
                        run_id=run_id,
                        source_id=inp.source_id,
                        provider=provider_name,
                        model=provider_model,
                        cached=True,
                        fallback=False,
                        fallback_reason=FallbackReason.CONTENT_CACHED,
                    )
                )
                return _result_from_json(cached_payload)
        if route == ProviderRoute.HEURISTICS_ONLY:
            budget_enforcer.record_usage(
                _usage_record(
                    run_id=run_id,
                    source_id=inp.source_id,
                    provider="heuristics",
                    model=provider_model,
                    cached=False,
                    fallback=True,
                    fallback_reason=reason,
                )
            )
            return _fallback_result(heuristic_result, prefix="budget_fallback")

    # Escalation was requested but no provider is available.
    if llm_provider is None:
        if budget_enforcer is not None and run_id is not None:
            budget_enforcer.record_usage(
                _usage_record(
                    run_id=run_id,
                    source_id=inp.source_id,
                    provider="heuristics",
                    model="",
                    cached=False,
                    fallback=True,
                    fallback_reason=FallbackReason.NO_PROVIDER,
                )
            )
        if heuristic_result.confidence_band == ConfidenceBand.LOW:
            logger.debug(
                "llm_escalation_skipped",
                extra={
                    "message_id": inp.message_id,
                    "reason": heuristic_result.reason_code,
                    "confidence_band": heuristic_result.confidence_band,
                },
            )
            return _fallback_result(heuristic_result, prefix="no_provider")
        return heuristic_result

    # Escalate to LLM
    logger.debug(
        "llm_escalation_requested",
        extra={
            "message_id": inp.message_id,
            "reason": heuristic_result.reason_code,
            "band": heuristic_result.confidence_band,
        },
    )
    started = datetime.now(UTC)
    try:
        llm_result = llm_provider.classify(inp, heuristic_result)
    except Exception:
        logger.exception(
            "llm_escalation_failed; falling back to heuristics",
            extra={"message_id": inp.message_id},
        )
        llm_result = None
    duration_ms = int((datetime.now(UTC) - started).total_seconds() * 1000)

    if llm_result is not None:
        if (
            budget_enforcer is not None
            and run_id is not None
            and budget_enforcer.policy.max_latency_ms > 0
            and duration_ms > budget_enforcer.policy.max_latency_ms
        ):
            budget_enforcer.record_usage(
                _usage_record(
                    run_id=run_id,
                    source_id=inp.source_id,
                    provider=provider_name,
                    model=provider_model,
                    duration_ms=duration_ms,
                    cached=False,
                    fallback=True,
                    fallback_reason=FallbackReason.LATENCY_SPIKE,
                )
            )
            logger.info(
                "llm_result_rejected_for_latency",
                extra={
                    "message_id": inp.message_id,
                    "duration_ms": duration_ms,
                    "threshold_ms": budget_enforcer.policy.max_latency_ms,
                },
            )
            return _fallback_result(heuristic_result, prefix="latency_spike")
        if budget_enforcer is not None and run_id is not None:
            budget_enforcer.record_usage(
                _usage_record(
                    run_id=run_id,
                    source_id=inp.source_id,
                    provider=provider_name,
                    model=provider_model,
                    duration_ms=duration_ms,
                    cached=False,
                    fallback=False,
                    fallback_reason=FallbackReason.NOT_APPLICABLE,
                )
            )
            if cache_key is not None:
                budget_enforcer.put_cached_result(cache_key, _result_to_json(llm_result))
        logger.debug(
            "llm_result_accepted",
            extra={"message_id": inp.message_id, "class": llm_result.task_class},
        )
        return llm_result

    # LLM returned None — fall back to heuristics, marking low-conf as review
    logger.debug(
        "llm_returned_none; falling back to heuristics",
        extra={"message_id": inp.message_id},
    )
    if budget_enforcer is not None and run_id is not None:
        budget_enforcer.record_usage(
            _usage_record(
                run_id=run_id,
                source_id=inp.source_id,
                provider=provider_name,
                model=provider_model,
                duration_ms=duration_ms,
                cached=False,
                fallback=True,
                fallback_reason=FallbackReason.PROVIDER_ERROR,
            )
        )
    return _fallback_result(heuristic_result, prefix="llm_fallback")


def _fallback_result(heuristic_result: ClassifierResult, *, prefix: str) -> ClassifierResult:
    if heuristic_result.confidence_band != ConfidenceBand.LOW:
        return heuristic_result
    return ClassifierResult(
        task_class=TaskClass.NEEDS_REVIEW,
        confidence_band=ConfidenceBand.LOW,
        promotion_rec=PromotionRec.REVIEW,
        reason_code=f"{prefix}_{heuristic_result.reason_code}",
        matched_signal=heuristic_result.matched_signal,
        escalate_to_llm=False,
        excerpt=heuristic_result.excerpt,
    )


def _provider_name(llm_provider: LLMProvider | None) -> str:
    if llm_provider is None:
        return "heuristics"
    provider_name = getattr(llm_provider, "provider_name", "")
    if isinstance(provider_name, str) and provider_name:
        return provider_name
    return llm_provider.__class__.__name__.lower()


def _provider_model(llm_provider: LLMProvider | None) -> str:
    if llm_provider is None:
        return ""
    model = getattr(llm_provider, "model", "")
    if isinstance(model, str) and model:
        return model
    return llm_provider.__class__.__name__.lower()


def _usage_record(
    *,
    run_id: str,
    source_id: str,
    provider: str,
    model: str,
    duration_ms: int = 0,
    cached: bool,
    fallback: bool,
    fallback_reason: FallbackReason,
) -> UsageRecord:
    return UsageRecord(
        call_id=str(uuid4()),
        provider=provider,
        model=model,
        prompt_tokens=0,
        completion_tokens=0,
        duration_ms=duration_ms,
        cached=cached,
        fallback=fallback,
        fallback_reason=fallback_reason,
        run_id=run_id,
        source_id=source_id,
        created_at=datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    )


def _result_to_json(result: ClassifierResult) -> str:
    return json.dumps(
        {
            "task_class": str(result.task_class),
            "confidence_band": str(result.confidence_band),
            "promotion_rec": str(result.promotion_rec),
            "reason_code": result.reason_code,
            "matched_signal": result.matched_signal,
            "escalate_to_llm": result.escalate_to_llm,
            "excerpt": result.excerpt,
        },
        ensure_ascii=False,
        sort_keys=True,
    )


def _result_from_json(payload: str) -> ClassifierResult:
    parsed = json.loads(payload)
    return ClassifierResult(
        task_class=TaskClass(parsed["task_class"]),
        confidence_band=ConfidenceBand(parsed["confidence_band"]),
        promotion_rec=PromotionRec(parsed["promotion_rec"]),
        reason_code=str(parsed["reason_code"]),
        matched_signal=str(parsed.get("matched_signal") or ""),
        escalate_to_llm=bool(parsed.get("escalate_to_llm", False)),
        excerpt=str(parsed.get("excerpt") or ""),
    )
