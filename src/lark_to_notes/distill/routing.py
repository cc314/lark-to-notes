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

import logging
from typing import Protocol, runtime_checkable

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

    Returns:
        The final :class:`ClassifierResult` after routing.
    """
    if classifier is None:
        classifier = default_classifier

    heuristic_result = classifier.classify(inp)

    # Fast path: no escalation requested — return as-is
    if not heuristic_result.escalate_to_llm:
        return heuristic_result

    # Escalation was requested but no provider is available.
    if llm_provider is None:
        if heuristic_result.confidence_band == ConfidenceBand.LOW:
            logger.debug(
                "llm_escalation_skipped: no provider; demoting to needs_review",
                extra={"message_id": inp.message_id, "reason": heuristic_result.reason_code},
            )
            return ClassifierResult(
                task_class=TaskClass.NEEDS_REVIEW,
                confidence_band=ConfidenceBand.LOW,
                promotion_rec=PromotionRec.REVIEW,
                reason_code="no_provider_" + heuristic_result.reason_code,
                matched_signal=heuristic_result.matched_signal,
                escalate_to_llm=False,
                excerpt=heuristic_result.excerpt,
            )
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
    try:
        llm_result = llm_provider.classify(inp, heuristic_result)
    except Exception:
        logger.exception(
            "llm_escalation_failed; falling back to heuristics",
            extra={"message_id": inp.message_id},
        )
        llm_result = None

    if llm_result is not None:
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
    if heuristic_result.confidence_band == ConfidenceBand.LOW:
        return ClassifierResult(
            task_class=TaskClass.NEEDS_REVIEW,
            confidence_band=ConfidenceBand.LOW,
            promotion_rec=PromotionRec.REVIEW,
            reason_code="llm_fallback_" + heuristic_result.reason_code,
            matched_signal=heuristic_result.matched_signal,
            escalate_to_llm=False,
            excerpt=heuristic_result.excerpt,
        )
    return heuristic_result
