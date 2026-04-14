"""Retry policy with exponential backoff and jitter.

The :class:`RetryPolicy` is an immutable configuration object.  It does not
sleep on its own — callers use :meth:`RetryPolicy.delay_for` to compute the
wait time and then sleep themselves.  This keeps the policy logic pure and
easily testable without monkeypatching ``time.sleep``.

Usage::

    policy = RetryPolicy(max_attempts=4, base_delay_s=1.0, max_delay_s=30.0)

    for attempt in range(policy.max_attempts):
        try:
            result = do_work()
            break
        except TransientError as exc:
            if not policy.should_retry(attempt, exc):
                raise
            time.sleep(policy.delay_for(attempt))
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Permanent-failure sentinel
# ---------------------------------------------------------------------------


class PermanentError(Exception):
    """Raised to signal that an item should be quarantined without retrying."""


# ---------------------------------------------------------------------------
# Retry policy
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Immutable retry configuration with exponential backoff and jitter.

    Attributes:
        max_attempts:   Maximum number of attempts (including the first).
                        Must be >= 1.
        base_delay_s:   Base delay in seconds before the first retry.
        max_delay_s:    Upper cap on computed delay (before jitter).
        jitter_factor:  Fraction of the computed delay added as random jitter.
                        ``0.0`` disables jitter; ``1.0`` doubles the range.
    """

    max_attempts: int = 3
    base_delay_s: float = 1.0
    max_delay_s: float = 60.0
    jitter_factor: float = 0.25

    def should_retry(self, attempt: int, exc: BaseException) -> bool:
        """Return ``True`` if the call should be retried after *attempt*.

        A :class:`PermanentError` is never retried, regardless of
        ``max_attempts``.

        Args:
            attempt: Zero-based attempt index that just failed.
            exc:     The exception that was raised.

        Returns:
            ``True`` when ``attempt < max_attempts - 1`` and the exception is
            not a :class:`PermanentError`.
        """
        if isinstance(exc, PermanentError):
            return False
        return attempt < self.max_attempts - 1

    def delay_for(self, attempt: int) -> float:
        """Return the delay in seconds to wait before retry *attempt + 1*.

        Uses full-jitter exponential backoff:
        ``delay = min(base * 2^attempt, max) * (1 + jitter_factor * U[0, 1])``

        Args:
            attempt: Zero-based attempt index that just failed.

        Returns:
            Delay in seconds (always >= 0).
        """
        base = self.base_delay_s * math.pow(2.0, attempt)
        capped = min(base, self.max_delay_s)
        jitter = capped * self.jitter_factor * random.random()  # noqa: S311
        return capped + jitter
