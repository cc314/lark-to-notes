"""Tests for :func:`run_lark_cli_json_retryable`."""

from __future__ import annotations

import subprocess
from unittest.mock import patch

import pytest

from lark_to_notes.live.lark_cli import (
    LarkCliApiError,
    LarkCliInvocationError,
    run_lark_cli_json_retryable,
)
from lark_to_notes.runtime.retry import RetryPolicy


def test_retryable_succeeds_after_transient_invocation_errors() -> None:
    policy = RetryPolicy(max_attempts=4, base_delay_s=0.0, max_delay_s=0.0, jitter_factor=0.0)
    side_effect: list[object] = [
        LarkCliInvocationError("temporary glitch"),
        LarkCliInvocationError("another glitch"),
        {"ok": True, "result": "ok"},
    ]

    with patch(
        "lark_to_notes.live.lark_cli.run_lark_cli_json",
        side_effect=side_effect,
    ) as mocked:
        out = run_lark_cli_json_retryable(["im", "list"], policy=policy, sleep_fn=lambda _s: None)
    assert out == {"ok": True, "result": "ok"}
    assert mocked.call_count == 3


def test_retryable_no_retry_on_unauthorized_api_error() -> None:
    policy = RetryPolicy(max_attempts=4, base_delay_s=0.0, max_delay_s=0.0, jitter_factor=0.0)
    exc = LarkCliApiError("Unauthorized", code=401)

    with (
        patch("lark_to_notes.live.lark_cli.run_lark_cli_json", side_effect=exc) as mocked,
        pytest.raises(LarkCliApiError),
    ):
        run_lark_cli_json_retryable(["x"], policy=policy, sleep_fn=lambda _s: None)
    assert mocked.call_count == 1


def test_retryable_retries_then_ok_on_transient_api_error() -> None:
    policy = RetryPolicy(max_attempts=4, base_delay_s=0.0, max_delay_s=0.0, jitter_factor=0.0)
    side_effect: list[object] = [
        LarkCliApiError("Service Unavailable", code=503),
        {"ok": True},
    ]

    with patch("lark_to_notes.live.lark_cli.run_lark_cli_json", side_effect=side_effect) as mocked:
        out = run_lark_cli_json_retryable(["x"], policy=policy, sleep_fn=lambda _s: None)
    assert out == {"ok": True}
    assert mocked.call_count == 2


def test_retryable_retries_subprocess_timeout() -> None:
    policy = RetryPolicy(max_attempts=3, base_delay_s=0.0, max_delay_s=0.0, jitter_factor=0.0)
    te = subprocess.TimeoutExpired(cmd=["lark-cli"], timeout=1.0)
    side_effect: list[object] = [te, {"ok": True}]

    with patch("lark_to_notes.live.lark_cli.run_lark_cli_json", side_effect=side_effect) as mocked:
        out = run_lark_cli_json_retryable(["x"], policy=policy, sleep_fn=lambda _s: None)
    assert out == {"ok": True}
    assert mocked.call_count == 2
