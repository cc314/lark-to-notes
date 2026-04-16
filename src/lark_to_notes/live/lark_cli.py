"""Thin ``lark-cli`` process wrapper with JSON envelope handling."""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
import time
from typing import TYPE_CHECKING, Any

from lark_to_notes.runtime.retry import RetryPolicy

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

_DEFAULT_LARK_RETRY = RetryPolicy(
    max_attempts=4,
    base_delay_s=1.5,
    max_delay_s=30.0,
    jitter_factor=0.2,
)


class LarkCliError(RuntimeError):
    """Base class for ``lark-cli`` integration failures."""


class LarkCliNotFoundError(LarkCliError):
    """Raised when ``lark-cli`` is not available on ``PATH``."""


class LarkCliInvocationError(LarkCliError):
    """Raised when ``lark-cli`` exits unexpectedly or returns non-JSON output."""


class LarkCliApiError(LarkCliError):
    """Raised when ``lark-cli`` returns ``{"ok": false, ...}``."""

    def __init__(
        self,
        message: str,
        *,
        code: object = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.payload = payload


def resolve_lark_cli_binary() -> str:
    """Return the absolute path to ``lark-cli`` on ``PATH``."""

    path = shutil.which("lark-cli")
    if not path:
        raise LarkCliNotFoundError(
            "lark-cli was not found on PATH; install it or adjust PATH "
            "so document adapters can run."
        )
    return path


def _extract_json_object(text: str) -> dict[str, Any] | None:
    """Return the first top-level JSON object found in *text* (handles progress prefixes)."""

    start = text.find("{")
    if start < 0:
        return None
    decoder = json.JSONDecoder()
    try:
        obj, _end = decoder.raw_decode(text[start:])
    except json.JSONDecodeError:
        return None
    return obj if isinstance(obj, dict) else None


def _decode_cli_json(stdout: str, stderr: str) -> dict[str, Any]:
    """Parse the JSON object ``lark-cli`` emitted (stdout or stderr)."""

    for blob in (stdout.strip(), stderr.strip(), (stdout + "\n" + stderr).strip()):
        if not blob:
            continue
        parsed = _extract_json_object(blob)
        if parsed is not None:
            return parsed

    raise LarkCliInvocationError(
        "lark-cli did not emit a JSON object on stdout or stderr",
    )


def _auth_hint_in_text(text: str) -> bool:
    lowered = text.lower()
    return any(
        token in lowered
        for token in (
            "401",
            "403",
            "unauthorized",
            "forbidden",
            "permission denied",
            "token expired",
            "access token",
            "invalid tenant",
            "authentication",
            "not logged in",
        )
    )


def _is_retryable_lark_cli_exception(exc: BaseException) -> bool:
    if isinstance(exc, subprocess.TimeoutExpired):
        return True
    if isinstance(exc, LarkCliInvocationError):
        return not _auth_hint_in_text(str(exc))
    if isinstance(exc, LarkCliApiError):
        if _auth_hint_in_text(str(exc)):
            return False
        msg = str(exc).lower()
        return any(
            hint in msg
            for hint in (
                "timeout",
                "503",
                "502",
                "504",
                "429",
                "rate limit",
                "internal error",
                "unavailable",
                "try again",
                "network",
            )
        )
    return False


def run_lark_cli_json_retryable(
    argv: list[str],
    *,
    timeout: float = 180.0,
    env: dict[str, str] | None = None,
    policy: RetryPolicy | None = None,
    sleep_fn: Callable[[float], None] | None = None,
) -> dict[str, Any]:
    """Like :func:`run_lark_cli_json` but retries transient subprocess/API failures."""

    policy = policy or _DEFAULT_LARK_RETRY
    sleeper = sleep_fn or time.sleep
    attempt = 0
    while True:
        try:
            return run_lark_cli_json(argv, timeout=timeout, env=env)
        except (LarkCliInvocationError, LarkCliApiError, subprocess.TimeoutExpired) as exc:
            if not _is_retryable_lark_cli_exception(exc):
                raise
            if not policy.should_retry(attempt, exc):
                raise
            delay = policy.delay_for(attempt)
            logger.warning(
                "lark_cli_transient_failure_retry",
                extra={"attempt": attempt + 1, "delay_s": delay, "error": str(exc)},
            )
            sleeper(delay)
            attempt += 1


def run_lark_cli_json(
    argv: list[str],
    *,
    timeout: float = 180.0,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Run ``lark-cli`` with *argv* (excluding the binary) and parse JSON output.

    Some ``lark-cli`` subcommands write progress to stdout and the JSON envelope
    to stderr; this helper inspects both streams.
    """

    binary = resolve_lark_cli_binary()
    cmd = [binary, *argv]
    logger.debug("run_lark_cli_json", extra={"argv": cmd})
    proc = subprocess.run(  # noqa: S603 - argv are caller-built tokens, not a shell string
        cmd,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )
    payload = _decode_cli_json(proc.stdout or "", proc.stderr or "")
    if payload.get("ok") is False:
        err = payload.get("error")
        message = "lark-cli request failed"
        code: object = None
        if isinstance(err, dict):
            message = str(err.get("message") or err.get("type") or message)
            code = err.get("code")
        raise LarkCliApiError(message, code=code, payload=payload)
    if proc.returncode != 0:
        raise LarkCliInvocationError(
            f"lark-cli exited with status {proc.returncode}: "
            f"{(proc.stderr or proc.stdout or '').strip()[:500]}",
        )
    return payload
