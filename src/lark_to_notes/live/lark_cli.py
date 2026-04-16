"""Thin ``lark-cli`` process wrapper with JSON envelope handling."""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
from typing import Any

logger = logging.getLogger(__name__)


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
