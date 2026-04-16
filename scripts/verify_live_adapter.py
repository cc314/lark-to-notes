#!/usr/bin/env python3
"""Offline smoke checks for the in-repo live adapter surface (no Lark API).

Emits one JSON object per step on **stderr** so supervisors can scrape logs.
Human-readable banners go to **stdout**. Uses the same ``lark-to-notes``
entrypoints as operators (``doctor``, ``sync-events``) against a disposable DB.

With ``--artifacts-dir``, each step record is also appended as one NDJSON line
to ``verify_live_steps.jsonl`` under that directory (for CI or post-mortem).

Exit codes: 0 success, 1 failure.

Usage::

    uv run python scripts/verify_live_adapter.py
    uv run python scripts/verify_live_adapter.py --artifacts-dir /tmp/live-verify-artifacts
"""

from __future__ import annotations

import argparse
import io
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SRC = _REPO_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from lark_to_notes.cli import run  # noqa: E402

_FIXTURE_CORPUS = _REPO_ROOT / "tests" / "fixtures" / "lark-worker" / "fixture-corpus"


def _log_event(payload: dict[str, Any], *, artifact_path: Path | None) -> None:
    line = json.dumps(payload, default=str)
    print(line, file=sys.stderr, flush=True)
    if artifact_path is not None:
        with artifact_path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Offline live-adapter smoke checks (no Lark API).")
    parser.add_argument(
        "--artifacts-dir",
        type=Path,
        default=None,
        help="Append each structured step as NDJSON lines to verify_live_steps.jsonl here.",
    )
    args = parser.parse_args()

    artifact_path: Path | None = None
    if args.artifacts_dir is not None:
        args.artifacts_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = args.artifacts_dir / "verify_live_steps.jsonl"
        if artifact_path.exists():
            artifact_path.unlink()

    tmp = Path(tempfile.mkdtemp(prefix="lark-verify-live-"))
    db_path = tmp / "verify.db"

    print(f"Using temp DB: {db_path}", flush=True)

    _log_event(
        {"step": "doctor", "status": "start", "db": str(db_path)},
        artifact_path=artifact_path,
    )
    old_out = sys.stdout
    try:
        sys.stdout = io.StringIO()
        rc = run(
            [
                "doctor",
                "--db",
                str(db_path),
                "--fixture-corpus",
                str(_FIXTURE_CORPUS),
                "--json",
            ]
        )
    finally:
        sys.stdout = old_out
    if rc != 0:
        _log_event(
            {"step": "doctor", "status": "failed", "exit_code": rc},
            artifact_path=artifact_path,
        )
        return 1
    _log_event({"step": "doctor", "status": "ok"}, artifact_path=artifact_path)

    envelope = {
        "header": {"event_type": "im.message.receive_v1"},
        "event": {
            "message": {
                "message_id": "om_verify_script",
                "chat_id": "ou_chat",
                "create_time": "1713096000000",
                "body": {"content": json.dumps({"text": "verify_live_adapter probe"})},
                "sender": {"id": "ou_sender", "name": "Script"},
            }
        },
    }
    _log_event({"step": "sync_events", "status": "start"}, artifact_path=artifact_path)
    old_stdin = sys.stdin
    try:
        sys.stdin = io.StringIO(json.dumps(envelope) + "\n")
        sys.stdout = io.StringIO()
        rc = run(
            [
                "sync-events",
                "--db",
                str(db_path),
                "--source-id",
                "dm:ou_verify",
                "--coalesce-window-seconds",
                "0",
                "--json",
            ]
        )
    finally:
        sys.stdin = old_stdin
        sys.stdout = old_out

    if rc != 0:
        _log_event(
            {"step": "sync_events", "status": "failed", "exit_code": rc},
            artifact_path=artifact_path,
        )
        return 1
    _log_event({"step": "sync_events", "status": "ok"}, artifact_path=artifact_path)
    print("All live-adapter offline checks passed.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
