"""Operator hints for supervising canonical live CLIs (lw-tps).

Background execution is intentionally **outside** the Python package: launchd,
systemd, or another supervisor runs the same ``lark-to-notes`` entrypoints an
operator would type interactively.  This module only emits structured reminders
for ``doctor`` and other diagnostics.
"""

from __future__ import annotations

import os
from typing import Any


def supervised_live_hints(*, db_path: str | os.PathLike[str]) -> dict[str, Any]:
    """Return JSON-serializable guidance for supervised live sync."""

    db_s = os.fspath(db_path)
    return {
        "model": "canonical_cli_wrapper",
        "summary": (
            "Supervisors should wrap `uv run lark-to-notes sync-daemon` and/or a "
            "shell script that pipes `lark-cli event +subscribe` into `sync-events`; "
            "there is no second in-repo daemon process."
        ),
        "contrib_relpaths": {
            "launchd_plist": "contrib/sync-daemon.launchd.example.plist",
            "launchd_plist_scripts": (
                "scripts/macos/launchd/com.lark-to-notes.sync-daemon.example.plist"
            ),
            "events_pipeline_sh": "contrib/sync-events-pipeline.example.sh",
        },
        "argv_templates": {
            "sync_daemon": [
                "uv",
                "run",
                "lark-to-notes",
                "sync-daemon",
                "--config",
                "<LIVE_JSON>",
                "--db",
                db_s,
            ],
            "sync_events": [
                "uv",
                "run",
                "lark-to-notes",
                "sync-events",
                "--db",
                db_s,
                "--source-id",
                "<SOURCE_ID>",
            ],
        },
        "reconcile": (
            "Live reconcile uses `ChatLiveAdapter.collect_live_source_states` and "
            "repair via the same in-repo poll path (`_worker_poll_once`); there is "
            "no worker-state mirror on the canonical database."
        ),
        "pipeline_reminder": (
            "Do not encode `lark-cli … | lark-to-notes …` directly in a LaunchAgent "
            "`ProgramArguments` array; use a shell wrapper script (see README)."
        ),
    }
