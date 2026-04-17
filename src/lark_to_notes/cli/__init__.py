"""CLI entry point for lark-to-notes."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
import tomllib
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from lark_to_notes import __version__
from lark_to_notes.feedback import (
    apply_feedback_artifact,
    load_feedback_artifact,
)
from lark_to_notes.feedback.draft import render_feedback_draft_yaml
from lark_to_notes.intake.replay import replay_jsonl_dir
from lark_to_notes.storage.db import connect, init_db, list_watched_sources
from lark_to_notes.storage.schema import SCHEMA_VERSION, all_versions
from lark_to_notes.testing import FixtureReplayFile, load_fixture_corpus

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable

    from lark_to_notes.config.sources import WatchedSource

# Valid source_type values recognised by the watched-sources governance layer.
_VALID_SOURCE_TYPES = frozenset(["dm", "group", "doc"])


def main() -> None:
    """Run the lark-to-notes CLI."""
    raise SystemExit(run())


def run(argv: list[str] | None = None) -> int:
    """Parse *argv*, execute the selected command, and return an exit code."""
    parser = _build_parser()
    args = parser.parse_args(argv)
    handler = cast("Callable[[argparse.Namespace], int]", args.handler)
    return handler(args)


def _add_reaction_scope_preflight_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--require-reaction-scopes",
        action="store_true",
        help=(
            "Before work: verify im:message.reactions:read via `lark-cli auth check` "
            "(exit 2 if missing)."
        ),
    )
    parser.add_argument(
        "--lark-profile",
        default=None,
        help="Optional `lark-cli --profile` value when --require-reaction-scopes is set.",
    )


def _reaction_preflight_exit_code(
    *,
    command: str,
    args: argparse.Namespace,
    json_output: bool,
) -> int | None:
    if not bool(getattr(args, "require_reaction_scopes", False)):
        return None
    from lark_to_notes.live.reaction_preflight import reaction_scope_preflight_check

    pf = reaction_scope_preflight_check(profile=getattr(args, "lark_profile", None))
    if pf.get("result") == "pass":
        return None
    if json_output:
        print(
            json.dumps(
                {"error": "reaction_scope_preflight_failed", "preflight": pf},
                ensure_ascii=False,
                indent=2,
            ),
        )
    else:
        print(
            f"{command}: reaction scope preflight failed ({pf.get('result')})",
            file=sys.stderr,
        )
        print(f"remediation: {pf.get('remediation_hint', '')}", file=sys.stderr)
    return 2


def _handle_preflight_reactions(args: argparse.Namespace) -> int:
    from lark_to_notes.live.reaction_preflight import reaction_scope_preflight_check

    pf = reaction_scope_preflight_check(profile=getattr(args, "lark_profile", None))
    if args.json:
        print(json.dumps(pf, ensure_ascii=False, indent=2))
    else:
        print(f"preflight_reactions: result={pf.get('result')} run_id={pf.get('run_id')}")
        print(f"check_name: {pf.get('check_name')}")
        print(f"tenant_app_id: {pf.get('tenant_app_id')}")
        print(f"hint: {pf.get('remediation_hint', '')}")
    if args.strict and pf.get("result") != "pass":
        return 2
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="lark-to-notes",
        description="Local Lark-to-notes operator CLI",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    sources_parser = subparsers.add_parser("sources", help="Inspect watched sources")
    sources_subparsers = sources_parser.add_subparsers(dest="sources_command", required=True)
    sources_list_parser = sources_subparsers.add_parser(
        "list",
        help="List watched sources from the SQLite store",
    )
    sources_list_parser.add_argument("--db", type=Path, default=_default_db_path())
    sources_list_parser.add_argument(
        "--all",
        action="store_true",
        help="Include disabled watched sources",
    )
    sources_list_parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON",
    )
    sources_list_parser.set_defaults(handler=_handle_sources_list)

    sources_validate_parser = sources_subparsers.add_parser(
        "validate",
        help="Validate watched sources configuration and report issues",
    )
    sources_validate_parser.add_argument("--db", type=Path, default=_default_db_path())
    sources_validate_parser.add_argument(
        "--all",
        action="store_true",
        help="Include disabled watched sources in validation",
    )
    sources_validate_parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON",
    )
    sources_validate_parser.set_defaults(handler=_handle_sources_validate)

    replay_parser = subparsers.add_parser(
        "replay",
        help="Replay raw JSONL logs into the SQLite store",
    )
    replay_parser.add_argument("--db", type=Path, default=_default_db_path())
    replay_parser.add_argument(
        "--raw-dir",
        type=Path,
        default=_default_vault_root() / "raw" / "lark-worker",
        help="Directory containing raw JSONL logs (default: <vault_root>/raw/lark-worker)",
    )
    replay_parser.add_argument("--glob", default="*.jsonl")
    replay_parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    replay_parser.set_defaults(handler=_handle_replay)

    doctor_parser = subparsers.add_parser(
        "doctor",
        help="Report repository and fixture-harness health",
    )
    doctor_parser.add_argument(
        "--fixture-corpus",
        type=Path,
        default=_default_vault_root() / "raw" / "lark-worker" / "fixture-corpus",
        help="Fixture corpus root (default: <vault_root>/raw/lark-worker/fixture-corpus)",
    )
    doctor_parser.add_argument("--db", type=Path, default=_default_db_path())
    doctor_parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    doctor_parser.set_defaults(handler=_handle_doctor)

    feedback_parser = subparsers.add_parser(
        "feedback",
        help="Structured review feedback: import YAML or draft review-lane stubs",
    )
    feedback_subparsers = feedback_parser.add_subparsers(dest="feedback_command", required=True)
    feedback_import_parser = feedback_subparsers.add_parser(
        "import",
        help="Import a YAML feedback sidecar into the SQLite store",
    )
    feedback_import_parser.add_argument("artifact", type=Path)
    feedback_import_parser.add_argument("--db", type=Path, default=_default_db_path())
    feedback_import_parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON",
    )
    feedback_import_parser.set_defaults(handler=_handle_feedback_import)

    feedback_draft_parser = feedback_subparsers.add_parser(
        "draft",
        help="Write a YAML feedback sidecar listing review-lane tasks (edit before import)",
    )
    feedback_draft_parser.add_argument(
        "--out",
        type=Path,
        required=True,
        help="Output path for the draft YAML file",
    )
    feedback_draft_parser.add_argument("--db", type=Path, default=_default_db_path())
    feedback_draft_parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Maximum review-lane tasks to include (default: 200)",
    )
    feedback_draft_parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON summary",
    )
    feedback_draft_parser.set_defaults(handler=_handle_feedback_draft)

    reclassify_parser = subparsers.add_parser(
        "reclassify",
        help="Re-run the heuristic classifier on stored raw messages and upsert tasks",
    )
    reclassify_parser.add_argument("--db", type=Path, default=_default_db_path())
    reclassify_parser.add_argument(
        "--source-id",
        default=None,
        help="Restrict to messages from this source (optional)",
    )
    reclassify_parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Maximum raw messages to process (default: 500)",
    )
    reclassify_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Classify but do not write tasks to the database",
    )
    reclassify_parser.add_argument(
        "--extra-high-pattern",
        action="append",
        default=[],
        metavar="REGEX::SIGNAL",
        help="Add a deterministic high-confidence classifier pattern",
    )
    reclassify_parser.add_argument(
        "--extra-medium-pattern",
        action="append",
        default=[],
        metavar="REGEX::SIGNAL",
        help="Add a deterministic medium-confidence classifier pattern",
    )
    reclassify_parser.add_argument(
        "--min-content-length-for-llm",
        type=int,
        default=800,
        help="Minimum content length that marks heuristic matches for LLM escalation",
    )
    reclassify_parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON",
    )
    reclassify_parser.set_defaults(handler=_handle_reclassify)

    render_parser = subparsers.add_parser(
        "render",
        help="Render open tasks into vault notes via the note-writer pipeline",
    )
    render_parser.add_argument("--db", type=Path, default=_default_db_path())
    render_parser.add_argument(
        "--vault-root",
        type=Path,
        default=_default_vault_root(),
        help="Root of the Obsidian vault (default: config file or current directory)",
    )
    render_parser.add_argument(
        "--status",
        default="open",
        help="Only render tasks with this status (default: open)",
    )
    render_parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Maximum tasks to render (default: 200)",
    )
    render_parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON",
    )
    render_parser.set_defaults(handler=_handle_render)

    reconcile_parser = subparsers.add_parser(
        "reconcile",
        help="Compare stored checkpoints against known sources and report cursor gaps",
    )
    reconcile_parser.add_argument("--db", type=Path, default=_default_db_path())
    reconcile_parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help=(
            "Optional worker-style live JSON (same file as sync-once) for lark-cli-backed reconcile"
        ),
    )
    reconcile_parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON",
    )
    reconcile_parser.set_defaults(handler=_handle_reconcile)

    budget_parser = subparsers.add_parser(
        "budget",
        help="Inspect LLM budget usage records",
    )
    budget_subparsers = budget_parser.add_subparsers(dest="budget_command", required=True)
    budget_status_parser = budget_subparsers.add_parser(
        "status",
        help="Show current LLM usage snapshot",
    )
    budget_status_parser.add_argument("--db", type=Path, default=_default_db_path())
    budget_status_parser.add_argument(
        "--run-id",
        default=None,
        help="Scope to a specific intake-run UUID",
    )
    budget_status_parser.add_argument(
        "--day",
        default=None,
        help="Scope to a calendar day in YYYY-MM-DD format",
    )
    budget_status_parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON",
    )
    budget_status_parser.set_defaults(handler=_handle_budget_status)

    sync_once_parser = subparsers.add_parser(
        "sync-once",
        help="Poll all enabled sources once and ingest new messages",
    )
    sync_once_parser.add_argument(
        "--config",
        type=Path,
        default=_default_worker_config_path(),
        help="Path to worker-style live JSON (vault_root, sources, poll settings)",
    )
    sync_once_parser.add_argument("--db", type=Path, default=_default_db_path())
    sync_once_parser.add_argument(
        "--no-sync",
        action="store_true",
        help="Skip note sync after polling",
    )
    sync_once_parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    _add_reaction_scope_preflight_flags(sync_once_parser)
    sync_once_parser.set_defaults(handler=_handle_sync_once)

    sync_daemon_parser = subparsers.add_parser(
        "sync-daemon",
        help="Run continuous polling loop for all enabled sources",
    )
    sync_daemon_parser.add_argument(
        "--config",
        type=Path,
        default=_default_worker_config_path(),
        help="Path to worker-style live JSON (vault_root, sources, poll settings)",
    )
    sync_daemon_parser.add_argument("--db", type=Path, default=_default_db_path())
    sync_daemon_parser.add_argument(
        "--poll-interval",
        type=float,
        default=None,
        help="Override the polling interval in seconds",
    )
    sync_daemon_parser.add_argument(
        "--max-cycles",
        type=int,
        default=None,
        help="Stop after N polling cycles (useful for testing and supervised runs)",
    )
    sync_daemon_parser.add_argument(
        "--no-sync",
        action="store_true",
        help="Skip note sync after each polling cycle",
    )
    sync_daemon_parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON",
    )
    _add_reaction_scope_preflight_flags(sync_daemon_parser)
    sync_daemon_parser.set_defaults(handler=_handle_sync_daemon)

    backfill_parser = subparsers.add_parser(
        "backfill",
        help="Re-ingest historical messages for watched sources",
    )
    backfill_parser.add_argument(
        "--config",
        type=Path,
        default=_default_worker_config_path(),
        help="Path to worker-style live JSON (vault_root, sources, poll settings)",
    )
    backfill_parser.add_argument("--db", type=Path, default=_default_db_path())
    backfill_parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="Override the historical lookback window in days",
    )
    backfill_parser.add_argument(
        "--source-id",
        action="append",
        default=[],
        help="Limit backfill to one or more configured source_id values",
    )
    backfill_parser.add_argument(
        "--sync-notes",
        action="store_true",
        help="Also sync notes after backfill",
    )
    backfill_parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    backfill_parser.set_defaults(handler=_handle_backfill)

    sync_events_parser = subparsers.add_parser(
        "sync-events",
        help="Read NDJSON chat event lines from stdin into the mixed chat-intake ledger",
    )
    sync_events_parser.add_argument("--db", type=Path, default=_default_db_path())
    sync_events_parser.add_argument(
        "--source-id",
        required=True,
        help="Watched source_id to attribute events to (must match polling, e.g. dm:ou_xxx)",
    )
    sync_events_parser.add_argument(
        "--worker-source-type",
        default="dm_user",
        help="Worker-style source_type string for normalization (default: dm_user)",
    )
    sync_events_parser.add_argument(
        "--chat-type",
        default="p2p",
        help="Chat subtype for RawMessage (default: p2p for DM)",
    )
    sync_events_parser.add_argument(
        "--chat-id",
        default=None,
        help="Override chat_id when the event payload omits it",
    )
    sync_events_parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON",
    )
    sync_events_parser.add_argument(
        "--stage-log-ndjson",
        action="store_true",
        help=(
            "Emit one JSON object per decoded stdin envelope to stderr "
            "(lw-pzj.10.6: ts, stage, event_type, event_id, source_id, message_id, "
            "result, reason_code, duration_ms, run_id)"
        ),
    )
    sync_events_parser.add_argument(
        "--coalesce-window-seconds",
        type=int,
        default=60,
        help="Ledger coalescing window for event observations (default: 60)",
    )
    sync_events_parser.add_argument(
        "--no-drain",
        action="store_true",
        help="Skip draining ready chat-intake rows into raw_messages (default: drain after stdin)",
    )
    sync_events_parser.add_argument(
        "--max-reactions-per-run",
        type=int,
        default=0,
        help=(
            "Defer validated IM reaction envelopes after N per stdin batch "
            "(0=unlimited; persists deferral rows)"
        ),
    )
    sync_events_parser.add_argument(
        "--max-reactions-per-source",
        type=int,
        default=0,
        help="Defer validated reaction envelopes for --source-id after N per batch (0=unlimited)",
    )
    sync_events_parser.add_argument(
        "--reaction-governance-version",
        default="",
        help="Override governance_version on new reaction rows (default: built-in intake version)",
    )
    sync_events_parser.add_argument(
        "--reaction-policy-version",
        default="",
        help="Stamp policy_version on new reaction rows (default empty)",
    )
    _add_reaction_scope_preflight_flags(sync_events_parser)
    sync_events_parser.set_defaults(handler=_handle_sync_events)

    preflight_parser = subparsers.add_parser(
        "preflight",
        help="Lark capability probes before live ingest (lw-pzj.14.3)",
    )
    preflight_subparsers = preflight_parser.add_subparsers(dest="preflight_command", required=True)
    preflight_reactions_parser = preflight_subparsers.add_parser(
        "reactions",
        help="Verify im:message.reactions:read via `lark-cli auth check`",
    )
    preflight_reactions_parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit with code 2 when the scope check does not pass",
    )
    preflight_reactions_parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON",
    )
    preflight_reactions_parser.add_argument(
        "--lark-profile",
        default=None,
        dest="lark_profile",
        help="Forwarded to lark-cli as --profile (optional)",
    )
    preflight_reactions_parser.set_defaults(handler=_handle_preflight_reactions)

    return parser


def _handle_sources_list(args: argparse.Namespace) -> int:
    db_path: Path = args.db
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(db_path)
    init_db(conn)
    sources = list_watched_sources(conn, enabled_only=not args.all)
    payload = {
        "db_path": str(db_path),
        "count": len(sources),
        "sources": [_watched_source_to_dict(source) for source in sources],
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"db_path: {payload['db_path']}")
        print(f"count: {payload['count']}")
        for source in sources:
            status = "enabled" if source.enabled else "disabled"
            print(f"- {source.source_id} [{status}] {source.name}")
    return 0


def _handle_replay(args: argparse.Namespace) -> int:
    db_path: Path = args.db
    raw_dir: Path = args.raw_dir
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(db_path)
    init_db(conn)
    results = replay_jsonl_dir(conn, raw_dir=raw_dir, glob=args.glob)
    files = tuple(
        FixtureReplayFile(filename=filename, total_records=totals[0], inserted_records=totals[1])
        for filename, totals in results.items()
    )
    payload = {
        "db_path": str(db_path),
        "raw_dir": str(raw_dir),
        "glob": args.glob,
        "file_count": len(files),
        "total_records": sum(item.total_records for item in files),
        "inserted_records": sum(item.inserted_records for item in files),
        "files": [_replay_file_to_dict(item) for item in files],
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"db_path: {payload['db_path']}")
        print(f"raw_dir: {payload['raw_dir']}")
        print(
            f"replayed {payload['file_count']} file(s), "
            f"{payload['total_records']} record(s), inserted {payload['inserted_records']}"
        )
    return 0


def _reaction_pipeline_artifact_links(
    *,
    runtime_db_path: Path,
    fixture_corpus_root: Path,
) -> dict[str, Any]:
    """Pointers from ``reaction_pipeline_health`` to SQLite and replay inputs (lw-pzj.9.2)."""

    db_abs = str(runtime_db_path.expanduser().resolve())
    fixture_abs = str(fixture_corpus_root.expanduser().resolve())
    default_raw = (_default_vault_root() / "raw" / "lark-worker").expanduser().resolve()
    raw_abs = str(default_raw)
    return {
        "sqlite": {"runtime_db_path": db_abs},
        "replay_directories": {
            "doctor_fixture_corpus_root": fixture_abs,
            "default_vault_raw_lark_worker": raw_abs,
        },
        "quarantine": {
            "dead_letters_table": "dead_letters",
            "doctor_json_keys": (
                "runtime_diagnostics.recent_dead_letters",
                "runtime.dead_letter_count",
            ),
        },
        "reaction_ledger_tables": (
            "message_reaction_events",
            "reaction_orphan_queue",
            "reaction_intake_deferrals",
            "reaction_reconcile_observations",
        ),
        "argv_templates": {
            "doctor_json": [
                "uv",
                "run",
                "lark-to-notes",
                "doctor",
                "--db",
                db_abs,
                "--json",
            ],
            "replay_fixture_corpus": [
                "uv",
                "run",
                "lark-to-notes",
                "replay",
                "--db",
                db_abs,
                "--raw-dir",
                fixture_abs,
                "--json",
            ],
            "replay_default_raw_logs": [
                "uv",
                "run",
                "lark-to-notes",
                "replay",
                "--db",
                db_abs,
                "--raw-dir",
                raw_abs,
                "--json",
            ],
        },
    }


def _handle_doctor(args: argparse.Namespace) -> int:
    from lark_to_notes.intake.ledger import chat_intake_ledger_counts
    from lark_to_notes.intake.reaction_deferrals import (
        classify_reaction_pipeline_doctor_status,
        reaction_intake_deferral_metrics,
    )
    from lark_to_notes.intake.reaction_store import (
        latest_message_reaction_event_seen_at,
        reaction_attach_reconcile_latency_ms,
        reaction_correlation_counts,
        reaction_ledger_governance_sample_for_doctor,
        reaction_orphan_backlog_metrics,
    )
    from lark_to_notes.runtime.models import RunStatus
    from lark_to_notes.runtime.registry import health_report, list_dead_letters, list_runs
    from lark_to_notes.runtime.supervised import supervised_live_hints

    corpus = load_fixture_corpus(args.fixture_corpus)
    replay_conn = connect(":memory:")
    init_db(replay_conn)
    replay_summary = corpus.replay_summary(replay_conn)
    runtime_db_path: Path = args.db
    runtime_db_path.parent.mkdir(parents=True, exist_ok=True)
    runtime_conn = connect(runtime_db_path)
    init_db(runtime_conn)
    runtime_health = health_report(runtime_conn)
    failed_runs = [
        {
            "run_id": run.run_id,
            "command": run.command,
            "started_at": run.started_at,
            "finished_at": run.finished_at,
            "items_failed": run.items_failed,
            "error": run.error,
        }
        for run in list_runs(runtime_conn, status=RunStatus.FAILED, limit=5)
    ]
    dead_letters = [
        {
            "dl_id": item.dl_id,
            "source_id": item.source_id,
            "raw_message_id": item.raw_message_id,
            "attempt_count": item.attempt_count,
            "last_error": item.last_error,
            "quarantined_at": item.quarantined_at,
        }
        for item in list_dead_letters(runtime_conn, limit=5)
    ]
    supervised_live = supervised_live_hints(db_path=runtime_db_path)
    chat_intake = chat_intake_ledger_counts(runtime_conn)
    rx = reaction_correlation_counts(runtime_conn)
    reaction_event_row_count = rx["total"]
    reaction_orphan_row_count = rx["orphan"]
    reaction_linked_row_count = rx["linked_to_raw_message"]
    rx_backlog = reaction_orphan_backlog_metrics(runtime_conn)
    rx_attach = reaction_attach_reconcile_latency_ms(runtime_conn)
    defer_metrics = reaction_intake_deferral_metrics(runtime_conn)
    last_rx_seen = latest_message_reaction_event_seen_at(runtime_conn)
    reaction_pipeline_status = classify_reaction_pipeline_doctor_status(
        dead_letter_count=runtime_health.dead_letter_count,
        error_rate=float(runtime_health.error_rate),
        deferral_row_count=int(defer_metrics["deferral_row_count"]),
        orphan_queue_depth=int(rx_backlog["queue_depth"]),
        correlation_orphan_rows=int(reaction_orphan_row_count),
    )
    reaction_artifact_links = _reaction_pipeline_artifact_links(
        runtime_db_path=runtime_db_path,
        fixture_corpus_root=corpus.root,
    )
    governance_ledger_sample = reaction_ledger_governance_sample_for_doctor(runtime_conn)
    reaction_pipeline_health = {
        "status": reaction_pipeline_status,
        "counts": {
            "reaction_events_ingested": reaction_event_row_count,
            "reaction_events_quarantined": None,
            "reactions_rendered_to_vault": None,
            "reactions_distilled": None,
        },
        "timestamps": {
            "last_reaction_event_first_seen_at": last_rx_seen,
        },
        "cap_and_deferral": defer_metrics,
        "signals": {
            "orphan_correlation_rows_not_linked_to_raw": reaction_orphan_row_count,
            "orphan_queue_rows_waiting_on_parent_raw": int(rx_backlog["queue_depth"]),
            "dead_letter_count_total_runtime": runtime_health.dead_letter_count,
            "completed_run_error_rate": runtime_health.error_rate,
        },
        "artifact_links": reaction_artifact_links,
        "governance_ledger_sample": governance_ledger_sample,
        "notes": (
            "Quarantined per-reaction, vault-rendered, and distilled counts are "
            "reserved for later pipeline stages; deferrals and ingest timestamps "
            "are populated from SQLite today (lw-pzj.9.1). Use ``artifact_links`` "
            "for DB paths, replay roots, and argv templates (lw-pzj.9.2). "
            "``governance_ledger_sample`` aggregates stamped governance/policy "
            "tuples from ``message_reaction_events`` plus ``compare_as_of`` "
            "(expected vs dominant ledger tuple, mismatch flag) (lw-pzj.9.4)."
        ),
    }
    payload = {
        "status": "ok" if replay_summary.total_records == corpus.record_count else "error",
        "schema_version": SCHEMA_VERSION,
        "migrations": all_versions(),
        "db_path": str(runtime_db_path),
        "fixture_corpus": {
            "root": str(corpus.root),
            "record_count": corpus.record_count,
            "scenario_count": len(corpus.scenario_names),
            "missing_scenarios": list(corpus.missing_scenarios),
            "source_access_surfaces": list(corpus.coverage().source_access_surfaces),
        },
        "replay": {
            "file_count": replay_summary.file_count,
            "total_records": replay_summary.total_records,
            "inserted_records": replay_summary.inserted_records,
            "db_record_count": replay_summary.db_record_count,
            "matches_manifest": replay_summary.total_records == corpus.record_count,
        },
        "runtime": asdict(runtime_health),
        "runtime_diagnostics": {
            "recent_failed_runs": failed_runs,
            "recent_dead_letters": dead_letters,
        },
        "chat_intake_ledger": chat_intake,
        "message_reaction_events": {
            "row_count": reaction_event_row_count,
            "orphan_row_count": reaction_orphan_row_count,
            "linked_row_count": reaction_linked_row_count,
            "orphan_backlog": rx_backlog,
            "attach_reconcile_latency_ms": rx_attach,
        },
        "reaction_pipeline_health": reaction_pipeline_health,
        "supervised_live": supervised_live,
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"status: {payload['status']}")
        print(f"schema_version: {SCHEMA_VERSION}")
        print(f"runtime_db: {payload['db_path']}")
        print(
            f"fixture_corpus: {corpus.record_count} records across "
            f"{len(corpus.scenario_names)} scenarios"
        )
        print(
            f"replay: {replay_summary.file_count} file(s), {replay_summary.total_records} record(s)"
        )
        print(
            "runtime: "
            f"{runtime_health.run_count_total} run(s), "
            f"{runtime_health.dead_letter_count} dead letter(s), "
            f"error_rate={runtime_health.error_rate:.3f}"
        )
        print(
            "runtime_diag: "
            f"{len(failed_runs)} failed run(s), "
            f"{len(dead_letters)} recent dead letter(s), "
            f"duplicates={runtime_health.duplicate_event_count}"
        )
        pend = chat_intake["pending_ready"] + chat_intake["pending_coalescing"]
        print(
            "chat_intake: "
            f"{pend} pending "
            f"({chat_intake['pending_ready']} ready, "
            f"{chat_intake['pending_coalescing']} coalescing), "
            f"{chat_intake['processed']} processed"
        )
        print(
            "reaction_events: "
            f"{reaction_event_row_count} row(s) "
            f"({reaction_orphan_row_count} orphan, {reaction_linked_row_count} linked); "
            f"orphan_queue_depth={rx_backlog['queue_depth']}, "
            f"attach_reconcile_samples={rx_attach['attach_reconcile_sample_count']}"
        )
        print(
            "reaction_orphan_queue: "
            f"depth={rx_backlog['queue_depth']} "
            f"dwell_p50_s={rx_backlog['dwell_seconds_p50']} "
            f"attach_ms_p50={rx_attach['attach_reconcile_ms_p50']}"
        )
        print(
            "reaction_pipeline_health: "
            f"status={reaction_pipeline_status} "
            f"deferrals={defer_metrics['deferral_row_count']}"
        )
        gls = governance_ledger_sample
        ca = gls["compare_as_of"]
        print(
            "reaction_governance_sample: "
            f"drift={gls['hints']['drift_from_builtin_governance']} "
            f"mismatch={ca['mismatch_vs_runtime_intake_caps']} "
            f"tuples={len(gls['tuples'])} "
            "(see `doctor --json` → reaction_pipeline_health.governance_ledger_sample)"
        )
        replay_dirs = reaction_artifact_links["replay_directories"]
        print(
            "reaction_artifact_links: "
            f"db={reaction_artifact_links['sqlite']['runtime_db_path']} "
            f"fixture_raw={replay_dirs['doctor_fixture_corpus_root']} "
            "(see `doctor --json` → reaction_pipeline_health.artifact_links)"
        )
        print(
            f"supervised_live: {supervised_live['model']} (see `doctor --json` key supervised_live)"
        )
    return 0


def _handle_feedback_import(args: argparse.Namespace) -> int:
    import yaml

    db_path: Path = args.db
    artifact_path: Path = args.artifact
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(db_path)
    init_db(conn)
    try:
        artifact = load_feedback_artifact(artifact_path)
    except (yaml.YAMLError, ValueError) as exc:
        error_payload = {
            "error": str(exc),
            "artifact_path": str(artifact_path),
        }
        if args.json:
            print(json.dumps(error_payload, ensure_ascii=False, indent=2))
        else:
            print(f"error: {exc}")
        return 1
    try:
        result = apply_feedback_artifact(
            conn,
            artifact,
            artifact_path=str(artifact_path),
        )
    except LookupError as exc:
        error_payload = {
            "error": str(exc),
            "artifact_path": str(artifact_path),
        }
        if args.json:
            print(json.dumps(error_payload, ensure_ascii=False, indent=2))
        else:
            print(f"error: {exc}")
        return 1
    conn.commit()
    payload = {
        "db_path": str(db_path),
        "artifact_path": str(artifact_path),
        "applied_task_count": len(result.applied_task_ids),
        "feedback_event_count": len(result.feedback_event_ids),
        "applied_task_ids": list(result.applied_task_ids),
        "feedback_event_ids": list(result.feedback_event_ids),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"db_path: {payload['db_path']}")
        print(f"artifact_path: {payload['artifact_path']}")
        print(
            f"imported {payload['feedback_event_count']} feedback event(s), "
            f"applied {payload['applied_task_count']} task override(s)"
        )
    return 0


def _handle_feedback_draft(args: argparse.Namespace) -> int:
    from lark_to_notes.tasks.registry import list_review_feedback_candidates

    db_path: Path = args.db
    out_path: Path = args.out
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(db_path)
    init_db(conn)
    candidates = list_review_feedback_candidates(conn, limit=args.limit)
    text = render_feedback_draft_yaml(candidates)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(text, encoding="utf-8")
    payload: dict[str, Any] = {
        "db_path": str(db_path),
        "out_path": str(out_path),
        "task_count": len(candidates),
        "task_ids": [t.task_id for t in candidates],
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"wrote {payload['task_count']} task(s) to {payload['out_path']}")
        print(f"db_path: {payload['db_path']}")
        print("edit each action, then run: lark-to-notes feedback import <path> --db ...")
    return 0


def _handle_sources_validate(args: argparse.Namespace) -> int:
    db_path: Path = args.db
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(db_path)
    init_db(conn)
    sources = list_watched_sources(conn, enabled_only=not args.all)

    results: list[dict[str, Any]] = []
    has_error = False
    for source in sources:
        issues: list[str] = []
        if not source.name or not source.name.strip():
            issues.append("name is empty")
        if not source.external_id or not source.external_id.strip():
            issues.append("external_id is empty")
        if str(source.source_type) not in _VALID_SOURCE_TYPES:
            issues.append(f"unrecognised source_type '{source.source_type}'")
        status = "error" if issues else "ok"
        if status == "error":
            has_error = True
        results.append(
            {
                "source_id": source.source_id,
                "name": source.name,
                "enabled": source.enabled,
                "status": status,
                "issues": issues,
            }
        )

    payload = {
        "db_path": str(db_path),
        "count": len(results),
        "overall": "error" if has_error else "ok",
        "sources": results,
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"db_path: {payload['db_path']}")
        print(f"overall: {payload['overall']}  ({payload['count']} source(s) checked)")
        for item in results:
            tag = "✗" if item["issues"] else "✓"
            print(f"  {tag} {item['source_id']}  [{item['status']}]")
            for issue in item["issues"]:
                print(f"      - {issue}")
    return 1 if has_error else 0


def _handle_reclassify(args: argparse.Namespace) -> int:
    from uuid import uuid4

    from lark_to_notes.budget import BudgetEnforcer, BudgetPolicy
    from lark_to_notes.distill import DistillInput, HeuristicClassifier, classify_with_routing
    from lark_to_notes.intake.ledger import list_raw_messages
    from lark_to_notes.tasks import derive_fingerprint, upsert_task
    from lark_to_notes.tasks.registry import get_task_by_fingerprint

    db_path: Path = args.db
    dry_run: bool = args.dry_run
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(db_path)
    init_db(conn)

    messages = list_raw_messages(conn, source_id=args.source_id, limit=args.limit)
    try:
        extra_high_patterns = _parse_classifier_pattern_args(args.extra_high_pattern)
        extra_medium_patterns = _parse_classifier_pattern_args(args.extra_medium_pattern)
    except ValueError as exc:
        error_payload = {"db_path": str(db_path), "error": str(exc)}
        if args.json:
            print(json.dumps(error_payload, ensure_ascii=False, indent=2))
        else:
            print(f"error: {exc}")
        return 1
    classifier = HeuristicClassifier(
        extra_high_patterns=extra_high_patterns,
        extra_medium_patterns=extra_medium_patterns,
        min_content_length_for_llm=args.min_content_length_for_llm,
    )
    budget_enforcer = BudgetEnforcer(conn, BudgetPolicy())
    budget_run_id = f"reclassify:{uuid4()}"

    inserted = 0
    skipped = 0
    for msg in messages:
        dinput = DistillInput(
            message_id=msg.message_id,
            source_id=msg.source_id,
            source_type=msg.source_type,
            content=msg.content,
            sender_name=msg.sender_name,
            direction=msg.direction,
            created_at=msg.created_at,
        )
        result = classify_with_routing(
            dinput,
            classifier=classifier,
            llm_provider=None,
            budget_enforcer=budget_enforcer,
            run_id=budget_run_id,
        )
        fp = derive_fingerprint(
            msg.content,
            msg.source_id,
            msg.created_at,
            source_type=msg.source_type,
        )
        if dry_run:
            # Check fingerprint without writing.
            if get_task_by_fingerprint(conn, fp) is None:
                inserted += 1
            else:
                skipped += 1
        else:
            _task_id, is_new = upsert_task(
                conn,
                fingerprint=fp,
                title=msg.content[:80].strip(),
                task_class=str(result.task_class),
                confidence_band=str(result.confidence_band),
                summary=result.excerpt or "",
                reason_code=result.reason_code,
                promotion_rec=str(result.promotion_rec),
                created_from_raw_record_id=msg.message_id,
            )
            if is_new:
                inserted += 1
            else:
                skipped += 1

    if not dry_run:
        conn.commit()

    payload: dict[str, Any] = {
        "db_path": str(db_path),
        "dry_run": dry_run,
        "messages_processed": len(messages),
        "tasks_inserted": inserted,
        "tasks_skipped": skipped,
        "budget_run_id": budget_run_id,
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        verb = "would insert" if dry_run else "inserted"
        print(f"db_path: {payload['db_path']}")
        print(
            f"processed {payload['messages_processed']} message(s); "
            f"{verb} {payload['tasks_inserted']} task(s), "
            f"skipped {payload['tasks_skipped']} existing"
        )
    return 0


def _parse_classifier_pattern_args(raw_values: list[str]) -> list[tuple[str, str]]:
    parsed: list[tuple[str, str]] = []
    for raw in raw_values:
        regex, sep, signal = raw.partition("::")
        regex = regex.strip()
        signal = signal.strip()
        if sep == "" or regex == "" or signal == "":
            raise ValueError(
                f"classifier pattern must use REGEX::SIGNAL format (received: {raw!r})"
            )
        parsed.append((regex, signal))
    return parsed


def _handle_render(args: argparse.Namespace) -> int:
    from lark_to_notes.render.models import RenderItem
    from lark_to_notes.render.writer import NoteWriter
    from lark_to_notes.tasks.registry import list_tasks

    db_path: Path = args.db
    vault_root: Path = args.vault_root.resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(db_path)
    init_db(conn)

    tasks = list_tasks(conn, status=args.status, limit=args.limit)
    writer = NoteWriter(vault_root)

    rendered = 0
    errors: list[dict[str, str]] = []
    for task in tasks:
        item = RenderItem(
            task_id=task.task_id,
            fingerprint=task.fingerprint,
            title=task.title,
            promotion_rec=task.promotion_rec,
            reason_code=task.reason_code,
            confidence_band=task.confidence_band,
            task_class=task.task_class,
            status=task.status,
            summary=task.summary,
            assignee_refs=task.assignee_refs,
            due_at=task.due_at,
            source_message_id=task.created_from_raw_record_id,
        )
        try:
            writer.render_pipeline(item)
            rendered += 1
        except Exception as exc:
            errors.append({"task_id": task.task_id, "error": str(exc)})

    payload = {
        "db_path": str(db_path),
        "vault_root": str(vault_root),
        "status_filter": args.status,
        "tasks_found": len(tasks),
        "rendered": rendered,
        "errors": errors,
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"vault_root: {payload['vault_root']}")
        print(f"rendered {rendered}/{len(tasks)} task(s) (status={args.status!r})")
        for err in errors:
            print(f"  ✗ {err['task_id']}: {err['error']}")
    return 1 if errors else 0


def _handle_reconcile(args: argparse.Namespace) -> int:
    from lark_to_notes.runtime.executor import execute_reconcile_run
    from lark_to_notes.runtime.reconcile import reconcile_cursors

    db_path: Path = args.db
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(db_path)
    init_db(conn)
    config_path: Path | None = args.config

    mode = "stored"
    verification_report = None
    repair_triggered = False
    worker_config_path: str | None = None
    live_paths: dict[str, str] = {}
    live_result = None
    if config_path is not None:
        try:
            config, service = _load_worker_service(config_path, conn)
        except Exception as exc:
            return _print_live_worker_error(
                command="reconcile",
                db_path=db_path,
                config_path=config_path,
                exc=exc,
                json_output=args.json,
            )
        worker_config_path = str(config_path)
        mode = "live"
        live_paths = _live_runtime_payload_fields(db_path=db_path, worker_config=config)
        source_states = _collect_live_source_states(service, conn)

        def repair(_source_id: str, _stored_cursor: str | None) -> None:
            nonlocal repair_triggered
            if repair_triggered:
                return
            _worker_poll_once(service, sync_notes=True)
            repair_triggered = True

        live_result = execute_reconcile_run(
            conn,
            source_states,
            repair_fn=repair,
            command="reconcile",
        )
        if repair_triggered:
            verification_states = _collect_live_source_states(service, conn)
            verification_report = reconcile_cursors(conn, verification_states)
        else:
            verification_report = live_result.report
    else:
        source_states = _stored_source_states(conn)
        live_result = execute_reconcile_run(conn, source_states, command="reconcile")
        verification_report = live_result.report

    payload = {
        "db_path": str(db_path),
        "mode": mode,
        "worker_config": worker_config_path,
        "runtime_run_id": live_result.run.run_id,
        "sources_checked": verification_report.source_ids_checked,
        "gaps_found": verification_report.gaps_found,
        "repairs_attempted": live_result.report.repairs_attempted,
        "repairs_succeeded": live_result.report.repairs_succeeded,
        "repair_sync_triggered": repair_triggered,
        "gap_details": list(verification_report.gap_details),
        "initial_gap_details": list(live_result.report.gap_details),
        **live_paths,
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"db_path: {payload['db_path']}")
        print(f"mode: {mode}")
        print(
            f"checked {verification_report.source_ids_checked} source(s); "
            f"{verification_report.gaps_found} gap(s) remain; "
            f"{live_result.report.repairs_succeeded}/"
            f"{live_result.report.repairs_attempted} repair(s) succeeded"
        )
        for detail in verification_report.gap_details:
            print(f"  ! {detail}")
    return 1 if verification_report.gaps_found else 0


def _handle_budget_status(args: argparse.Namespace) -> int:
    import datetime

    from lark_to_notes.budget import BudgetEnforcer, BudgetPolicy

    db_path: Path = args.db
    run_id: str | None = args.run_id
    day: str | None = args.day
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(db_path)
    init_db(conn)
    enforcer = BudgetEnforcer(conn, BudgetPolicy())

    if run_id:
        snap = enforcer.get_run_snapshot(run_id)
        scope = f"run_id={run_id}"
    elif day:
        snap = enforcer.get_day_snapshot(day)
        scope = f"day={day}"
    else:
        today = datetime.date.today().isoformat()
        snap = enforcer.get_day_snapshot(today)
        scope = f"day={today} (default)"
    quality_metrics_report = enforcer.get_quality_metrics_report()
    quality_metrics = quality_metrics_report.overall

    payload: dict[str, Any] = {
        "db_path": str(db_path),
        "scope": scope,
        "cache_hit_rate": snap.cache_hit_rate,
        "quality_metrics": asdict(quality_metrics),
        "quality_metrics_scopes": {
            "latest_artifact_path": quality_metrics_report.latest_artifact_path,
            "latest_artifact": asdict(quality_metrics_report.latest_artifact),
            "rolling_7d": asdict(quality_metrics_report.rolling_7d),
            "rolling_30d": asdict(quality_metrics_report.rolling_30d),
            "by_artifact_path": {
                key: asdict(value) for key, value in quality_metrics_report.by_artifact_path.items()
            },
            "by_day": {key: asdict(value) for key, value in quality_metrics_report.by_day.items()},
        },
        "quality_metrics_breakdown": {
            "by_target_type": {
                key: asdict(value) for key, value in quality_metrics_report.by_target_type.items()
            },
            "by_source_type": {
                key: asdict(value) for key, value in quality_metrics_report.by_source_type.items()
            },
            "by_policy_version": {
                key: asdict(value)
                for key, value in quality_metrics_report.by_policy_version.items()
            },
            "by_promotion_rec": {
                key: asdict(value) for key, value in quality_metrics_report.by_promotion_rec.items()
            },
        },
    }
    if snap.call_count == 0:
        payload["status"] = "no_records"
    else:
        payload.update(
            {
                "status": "ok",
                "call_count": snap.call_count,
                "net_llm_call_count": snap.net_llm_call_count,
                "cached_count": snap.cached_count,
                "fallback_count": snap.fallback_count,
                "prompt_tokens": snap.prompt_tokens_sum,
                "completion_tokens": snap.completion_tokens_sum,
                "total_tokens": snap.total_tokens,
                "p95_latency_ms": snap.p95_latency_ms,
            }
        )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        if snap.call_count == 0:
            print(f"scope: {scope}")
            print("status: no_records")
        else:
            print(f"scope: {scope}")
            print(f"net_llm_calls: {snap.net_llm_call_count}  (total: {snap.call_count})")
            print(
                f"cached: {snap.cached_count}  fallbacks: {snap.fallback_count}  "
                f"cache_hit_rate: {snap.cache_hit_rate:.2%}"
            )
            print(
                f"tokens: {snap.prompt_tokens_sum} prompt / {snap.completion_tokens_sum} completion"
            )
            p95 = snap.p95_latency_ms
            print(f"p95_latency_ms: {p95 if p95 is not None else 'n/a'}")
        print(
            "quality: "
            f"confirm={quality_metrics.confirm_count} "
            f"dismiss={quality_metrics.dismiss_count} "
            f"duplicate={quality_metrics.duplicate_count} "
            f"review={quality_metrics.review_rate:.2%}"
        )
        print(
            "quality_rolling: "
            f"7d={quality_metrics_report.rolling_7d.total_events} "
            f"30d={quality_metrics_report.rolling_30d.total_events}"
        )
        latest_artifact = quality_metrics_report.latest_artifact_path or "none"
        print(
            "quality_latest_artifact: "
            f"{latest_artifact} ({quality_metrics_report.latest_artifact.total_events} events)"
        )
        if quality_metrics_report.by_day:
            day_summary = ", ".join(
                f"{day_key}={metrics.total_events}"
                for day_key, metrics in sorted(quality_metrics_report.by_day.items())[-7:]
            )
            print(f"quality_by_day_recent: {day_summary}")
        if quality_metrics_report.by_target_type:
            target_summary = ", ".join(
                f"{target}={metrics.total_events}"
                for target, metrics in sorted(quality_metrics_report.by_target_type.items())
            )
            print(f"quality_by_target_type: {target_summary}")
        if quality_metrics_report.by_source_type:
            source_summary = ", ".join(
                f"{source_type}={metrics.total_events}"
                for source_type, metrics in sorted(quality_metrics_report.by_source_type.items())
            )
            print(f"quality_by_source_type: {source_summary}")
        if quality_metrics_report.by_policy_version:
            policy_summary = ", ".join(
                f"{policy_version}={metrics.total_events}"
                for policy_version, metrics in sorted(
                    quality_metrics_report.by_policy_version.items()
                )
            )
            print(f"quality_by_policy_version: {policy_summary}")
        if quality_metrics_report.by_promotion_rec:
            promotion_summary = ", ".join(
                f"{promotion_rec}={metrics.total_events}"
                for promotion_rec, metrics in sorted(
                    quality_metrics_report.by_promotion_rec.items()
                )
            )
            print(f"quality_by_promotion_rec: {promotion_summary}")
    return 0


def _handle_sync_once(args: argparse.Namespace) -> int:
    db_path: Path = args.db
    config_path: Path = args.config
    db_path.parent.mkdir(parents=True, exist_ok=True)
    runtime_conn = connect(db_path)
    init_db(runtime_conn)
    try:
        config, service = _load_worker_service(config_path, runtime_conn)
    except Exception as exc:
        return _print_live_worker_error(
            command="sync-once",
            db_path=db_path,
            config_path=config_path,
            exc=exc,
            json_output=args.json,
            stage="load_worker_service",
        )

    gate = _reaction_preflight_exit_code(command="sync-once", args=args, json_output=args.json)
    if gate is not None:
        return gate

    from lark_to_notes.runtime.lock import RuntimeLock
    from lark_to_notes.runtime.registry import finish_run, health_report, start_run

    run = start_run(runtime_conn, "sync-once")
    result: dict[str, int]
    try:
        with RuntimeLock(_runtime_lock_path(config), owner_tag=f"sync-once:{run.run_id}"):
            service.initialize()
            result = _worker_poll_once(service, sync_notes=not args.no_sync)
        completed = (
            finish_run(
                runtime_conn,
                run.run_id,
                items_processed=result.get("inserted_messages", 0),
            )
            or run
        )
    except Exception as exc:
        finish_run(runtime_conn, run.run_id, items_failed=1, error=str(exc))
        return _print_live_worker_error(
            command="sync-once",
            db_path=db_path,
            config_path=config_path,
            exc=exc,
            json_output=args.json,
            run_id=run.run_id,
            stage="runtime_cycle",
        )

    payload = {
        "db_path": str(db_path),
        "config_path": str(config_path),
        "runtime_run_id": completed.run_id,
        "inserted_messages": result.get("inserted_messages", 0),
        "distilled_items": result.get("distilled_items", 0),
        "sync_notes": not args.no_sync,
        "runtime": asdict(health_report(runtime_conn)),
        **_live_runtime_payload_fields(db_path=db_path, worker_config=config),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"db_path: {payload['db_path']}")
        print(f"config_path: {payload['config_path']}")
        print(f"runtime_run_id: {payload['runtime_run_id']}")
        print(
            f"inserted_messages: {payload['inserted_messages']}  "
            f"distilled_items: {payload['distilled_items']}"
        )
    return 0


def _handle_sync_daemon(args: argparse.Namespace) -> int:
    db_path: Path = args.db
    config_path: Path = args.config
    db_path.parent.mkdir(parents=True, exist_ok=True)
    runtime_conn = connect(db_path)
    init_db(runtime_conn)
    try:
        config, service = _load_worker_service(config_path, runtime_conn)
    except Exception as exc:
        return _print_live_worker_error(
            command="sync-daemon",
            db_path=db_path,
            config_path=config_path,
            exc=exc,
            json_output=args.json,
            stage="load_worker_service",
        )

    gate = _reaction_preflight_exit_code(command="sync-daemon", args=args, json_output=args.json)
    if gate is not None:
        return gate

    from lark_to_notes.runtime.lock import RuntimeLock
    from lark_to_notes.runtime.registry import finish_run, health_report, start_run

    poll_interval = (
        args.poll_interval if args.poll_interval is not None else config.poll_interval_seconds
    )
    cycle_count = 0
    idle_cycles = 0
    inserted_messages = 0
    distilled_items = 0
    run_ids: list[str] = []

    try:
        while args.max_cycles is None or cycle_count < args.max_cycles:
            cycle_count += 1
            run = start_run(runtime_conn, "sync-daemon")
            run_ids.append(run.run_id)
            try:
                with RuntimeLock(_runtime_lock_path(config), owner_tag=f"sync-daemon:{run.run_id}"):
                    service.initialize()
                    result = _worker_poll_once(service, sync_notes=not args.no_sync)
                finish_run(
                    runtime_conn,
                    run.run_id,
                    items_processed=result.get("inserted_messages", 0),
                )
            except Exception as exc:
                finish_run(runtime_conn, run.run_id, items_failed=1, error=str(exc))
                return _print_live_worker_error(
                    command="sync-daemon",
                    db_path=db_path,
                    config_path=config_path,
                    exc=exc,
                    json_output=args.json,
                    run_id=run.run_id,
                    stage=f"cycle:{cycle_count}",
                )

            inserted_messages += result.get("inserted_messages", 0)
            distilled_items += result.get("distilled_items", 0)
            if result.get("inserted_messages", 0) == 0 and result.get("distilled_items", 0) == 0:
                idle_cycles += 1

            if args.max_cycles is None or cycle_count < args.max_cycles:
                time.sleep(poll_interval)
    except KeyboardInterrupt:
        pass

    payload = {
        "db_path": str(db_path),
        "config_path": str(config_path),
        "cycle_count": cycle_count,
        "idle_cycles": idle_cycles,
        "inserted_messages": inserted_messages,
        "distilled_items": distilled_items,
        "run_ids": run_ids,
        "runtime": asdict(health_report(runtime_conn)),
        **_live_runtime_payload_fields(db_path=db_path, worker_config=config),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"db_path: {payload['db_path']}")
        print(f"config_path: {payload['config_path']}")
        print(
            f"cycle_count: {cycle_count}  idle_cycles: {idle_cycles}  "
            f"inserted_messages: {inserted_messages}  distilled_items: {distilled_items}"
        )
    return 0


def _handle_backfill(args: argparse.Namespace) -> int:
    db_path: Path = args.db
    config_path: Path = args.config
    db_path.parent.mkdir(parents=True, exist_ok=True)
    runtime_conn = connect(db_path)
    init_db(runtime_conn)
    try:
        config, service = _load_worker_service(config_path, runtime_conn)
    except Exception as exc:
        return _print_live_worker_error(
            command="backfill",
            db_path=db_path,
            config_path=config_path,
            exc=exc,
            json_output=args.json,
            stage="load_worker_service",
        )

    from lark_to_notes.runtime.lock import RuntimeLock
    from lark_to_notes.runtime.registry import finish_run, health_report, start_run

    run = start_run(runtime_conn, "backfill")
    source_ids = set(args.source_id) or None
    try:
        with RuntimeLock(_runtime_lock_path(config), owner_tag=f"backfill:{run.run_id}"):
            service.initialize()
            result = _worker_backfill(
                service,
                lookback_days=args.days,
                source_ids=source_ids,
                sync_notes=args.sync_notes,
            )
        completed = (
            finish_run(
                runtime_conn,
                run.run_id,
                items_processed=result.get("inserted_messages", 0),
            )
            or run
        )
    except Exception as exc:
        finish_run(runtime_conn, run.run_id, items_failed=1, error=str(exc))
        return _print_live_worker_error(
            command="backfill",
            db_path=db_path,
            config_path=config_path,
            exc=exc,
            json_output=args.json,
            run_id=run.run_id,
            stage="runtime_backfill",
        )

    payload = {
        "db_path": str(db_path),
        "config_path": str(config_path),
        "runtime_run_id": completed.run_id,
        "sources_scanned": result.get("sources_scanned", 0),
        "inserted_messages": result.get("inserted_messages", 0),
        "distilled_items": result.get("distilled_items", 0),
        "sync_notes": args.sync_notes,
        "runtime": asdict(health_report(runtime_conn)),
        **_live_runtime_payload_fields(db_path=db_path, worker_config=config),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"db_path: {payload['db_path']}")
        print(f"config_path: {payload['config_path']}")
        print(f"runtime_run_id: {payload['runtime_run_id']}")
        print(
            f"sources_scanned: {payload['sources_scanned']}  "
            f"inserted_messages: {payload['inserted_messages']}  "
            f"distilled_items: {payload['distilled_items']}"
        )
    return 0


def _handle_sync_events(args: argparse.Namespace) -> int:
    """Ingest ``lark-cli event +subscribe`` NDJSON from stdin (``im.message.receive_v1``)."""

    import sys

    from lark_to_notes.intake.reaction_caps import (
        REACTION_INTAKE_GOVERNANCE_VERSION,
        ReactionIntakeCaps,
        ReactionIntakeCapState,
    )
    from lark_to_notes.live.chat_events import ingest_chat_event_ndjson_lines
    from lark_to_notes.runtime.executor import drain_ready_chat_intake
    from lark_to_notes.runtime.registry import finish_run, health_report, start_run
    from lark_to_notes.runtime.retry import RetryPolicy

    db_path: Path = args.db
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(db_path)
    init_db(conn)
    gate = _reaction_preflight_exit_code(command="sync-events", args=args, json_output=args.json)
    if gate is not None:
        return gate
    chat_id_override: str | None = args.chat_id
    coalesce_window_seconds: int = int(args.coalesce_window_seconds)
    gov = str(args.reaction_governance_version).strip() or REACTION_INTAKE_GOVERNANCE_VERSION
    pol = str(args.reaction_policy_version).strip()
    caps = ReactionIntakeCaps(
        max_reaction_envelopes_per_run=int(args.max_reactions_per_run),
        max_reaction_envelopes_per_source_per_run=int(args.max_reactions_per_source),
        governance_version=gov,
        policy_version=pol,
    )
    cap_state = ReactionIntakeCapState()
    reaction_intake_run_id: str | None = None
    if caps.limits_active:
        reaction_intake_run_id = start_run(conn, "sync-events-ndjson").run_id
    stage_log = None
    if args.stage_log_ndjson:

        def _stage_log_sink(record: dict[str, Any]) -> None:
            print(json.dumps(record, ensure_ascii=False), file=sys.stderr, flush=True)

        stage_log = _stage_log_sink
    try:
        outcome = ingest_chat_event_ndjson_lines(
            conn,
            sys.stdin,
            source_id=str(args.source_id),
            worker_source_type=str(args.worker_source_type),
            chat_type=str(args.chat_type),
            chat_id_override=chat_id_override,
            coalesce_window_seconds=coalesce_window_seconds,
            caps=caps,
            cap_state=cap_state,
            reaction_intake_run_id=reaction_intake_run_id,
            stage_log=stage_log,
        )
    except BaseException:
        if reaction_intake_run_id:
            finish_run(
                conn,
                reaction_intake_run_id,
                items_failed=1,
                error="sync-events ndjson ingest raised",
            )
        raise
    else:
        if reaction_intake_run_id:
            finish_run(
                conn,
                reaction_intake_run_id,
                items_processed=int(outcome.chat_envelopes_ingested)
                + int(outcome.reaction_rows_inserted)
                + int(outcome.reaction_cap_deferred),
            )
    drain_processed = 0
    batch = None
    if not args.no_drain:
        lock_path = db_path.parent / "lark-to-notes.runtime.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        batch = drain_ready_chat_intake(
            conn,
            lock_path=lock_path,
            command="sync-events",
            retry_policy=RetryPolicy(),
        )
        drain_processed = batch.items_processed
    payload = {
        "db_path": str(db_path),
        "source_id": str(args.source_id),
        "json_objects": outcome.json_objects,
        "envelopes_ingested": outcome.chat_envelopes_ingested,
        "reaction_rows_inserted": outcome.reaction_rows_inserted,
        "reaction_rows_inserted_add": outcome.reaction_rows_inserted_add,
        "reaction_rows_inserted_remove": outcome.reaction_rows_inserted_remove,
        "reaction_quarantined": outcome.reaction_quarantined,
        "chat_receive_observation_exceptions": outcome.chat_receive_observation_exceptions,
        "reaction_validation_rejects": outcome.reaction_validation_rejects,
        "reaction_insert_exceptions": outcome.reaction_insert_exceptions,
        "reaction_parse_none_after_validate": outcome.reaction_parse_none_after_validate,
        "reaction_benign_duplicate_replays": outcome.reaction_benign_duplicate_replays,
        "reaction_cap_deferred": outcome.reaction_cap_deferred,
        "last_reaction_cap_reason_code": outcome.last_reaction_cap_reason_code,
        "reaction_intake_run_id": reaction_intake_run_id,
        "last_chat_quarantine_event_id": outcome.last_chat_quarantine_event_id,
        "last_chat_quarantine_payload_hash": outcome.last_chat_quarantine_payload_hash,
        "last_chat_quarantine_reason_code": outcome.last_chat_quarantine_reason_code,
        "last_reaction_quarantine_event_id": outcome.last_reaction_quarantine_event_id,
        "last_reaction_quarantine_payload_hash": outcome.last_reaction_quarantine_payload_hash,
        "last_reaction_quarantine_reason_code": outcome.last_reaction_quarantine_reason_code,
        "chat_intake_drained": drain_processed,
        "drain_skipped": bool(args.no_drain),
        "runtime": asdict(health_report(conn)),
        "drain_batch": None
        if batch is None
        else {
            "run_id": batch.run.run_id,
            "items_total": batch.items_total,
            "items_processed": batch.items_processed,
            "items_failed": batch.items_failed,
            "retry_count": batch.retry_count,
            "dead_letter_ids": list(batch.dead_letter_ids),
        },
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"db_path: {payload['db_path']}")
        print(f"source_id: {payload['source_id']}")
        jo = outcome.json_objects
        ev = outcome.chat_envelopes_ingested
        rr = outcome.reaction_rows_inserted
        ra = outcome.reaction_rows_inserted_add
        rrm = outcome.reaction_rows_inserted_remove
        rq = outcome.reaction_quarantined
        vq = outcome.reaction_validation_rejects
        iq = outcome.reaction_insert_exceptions
        print(
            f"json_objects: {jo}  envelopes_ingested: {ev}  reaction_rows_inserted: {rr}  "
            f"rx_add={ra} rx_remove={rrm} rx_quarantined={rq}  "
            f"rx_quarantine_detail: val={vq} ins_exc={iq}  "
            f"chat_intake_drained: {drain_processed}  drain_skipped: {payload['drain_skipped']}"
        )
    return 0


def _watched_source_to_dict(source: WatchedSource) -> dict[str, Any]:
    payload = asdict(source)
    payload["source_type"] = str(source.source_type)
    return payload


def _replay_file_to_dict(file_summary: FixtureReplayFile) -> dict[str, Any]:
    return {
        "filename": file_summary.filename,
        "total_records": file_summary.total_records,
        "inserted_records": file_summary.inserted_records,
    }


def _stored_source_states(conn: Any) -> dict[str, Any]:
    from lark_to_notes.runtime.reconcile import SourceState

    rows = conn.execute(
        "SELECT source_id, last_message_id, last_message_timestamp FROM checkpoints"
    ).fetchall()
    return {
        row[0]: SourceState(
            source_id=row[0],
            latest_message_id=row[1] or "",
            latest_message_timestamp=row[2] or "",
        )
        for row in rows
    }


def _load_worker_service(config_path: Path, runtime_conn: sqlite3.Connection) -> tuple[Any, Any]:
    """Load the in-repo live chat adapter (``lark-cli`` transport + canonical ledger)."""

    from lark_to_notes.live.chat_live import ChatLiveAdapter
    from lark_to_notes.live.worker_config import load_live_worker_config

    resolved = config_path.expanduser().resolve()
    snapshot = load_live_worker_config(resolved)
    adapter = ChatLiveAdapter(snapshot, runtime_conn)
    return adapter.config, adapter


def _worker_poll_once(service: Any, *, sync_notes: bool) -> dict[str, int]:
    return cast("dict[str, int]", service.poll_once(sync_notes=sync_notes))


def _worker_backfill(
    service: Any,
    *,
    lookback_days: int | None,
    source_ids: set[str] | None,
    sync_notes: bool,
) -> dict[str, int]:
    return cast(
        "dict[str, int]",
        service.backfill_history(
            lookback_days=lookback_days,
            source_ids=source_ids,
            sync_notes=sync_notes,
        ),
    )


def _collect_live_source_states(service: Any, runtime_conn: Any) -> dict[str, Any]:
    from lark_to_notes.live.chat_live import ChatLiveAdapter

    if isinstance(service, ChatLiveAdapter):
        return service.collect_live_source_states(runtime_conn)
    raise TypeError("live reconcile requires ChatLiveAdapter from _load_worker_service")


def _runtime_lock_path(worker_config: Any) -> Path:
    lock_path = cast("Path", worker_config.vault_root) / "var" / "lark-to-notes.runtime.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    return lock_path


def _live_runtime_payload_fields(*, db_path: Path, worker_config: Any) -> dict[str, str]:
    """Stable JSON diagnostics for supervised live commands (machine-readable output)."""

    return {
        "canonical_db_path": str(db_path.expanduser().resolve()),
        "vault_root": str(cast("Path", worker_config.vault_root)),
        "runtime_lock_path": str(_runtime_lock_path(worker_config)),
        "config_state_db": str(cast("Path", worker_config.state_db)),
        "worker_state_db": str(cast("Path", worker_config.state_db)),
    }


def _print_live_worker_error(
    *,
    command: str,
    db_path: Path,
    config_path: Path,
    exc: Exception,
    json_output: bool,
    run_id: str | None = None,
    stage: str = "unknown",
) -> int:
    error_text = str(exc).strip() or exc.__class__.__name__
    lowered = error_text.lower()
    auth_related = any(
        token in lowered
        for token in (
            "auth",
            "token",
            "scope",
            "permission",
            "unauthorized",
            "forbidden",
            "401",
            "403",
            "expired",
        )
    )
    if auth_related:
        error_kind = "auth_scope"
        next_actions = [
            "Run `lark-cli auth login --as user` to refresh credentials.",
            "Verify source visibility and scopes for the configured app.",
            "Retry the command after re-authentication.",
        ]
    else:
        error_kind = "runtime_failure"
        next_actions = [
            "Inspect runtime diagnostics (`doctor --json`) for failed runs/dead letters.",
            "Retry once if this was a transient upstream or network issue.",
            "Escalate with run_id and stage details if the failure repeats.",
        ]
    payload = {
        "status": "error",
        "command": command,
        "db_path": str(db_path),
        "config_path": str(config_path),
        "runtime_run_id": run_id,
        "stage": stage,
        "error_kind": error_kind,
        "error": error_text,
        "auth_related": auth_related,
        "next_actions": next_actions,
    }
    if json_output:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"error: {error_text}")
        print(f"stage: {stage}")
        print(f"error_kind: {error_kind}")
        for action in next_actions:
            print(f"- {action}")
    return 1


def _load_project_config() -> dict[str, Any]:
    """Load config from standard locations (first match wins).

    Search order:
    1. ``$LARK_TO_NOTES_CONFIG`` env var (explicit path)
    2. ``~/.config/lark-to-notes/config.toml``
    3. ``./lark-to-notes.toml`` (current working directory)
    """
    candidates: list[Path | None] = [
        Path(os.environ["LARK_TO_NOTES_CONFIG"]) if "LARK_TO_NOTES_CONFIG" in os.environ else None,
        Path.home() / ".config" / "lark-to-notes" / "config.toml",
        Path("lark-to-notes.toml"),
    ]
    for path in candidates:
        if path is not None and path.exists():
            with path.open("rb") as f:
                return tomllib.load(f)
    return {}


_PROJECT_CONFIG: dict[str, Any] | None = None


def _project_config() -> dict[str, Any]:
    global _PROJECT_CONFIG
    if _PROJECT_CONFIG is None:
        _PROJECT_CONFIG = _load_project_config()
    return _PROJECT_CONFIG


def _default_db_path() -> Path:
    cfg = _project_config()
    if "db_path" in cfg:
        return Path(str(cfg["db_path"])).expanduser()
    return Path("var") / "lark-to-notes.db"


def _default_vault_root() -> Path:
    cfg = _project_config()
    if "vault_root" in cfg:
        return Path(str(cfg["vault_root"])).expanduser()
    return Path(".")


def _default_worker_config_path() -> Path:
    worker_dir = Path(__file__).resolve().parents[3] / "automation" / "lark_worker"
    config_path = worker_dir / "config.json"
    if config_path.exists():
        return config_path
    return worker_dir / "example-config.json"
