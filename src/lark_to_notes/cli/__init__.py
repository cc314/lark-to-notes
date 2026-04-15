"""CLI entry point for lark-to-notes."""

from __future__ import annotations

import argparse
import importlib
import json
import time
from dataclasses import asdict
from datetime import date, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from lark_to_notes import __version__
from lark_to_notes.feedback import (
    apply_feedback_artifact,
    load_feedback_artifact,
)
from lark_to_notes.intake.replay import replay_jsonl_dir
from lark_to_notes.storage.db import connect, init_db, list_watched_sources
from lark_to_notes.storage.schema import SCHEMA_VERSION, all_versions
from lark_to_notes.testing import FixtureReplayFile, load_fixture_corpus

if TYPE_CHECKING:
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
    replay_parser.add_argument("--raw-dir", type=Path, default=Path("raw") / "lark-worker")
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
        default=Path("raw") / "lark-worker" / "fixture-corpus",
    )
    doctor_parser.add_argument("--db", type=Path, default=_default_db_path())
    doctor_parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    doctor_parser.set_defaults(handler=_handle_doctor)

    feedback_parser = subparsers.add_parser(
        "feedback",
        help="Import structured review feedback",
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
        default=Path("."),
        help="Root of the Obsidian vault (default: current directory)",
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
        help="Optional lark_worker JSON config for live source reconciliation",
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
        help="Path to the live lark_worker JSON config",
    )
    sync_once_parser.add_argument("--db", type=Path, default=_default_db_path())
    sync_once_parser.add_argument(
        "--no-sync",
        action="store_true",
        help="Skip note sync after polling",
    )
    sync_once_parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    sync_once_parser.set_defaults(handler=_handle_sync_once)

    sync_daemon_parser = subparsers.add_parser(
        "sync-daemon",
        help="Run continuous polling loop for all enabled sources",
    )
    sync_daemon_parser.add_argument(
        "--config",
        type=Path,
        default=_default_worker_config_path(),
        help="Path to the live lark_worker JSON config",
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
    sync_daemon_parser.set_defaults(handler=_handle_sync_daemon)

    backfill_parser = subparsers.add_parser(
        "backfill",
        help="Re-ingest historical messages for watched sources",
    )
    backfill_parser.add_argument(
        "--config",
        type=Path,
        default=_default_worker_config_path(),
        help="Path to the live lark_worker JSON config",
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


def _handle_doctor(args: argparse.Namespace) -> int:
    from lark_to_notes.runtime.registry import health_report

    corpus = load_fixture_corpus(args.fixture_corpus)
    replay_conn = connect(":memory:")
    init_db(replay_conn)
    replay_summary = corpus.replay_summary(replay_conn)
    runtime_db_path: Path = args.db
    runtime_db_path.parent.mkdir(parents=True, exist_ok=True)
    runtime_conn = connect(runtime_db_path)
    init_db(runtime_conn)
    runtime_health = health_report(runtime_conn)
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
    from lark_to_notes.distill import DistillInput, classify_with_routing, default_classifier
    from lark_to_notes.intake.ledger import list_raw_messages
    from lark_to_notes.tasks import derive_fingerprint, upsert_task
    from lark_to_notes.tasks.registry import get_task_by_fingerprint

    db_path: Path = args.db
    dry_run: bool = args.dry_run
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(db_path)
    init_db(conn)

    messages = list_raw_messages(conn, source_id=args.source_id, limit=args.limit)
    classifier = default_classifier
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
        fp = derive_fingerprint(msg.content, msg.source_id, msg.created_at)
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

    payload = {
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
    live_result = None
    if config_path is not None:
        try:
            config, service = _load_worker_service(config_path)
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
        _mirror_worker_state(conn, config)
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
            _mirror_worker_state(conn, config)
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
    quality_metrics = enforcer.get_quality_metrics()

    payload: dict[str, Any] = {
        "db_path": str(db_path),
        "scope": scope,
        "cache_hit_rate": snap.cache_hit_rate,
        "quality_metrics": asdict(quality_metrics),
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
    return 0


def _handle_sync_once(args: argparse.Namespace) -> int:
    db_path: Path = args.db
    config_path: Path = args.config
    db_path.parent.mkdir(parents=True, exist_ok=True)
    runtime_conn = connect(db_path)
    init_db(runtime_conn)
    try:
        config, service = _load_worker_service(config_path)
    except Exception as exc:
        return _print_live_worker_error(
            command="sync-once",
            db_path=db_path,
            config_path=config_path,
            exc=exc,
            json_output=args.json,
        )

    from lark_to_notes.runtime.lock import RuntimeLock
    from lark_to_notes.runtime.registry import finish_run, health_report, start_run

    run = start_run(runtime_conn, "sync-once")
    result: dict[str, int]
    try:
        with RuntimeLock(_runtime_lock_path(config), owner_tag=f"sync-once:{run.run_id}"):
            service.initialize()
            result = _worker_poll_once(service, sync_notes=not args.no_sync)
        _mirror_worker_state(runtime_conn, config)
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
        )

    payload = {
        "db_path": str(db_path),
        "config_path": str(config_path),
        "runtime_run_id": completed.run_id,
        "worker_state_db": str(config.state_db),
        "inserted_messages": result.get("inserted_messages", 0),
        "distilled_items": result.get("distilled_items", 0),
        "sync_notes": not args.no_sync,
        "runtime": asdict(health_report(runtime_conn)),
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
        config, service = _load_worker_service(config_path)
    except Exception as exc:
        return _print_live_worker_error(
            command="sync-daemon",
            db_path=db_path,
            config_path=config_path,
            exc=exc,
            json_output=args.json,
        )

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
                _mirror_worker_state(runtime_conn, config)
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
        "worker_state_db": str(config.state_db),
        "cycle_count": cycle_count,
        "idle_cycles": idle_cycles,
        "inserted_messages": inserted_messages,
        "distilled_items": distilled_items,
        "run_ids": run_ids,
        "runtime": asdict(health_report(runtime_conn)),
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
        config, service = _load_worker_service(config_path)
    except Exception as exc:
        return _print_live_worker_error(
            command="backfill",
            db_path=db_path,
            config_path=config_path,
            exc=exc,
            json_output=args.json,
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
        _mirror_worker_state(runtime_conn, config)
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
        )

    payload = {
        "db_path": str(db_path),
        "config_path": str(config_path),
        "runtime_run_id": completed.run_id,
        "worker_state_db": str(config.state_db),
        "sources_scanned": result.get("sources_scanned", 0),
        "inserted_messages": result.get("inserted_messages", 0),
        "distilled_items": result.get("distilled_items", 0),
        "sync_notes": args.sync_notes,
        "runtime": asdict(health_report(runtime_conn)),
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


def _load_worker_service(config_path: Path) -> tuple[Any, Any]:
    config_module = importlib.import_module("automation.lark_worker.config")
    service_module = importlib.import_module("automation.lark_worker.service")
    resolved = config_path.expanduser().resolve()
    config = cast("Any", config_module.load_config(resolved))
    worker_service = cast("Any", service_module.WorkerService)
    return config, worker_service(config)


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


def _mirror_worker_state(runtime_conn: Any, worker_config: Any) -> None:
    from lark_to_notes.config.sources import Checkpoint, WatchedSource
    from lark_to_notes.storage.db import upsert_checkpoint, upsert_watched_source

    worker_db_module = importlib.import_module("automation.lark_worker.db")
    worker_connect = cast("Any", worker_db_module.connect)
    with worker_connect(worker_config.state_db) as worker_conn:
        source_rows = worker_conn.execute(
            "SELECT source_id, source_type, external_id, name, enabled FROM watched_sources"
        ).fetchall()
        checkpoint_rows = worker_conn.execute(
            """
            SELECT source_id, last_message_id, last_message_timestamp, page_token, updated_at
            FROM checkpoints
            """
        ).fetchall()

    for row in source_rows:
        upsert_watched_source(
            runtime_conn,
            WatchedSource(
                source_id=row["source_id"],
                source_type=_map_worker_source_type(row["source_type"]),
                external_id=row["external_id"],
                name=row["name"],
                enabled=bool(row["enabled"]),
                config={"worker_source_type": row["source_type"]},
            ),
        )
    for row in checkpoint_rows:
        upsert_checkpoint(
            runtime_conn,
            Checkpoint(
                source_id=row["source_id"],
                last_message_id=row["last_message_id"],
                last_message_timestamp=row["last_message_timestamp"],
                page_token=row["page_token"],
                updated_at=row["updated_at"],
            ),
        )


def _collect_live_source_states(service: Any, runtime_conn: Any) -> dict[str, Any]:
    from lark_to_notes.runtime.reconcile import SourceState
    from lark_to_notes.storage.db import get_checkpoint

    end_date = (date.today() + timedelta(days=1)).isoformat()
    source_states: dict[str, Any] = {}
    for source in service.config.enabled_sources:
        checkpoint = get_checkpoint(runtime_conn, source.source_id)
        if source.source_type not in {"dm_user", "chat"}:
            if checkpoint is None:
                continue
            source_states[source.source_id] = SourceState(
                source_id=source.source_id,
                latest_message_id=checkpoint.last_message_id or "",
                latest_message_timestamp=checkpoint.last_message_timestamp or "",
            )
            continue

        response = service.client.list_chat_messages(
            source=source,
            start_date=_live_start_date(
                checkpoint.last_message_timestamp if checkpoint is not None else None,
                lookback_days=service.config.poll_lookback_days,
            ),
            end_date=end_date,
            page_size=1,
            sort="desc",
        )
        messages = response.get("data", {}).get("messages", [])
        if messages:
            latest = messages[0]
            source_states[source.source_id] = SourceState(
                source_id=source.source_id,
                latest_message_id=latest.get("message_id", ""),
                latest_message_timestamp=latest.get("create_time", ""),
            )
            continue
        if checkpoint is None:
            continue
        source_states[source.source_id] = SourceState(
            source_id=source.source_id,
            latest_message_id=checkpoint.last_message_id or "",
            latest_message_timestamp=checkpoint.last_message_timestamp or "",
        )
    return source_states


def _live_start_date(timestamp: str | None, *, lookback_days: int) -> str:
    if timestamp:
        return timestamp.split("T", 1)[0].split(" ", 1)[0]
    return (date.today() - timedelta(days=lookback_days)).isoformat()


def _map_worker_source_type(source_type: str) -> Any:
    from lark_to_notes.config.sources import SourceType

    mapping = {
        "dm_user": SourceType.DM,
        "dm": SourceType.DM,
        "chat": SourceType.GROUP,
        "group": SourceType.GROUP,
        "doc": SourceType.DOC,
    }
    if source_type not in mapping:
        raise ValueError(f"unsupported worker source_type: {source_type!r}")
    return mapping[source_type]


def _runtime_lock_path(worker_config: Any) -> Path:
    lock_path = cast("Path", worker_config.vault_root) / "var" / "lark-to-notes.runtime.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    return lock_path


def _print_live_worker_error(
    *,
    command: str,
    db_path: Path,
    config_path: Path,
    exc: Exception,
    json_output: bool,
    run_id: str | None = None,
) -> int:
    error_text = str(exc).strip() or exc.__class__.__name__
    guidance = (
        "If this is an auth or scope-expiry issue, re-authenticate with "
        "`lark-cli auth login --as user` and confirm the watched sources are "
        "visible to the configured Lark app."
    )
    payload = {
        "status": "error",
        "command": command,
        "db_path": str(db_path),
        "config_path": str(config_path),
        "runtime_run_id": run_id,
        "error": error_text,
        "guidance": guidance,
    }
    if json_output:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"error: {error_text}")
        print(guidance)
    return 1


def _default_db_path() -> Path:
    return Path("var") / "lark-to-notes.db"


def _default_worker_config_path() -> Path:
    worker_dir = Path(__file__).resolve().parents[3] / "automation" / "lark_worker"
    config_path = worker_dir / "config.json"
    if config_path.exists():
        return config_path
    return worker_dir / "example-config.json"
