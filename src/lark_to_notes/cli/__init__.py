"""CLI entry point for lark-to-notes."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
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

    # ---------------------------------------------------------------------------
    # Lark-connectivity stubs (require automation/lark_worker at runtime)
    # ---------------------------------------------------------------------------
    for cmd, help_text in [
        ("sync-once", "Poll all enabled sources once and ingest new messages"),
        ("sync-daemon", "Run continuous polling loop for all enabled sources"),
        ("backfill", "Re-ingest historical messages for a watched source"),
    ]:
        stub_parser = subparsers.add_parser(cmd, help=help_text)
        stub_parser.set_defaults(handler=_handle_lark_stub, lark_command=cmd)

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
    corpus = load_fixture_corpus(args.fixture_corpus)
    conn = connect(":memory:")
    init_db(conn)
    replay_summary = corpus.replay_summary(conn)
    payload = {
        "status": "ok" if replay_summary.total_records == corpus.record_count else "error",
        "schema_version": SCHEMA_VERSION,
        "migrations": all_versions(),
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
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"status: {payload['status']}")
        print(f"schema_version: {SCHEMA_VERSION}")
        print(
            f"fixture_corpus: {corpus.record_count} records across "
            f"{len(corpus.scenario_names)} scenarios"
        )
        print(
            f"replay: {replay_summary.file_count} file(s), {replay_summary.total_records} record(s)"
        )
    return 0


def _handle_feedback_import(args: argparse.Namespace) -> int:
    db_path: Path = args.db
    artifact_path: Path = args.artifact
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(db_path)
    init_db(conn)
    artifact = load_feedback_artifact(artifact_path)
    result = apply_feedback_artifact(
        conn,
        artifact,
        artifact_path=str(artifact_path),
    )
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
    from lark_to_notes.distill import DistillInput, default_classifier
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
        result = classifier.classify(dinput)
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
    from lark_to_notes.runtime.reconcile import SourceState, reconcile_cursors

    db_path: Path = args.db
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(db_path)
    init_db(conn)

    # Build SourceState from stored checkpoints — the "current" state is
    # whatever we last recorded.  This surfaces cursor gaps without requiring
    # a live Lark connection.
    rows = conn.execute(
        "SELECT source_id, last_message_id, last_message_timestamp FROM checkpoints"
    ).fetchall()
    source_states: dict[str, SourceState] = {}
    for row in rows:
        source_states[row[0]] = SourceState(
            source_id=row[0],
            latest_message_id=row[1] or "",
            latest_message_timestamp=row[2] or "",
        )

    report = reconcile_cursors(conn, source_states)
    conn.commit()

    payload = {
        "db_path": str(db_path),
        "sources_checked": report.source_ids_checked,
        "gaps_found": report.gaps_found,
        "repairs_attempted": report.repairs_attempted,
        "repairs_succeeded": report.repairs_succeeded,
        "gap_details": list(report.gap_details),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"db_path: {payload['db_path']}")
        print(
            f"checked {report.source_ids_checked} source(s); "
            f"{report.gaps_found} gap(s) detected; "
            f"{report.repairs_succeeded}/{report.repairs_attempted} repair(s) succeeded"
        )
        for detail in report.gap_details:
            print(f"  ! {detail}")
    return 1 if report.gaps_found > report.repairs_succeeded else 0


def _handle_budget_status(args: argparse.Namespace) -> int:
    import datetime

    from lark_to_notes.budget.store import get_day_budget_snapshot, get_run_budget_snapshot

    db_path: Path = args.db
    run_id: str | None = args.run_id
    day: str | None = args.day
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(db_path)
    init_db(conn)

    if run_id:
        snap = get_run_budget_snapshot(conn, run_id)
        scope = f"run_id={run_id}"
    elif day:
        snap = get_day_budget_snapshot(conn, day)
        scope = f"day={day}"
    else:
        today = datetime.date.today().isoformat()
        snap = get_day_budget_snapshot(conn, today)
        scope = f"day={today} (default)"

    payload: dict[str, Any] = {
        "db_path": str(db_path),
        "scope": scope,
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
                f"cached: {snap.cached_count}  "
                f"fallbacks: {snap.fallback_count}"
            )
            print(
                f"tokens: {snap.prompt_tokens_sum} prompt / "
                f"{snap.completion_tokens_sum} completion"
            )
            p95 = snap.p95_latency_ms
            print(f"p95_latency_ms: {p95 if p95 is not None else 'n/a'}")
    return 0


def _handle_lark_stub(args: argparse.Namespace) -> int:
    cmd: str = args.lark_command
    msg = (
        f"'{cmd}' requires a live Lark connection which is provided by "
        "automation/lark_worker/. Run the worker process separately and use "
        "'replay' to ingest its JSONL output into the local store."
    )
    print(f"error: {msg}")
    return 1


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


def _default_db_path() -> Path:
    return Path("var") / "lark-to-notes.db"
