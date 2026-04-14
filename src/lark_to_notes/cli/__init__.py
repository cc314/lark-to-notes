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
