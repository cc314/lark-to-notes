from __future__ import annotations

import argparse
import json
from pathlib import Path

from .config import load_config
from .fixture_corpus import build_fixture_corpus
from .launchd import default_label, install_launch_agent, read_status, render_plist
from .service import WorkerService
from .source_access import build_source_access_report


def main(argv: list[str] | None = None) -> int:
    config_parser = argparse.ArgumentParser(add_help=False)
    config_parser.add_argument(
        "--config",
        default=str(default_config_path()),
        help="Path to worker JSON config",
    )

    parser = argparse.ArgumentParser(description="Local Lark worker for the notes vault")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("init-db", help="Create the SQLite state database", parents=[config_parser])

    poll_parser = subparsers.add_parser("poll-once", help="Poll all configured Lark sources once", parents=[config_parser])
    poll_parser.add_argument("--no-sync", action="store_true", help="Skip note sync after polling")

    history_parser = subparsers.add_parser(
        "backfill-history",
        help="Backfill older messages with a dedicated history lookback window",
        parents=[config_parser],
    )
    history_parser.add_argument("--days", type=int, help="Override configured history lookback days")
    history_parser.add_argument(
        "--source-id",
        action="append",
        default=[],
        help="Limit backfill to one or more configured source_id values",
    )
    history_parser.add_argument("--sync-notes", action="store_true", help="Also sync notes after backfill")

    sync_parser = subparsers.add_parser("sync-notes", help="Render notes from stored distilled items", parents=[config_parser])
    sync_parser.add_argument("--date", help="Sync only a specific YYYY-MM-DD note date")

    source_access_parser = subparsers.add_parser(
        "probe-source-access",
        help="Probe Lark source surfaces and write a local fixture-style access report",
        parents=[config_parser],
    )
    source_access_parser.add_argument(
        "--doc",
        action="append",
        default=[],
        help="Document URL or token to probe for doc fetch/comments/replies",
    )
    source_access_parser.add_argument(
        "--output-dir",
        default="raw/lark-worker/source-access",
        help="Directory for access report artifacts, relative to vault_root unless absolute",
    )
    source_access_parser.add_argument(
        "--days",
        type=int,
        help="Override the lookback window used for chat sampling",
    )
    source_access_parser.add_argument(
        "--page-size",
        type=int,
        default=1,
        help="Limit each probe to a small sample size",
    )

    fixture_parser = subparsers.add_parser(
        "build-fixture-corpus",
        help="Build a reusable fixture corpus manifest from captured raw logs and source-access artifacts",
        parents=[config_parser],
    )
    fixture_parser.add_argument(
        "--output-dir",
        default="raw/lark-worker/fixture-corpus",
        help="Directory for fixture corpus artifacts, relative to vault_root unless absolute",
    )
    fixture_parser.add_argument(
        "--source-access-dir",
        default="raw/lark-worker/source-access",
        help="Directory containing source-access artifacts to fold into the corpus",
    )

    subparsers.add_parser("listen-events", help="Listen for bot-visible Lark events and ingest them", parents=[config_parser])

    run_parser = subparsers.add_parser("run", help="Run the poll loop continuously", parents=[config_parser])
    run_parser.add_argument("--with-events", action="store_true", help="Also listen for bot events while polling")

    launchd_install_parser = subparsers.add_parser(
        "install-launchd",
        help="Install and start a user LaunchAgent for the worker",
        parents=[config_parser],
    )
    launchd_install_parser.add_argument("--label", help="Override the launchd label")
    launchd_install_parser.add_argument("--with-events", action="store_true", help="Also run the bot event listener")
    launchd_install_parser.add_argument("--no-start", action="store_true", help="Install but do not kickstart the agent")

    launchd_status_parser = subparsers.add_parser(
        "launchd-status",
        help="Show launchd status for the worker agent",
        parents=[config_parser],
    )
    launchd_status_parser.add_argument("--label", help="Override the launchd label")

    launchd_plist_parser = subparsers.add_parser(
        "print-launchd-plist",
        help="Print the generated launchd plist for the worker",
        parents=[config_parser],
    )
    launchd_plist_parser.add_argument("--label", help="Override the launchd label")
    launchd_plist_parser.add_argument("--with-events", action="store_true", help="Also run the bot event listener")

    args = parser.parse_args(argv)
    config_path = Path(args.config).expanduser().resolve()
    config = load_config(config_path)
    service = WorkerService(config)

    if args.command == "init-db":
        service.initialize()
        print(json.dumps({"ok": True, "db": str(config.state_db)}, indent=2))
        return 0

    if args.command == "poll-once":
        result = service.poll_once(sync_notes=not args.no_sync)
        print(json.dumps({"ok": True, **result}, indent=2))
        return 0

    if args.command == "backfill-history":
        result = service.backfill_history(
            lookback_days=args.days,
            source_ids=set(args.source_id) or None,
            sync_notes=args.sync_notes,
        )
        print(json.dumps({"ok": True, **result}, indent=2))
        return 0

    if args.command == "sync-notes":
        if args.date:
            service.sync_notes(touched_dates={args.date})
        else:
            service.sync_notes()
        print(json.dumps({"ok": True}, indent=2))
        return 0

    if args.command == "probe-source-access":
        output_dir = Path(args.output_dir).expanduser()
        if not output_dir.is_absolute():
            output_dir = (config.vault_root / output_dir).resolve()
        result = build_source_access_report(
            config,
            doc_targets=list(args.doc),
            output_dir=output_dir,
            lookback_days=args.days,
            page_size=args.page_size,
        )
        print(json.dumps({"ok": True, **result}, indent=2, ensure_ascii=False))
        return 0

    if args.command == "build-fixture-corpus":
        output_dir = Path(args.output_dir).expanduser()
        if not output_dir.is_absolute():
            output_dir = (config.vault_root / output_dir).resolve()
        source_access_dir = Path(args.source_access_dir).expanduser()
        if not source_access_dir.is_absolute():
            source_access_dir = (config.vault_root / source_access_dir).resolve()
        result = build_fixture_corpus(
            config,
            output_dir=output_dir,
            source_access_dir=source_access_dir,
        )
        print(json.dumps({"ok": True, **result}, indent=2, ensure_ascii=False))
        return 0

    if args.command == "listen-events":
        service.listen_events(sync_notes=True)
        return 0

    if args.command == "run":
        service.run_forever(with_events=args.with_events)
        return 0

    if args.command == "install-launchd":
        result = install_launch_agent(
            config=config,
            config_path=config_path,
            label=args.label,
            with_events=args.with_events,
            start=not args.no_start,
        )
        print(json.dumps({"ok": True, **result}, indent=2))
        return 0

    if args.command == "launchd-status":
        result = read_status(config=config, label=args.label)
        print(json.dumps({"ok": True, **result}, indent=2))
        return 0

    if args.command == "print-launchd-plist":
        payload = render_plist(
            config=config,
            config_path=config_path,
            label=args.label or default_label(config),
            with_events=args.with_events,
        )
        print(payload.decode("utf-8"))
        return 0

    return 1


def default_config_path() -> Path:
    module_dir = Path(__file__).resolve().parent
    config_path = module_dir / "config.json"
    if config_path.exists():
        return config_path
    return module_dir / "example-config.json"
