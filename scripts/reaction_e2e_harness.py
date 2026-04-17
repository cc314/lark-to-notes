#!/usr/bin/env python3
"""Deterministic reaction-path E2E harness (lw-pzj.10.7).

Exit codes (stable):
  0 — all CLI steps succeeded and golden expectations matched.
  1 — fixture/IO/parse failure, or any CLI subcommand returned non-zero.
  2 — golden counter / JSON subset drift (observed values differ from golden.json).

Logging contract:
  stdout — **NDJSON only** (one JSON object per line) for machine consumers.
  stderr — human-readable banners, ISO-8601 timestamps, and absolute paths.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SRC = _REPO_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))


def _utc_ts() -> str:
    return datetime.now(UTC).isoformat()


def _stderr(msg: str) -> None:
    print(f"[{_utc_ts()}] {msg}", file=sys.stderr, flush=True)


def _emit(record: dict[str, Any]) -> None:
    line = json.dumps({**record, "ts": _utc_ts()}, ensure_ascii=False)
    print(line, file=sys.stdout, flush=True)


def _step_footer(
    *,
    name: str,
    status: str,
    t0: float,
    exit_code: int,
    extra: dict[str, Any] | None = None,
) -> None:
    duration_ms = round((time.monotonic() - t0) * 1000)
    row: dict[str, Any] = {
        "kind": "step",
        "step": name,
        "status": status,
        "duration_ms": duration_ms,
        "exit_code": exit_code,
    }
    if extra:
        row["detail"] = extra
    _emit(row)


def _invoke_cli(argv: list[str], *, stdin_text: str | None = None) -> tuple[int, str]:
    from lark_to_notes.cli import run

    old_in = sys.stdin
    old_out = sys.stdout
    buf = io.StringIO()
    try:
        if stdin_text is not None:
            sys.stdin = io.StringIO(stdin_text)
        else:
            sys.stdin = io.StringIO("")
        sys.stdout = buf
        rc = run(argv)
    finally:
        sys.stdin = old_in
        sys.stdout = old_out
    return rc, buf.getvalue()


def _nested_subset(actual: Any, expected: Any, path: str = "$") -> tuple[bool, str]:
    if isinstance(expected, dict):
        if not isinstance(actual, dict):
            return False, f"{path}: expected object, got {type(actual).__name__}"
        for key, ev in expected.items():
            if key not in actual:
                return False, f"{path}: missing key {key!r}"
            ok, err = _nested_subset(actual[key], ev, f"{path}.{key}")
            if not ok:
                return False, err
        return True, ""
    if isinstance(expected, list):
        if not isinstance(actual, list):
            return False, f"{path}: expected array, got {type(actual).__name__}"
        if len(actual) != len(expected):
            return (
                False,
                f"{path}: expected {len(expected)} list entries, got {len(actual)}",
            )
        for i, ev in enumerate(expected):
            ok, err = _nested_subset(actual[i], ev, f"{path}[{i}]")
            if not ok:
                return False, err
        return True, ""
    if actual != expected:
        return False, f"{path}: expected {expected!r}, got {actual!r}"
    return True, ""


def _parse_cli_json(stdout: str) -> dict[str, Any]:
    stdout = stdout.strip()
    if not stdout:
        raise ValueError("empty CLI stdout")
    return cast(dict[str, Any], json.loads(stdout))


def main() -> int:
    default_fixture = _REPO_ROOT / "tests/fixtures/reaction-e2e/harness.ndjson"
    default_golden = _REPO_ROOT / "tests/fixtures/reaction-e2e/golden.json"
    default_corpus = _REPO_ROOT / "tests/fixtures/lark-worker/fixture-corpus"

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="SQLite path (default: tempfile under --artifacts-dir or system temp)",
    )
    parser.add_argument(
        "--fixture",
        type=Path,
        default=default_fixture,
        help="NDJSON stdin fixture",
    )
    parser.add_argument(
        "--golden",
        type=Path,
        default=default_golden,
        help="Golden JSON expectations",
    )
    parser.add_argument(
        "--fixture-corpus",
        type=Path,
        default=default_corpus,
        help="Doctor --fixture-corpus path",
    )
    parser.add_argument(
        "--artifacts-dir",
        type=Path,
        default=None,
        help="If set, mkdir -p and write doctor/sync JSON snapshots here",
    )
    parser.add_argument(
        "--source-id",
        default="dm:ou_e2e_harness",
        help="sync-events --source-id",
    )
    parser.add_argument(
        "--markdown-path",
        type=Path,
        default=None,
        help="Optional markdown note for vault-reconcile-reactions dry-run",
    )
    args = parser.parse_args()

    art_dir: Path | None = args.artifacts_dir
    if art_dir is not None:
        art_dir = art_dir.expanduser().resolve()
        art_dir.mkdir(parents=True, exist_ok=True)

    db_path = args.db
    if db_path is None:
        if art_dir is not None:
            db_path = art_dir / "reaction_e2e.sqlite"
        else:
            fd, tmp = tempfile.mkstemp(suffix=".sqlite")
            os.close(fd)
            db_path = Path(tmp)
    db_path = db_path.expanduser().resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    fixture_path = args.fixture.expanduser().resolve()
    golden_path = args.golden.expanduser().resolve()
    corpus_path = args.fixture_corpus.expanduser().resolve()

    _stderr("=== reaction E2E harness ===")
    _stderr(f"repo_root={_REPO_ROOT}")
    _stderr(f"db_path={db_path}")
    _stderr(f"fixture_path={fixture_path}")
    _stderr(f"golden_path={golden_path}")
    _stderr(f"fixture_corpus={corpus_path}")
    if art_dir is not None:
        _stderr(f"artifacts_dir={art_dir}")

    _emit({"kind": "harness.started", "db_path": str(db_path), "fixture": str(fixture_path)})

    if not fixture_path.is_file():
        _stderr(f"error: fixture not found: {fixture_path}")
        _emit(
            {
                "kind": "harness.complete",
                "status": "error",
                "exit_code": 1,
                "reason": "missing_fixture",
            },
        )
        return 1
    if not golden_path.is_file():
        _stderr(f"error: golden not found: {golden_path}")
        _emit(
            {
                "kind": "harness.complete",
                "status": "error",
                "exit_code": 1,
                "reason": "missing_golden",
            },
        )
        return 1

    golden = json.loads(golden_path.read_text(encoding="utf-8"))
    observed: dict[str, Any] = {}

    # --- sync-events ---
    t0 = time.monotonic()
    _stderr("--- step: sync-events (stdin from fixture) ---")
    ndjson_body = fixture_path.read_text(encoding="utf-8")
    sync_argv = [
        "sync-events",
        "--db",
        str(db_path),
        "--source-id",
        str(args.source_id),
        "--coalesce-window-seconds",
        "0",
        "--json",
    ]
    rc, sync_out = _invoke_cli(sync_argv, stdin_text=ndjson_body)
    if rc != 0:
        _step_footer(name="sync_events", status="failed", t0=t0, exit_code=rc)
        _emit(
            {
                "kind": "harness.complete",
                "status": "error",
                "exit_code": 1,
                "reason": "sync_events_cli",
            },
        )
        return 1
    sync_payload = _parse_cli_json(sync_out)
    observed["sync_events"] = {
        "json_objects": sync_payload["json_objects"],
        "envelopes_ingested": sync_payload["envelopes_ingested"],
        "reaction_rows_inserted": sync_payload["reaction_rows_inserted"],
        "reaction_quarantined": sync_payload["reaction_quarantined"],
    }
    if art_dir is not None:
        p = art_dir / "sync_events.payload.json"
        sync_txt = json.dumps(sync_payload, ensure_ascii=False, indent=2) + "\n"
        p.write_text(sync_txt, encoding="utf-8")
        _stderr(f"wrote {p}")
    _step_footer(
        name="sync_events",
        status="ok",
        t0=t0,
        exit_code=0,
        extra={"reaction_rows_inserted": sync_payload["reaction_rows_inserted"]},
    )

    # --- doctor ---
    t0 = time.monotonic()
    _stderr("--- step: doctor --json ---")
    doc_argv = [
        "doctor",
        "--db",
        str(db_path),
        "--fixture-corpus",
        str(corpus_path),
        "--json",
    ]
    rc, doc_out = _invoke_cli(doc_argv)
    if rc != 0:
        _step_footer(name="doctor", status="failed", t0=t0, exit_code=rc)
        _emit(
            {
                "kind": "harness.complete",
                "status": "error",
                "exit_code": 1,
                "reason": "doctor_cli",
            },
        )
        return 1
    doc_payload = _parse_cli_json(doc_out)
    rph = doc_payload.get("reaction_pipeline_health")
    if not isinstance(rph, dict):
        _stderr("error: doctor payload missing reaction_pipeline_health")
        _emit(
            {
                "kind": "harness.complete",
                "status": "error",
                "exit_code": 1,
                "reason": "doctor_shape",
            },
        )
        return 1
    observed["doctor"] = {
        "reaction_pipeline_health": {
            "counts": {"reaction_events_ingested": rph["counts"]["reaction_events_ingested"]},
            "signals": {
                "orphan_queue_rows_waiting_on_parent_raw": rph["signals"][
                    "orphan_queue_rows_waiting_on_parent_raw"
                ],
            },
        }
    }
    if art_dir is not None:
        p = art_dir / "doctor.payload.json"
        p.write_text(json.dumps(doc_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        _stderr(f"wrote {p}")
    _step_footer(
        name="doctor",
        status="ok",
        t0=t0,
        exit_code=0,
        extra={"reaction_events_ingested": rph["counts"]["reaction_events_ingested"]},
    )

    # --- replay-reactions ---
    t0 = time.monotonic()
    _stderr("--- step: replay-reactions --json ---")
    rr_argv = ["replay-reactions", "--db", str(db_path), "--json"]
    rc, rr_out = _invoke_cli(rr_argv)
    if rc != 0:
        _step_footer(name="replay_reactions", status="failed", t0=t0, exit_code=rc)
        _emit(
            {
                "kind": "harness.complete",
                "status": "error",
                "exit_code": 1,
                "reason": "replay_reactions_cli",
            },
        )
        return 1
    rr_payload = _parse_cli_json(rr_out)
    observed["replay_reactions"] = {
        "pairs_processed": rr_payload["pairs_processed"],
        "rows_attached": rr_payload["rows_attached"],
    }
    _step_footer(
        name="replay_reactions",
        status="ok",
        t0=t0,
        exit_code=0,
        extra=observed["replay_reactions"],
    )

    # --- reaction-reclassify-map ---
    t0 = time.monotonic()
    _stderr("--- step: reaction-reclassify-map --json ---")
    rc, map_out = _invoke_cli(["reaction-reclassify-map", "--json"])
    if rc != 0:
        _step_footer(name="reaction_reclassify_map", status="failed", t0=t0, exit_code=rc)
        _emit(
            {
                "kind": "harness.complete",
                "status": "error",
                "exit_code": 1,
                "reason": "reaction_reclassify_map_cli",
            },
        )
        return 1
    map_payload = _parse_cli_json(map_out)
    observed["reaction_reclassify_map"] = {
        "bead": map_payload["bead"],
        "policy_bump_distill_stages": map_payload["policy_bump_distill_stages"],
        "governance_only_bump_distill_stages": map_payload["governance_only_bump_distill_stages"],
    }
    _step_footer(name="reaction_reclassify_map", status="ok", t0=t0, exit_code=0)

    # --- vault-reconcile-reactions (optional) ---
    t0 = time.monotonic()
    md_path = args.markdown_path
    if md_path is None or not md_path.is_file():
        _stderr(
            "vault-reconcile-reactions: skipped (no --markdown-path or file missing; "
            "pass a note path to exercise reconcile JSON)",
        )
        _step_footer(
            name="vault_reconcile_reactions",
            status="skipped",
            t0=t0,
            exit_code=0,
            extra={"reason": "no_markdown_path"},
        )
    else:
        _stderr(
            "--- step: vault-reconcile-reactions --json (read-only) "
            f"path={md_path.resolve()} ---",
        )
        vr_argv = [
            "vault-reconcile-reactions",
            "--db",
            str(db_path),
            "--markdown-path",
            str(md_path.resolve()),
            "--json",
        ]
        rc, vr_out = _invoke_cli(vr_argv)
        if rc not in (0, 1):
            # CLI returns 1 when drift_count > 0; other codes are unexpected
            _step_footer(name="vault_reconcile_reactions", status="failed", t0=t0, exit_code=rc)
            _emit(
                {
                    "kind": "harness.complete",
                    "status": "error",
                    "exit_code": 1,
                    "reason": "vault_reconcile_reactions_cli",
                },
            )
            return 1
        vr_payload = _parse_cli_json(vr_out)
        observed["vault_reconcile_reactions"] = {"drift_count": vr_payload["drift_count"]}
        if art_dir is not None:
            p = art_dir / "vault_reconcile.payload.json"
            vr_txt = json.dumps(vr_payload, ensure_ascii=False, indent=2) + "\n"
            p.write_text(vr_txt, encoding="utf-8")
            _stderr(f"wrote {p}")
        _step_footer(
            name="vault_reconcile_reactions",
            status="ok",
            t0=t0,
            exit_code=rc,
            extra={"drift_count": vr_payload["drift_count"]},
        )

    # --- golden ---
    ok, err = _nested_subset(observed, golden)
    if not ok:
        _stderr(f"golden mismatch: {err}")
        _emit(
            {
                "kind": "golden_mismatch",
                "error": err,
                "observed": observed,
            },
        )
        _emit(
            {
                "kind": "harness.complete",
                "status": "golden_mismatch",
                "exit_code": 2,
            },
        )
        return 2

    _emit({"kind": "harness.complete", "status": "ok", "exit_code": 0})
    _stderr("=== reaction E2E harness finished OK ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
