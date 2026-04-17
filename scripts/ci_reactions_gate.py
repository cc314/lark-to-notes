#!/usr/bin/env python3
"""Canonical CI gate for reaction-related verification (lw-pzj.10.12).

Runs, in order (unless ``--only`` narrows the set):

1. ``uv run ruff check .``
2. ``uv run ruff format --check .``
3. ``uv run mypy --strict src tests``
4. ``uv run pytest``
5. ``uv run python scripts/reaction_e2e_harness.py`` (``ARTIFACT_DIR`` optional)
6. ``doctor --json`` subset compare against ``tests/fixtures/ci-gate/doctor_subset.json``

Stdout emits one NDJSON object per line (``kind: ci_gate.*``). Stderr carries human banners,
timestamps, and absolute paths. Exit ``0`` if all selected steps succeed; ``1`` on first failure.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, cast

_REPO = Path(__file__).resolve().parents[1]
_DEFAULT_CORPUS = _REPO / "tests/fixtures/lark-worker/fixture-corpus"
_DEFAULT_DOCTOR_GOLDEN = _REPO / "tests/fixtures/ci-gate/doctor_subset.json"
_E2E_SCRIPT = _REPO / "scripts/reaction_e2e_harness.py"

_STEP_CHOICES = (
    "ruff_check",
    "ruff_format",
    "mypy",
    "pytest",
    "e2e",
    "doctor",
)


def _log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def _emit(row: dict[str, Any]) -> None:
    print(json.dumps(row, ensure_ascii=False), file=sys.stdout, flush=True)


def _uv_bin() -> str:
    u = shutil.which("uv")
    if not u:
        _log("error: uv not found on PATH (install uv or use the project dev shell)")
        sys.exit(1)
    return u


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


def _doctor_extract_subset(payload: dict[str, Any]) -> dict[str, Any]:
    rph = payload.get("reaction_pipeline_health")
    if not isinstance(rph, dict):
        raise ValueError("doctor JSON missing reaction_pipeline_health object")
    mrx = payload.get("message_reaction_events")
    if not isinstance(mrx, dict):
        raise ValueError("doctor JSON missing message_reaction_events object")
    counts = rph.get("counts")
    if not isinstance(counts, dict):
        raise ValueError("reaction_pipeline_health.counts not an object")
    cap = rph.get("cap_and_deferral")
    if not isinstance(cap, dict):
        raise ValueError("reaction_pipeline_health.cap_and_deferral not an object")
    return {
        "status": payload.get("status"),
        "schema_version": payload.get("schema_version"),
        "reaction_pipeline_health": {
            "status": rph.get("status"),
            "counts": {"reaction_events_ingested": counts.get("reaction_events_ingested")},
            "cap_and_deferral": {"deferral_row_count": cap.get("deferral_row_count")},
        },
        "message_reaction_events": {
            "row_count": mrx.get("row_count"),
            "orphan_row_count": mrx.get("orphan_row_count"),
            "linked_row_count": mrx.get("linked_row_count"),
        },
    }


def _run_uv(argv: list[str], *, step: str) -> bool:
    uv = _uv_bin()
    cmd = [uv, "run", *argv]
    _log(f"=== {step}: {' '.join(cmd)}")
    t0 = time.perf_counter()
    r = subprocess.run(cmd, cwd=_REPO)
    ms = int((time.perf_counter() - t0) * 1000)
    _emit(
        {
            "kind": "ci_gate.step",
            "step": step,
            "exit_code": r.returncode,
            "duration_ms": ms,
        },
    )
    if r.returncode != 0:
        _log(f"FAIL {step} exit={r.returncode} ({ms}ms)")
        return False
    _log(f"OK {step} ({ms}ms)")
    return True


def _step_doctor(*, golden_path: Path, corpus_path: Path) -> bool:
    tmpd = Path(tempfile.mkdtemp(prefix="l2n-ci-gate-doctor-"))
    db_path = tmpd / "gate.sqlite"
    try:
        from lark_to_notes.storage.db import connect, init_db

        conn = connect(db_path)
        init_db(conn)
        conn.close()

        uv = _uv_bin()
        cmd = [
            uv,
            "run",
            "lark-to-notes",
            "doctor",
            "--db",
            str(db_path),
            "--fixture-corpus",
            str(corpus_path),
            "--json",
        ]
        _log(f"=== doctor_snapshot: {' '.join(cmd)}")
        t0 = time.perf_counter()
        r = subprocess.run(cmd, cwd=_REPO, capture_output=True, text=True)
        ms = int((time.perf_counter() - t0) * 1000)
        _emit(
            {"kind": "ci_gate.step", "step": "doctor", "exit_code": r.returncode, "duration_ms": ms}
        )
        if r.returncode != 0:
            _log(f"FAIL doctor exit={r.returncode} ({ms}ms)")
            if r.stderr:
                _log(r.stderr.strip())
            return False
        payload = cast("dict[str, Any]", json.loads(r.stdout))
        observed = _doctor_extract_subset(payload)
        expected = cast("dict[str, Any]", json.loads(golden_path.read_text(encoding="utf-8")))
        ok, err = _nested_subset(observed, expected)
        if not ok:
            _log(f"FAIL doctor golden subset mismatch: {err}")
            _emit(
                {
                    "kind": "ci_gate.doctor_mismatch",
                    "detail": err,
                    "observed": observed,
                },
            )
            return False
        _log(f"OK doctor golden subset ({ms}ms)")
        return True
    finally:
        shutil.rmtree(tmpd, ignore_errors=True)


def _step_e2e() -> bool:
    art_raw = os.environ.get("ARTIFACT_DIR")
    if art_raw:
        art = Path(art_raw).expanduser().resolve()
        art.mkdir(parents=True, exist_ok=True)
        _log(f"ARTIFACT_DIR={art}")
    else:
        art = Path(tempfile.mkdtemp(prefix="l2n-ci-gate-e2e-"))
        _log(f"ARTIFACT_DIR unset; using temp {art}")
    uv = _uv_bin()
    cmd = [
        uv,
        "run",
        "python",
        str(_E2E_SCRIPT),
        "--artifacts-dir",
        str(art),
    ]
    _log(f"=== e2e_harness: {' '.join(cmd)}")
    t0 = time.perf_counter()
    r = subprocess.run(cmd, cwd=_REPO)
    ms = int((time.perf_counter() - t0) * 1000)
    _emit({"kind": "ci_gate.step", "step": "e2e", "exit_code": r.returncode, "duration_ms": ms})
    if r.returncode != 0:
        _log(f"FAIL e2e exit={r.returncode} ({ms}ms)")
        return False
    _log(f"OK e2e ({ms}ms)")
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--only",
        nargs="+",
        choices=_STEP_CHOICES,
        metavar="STEP",
        help=f"Run only these steps (default: all {_STEP_CHOICES})",
    )
    parser.add_argument(
        "--doctor-golden",
        type=Path,
        default=_DEFAULT_DOCTOR_GOLDEN,
        help="Expected doctor JSON subset for empty DB + fixture corpus",
    )
    parser.add_argument(
        "--fixture-corpus",
        type=Path,
        default=_DEFAULT_CORPUS,
        help="Fixture corpus path for doctor snapshot step",
    )
    args = parser.parse_args(argv)

    steps: tuple[str, ...] = tuple(args.only) if args.only else _STEP_CHOICES
    golden = args.doctor_golden.expanduser().resolve()
    corpus = args.fixture_corpus.expanduser().resolve()

    t_gate = time.perf_counter()
    _log("=== ci_reactions_gate (lw-pzj.10.12) ===")
    _log(f"repo_root={_REPO}")
    _log(f"platform={platform.platform()}")
    _log(f"python={sys.version.split()[0]} executable={sys.executable}")
    _log(f"steps={list(steps)}")
    _emit({"kind": "ci_gate.started", "steps": list(steps), "repo_root": str(_REPO)})

    for step in steps:
        if step == "ruff_check":
            if not _run_uv(["ruff", "check", "."], step="ruff_check"):
                _emit({"kind": "ci_gate.complete", "status": "failed", "failed_step": "ruff_check"})
                return 1
        elif step == "ruff_format":
            if not _run_uv(["ruff", "format", "--check", "."], step="ruff_format"):
                _emit(
                    {"kind": "ci_gate.complete", "status": "failed", "failed_step": "ruff_format"}
                )
                return 1
        elif step == "mypy":
            if not _run_uv(["mypy", "--strict", "src", "tests"], step="mypy"):
                _emit({"kind": "ci_gate.complete", "status": "failed", "failed_step": "mypy"})
                return 1
        elif step == "pytest":
            if not _run_uv(["pytest"], step="pytest"):
                _emit({"kind": "ci_gate.complete", "status": "failed", "failed_step": "pytest"})
                return 1
        elif step == "e2e":
            if not _step_e2e():
                _emit({"kind": "ci_gate.complete", "status": "failed", "failed_step": "e2e"})
                return 1
        elif step == "doctor":
            if not golden.is_file():
                _log(f"error: doctor golden not found: {golden}")
                _emit({"kind": "ci_gate.complete", "status": "failed", "failed_step": "doctor"})
                return 1
            if not corpus.is_dir():
                _log(f"error: fixture corpus not found: {corpus}")
                _emit({"kind": "ci_gate.complete", "status": "failed", "failed_step": "doctor"})
                return 1
            if not _step_doctor(golden_path=golden, corpus_path=corpus):
                _emit({"kind": "ci_gate.complete", "status": "failed", "failed_step": "doctor"})
                return 1

    total_ms = int((time.perf_counter() - t_gate) * 1000)
    _emit({"kind": "ci_gate.complete", "status": "ok", "duration_ms": total_ms})
    _log(f"=== ci_reactions_gate OK ({total_ms}ms total) ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
