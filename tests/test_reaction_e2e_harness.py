"""Smoke tests for ``scripts/reaction_e2e_harness.py`` (lw-pzj.10.7)."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_SCRIPT = _REPO / "scripts" / "reaction_e2e_harness.py"
_FIXTURE = _REPO / "tests" / "fixtures" / "reaction-e2e" / "harness.ndjson"
_GOLDEN = _REPO / "tests" / "fixtures" / "reaction-e2e" / "golden.json"


def test_reaction_e2e_harness_stdout_is_ndjson_only(tmp_path: Path) -> None:
    art = tmp_path / "art"
    proc = subprocess.run(
        [
            sys.executable,
            str(_SCRIPT),
            "--artifacts-dir",
            str(art),
            "--fixture",
            str(_FIXTURE),
            "--golden",
            str(_GOLDEN),
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=str(_REPO),
    )
    assert proc.returncode == 0, proc.stderr
    lines = [ln for ln in proc.stdout.splitlines() if ln.strip()]
    for ln in lines:
        obj = json.loads(ln)
        assert "kind" in obj
    kinds = [json.loads(ln)["kind"] for ln in lines]
    assert kinds[0] == "harness.started"
    assert kinds[-1] == "harness.complete"
    assert (art / "sync_events.payload.json").is_file()
    assert "reaction E2E harness" in proc.stderr


def test_reaction_e2e_harness_golden_mismatch_exit_2(tmp_path: Path) -> None:
    bad_golden = tmp_path / "bad_golden.json"
    bad_golden.write_text(
        json.dumps(
            {
                "sync_events": {"reaction_rows_inserted": 99},
            },
        ),
        encoding="utf-8",
    )
    proc = subprocess.run(
        [
            sys.executable,
            str(_SCRIPT),
            "--db",
            str(tmp_path / "x.sqlite"),
            "--fixture",
            str(_FIXTURE),
            "--golden",
            str(bad_golden),
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=str(_REPO),
    )
    assert proc.returncode == 2
    assert "golden mismatch" in proc.stderr
