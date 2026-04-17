"""Smoke tests for ``scripts/ci_reactions_gate.py`` (lw-pzj.10.12)."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_GATE = _REPO / "scripts" / "ci_reactions_gate.py"


def test_ci_reactions_gate_doctor_only_emits_ndjson() -> None:
    proc = subprocess.run(
        [sys.executable, str(_GATE), "--only", "doctor"],
        check=False,
        capture_output=True,
        text=True,
        cwd=str(_REPO),
    )
    assert proc.returncode == 0, proc.stderr
    lines = [ln for ln in proc.stdout.splitlines() if ln.strip()]
    kinds = [json.loads(ln)["kind"] for ln in lines]
    assert kinds[0] == "ci_gate.started"
    assert any(k == "ci_gate.step" for k in kinds)
    assert kinds[-1] == "ci_gate.complete"
    last = json.loads(lines[-1])
    assert last["status"] == "ok"
    assert "ci_reactions_gate" in proc.stderr
