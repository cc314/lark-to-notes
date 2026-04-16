"""Tests for supervised live operator hints."""

from __future__ import annotations

from pathlib import Path

from lark_to_notes.runtime.supervised import supervised_live_hints


def test_supervised_live_hints_contains_templates() -> None:
    hints = supervised_live_hints(db_path=Path("var/demo.db"))
    assert hints["model"] == "canonical_cli_wrapper"
    assert "contrib_relpaths" in hints
    assert "argv_templates" in hints
    assert "sync_daemon" in hints["argv_templates"]
    assert hints["argv_templates"]["sync_daemon"][-1] == "var/demo.db"
