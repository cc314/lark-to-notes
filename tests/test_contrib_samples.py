"""Sanity checks for operator-facing contrib samples."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]


def test_launchd_example_plist_is_well_formed_xml() -> None:
    plist = _REPO_ROOT / "contrib" / "sync-daemon.launchd.example.plist"
    assert plist.is_file()
    ET.parse(plist)


def test_sync_events_pipeline_example_script_exists() -> None:
    script = _REPO_ROOT / "contrib" / "sync-events-pipeline.example.sh"
    assert script.is_file()
    first = script.read_text(encoding="utf-8").splitlines()[0]
    assert first.startswith("#!")
