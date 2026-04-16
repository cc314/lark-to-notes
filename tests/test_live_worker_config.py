"""Tests for stdlib-only live worker JSON config parsing."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from lark_to_notes.live.worker_config import (
    LiveWorkerConfigError,
    load_live_worker_config,
    parse_live_worker_config_mapping,
)


def _write(tmp: Path, name: str, payload: dict[str, Any]) -> Path:
    p = tmp / name
    p.write_text(json.dumps(payload), encoding="utf-8")
    return p


def test_load_live_worker_config_resolves_relative_paths(tmp_path: Path) -> None:
    cfg = {
        "vault_root": "vault",
        "state_db": "worker.db",
        "poll_interval_seconds": 120,
        "poll_lookback_days": 3,
        "sources": [],
    }
    path = _write(tmp_path, "live.json", cfg)
    snap = load_live_worker_config(path)
    assert snap.vault_root == (tmp_path / "vault").resolve()
    assert snap.state_db == (tmp_path / "worker.db").resolve()
    assert snap.poll_interval_seconds == 120
    assert snap.poll_lookback_days == 3
    assert snap.raw_sources == ()


def test_parse_requires_vault_root(tmp_path: Path) -> None:
    with pytest.raises(LiveWorkerConfigError, match="vault_root"):
        parse_live_worker_config_mapping({"state_db": "x.db"}, base_dir=tmp_path)


def test_parse_requires_state_db(tmp_path: Path) -> None:
    with pytest.raises(LiveWorkerConfigError, match="state_db"):
        parse_live_worker_config_mapping({"vault_root": "/tmp/v"}, base_dir=tmp_path)


def test_parse_rejects_invalid_poll_interval(tmp_path: Path) -> None:
    with pytest.raises(LiveWorkerConfigError, match="poll_interval_seconds"):
        parse_live_worker_config_mapping(
            {"vault_root": "/v", "state_db": "/w.db", "poll_interval_seconds": 0},
            base_dir=tmp_path,
        )


def test_parse_preserves_source_objects(tmp_path: Path) -> None:
    snap = parse_live_worker_config_mapping(
        {
            "vault_root": "/abs/vault",
            "state_db": "/abs/worker.db",
            "sources": [{"source_id": "dm:1", "enabled": True}],
        },
        base_dir=tmp_path,
    )
    assert snap.raw_sources == ({"source_id": "dm:1", "enabled": True},)
