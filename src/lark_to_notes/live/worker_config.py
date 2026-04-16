"""Parse the on-disk JSON config shape used by ``sync-once`` / ``sync-daemon``.

The historical ``automation.lark_worker`` package loads this file with richer
validation. For the in-repo live adapter we need a **stdlib-only** parser that
extracts ``vault_root``, ``state_db``, poll tuning, and ``sources[]`` so
:class:`~lark_to_notes.live.chat_live.ChatLiveAdapter` can resolve runtime lock
paths and upsert watched sources into the **canonical** ``--db`` SQLite file.

The ``state_db`` path is retained for compatibility with older shared configs;
canonical checkpoints and intake still live in the operator's ``--db``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast


class LiveWorkerConfigError(ValueError):
    """Raised when the live worker JSON config is missing required fields."""


@dataclass(frozen=True, slots=True)
class LiveWorkerConfigSnapshot:
    """Minimal validated view of a live worker JSON config file."""

    vault_root: Path
    state_db: Path
    poll_interval_seconds: int
    poll_lookback_days: int
    raw_sources: tuple[dict[str, Any], ...]


def _require_mapping(data: object) -> dict[str, Any]:
    if not isinstance(data, dict):
        raise LiveWorkerConfigError("worker config root must be a JSON object")
    return cast("dict[str, Any]", data)


def _require_positive_int(data: dict[str, Any], key: str, *, default: int) -> int:
    value = data.get(key, default)
    if not isinstance(value, int) or isinstance(value, bool):
        raise LiveWorkerConfigError(f"{key!r} must be an integer")
    if value <= 0:
        raise LiveWorkerConfigError(f"{key!r} must be positive")
    return value


def parse_live_worker_config_mapping(
    data: dict[str, Any],
    *,
    base_dir: Path,
) -> LiveWorkerConfigSnapshot:
    """Validate *data* and return a snapshot; resolve relative paths against *base_dir*."""
    vault_raw = data.get("vault_root")
    if not isinstance(vault_raw, str) or not vault_raw.strip():
        raise LiveWorkerConfigError("vault_root must be a non-empty string")
    state_raw = data.get("state_db")
    if not isinstance(state_raw, str) or not state_raw.strip():
        raise LiveWorkerConfigError("state_db must be a non-empty string")

    if Path(vault_raw).is_absolute():
        vault_root = Path(vault_raw)
    else:
        vault_root = (base_dir / vault_raw).resolve()
    if Path(state_raw).is_absolute():
        state_db = Path(state_raw)
    else:
        state_db = (base_dir / state_raw).resolve()

    poll_interval_seconds = _require_positive_int(data, "poll_interval_seconds", default=300)
    poll_lookback_days = _require_positive_int(data, "poll_lookback_days", default=7)

    sources_raw = data.get("sources", [])
    if sources_raw is None:
        sources_raw = []
    if not isinstance(sources_raw, list):
        raise LiveWorkerConfigError("sources must be a JSON array when provided")
    sources_list: list[dict[str, Any]] = []
    for idx, item in enumerate(sources_raw):
        if not isinstance(item, dict):
            raise LiveWorkerConfigError(f"sources[{idx}] must be an object")
        sources_list.append(cast("dict[str, Any]", item))

    return LiveWorkerConfigSnapshot(
        vault_root=vault_root,
        state_db=state_db,
        poll_interval_seconds=poll_interval_seconds,
        poll_lookback_days=poll_lookback_days,
        raw_sources=tuple(sources_list),
    )


def load_live_worker_config(path: Path) -> LiveWorkerConfigSnapshot:
    """Load and validate a worker-style JSON config from *path*."""
    resolved = path.expanduser().resolve()
    text = resolved.read_text(encoding="utf-8")
    try:
        loaded = json.loads(text)
    except json.JSONDecodeError as exc:
        raise LiveWorkerConfigError(f"invalid JSON in {resolved}: {exc}") from exc
    root = _require_mapping(loaded)
    return parse_live_worker_config_mapping(root, base_dir=resolved.parent)
