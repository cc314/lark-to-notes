from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path


DEFAULT_TASK_KEYWORDS = [
    "please",
    "can you",
    "check",
    "need",
    "follow up",
    "发我",
    "帮",
    "麻烦",
    "请",
    "有吗",
    "看看",
    "空的时候",
    "?",
    "？",
]

DEFAULT_EVENT_TYPES = ["im.message.receive_v1"]


@dataclass(slots=True)
class SourceConfig:
    source_id: str
    source_type: str
    lark_id: str
    name: str
    enabled: bool = True


@dataclass(slots=True)
class WorkerConfig:
    vault_root: Path
    state_db: Path
    raw_dir: Path
    current_tasks_note: Path
    poll_interval_seconds: int = 300
    poll_lookback_days: int = 2
    history_lookback_days: int = 30
    self_sender_names: list[str] = field(default_factory=list)
    self_sender_ids: list[str] = field(default_factory=list)
    task_keywords: list[str] = field(default_factory=lambda: list(DEFAULT_TASK_KEYWORDS))
    bot_event_types: list[str] = field(default_factory=lambda: list(DEFAULT_EVENT_TYPES))
    user_sources: list[SourceConfig] = field(default_factory=list)

    @property
    def enabled_sources(self) -> list[SourceConfig]:
        return [source for source in self.user_sources if source.enabled]


def _resolve_path(base: Path, value: str | None, fallback: str) -> Path:
    raw = (value or fallback).strip()
    candidate = Path(raw).expanduser()
    if candidate.is_absolute():
        return candidate
    return (base / candidate).resolve()


def load_config(config_path: str | Path) -> WorkerConfig:
    path = Path(config_path).expanduser().resolve()
    data = json.loads(path.read_text(encoding="utf-8"))

    config_dir = path.parent
    vault_root = _resolve_path(config_dir, data.get("vault_root"), ".")
    state_db = _resolve_path(config_dir, data.get("state_db"), "~/.local/share/notes-lark-worker/state.db")
    raw_dir = _resolve_path(vault_root, data.get("raw_dir"), "raw/lark-worker")
    current_tasks_note = _resolve_path(vault_root, data.get("current_tasks_note"), "area/current tasks/index.md")

    user_sources = [
        SourceConfig(
            source_id=entry["source_id"],
            source_type=entry["source_type"],
            lark_id=entry["lark_id"],
            name=entry.get("name", entry["source_id"]),
            enabled=entry.get("enabled", True),
        )
        for entry in data.get("user_sources", [])
    ]

    return WorkerConfig(
        vault_root=vault_root,
        state_db=state_db,
        raw_dir=raw_dir,
        current_tasks_note=current_tasks_note,
        poll_interval_seconds=int(data.get("poll_interval_seconds", 300)),
        poll_lookback_days=int(data.get("poll_lookback_days", 2)),
        history_lookback_days=int(data.get("history_lookback_days", 30)),
        self_sender_names=list(data.get("self_sender_names", [])),
        self_sender_ids=list(data.get("self_sender_ids", [])),
        task_keywords=list(data.get("task_keywords", DEFAULT_TASK_KEYWORDS)),
        bot_event_types=list(data.get("bot_event_types", DEFAULT_EVENT_TYPES)),
        user_sources=user_sources,
    )


def ensure_directories(config: WorkerConfig) -> None:
    config.state_db.parent.mkdir(parents=True, exist_ok=True)
    config.raw_dir.mkdir(parents=True, exist_ok=True)
