from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from .config import WorkerConfig


DAILY_CAPTURE_HEADING = "## Auto Captured Context"
DAILY_TASKS_HEADING = "## Auto Open Tasks"
CURRENT_TASKS_HEADING = "## Auto Synced Tasks"

CAPTURE_START = "<!-- lark-worker:auto-captured:start -->"
CAPTURE_END = "<!-- lark-worker:auto-captured:end -->"
TASKS_START = "<!-- lark-worker:auto-open-tasks:start -->"
TASKS_END = "<!-- lark-worker:auto-open-tasks:end -->"
CURRENT_START = "<!-- lark-worker:current-tasks:start -->"
CURRENT_END = "<!-- lark-worker:current-tasks:end -->"


def sync_daily_note(config: WorkerConfig, note_date: str, items: list[dict]) -> None:
    note_path = config.vault_root / "daily" / f"{note_date}.md"
    if not items and (not note_path.exists() or CAPTURE_START not in note_path.read_text(encoding="utf-8")):
        return
    if not note_path.exists():
        note_path.parent.mkdir(parents=True, exist_ok=True)
        note_path.write_text(_default_daily_note(note_date), encoding="utf-8")

    capture_lines = [
        _render_capture_line(item)
        for item in items
    ]
    task_lines = [
        _render_daily_task_line(item)
        for item in items
        if item["needs_current_tasks"] and item["task_status"] == "open"
    ]

    text = note_path.read_text(encoding="utf-8")
    text = _upsert_managed_section(text, DAILY_CAPTURE_HEADING, CAPTURE_START, CAPTURE_END, capture_lines)
    text = _upsert_managed_section(text, DAILY_TASKS_HEADING, TASKS_START, TASKS_END, task_lines)
    note_path.write_text(text, encoding="utf-8")


def sync_current_tasks_note(config: WorkerConfig, items: list[dict]) -> None:
    note_path = config.current_tasks_note
    if not items and (not note_path.exists() or CURRENT_START not in note_path.read_text(encoding="utf-8")):
        return
    if not note_path.exists():
        note_path.parent.mkdir(parents=True, exist_ok=True)
        note_path.write_text(_default_current_tasks_note(), encoding="utf-8")

    task_lines = [_render_current_task_line(item) for item in items]
    text = note_path.read_text(encoding="utf-8")
    text = _upsert_managed_section(text, CURRENT_TASKS_HEADING, CURRENT_START, CURRENT_END, task_lines)
    note_path.write_text(text, encoding="utf-8")


def _render_capture_line(item: dict) -> str:
    people = json.loads(item["related_people_json"])
    prefix = people[0] if people else item["source_id"]
    return f"- {prefix}: {item['summary']}"


def _render_daily_task_line(item: dict) -> str:
    return f"- [ ] {item['title']}"


def _render_current_task_line(item: dict) -> str:
    daily_link = f"[[daily/{item['note_date']}]]"
    return f"- [ ] {item['title']}. Source: {daily_link}"


def _upsert_managed_section(
    text: str,
    heading: str,
    start_marker: str,
    end_marker: str,
    body_lines: list[str],
) -> str:
    body = "\n".join(body_lines or ["- None."])
    block = f"{heading}\n{start_marker}\n{body}\n{end_marker}"

    if start_marker in text and end_marker in text:
        before, remainder = text.split(start_marker, 1)
        _, after = remainder.split(end_marker, 1)
        return f"{before}{start_marker}\n{body}\n{end_marker}{after}"

    if heading in text:
        return text.replace(heading, block, 1)

    suffix = "" if text.endswith("\n") else "\n"
    return f"{text}{suffix}\n{block}\n"


def _default_daily_note(note_date: str) -> str:
    return (
        "---\n"
        "type: daily\n"
        f"created: {note_date}\n"
        f"updated: {note_date}\n"
        f"date: {note_date}\n"
        "tags:\n"
        "  - daily\n"
        "people: []\n"
        "projects: []\n"
        "areas:\n"
        "  - \"[[area/current tasks/index|Current Tasks]]\"\n"
        "---\n\n"
        f"# {note_date}\n\n"
        "## Tasks\n\n"
        "## Captured Context\n\n"
        "## Decisions\n\n"
        "## Links\n"
    )


def _default_current_tasks_note() -> str:
    today = date.today().isoformat()
    return (
        "---\n"
        "type: area\n"
        f"created: {today}\n"
        f"updated: {today}\n"
        "tags:\n"
        "  - area\n"
        "  - tasks\n"
        "people: []\n"
        "projects: []\n"
        "---\n\n"
        "# Current Tasks\n\n"
        "This page is the durable list of open work, project items, and follow-ups promoted from daily notes.\n"
    )
