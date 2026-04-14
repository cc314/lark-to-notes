from __future__ import annotations

import re

from .config import WorkerConfig
from .models import DistilledItem, RawMessage
from .people import resolve_person


TIME_SENSITIVE_PATTERNS = [
    "现在有时间",
    "today",
    "asap",
    "urgent",
    "马上",
]


def distill_message(
    message: RawMessage,
    config: WorkerConfig,
    people_index: dict[str, object],
) -> DistilledItem | None:
    if _is_self_message(message, config):
        return None

    content = _clean_content(message.content)
    if not content:
        return None

    kind = "context"
    if _is_time_sensitive(content):
        kind = "follow_up"
    elif _is_task_like(content, config.task_keywords):
        kind = "task"

    related_people: list[str] = []
    resolved_person = resolve_person(message.sender_name, people_index)
    if resolved_person:
        related_people.append(resolved_person)

    title = _build_title(kind, message.sender_name, content)
    summary = content[:280]
    return DistilledItem(
        source_message_id=message.message_id,
        source_id=message.source_id,
        kind=kind,
        title=title,
        summary=summary,
        task_status="open" if kind in {"task", "follow_up"} else "info",
        confidence=0.75 if kind != "context" else 0.55,
        needs_current_tasks=kind in {"task", "follow_up"},
        related_people=related_people,
        note_date=message.note_date,
    )


def _is_self_message(message: RawMessage, config: WorkerConfig) -> bool:
    if message.sender_id and message.sender_id in config.self_sender_ids:
        return True
    if message.sender_name and message.sender_name in config.self_sender_names:
        return True
    return message.direction == "outgoing"


def _is_task_like(content: str, task_keywords: list[str]) -> bool:
    lowered = content.casefold()
    return any(keyword.casefold() in lowered for keyword in task_keywords)


def _is_time_sensitive(content: str) -> bool:
    lowered = content.casefold()
    return any(pattern.casefold() in lowered for pattern in TIME_SENSITIVE_PATTERNS)


def _clean_content(content: str) -> str:
    cleaned = re.sub(r"\s+", " ", content).strip()
    return cleaned


def _build_title(kind: str, sender_name: str, content: str) -> str:
    first_line = content.split(" ", 18)
    preview = " ".join(first_line).strip()
    if len(preview) > 96:
        preview = f"{preview[:93]}..."

    if kind == "context":
        return f"Capture note from {sender_name}: {preview}"
    if kind == "follow_up":
        return f"Follow up with {sender_name}: {preview}"
    return f"Task from {sender_name}: {preview}"
