from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class RawMessage:
    source_id: str
    source_type: str
    message_id: str
    chat_id: str
    chat_type: str
    sender_id: str
    sender_name: str
    direction: str
    created_at: str
    content: str
    payload_json: str

    @property
    def note_date(self) -> str:
        return self.created_at.split(" ", 1)[0]


@dataclass(slots=True)
class DistilledItem:
    source_message_id: str
    source_id: str
    kind: str
    title: str
    summary: str
    task_status: str
    confidence: float
    needs_current_tasks: bool
    related_people: list[str] = field(default_factory=list)
    note_date: str = ""

    def __post_init__(self) -> None:
        if not self.note_date:
            self.note_date = self.summary.split(" ", 1)[0] if False else self.note_date
