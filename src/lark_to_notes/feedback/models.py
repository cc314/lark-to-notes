"""Typed models for structured feedback artifacts and imported feedback events."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, cast

from lark_to_notes.distill.models import PromotionRec, TaskClass


class FeedbackTargetType(StrEnum):
    """What kind of entity a feedback record addresses."""

    TASK = "task"
    SOURCE_ITEM = "source_item"


class FeedbackAction(StrEnum):
    """Supported structured review actions."""

    CONFIRM = "confirm"
    DISMISS = "dismiss"
    MERGE = "merge"
    SNOOZE = "snooze"
    WRONG_CLASS = "wrong_class"
    MISSED_TASK = "missed_task"


@dataclass(frozen=True, slots=True)
class FeedbackDirective:
    """One structured feedback instruction for a task or source item."""

    action: FeedbackAction
    comment: str = ""
    actor_ref: str = ""
    title: str | None = None
    summary: str | None = None
    task_class: str | None = None
    promotion_rec: str | None = None
    merge_into_task_id: str | None = None
    due_at: str | None = None

    @classmethod
    def from_mapping(
        cls,
        value: object,
        *,
        target_type: FeedbackTargetType,
    ) -> FeedbackDirective:
        """Parse and validate a directive from a YAML-decoded mapping."""
        if not isinstance(value, dict):
            raise ValueError("feedback directive must be a mapping")

        data = cast("dict[str, object]", value)
        action = FeedbackAction(_require_string(data, "action"))
        task_class = _optional_string(data, "task_class")
        promotion_rec = _optional_string(data, "promotion_rec")

        if task_class is not None:
            TaskClass(task_class)
        if promotion_rec is not None:
            PromotionRec(promotion_rec)

        directive = cls(
            action=action,
            comment=_optional_string(data, "comment") or "",
            actor_ref=_optional_string(data, "actor_ref") or "",
            title=_optional_string(data, "title"),
            summary=_optional_string(data, "summary"),
            task_class=task_class,
            promotion_rec=promotion_rec,
            merge_into_task_id=_optional_string(data, "merge_into_task_id"),
            due_at=_optional_string(data, "due_at"),
        )
        directive.validate(target_type=target_type)
        return directive

    def validate(self, *, target_type: FeedbackTargetType) -> None:
        """Validate cross-field requirements for this directive."""
        if target_type is FeedbackTargetType.TASK and self.action is FeedbackAction.MISSED_TASK:
            raise ValueError("missed_task feedback must target a source item")
        if (
            target_type is FeedbackTargetType.SOURCE_ITEM
            and self.action is not FeedbackAction.MISSED_TASK
        ):
            raise ValueError("source_item feedback currently only supports missed_task")
        if self.action is FeedbackAction.MERGE and not self.merge_into_task_id:
            raise ValueError("merge feedback requires merge_into_task_id")
        if self.action is FeedbackAction.WRONG_CLASS and self.task_class is None:
            raise ValueError("wrong_class feedback requires task_class")

    def to_payload(self) -> dict[str, object]:
        """Return a plain mapping suitable for YAML or JSON serialization."""
        payload: dict[str, object] = {"action": self.action.value}
        optional_fields = {
            "comment": self.comment or None,
            "actor_ref": self.actor_ref or None,
            "title": self.title,
            "summary": self.summary,
            "task_class": self.task_class,
            "promotion_rec": self.promotion_rec,
            "merge_into_task_id": self.merge_into_task_id,
            "due_at": self.due_at,
        }
        for key, value in optional_fields.items():
            if value is not None:
                payload[key] = value
        return payload


@dataclass(frozen=True, slots=True)
class FeedbackArtifact:
    """YAML sidecar content keyed by stable task IDs and source-item IDs."""

    version: int = 1
    tasks: dict[str, FeedbackDirective] = field(default_factory=dict)
    source_items: dict[str, FeedbackDirective] = field(default_factory=dict)

    def entries(self) -> list[FeedbackEntry]:
        """Return all entries flattened into an ordered list."""
        task_entries = [
            FeedbackEntry(
                target_type=FeedbackTargetType.TASK,
                target_id=target_id,
                directive=directive,
            )
            for target_id, directive in self.tasks.items()
        ]
        source_entries = [
            FeedbackEntry(
                target_type=FeedbackTargetType.SOURCE_ITEM,
                target_id=target_id,
                directive=directive,
            )
            for target_id, directive in self.source_items.items()
        ]
        return task_entries + source_entries


@dataclass(frozen=True, slots=True)
class FeedbackEntry:
    """A flattened feedback item ready for persistence or application."""

    target_type: FeedbackTargetType
    target_id: str
    directive: FeedbackDirective


@dataclass(frozen=True, slots=True)
class FeedbackEventRecord:
    """One stored feedback event row from SQLite."""

    feedback_id: str
    target_type: str
    target_id: str
    action: str
    payload_json: str
    comment: str
    actor_ref: str
    created_at: str
    artifact_path: str

    @classmethod
    def from_db_row(cls, row: Any) -> FeedbackEventRecord:
        """Construct a record from a sqlite row object."""
        return cls(
            feedback_id=row["feedback_id"],
            target_type=row["target_type"],
            target_id=row["target_id"],
            action=row["action"],
            payload_json=row["payload_json"],
            comment=row["comment"] or "",
            actor_ref=row["actor_ref"] or "",
            created_at=row["created_at"],
            artifact_path=row["artifact_path"] or "",
        )

    @property
    def payload(self) -> dict[str, object]:
        """Return ``payload_json`` decoded into a plain mapping."""
        parsed = json.loads(self.payload_json)
        if isinstance(parsed, dict):
            return cast("dict[str, object]", parsed)
        raise ValueError("feedback event payload_json did not decode to an object")


@dataclass(frozen=True, slots=True)
class FeedbackApplyResult:
    """Summary of applying one artifact to the local store."""

    applied_task_ids: tuple[str, ...]
    feedback_event_ids: tuple[str, ...]


def _require_string(data: dict[str, object], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"feedback field {key!r} must be a non-empty string")
    return value


def _optional_string(data: dict[str, object], key: str) -> str | None:
    value = data.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"feedback field {key!r} must be a string when provided")
    stripped = value.strip()
    return stripped or None
