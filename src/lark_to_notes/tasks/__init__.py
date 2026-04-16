"""Action-item generation, stable task identity, and review-lane lifecycle."""

from __future__ import annotations

from lark_to_notes.tasks.fingerprint import derive_fingerprint
from lark_to_notes.tasks.models import TaskEvidence, TaskRecord, TaskStatus
from lark_to_notes.tasks.registry import (
    add_evidence,
    get_task,
    get_task_by_fingerprint,
    list_evidence,
    list_review_feedback_candidates,
    list_tasks,
    update_task_status,
    upsert_task,
)

__all__ = [
    "TaskEvidence",
    "TaskRecord",
    "TaskStatus",
    "add_evidence",
    "derive_fingerprint",
    "get_task",
    "get_task_by_fingerprint",
    "list_evidence",
    "list_review_feedback_candidates",
    "list_tasks",
    "update_task_status",
    "upsert_task",
]
