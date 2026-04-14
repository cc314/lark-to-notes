"""Persistence and task-override application for structured feedback."""

from __future__ import annotations

from typing import TYPE_CHECKING

from lark_to_notes.distill.models import PromotionRec, TaskClass
from lark_to_notes.feedback.models import (
    FeedbackAction,
    FeedbackApplyResult,
    FeedbackArtifact,
    FeedbackDirective,
    FeedbackEventRecord,
    FeedbackTargetType,
)
from lark_to_notes.feedback.store import insert_event, list_events
from lark_to_notes.tasks.models import TaskStatus
from lark_to_notes.tasks.registry import apply_task_override, get_task

if TYPE_CHECKING:
    import sqlite3


def apply_feedback_artifact(
    conn: sqlite3.Connection,
    artifact: FeedbackArtifact,
    *,
    artifact_path: str = "",
) -> FeedbackApplyResult:
    """Persist all feedback entries and apply task-facing overrides."""
    applied_task_ids: list[str] = []
    feedback_event_ids: list[str] = []

    for entry in artifact.entries():
        if entry.target_type is FeedbackTargetType.TASK and get_task(conn, entry.target_id) is None:
            raise LookupError(f"feedback references unknown task_id {entry.target_id!r}")

        if entry.target_type is not FeedbackTargetType.TASK:
            event_id = insert_event(conn, entry, artifact_path=artifact_path)
            feedback_event_ids.append(event_id)
            continue

        status, task_class, promotion_rec = _derive_task_override(entry.directive)
        changed = apply_task_override(
            conn,
            entry.target_id,
            action=entry.directive.action.value,
            artifact_path=artifact_path,
            comment=entry.directive.comment,
            actor_ref=entry.directive.actor_ref,
            status=status,
            task_class=task_class,
            promotion_rec=promotion_rec,
            title=entry.directive.title,
            summary=entry.directive.summary,
            due_at=entry.directive.due_at,
            merge_into_task_id=entry.directive.merge_into_task_id,
        )
        if not changed:
            raise LookupError(f"unable to apply feedback to task_id {entry.target_id!r}")
        event_id = insert_event(conn, entry, artifact_path=artifact_path)
        feedback_event_ids.append(event_id)
        applied_task_ids.append(entry.target_id)

    return FeedbackApplyResult(
        applied_task_ids=tuple(applied_task_ids),
        feedback_event_ids=tuple(feedback_event_ids),
    )


def list_feedback_events(
    conn: sqlite3.Connection,
    *,
    target_type: str | None = None,
    target_id: str | None = None,
    limit: int = 200,
) -> list[FeedbackEventRecord]:
    """Return stored feedback events, newest first."""
    return list_events(
        conn,
        target_type=target_type,
        target_id=target_id,
        limit=limit,
    )


def _derive_task_override(
    directive: FeedbackDirective,
) -> tuple[str | None, str | None, str | None]:
    task_class = directive.task_class
    promotion_rec = directive.promotion_rec

    if task_class is not None:
        TaskClass(task_class)
    if promotion_rec is not None:
        PromotionRec(promotion_rec)

    if directive.action is FeedbackAction.CONFIRM:
        return TaskStatus.OPEN.value, task_class, promotion_rec
    if directive.action is FeedbackAction.DISMISS:
        return TaskStatus.DISMISSED.value, task_class, promotion_rec
    if directive.action is FeedbackAction.MERGE:
        return TaskStatus.MERGED.value, task_class, promotion_rec
    if directive.action is FeedbackAction.SNOOZE:
        return TaskStatus.SNOOZED.value, task_class, promotion_rec
    if directive.action is FeedbackAction.WRONG_CLASS:
        if task_class is None:
            raise ValueError("wrong_class feedback requires task_class")
        if promotion_rec is None:
            promotion_rec = _default_promotion_for(task_class)
        return _default_status_for(task_class), task_class, promotion_rec
    if directive.action is FeedbackAction.MISSED_TASK:
        return None, task_class, promotion_rec
    raise ValueError(f"unsupported feedback action: {directive.action}")


def _default_promotion_for(task_class: str) -> str:
    normalized = TaskClass(task_class)
    if normalized is TaskClass.TASK:
        return PromotionRec.CURRENT_TASKS.value
    if normalized is TaskClass.NEEDS_REVIEW:
        return PromotionRec.REVIEW.value
    return PromotionRec.DAILY_ONLY.value


def _default_status_for(task_class: str) -> str:
    normalized = TaskClass(task_class)
    if normalized is TaskClass.NEEDS_REVIEW:
        return TaskStatus.NEEDS_REVIEW.value
    if normalized is TaskClass.CONTEXT:
        return TaskStatus.DISMISSED.value
    return TaskStatus.OPEN.value
