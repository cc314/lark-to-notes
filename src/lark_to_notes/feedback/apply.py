"""Compatibility wrappers for applying structured feedback entries."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from lark_to_notes.feedback.models import FeedbackArtifact, FeedbackEntry, FeedbackTargetType
from lark_to_notes.feedback.service import apply_feedback_artifact

if TYPE_CHECKING:
    import sqlite3

# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ApplyResult:
    """Outcome of applying one structured feedback entry."""

    feedback_id: str
    applied: bool
    skipped: bool
    message: str


@dataclass(frozen=True, slots=True)
class ImportReport:
    """Summary of a bulk feedback import operation."""

    total: int
    applied: int
    skipped: int
    details: tuple[ApplyResult, ...]


def import_events(
    conn: sqlite3.Connection,
    events: list[FeedbackEntry],
    *,
    artifact_path: str = "",
) -> ImportReport:
    """Persist and apply all *events* in order."""
    results: list[ApplyResult] = []
    for event in events:
        results.append(apply_event(conn, event, artifact_path=artifact_path))

    applied = sum(1 for r in results if r.applied)
    skipped = sum(1 for r in results if r.skipped)
    return ImportReport(
        total=len(results),
        applied=applied,
        skipped=skipped,
        details=tuple(results),
    )


def apply_event(
    conn: sqlite3.Connection,
    event: FeedbackEntry,
    *,
    artifact_path: str = "",
) -> ApplyResult:
    """Apply *event* to the task registry and persist it."""
    if event.target_type is FeedbackTargetType.TASK:
        artifact = FeedbackArtifact(tasks={event.target_id: event.directive})
        message = f"{event.directive.action.value}: task {event.target_id!r} updated"
        applied = True
    else:
        artifact = FeedbackArtifact(source_items={event.target_id: event.directive})
        message = (
            f"{event.directive.action.value}: source item {event.target_id!r} "
            "recorded for later replay"
        )
        applied = False

    result = apply_feedback_artifact(conn, artifact, artifact_path=artifact_path)
    return ApplyResult(
        feedback_id=result.feedback_event_ids[0],
        applied=applied,
        skipped=not applied,
        message=message,
    )
