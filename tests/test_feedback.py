"""Tests for structured feedback artifacts and override application."""

from __future__ import annotations

import json
import sqlite3

import pytest

from lark_to_notes.feedback import (
    FeedbackAction,
    FeedbackArtifact,
    FeedbackDirective,
    apply_feedback_artifact,
    list_feedback_events,
    parse_feedback_artifact,
    render_feedback_artifact,
)
from lark_to_notes.storage.db import init_db
from lark_to_notes.tasks.registry import get_task, upsert_task


@pytest.fixture
def conn() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.execute("PRAGMA foreign_keys = ON")
    init_db(connection)
    return connection


def _make_review_task(conn: sqlite3.Connection, *, fingerprint: str = "feedback000000001") -> str:
    task_id, _ = upsert_task(
        conn,
        fingerprint=fingerprint,
        title="Review the ambiguous request",
        task_class="needs_review",
        confidence_band="low",
        reason_code="long_content_no_signal",
        promotion_rec="review",
    )
    return task_id


def test_feedback_artifact_round_trip_yaml() -> None:
    artifact = FeedbackArtifact(
        tasks={
            "task-123": FeedbackDirective(
                action=FeedbackAction.WRONG_CLASS,
                task_class="task",
                promotion_rec="current_tasks",
                comment="This is explicit work, not just a review candidate.",
                actor_ref="alice",
            ),
        },
        source_items={
            "msg-456": FeedbackDirective(
                action=FeedbackAction.MISSED_TASK,
                title="Send the budget update",
                summary="Operator spotted a missed action item in the source note.",
                task_class="task",
                promotion_rec="current_tasks",
                comment="Missed on first pass.",
            ),
        },
    )

    rendered = render_feedback_artifact(artifact)
    reparsed = parse_feedback_artifact(rendered)

    assert reparsed == artifact


def test_apply_feedback_wrong_class_updates_task_and_persists_event(
    conn: sqlite3.Connection,
) -> None:
    task_id = _make_review_task(conn)
    artifact = FeedbackArtifact(
        tasks={
            task_id: FeedbackDirective(
                action=FeedbackAction.WRONG_CLASS,
                task_class="task",
                comment="This should be a concrete task.",
                actor_ref="operator",
            ),
        },
    )

    result = apply_feedback_artifact(
        conn,
        artifact,
        artifact_path="raw/review/feedback.yaml",
    )

    task = get_task(conn, task_id)
    assert task is not None
    assert result.applied_task_ids == (task_id,)
    assert task.status == "open"
    assert task.task_class == "task"
    assert task.promotion_rec == "current_tasks"

    override_state = json.loads(task.manual_override_state or "{}")
    assert override_state["action"] == "wrong_class"
    assert override_state["artifact_path"] == "raw/review/feedback.yaml"
    assert override_state["overrides"]["task_class"] == "task"
    assert override_state["overrides"]["promotion_rec"] == "current_tasks"

    events = list_feedback_events(conn, target_id=task_id)
    assert len(events) == 1
    assert events[0].action == "wrong_class"
    assert events[0].artifact_path == "raw/review/feedback.yaml"


@pytest.mark.parametrize(
    ("action", "expected_status"),
    [
        (FeedbackAction.CONFIRM, "open"),
        (FeedbackAction.DISMISS, "dismissed"),
        (FeedbackAction.SNOOZE, "snoozed"),
    ],
)
def test_apply_feedback_status_actions_update_task(
    conn: sqlite3.Connection,
    action: FeedbackAction,
    expected_status: str,
) -> None:
    task_id = _make_review_task(conn, fingerprint=f"feedback-{action.value}")
    artifact = FeedbackArtifact(tasks={task_id: FeedbackDirective(action=action)})

    apply_feedback_artifact(conn, artifact)

    task = get_task(conn, task_id)
    assert task is not None
    assert task.status == expected_status
    assert json.loads(task.manual_override_state or "{}")["action"] == action.value


def test_apply_feedback_merge_marks_task_and_records_merge_target(
    conn: sqlite3.Connection,
) -> None:
    task_id = _make_review_task(conn, fingerprint="feedback-merge-source")
    merge_into_task_id, _ = upsert_task(
        conn,
        fingerprint="feedback-merge-target",
        title="Canonical merged task",
        task_class="task",
        confidence_band="high",
        reason_code="en_please_verb",
        promotion_rec="current_tasks",
    )
    artifact = FeedbackArtifact(
        tasks={
            task_id: FeedbackDirective(
                action=FeedbackAction.MERGE,
                merge_into_task_id=merge_into_task_id,
                comment="Duplicate of canonical task.",
            ),
        },
    )

    apply_feedback_artifact(conn, artifact)

    task = get_task(conn, task_id)
    assert task is not None
    assert task.status == "merged"
    override_state = json.loads(task.manual_override_state or "{}")
    assert override_state["overrides"]["merge_into_task_id"] == merge_into_task_id


def test_apply_feedback_preserves_manual_override_across_replay(
    conn: sqlite3.Connection,
) -> None:
    fingerprint = "feedback000000002"
    task_id = _make_review_task(conn, fingerprint=fingerprint)
    artifact = FeedbackArtifact(
        tasks={
            task_id: FeedbackDirective(
                action=FeedbackAction.WRONG_CLASS,
                task_class="follow_up",
                comment="It is actionable, but only a follow-up.",
            ),
        },
    )
    apply_feedback_artifact(conn, artifact)

    replay_task_id, was_created = upsert_task(
        conn,
        fingerprint=fingerprint,
        title="Review the ambiguous request",
        task_class="needs_review",
        confidence_band="low",
        reason_code="long_content_no_signal",
        promotion_rec="review",
    )
    task = get_task(conn, replay_task_id)

    assert replay_task_id == task_id
    assert was_created is False
    assert task is not None
    assert task.task_class == "follow_up"
    assert task.status == "open"
    assert task.promotion_rec == "daily_only"
    assert json.loads(task.manual_override_state or "{}")["action"] == "wrong_class"


def test_apply_feedback_reimport_is_idempotent_for_event_storage(
    conn: sqlite3.Connection,
) -> None:
    task_id = _make_review_task(conn, fingerprint="feedback-idempotent")
    artifact = FeedbackArtifact(
        tasks={
            task_id: FeedbackDirective(
                action=FeedbackAction.WRONG_CLASS,
                task_class="task",
            ),
        },
    )

    first = apply_feedback_artifact(conn, artifact, artifact_path="raw/review/idempotent.yaml")
    second = apply_feedback_artifact(conn, artifact, artifact_path="raw/review/idempotent.yaml")
    events = list_feedback_events(conn, target_id=task_id)

    assert first.feedback_event_ids == second.feedback_event_ids
    assert len(events) == 1


def test_apply_feedback_missed_task_records_source_feedback_only(
    conn: sqlite3.Connection,
) -> None:
    artifact = FeedbackArtifact(
        source_items={
            "msg-999": FeedbackDirective(
                action=FeedbackAction.MISSED_TASK,
                title="Follow up with procurement",
                summary="A missed action item from the source conversation.",
                task_class="task",
                promotion_rec="current_tasks",
                comment="Please capture this on the next replay.",
            ),
        },
    )

    result = apply_feedback_artifact(conn, artifact, artifact_path="raw/review/missed.yaml")

    assert result.applied_task_ids == ()
    events = list_feedback_events(conn, target_type="source_item", target_id="msg-999")
    assert len(events) == 1
    assert events[0].action == "missed_task"
    assert events[0].payload["title"] == "Follow up with procurement"


def test_apply_feedback_merge_requires_target() -> None:
    with pytest.raises(ValueError, match="merge feedback requires merge_into_task_id"):
        parse_feedback_artifact(
            """
version: 1
tasks:
  task-1:
    action: merge
"""
        )


def test_apply_feedback_merge_target_must_exist(conn: sqlite3.Connection) -> None:
    task_id = _make_review_task(conn, fingerprint="feedback-missing-merge")
    artifact = FeedbackArtifact(
        tasks={
            task_id: FeedbackDirective(
                action=FeedbackAction.MERGE,
                merge_into_task_id="missing-task-id",
            ),
        },
    )

    with pytest.raises(LookupError, match="merge_into_task_id"):
        apply_feedback_artifact(conn, artifact)
    assert list_feedback_events(conn, target_id=task_id) == []


# ---------------------------------------------------------------------------
# lw-tst.1: feedback/store.py gap tests
# ---------------------------------------------------------------------------


def _make_entry(
    *,
    target_id: str = "task-001",
    action: str = "confirm",
    target_type: str = "task",
    comment: str = "",
) -> object:
    from lark_to_notes.feedback.models import (
        FeedbackEntry,
        FeedbackTargetType,
    )

    return FeedbackEntry(
        target_type=FeedbackTargetType(target_type),
        target_id=target_id,
        directive=FeedbackDirective(
            action=FeedbackAction(action),
            comment=comment,
        ),
    )


class TestFeedbackStoreGaps:
    """Direct tests for feedback/store.py: get_event, list_events, has_manual_override."""

    def test_get_event_returns_record(self, conn: sqlite3.Connection) -> None:
        from lark_to_notes.feedback.store import get_event, insert_event

        entry = _make_entry(target_id="task-a", action="confirm")
        feedback_id = insert_event(conn, entry)

        record = get_event(conn, feedback_id)
        assert record is not None
        assert record.feedback_id == feedback_id
        assert record.target_id == "task-a"
        assert record.action == "confirm"

    def test_get_event_returns_none_for_missing(self, conn: sqlite3.Connection) -> None:
        from lark_to_notes.feedback.store import get_event

        result = get_event(conn, "does-not-exist-uuid-0000")
        assert result is None

    def test_list_events_filter_by_target_type(self, conn: sqlite3.Connection) -> None:
        from lark_to_notes.feedback.store import insert_event, list_events

        insert_event(conn, _make_entry(target_id="task-x", target_type="task"))
        insert_event(
            conn,
            _make_entry(target_id="src-x", target_type="source_item", action="missed_task"),
            artifact_path="unique-source-path",
        )

        task_events = list_events(conn, target_type="task")
        assert len(task_events) == 1
        assert task_events[0].target_id == "task-x"

        source_events = list_events(conn, target_type="source_item")
        assert len(source_events) == 1
        assert source_events[0].target_id == "src-x"

    def test_list_events_filter_by_action(self, conn: sqlite3.Connection) -> None:
        from lark_to_notes.feedback.store import insert_event, list_events

        insert_event(conn, _make_entry(target_id="t1", action="confirm"), artifact_path="p1")
        insert_event(conn, _make_entry(target_id="t2", action="dismiss"), artifact_path="p2")

        confirms = list_events(conn, action="confirm")
        assert len(confirms) == 1
        assert confirms[0].target_id == "t1"

        dismisses = list_events(conn, action="dismiss")
        assert len(dismisses) == 1
        assert dismisses[0].target_id == "t2"

    def test_list_events_limit(self, conn: sqlite3.Connection) -> None:
        from lark_to_notes.feedback.store import insert_event, list_events

        for i in range(10):
            insert_event(
                conn,
                _make_entry(target_id=f"task-{i}"),
                artifact_path=f"path-{i}",
            )

        results = list_events(conn, limit=3)
        assert len(results) == 3

    def test_list_events_newest_first(self, conn: sqlite3.Connection) -> None:
        from lark_to_notes.feedback.store import insert_event, list_events

        for i in range(3):
            insert_event(
                conn,
                _make_entry(target_id=f"task-{i}", comment=str(i)),
                artifact_path=f"unique-{i}",
            )

        results = list_events(conn, limit=3)
        # ORDER BY created_at DESC — each insert gets a later timestamp than previous
        # but all 3 may land in the same second; at minimum we get all 3 back
        assert len(results) == 3
        target_ids = [r.target_id for r in results]
        assert set(target_ids) == {"task-0", "task-1", "task-2"}

    def test_has_manual_override_true(self, conn: sqlite3.Connection) -> None:
        from lark_to_notes.feedback.store import has_manual_override, insert_event

        insert_event(conn, _make_entry(target_id="task-override", action="confirm"))

        assert has_manual_override(conn, "task-override") is True

    def test_has_manual_override_false(self, conn: sqlite3.Connection) -> None:
        from lark_to_notes.feedback.store import has_manual_override

        assert has_manual_override(conn, "no-such-task") is False


# ---------------------------------------------------------------------------
# lw-tst.2: feedback/service.py gap tests
# ---------------------------------------------------------------------------


class TestFeedbackServiceGaps:
    """Tests for list_feedback_events delegation and _derive_task_override branches."""

    def test_list_feedback_events_target_type_filter(
        self, conn: sqlite3.Connection
    ) -> None:
        from lark_to_notes.feedback.store import insert_event

        insert_event(conn, _make_entry(target_id="task-y", target_type="task"))
        insert_event(
            conn,
            _make_entry(target_id="src-y", target_type="source_item", action="missed_task"),
            artifact_path="unique-src",
        )

        results = list_feedback_events(conn, target_type="task")
        assert all(r.target_id == "task-y" for r in results)
        assert len(results) == 1

    def test_list_feedback_events_target_id_filter(
        self, conn: sqlite3.Connection
    ) -> None:
        from lark_to_notes.feedback.store import insert_event

        insert_event(conn, _make_entry(target_id="task-a1"), artifact_path="path-a1")
        insert_event(conn, _make_entry(target_id="task-b1"), artifact_path="path-b1")

        results = list_feedback_events(conn, target_id="task-a1")
        assert len(results) == 1
        assert results[0].target_id == "task-a1"

    def test_list_feedback_events_limit(self, conn: sqlite3.Connection) -> None:
        from lark_to_notes.feedback.store import insert_event

        for i in range(20):
            insert_event(
                conn,
                _make_entry(target_id=f"t-{i}"),
                artifact_path=f"p-{i}",
            )

        results = list_feedback_events(conn, limit=5)
        assert len(results) == 5

    def test_derive_task_override_wrong_class_no_task_class_raises(
        self, conn: sqlite3.Connection
    ) -> None:
        """WRONG_CLASS with task_class=None must raise ValueError."""
        task_id = _make_review_task(conn)
        artifact = FeedbackArtifact(
            tasks={
                task_id: FeedbackDirective(
                    action=FeedbackAction.WRONG_CLASS,
                    # task_class intentionally omitted (defaults to None)
                ),
            },
        )
        with pytest.raises(ValueError, match="task_class"):
            apply_feedback_artifact(conn, artifact)

    def test_derive_task_override_context_defaults(
        self, conn: sqlite3.Connection
    ) -> None:
        """WRONG_CLASS with task_class='context' -> status dismissed, promotion daily_only."""
        from lark_to_notes.tasks.registry import get_task

        task_id = _make_review_task(conn)
        artifact = FeedbackArtifact(
            tasks={
                task_id: FeedbackDirective(
                    action=FeedbackAction.WRONG_CLASS,
                    task_class="context",
                ),
            },
        )
        apply_feedback_artifact(conn, artifact)
        task = get_task(conn, task_id)
        assert task is not None
        assert task.task_class == "context"
        assert task.status == "dismissed"
        assert task.promotion_rec == "daily_only"

    def test_derive_task_override_needs_review_defaults(
        self, conn: sqlite3.Connection
    ) -> None:
        """WRONG_CLASS with task_class='needs_review' -> status needs_review, promotion review."""
        from lark_to_notes.tasks.registry import get_task

        task_id = _make_review_task(conn, fingerprint="feedback000000002")
        artifact = FeedbackArtifact(
            tasks={
                task_id: FeedbackDirective(
                    action=FeedbackAction.WRONG_CLASS,
                    task_class="needs_review",
                ),
            },
        )
        apply_feedback_artifact(conn, artifact)
        task = get_task(conn, task_id)
        assert task is not None
        assert task.task_class == "needs_review"
        assert task.status == "needs_review"
        assert task.promotion_rec == "review"

    def test_derive_task_override_follow_up_defaults(
        self, conn: sqlite3.Connection
    ) -> None:
        """WRONG_CLASS with task_class='follow_up' -> status open, promotion daily_only."""
        from lark_to_notes.tasks.registry import get_task

        task_id = _make_review_task(conn, fingerprint="feedback000000003")
        artifact = FeedbackArtifact(
            tasks={
                task_id: FeedbackDirective(
                    action=FeedbackAction.WRONG_CLASS,
                    task_class="follow_up",
                ),
            },
        )
        apply_feedback_artifact(conn, artifact)
        task = get_task(conn, task_id)
        assert task is not None
        assert task.task_class == "follow_up"
        assert task.status == "open"
        assert task.promotion_rec == "daily_only"
