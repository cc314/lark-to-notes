"""Generate YAML feedback sidecar drafts for plannotator-style triage."""

from __future__ import annotations

from typing import TYPE_CHECKING

import yaml

if TYPE_CHECKING:
    from collections.abc import Sequence

    from lark_to_notes.tasks.models import TaskRecord

# Not a valid :class:`~lark_to_notes.feedback.models.FeedbackAction`; ``feedback import`` must fail
# until the operator replaces each placeholder with a real action.
DRAFT_ACTION_PLACEHOLDER = "EDIT_ACTION_BEFORE_IMPORT"


def _draft_header() -> str:
    return (
        "# Structured feedback draft — not importable until each `action` is replaced.\n"
        f"# Replace {DRAFT_ACTION_PLACEHOLDER!r} with one of: "
        "confirm, dismiss, snooze, wrong_class, merge\n"
        "# Then run: lark-to-notes feedback import THIS_FILE --db <path/to/state.db>\n"
        "#\n"
    )


def _task_hint_comment(record: TaskRecord) -> str:
    title = record.title.replace("\n", " ").replace("\r", " ").strip()
    if len(title) > 160:
        title = title[:157] + "..."
    return (
        f"candidate: {title!r} | status={record.status} | "
        f"task_class={record.task_class} | promotion_rec={record.promotion_rec}"
    )


def render_feedback_draft_yaml(tasks: Sequence[TaskRecord]) -> str:
    """Build YAML text listing *tasks* with non-importable placeholder actions."""
    tasks_payload: dict[str, dict[str, str]] = {}
    for record in tasks:
        tasks_payload[record.task_id] = {
            "action": DRAFT_ACTION_PLACEHOLDER,
            "comment": _task_hint_comment(record),
        }
    body = yaml.safe_dump(
        {"version": 1, "tasks": tasks_payload, "source_items": {}},
        sort_keys=False,
        allow_unicode=False,
    )
    return _draft_header() + body
