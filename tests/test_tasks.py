"""Tests for the task registry: fingerprint, models, CRUD, lifecycle."""

from __future__ import annotations

import sqlite3

import pytest

from lark_to_notes.storage.db import init_db
from lark_to_notes.tasks.fingerprint import _normalize_text, _week_bucket, derive_fingerprint
from lark_to_notes.tasks.models import TaskRecord, TaskStatus
from lark_to_notes.tasks.registry import (
    add_evidence,
    get_task,
    get_task_by_fingerprint,
    list_evidence,
    list_tasks,
    update_task_status,
    upsert_task,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def conn() -> sqlite3.Connection:
    """Return an in-memory SQLite connection with the full schema applied."""
    c = sqlite3.connect(":memory:")
    c.execute("PRAGMA foreign_keys = ON")
    init_db(c)
    return c


# ---------------------------------------------------------------------------
# Fingerprint tests
# ---------------------------------------------------------------------------


def test_normalize_text_lowercase() -> None:
    assert _normalize_text("Hello WORLD") == "hello world"


def test_normalize_text_strips_punctuation() -> None:
    result = _normalize_text("Please review this! It's urgent.")
    assert "!" not in result
    assert "'" not in result


def test_normalize_text_preserves_chinese() -> None:
    result = _normalize_text("请帮我确认一下这个需求")
    assert "请帮我确认一下这个需求" in result


def test_normalize_text_preserves_at_sign() -> None:
    result = _normalize_text("@Alice please review")
    assert "@" in result


def test_normalize_text_collapses_whitespace() -> None:
    result = _normalize_text("hello   world\n\tthere")
    assert "  " not in result


def test_week_bucket_lark_format() -> None:
    bucket = _week_bucket("2026-04-14 10:00")
    assert bucket.startswith("2026-W")


def test_week_bucket_iso_format() -> None:
    bucket = _week_bucket("2026-04-14T10:00:00Z")
    assert bucket.startswith("2026-W")


def test_week_bucket_same_week() -> None:
    assert _week_bucket("2026-04-13 08:00") == _week_bucket("2026-04-14 22:00")


def test_week_bucket_different_weeks() -> None:
    # Apr 14 is in a different week from Apr 6
    assert _week_bucket("2026-04-06 10:00") != _week_bucket("2026-04-14 10:00")


def test_week_bucket_unparseable_returns_unknown() -> None:
    assert _week_bucket("not-a-date") == "unknown"


def test_derive_fingerprint_is_deterministic() -> None:
    fp1 = derive_fingerprint("Please review", "dm-source:1", "2026-04-14 10:00")
    fp2 = derive_fingerprint("Please review", "dm-source:1", "2026-04-14 10:00")
    assert fp1 == fp2


def test_derive_fingerprint_different_content() -> None:
    fp1 = derive_fingerprint("Please review", "dm-source:1", "2026-04-14 10:00")
    fp2 = derive_fingerprint("Please merge", "dm-source:1", "2026-04-14 10:00")
    assert fp1 != fp2


def test_derive_fingerprint_different_source() -> None:
    fp1 = derive_fingerprint("Please review", "dm-alice:1", "2026-04-14 10:00")
    fp2 = derive_fingerprint("Please review", "dm-bob:1", "2026-04-14 10:00")
    assert fp1 != fp2


def test_derive_fingerprint_different_week() -> None:
    fp1 = derive_fingerprint("Please review", "dm-source:1", "2026-04-06 10:00")
    fp2 = derive_fingerprint("Please review", "dm-source:1", "2026-04-14 10:00")
    assert fp1 != fp2


def test_derive_fingerprint_is_16_chars() -> None:
    fp = derive_fingerprint("some content", "src:1", "2026-04-14 10:00")
    assert len(fp) == 16
    assert all(c in "0123456789abcdef" for c in fp)


def test_derive_fingerprint_same_within_week() -> None:
    # Monday and Thursday of same week → same fingerprint
    fp_mon = derive_fingerprint("Please review", "src:1", "2026-04-13 08:00")  # Mon
    fp_thu = derive_fingerprint("Please review", "src:1", "2026-04-16 17:00")  # Thu
    assert fp_mon == fp_thu


# ---------------------------------------------------------------------------
# TaskStatus model tests
# ---------------------------------------------------------------------------


def test_task_status_values() -> None:
    assert TaskStatus.OPEN.value == "open"
    assert TaskStatus.NEEDS_REVIEW.value == "needs_review"
    assert TaskStatus.COMPLETED.value == "completed"
    assert TaskStatus.DISMISSED.value == "dismissed"
    assert TaskStatus.MERGED.value == "merged"
    assert TaskStatus.SUPERSEDED.value == "superseded"
    assert TaskStatus.SNOOZED.value == "snoozed"


def test_task_status_terminal_states() -> None:
    terminal = TaskStatus.terminal_states()
    assert TaskStatus.COMPLETED in terminal
    assert TaskStatus.DISMISSED in terminal
    assert TaskStatus.MERGED in terminal
    assert TaskStatus.SUPERSEDED in terminal
    assert TaskStatus.OPEN not in terminal
    assert TaskStatus.NEEDS_REVIEW not in terminal
    assert TaskStatus.SNOOZED not in terminal


def test_task_status_is_terminal() -> None:
    assert TaskStatus.COMPLETED.is_terminal is True
    assert TaskStatus.OPEN.is_terminal is False
    assert TaskStatus.NEEDS_REVIEW.is_terminal is False


# ---------------------------------------------------------------------------
# Registry: upsert_task
# ---------------------------------------------------------------------------


def _make_task(conn: sqlite3.Connection, **overrides: str) -> tuple[str, bool]:
    kwargs: dict[str, str] = {
        "fingerprint": "abcdef0123456789",
        "title": "Review the PR",
        "task_class": "task",
        "confidence_band": "high",
        "summary": "Summary text",
        "reason_code": "en_please_verb",
        "promotion_rec": "current_tasks",
    }
    kwargs.update(overrides)
    return upsert_task(
        conn,
        fingerprint=kwargs["fingerprint"],
        title=kwargs["title"],
        task_class=kwargs["task_class"],
        confidence_band=kwargs["confidence_band"],
        summary=kwargs.get("summary", ""),
        reason_code=kwargs.get("reason_code", ""),
        promotion_rec=kwargs.get("promotion_rec", "review"),
    )


def test_upsert_task_creates_new(conn: sqlite3.Connection) -> None:
    task_id, was_created = _make_task(conn)
    assert was_created is True
    assert len(task_id) == 36  # UUID


def test_upsert_task_idempotent(conn: sqlite3.Connection) -> None:
    id1, c1 = _make_task(conn)
    id2, c2 = _make_task(conn)
    assert id1 == id2
    assert c1 is True
    assert c2 is False


def test_upsert_task_high_confidence_status_is_open(conn: sqlite3.Connection) -> None:
    task_id, _ = _make_task(conn, confidence_band="high", task_class="task")
    task = get_task(conn, task_id)
    assert task is not None
    assert task.status == "open"


def test_upsert_task_low_confidence_status_is_needs_review(conn: sqlite3.Connection) -> None:
    task_id, _ = upsert_task(
        conn,
        fingerprint="low1234567890123",
        title="Uncertain thing",
        task_class="needs_review",
        confidence_band="low",
        reason_code="long_content_no_signal",
        promotion_rec="review",
    )
    task = get_task(conn, task_id)
    assert task is not None
    assert task.status == "needs_review"


def test_upsert_task_different_fingerprint_creates_new(conn: sqlite3.Connection) -> None:
    id1, _ = _make_task(conn, fingerprint="aaaa000000000000")
    id2, _ = _make_task(conn, fingerprint="bbbb000000000000")
    assert id1 != id2


# ---------------------------------------------------------------------------
# Registry: get_task / get_task_by_fingerprint
# ---------------------------------------------------------------------------


def test_get_task_returns_record(conn: sqlite3.Connection) -> None:
    task_id, _ = _make_task(conn)
    task = get_task(conn, task_id)
    assert task is not None
    assert isinstance(task, TaskRecord)
    assert task.task_id == task_id
    assert task.title == "Review the PR"


def test_get_task_missing_returns_none(conn: sqlite3.Connection) -> None:
    assert get_task(conn, "does-not-exist") is None


def test_get_task_by_fingerprint_returns_record(conn: sqlite3.Connection) -> None:
    task_id, _ = _make_task(conn, fingerprint="fp_test0000000000")
    task = get_task_by_fingerprint(conn, "fp_test0000000000")
    assert task is not None
    assert task.task_id == task_id


def test_get_task_by_fingerprint_missing_returns_none(conn: sqlite3.Connection) -> None:
    assert get_task_by_fingerprint(conn, "nonexistent_fp___") is None


def test_task_record_assignee_refs_tuple(conn: sqlite3.Connection) -> None:
    task_id, _ = upsert_task(
        conn,
        fingerprint="fp_refs000000000",
        title="Assign to Alice",
        task_class="task",
        confidence_band="high",
        reason_code="en_at_assign",
        promotion_rec="current_tasks",
        assignee_refs=["alice", "bob"],
    )
    task = get_task(conn, task_id)
    assert task is not None
    assert task.assignee_refs == ("alice", "bob")


# ---------------------------------------------------------------------------
# Registry: list_tasks
# ---------------------------------------------------------------------------


def test_list_tasks_empty(conn: sqlite3.Connection) -> None:
    assert list_tasks(conn) == []


def test_list_tasks_returns_all(conn: sqlite3.Connection) -> None:
    _make_task(conn, fingerprint="fp_list000000001")
    _make_task(conn, fingerprint="fp_list000000002")
    tasks = list_tasks(conn)
    assert len(tasks) == 2


def test_list_tasks_filtered_by_status(conn: sqlite3.Connection) -> None:
    _make_task(conn, fingerprint="fp_open000000000", confidence_band="high")
    upsert_task(
        conn,
        fingerprint="fp_review00000000",
        title="Low conf",
        task_class="needs_review",
        confidence_band="low",
        reason_code="x",
        promotion_rec="review",
    )
    open_tasks = list_tasks(conn, status="open")
    review_tasks = list_tasks(conn, status="needs_review")
    assert len(open_tasks) == 1
    assert len(review_tasks) == 1


# ---------------------------------------------------------------------------
# Registry: update_task_status
# ---------------------------------------------------------------------------


def test_update_task_status_open_to_completed(conn: sqlite3.Connection) -> None:
    task_id, _ = _make_task(conn)
    changed = update_task_status(conn, task_id, "completed")
    assert changed is True
    task = get_task(conn, task_id)
    assert task is not None
    assert task.status == "completed"


def test_update_task_status_terminal_is_sticky(conn: sqlite3.Connection) -> None:
    task_id, _ = _make_task(conn)
    update_task_status(conn, task_id, "completed")
    changed = update_task_status(conn, task_id, "open")
    assert changed is False
    task = get_task(conn, task_id)
    assert task is not None
    assert task.status == "completed"


def test_update_task_status_force_overrides_terminal(conn: sqlite3.Connection) -> None:
    task_id, _ = _make_task(conn)
    update_task_status(conn, task_id, "completed")
    changed = update_task_status(conn, task_id, "open", force=True)
    assert changed is True
    task = get_task(conn, task_id)
    assert task is not None
    assert task.status == "open"


def test_update_task_status_nonexistent_returns_false(conn: sqlite3.Connection) -> None:
    changed = update_task_status(conn, "no-such-id", "completed")
    assert changed is False


def test_update_task_status_invalid_raises(conn: sqlite3.Connection) -> None:
    task_id, _ = _make_task(conn)
    with pytest.raises(ValueError):
        update_task_status(conn, task_id, "invalid_status")


# ---------------------------------------------------------------------------
# Registry: add_evidence / list_evidence
# ---------------------------------------------------------------------------


def test_add_evidence_returns_id(conn: sqlite3.Connection) -> None:
    task_id, _ = _make_task(conn)
    eid = add_evidence(conn, task_id, excerpt="Please review this", evidence_role="primary")
    assert len(eid) == 36  # UUID


def test_add_evidence_multiple(conn: sqlite3.Connection) -> None:
    task_id, _ = _make_task(conn)
    add_evidence(conn, task_id, excerpt="first signal", evidence_role="primary")
    add_evidence(conn, task_id, excerpt="second signal", evidence_role="corroboration")
    evidence = list_evidence(conn, task_id)
    assert len(evidence) == 2
    assert evidence[0].excerpt == "first signal"
    assert evidence[1].evidence_role == "corroboration"


def test_evidence_inherits_task_id(conn: sqlite3.Connection) -> None:
    task_id, _ = _make_task(conn)
    eid = add_evidence(conn, task_id, excerpt="x")
    evidence_list = list_evidence(conn, task_id)
    found = next(e for e in evidence_list if e.evidence_id == eid)
    assert found.task_id == task_id


def test_add_evidence_nonexistent_task_raises(conn: sqlite3.Connection) -> None:
    with pytest.raises(sqlite3.IntegrityError):
        add_evidence(conn, "no-such-task", excerpt="x")


def test_evidence_cascade_delete(conn: sqlite3.Connection) -> None:
    task_id, _ = _make_task(conn)
    add_evidence(conn, task_id, excerpt="will be deleted")
    conn.execute("DELETE FROM tasks WHERE task_id = ?", (task_id,))
    evidence = list_evidence(conn, task_id)
    assert evidence == []


# ---------------------------------------------------------------------------
# Replay stability test
# ---------------------------------------------------------------------------


def test_fingerprint_stable_across_replays() -> None:
    """Same content + source + timestamp always yields same fingerprint."""
    content = "Please review the deployment plan"
    source = "dm-alice:s1"
    created_at = "2026-04-14 09:30"

    fingerprints = [derive_fingerprint(content, source, created_at) for _ in range(5)]
    assert len(set(fingerprints)) == 1, "Fingerprint must be stable across replays"


def test_upsert_replay_returns_same_task_id(conn: sqlite3.Connection) -> None:
    """Re-processing the same (fingerprint) never creates a second task."""
    fp = derive_fingerprint("Please send the report", "dm-bob:s1", "2026-04-14 10:00")
    id1, c1 = upsert_task(
        conn, fingerprint=fp, title="Send report", task_class="task",
        confidence_band="high", reason_code="en_please_verb", promotion_rec="current_tasks",
    )
    id2, c2 = upsert_task(
        conn, fingerprint=fp, title="Send report", task_class="task",
        confidence_band="high", reason_code="en_please_verb", promotion_rec="current_tasks",
    )
    assert id1 == id2
    assert c1 is True
    assert c2 is False
    assert len(list_tasks(conn)) == 1


# ---------------------------------------------------------------------------
# TaskRecord.task_status property
# ---------------------------------------------------------------------------


def test_task_record_task_status_property(conn: sqlite3.Connection) -> None:
    task_id, _ = _make_task(conn)
    task = get_task(conn, task_id)
    assert task is not None
    assert task.task_status == TaskStatus.OPEN


def test_task_record_task_status_invalid_raises(conn: sqlite3.Connection) -> None:
    # Construct a TaskRecord in memory with an invalid status string
    # (bypassing the DB CHECK constraint) to verify the property raises ValueError.
    task = TaskRecord(
        task_id="fake-id",
        fingerprint="fp000000000000ff",
        title="test",
        status="bogus_status",
        task_class="task",
        confidence_band="high",
        summary="",
        reason_code="",
        promotion_rec="current_tasks",
        assignee_refs=(),
        due_at=None,
        manual_override_state=None,
        created_from_raw_record_id=None,
        created_at="2026-01-01T00:00:00Z",
        last_updated_at="2026-01-01T00:00:00Z",
    )
    with pytest.raises(ValueError):
        _ = task.task_status
