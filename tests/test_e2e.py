"""End-to-end integration tests for the full raw-to-vault pipeline.

These tests exercise the complete path from raw-message insertion through
reclassification and vault rendering, proving that the layers compose
correctly and that the system is safe to run repeatedly.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from lark_to_notes.cli import run
from lark_to_notes.config.sources import SourceType, WatchedSource, make_source_id
from lark_to_notes.feedback import (
    FeedbackAction,
    FeedbackArtifact,
    FeedbackDirective,
    render_feedback_artifact,
)
from lark_to_notes.intake.ledger import insert_raw_message, list_raw_messages
from lark_to_notes.intake.models import RawMessage
from lark_to_notes.storage.db import connect, init_db, upsert_watched_source
from lark_to_notes.tasks.registry import get_task, list_tasks

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "state.db"
    return db_path


def _seed_source(conn: object, external_id: str = "ou_e2e") -> WatchedSource:
    source = WatchedSource(
        source_id=make_source_id(SourceType.DM, external_id),
        source_type=SourceType.DM,
        external_id=external_id,
        name=f"E2E Source ({external_id})",
    )
    upsert_watched_source(conn, source)  # type: ignore[arg-type]
    return source


def _make_raw_message(
    message_id: str,
    content: str,
    source_id: str = "dm:ou_e2e",
    created_at: str = "2026-05-01T10:00:00Z",
) -> RawMessage:
    return RawMessage(
        message_id=message_id,
        source_id=source_id,
        source_type="dm_user",
        chat_id="ou_chat",
        chat_type="p2p",
        sender_id="ou_sender",
        sender_name="Alice",
        direction="incoming",
        created_at=created_at,
        content=content,
        payload={},
        ingested_at="2026-05-01T10:00:00Z",
    )


# ---------------------------------------------------------------------------
# Full pipeline: raw message → reclassify → render → verify vault files
# ---------------------------------------------------------------------------


def test_full_pipeline_creates_vault_files(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Insert a raw message, reclassify, render — verify vault files are created."""
    db_path = _make_db(tmp_path)
    vault_root = tmp_path / "vault"
    vault_root.mkdir()
    conn = connect(db_path)
    init_db(conn)
    _seed_source(conn)

    msg = _make_raw_message("om_e2e_full_1", "Please review the Q2 report by Friday")
    insert_raw_message(conn, msg)
    conn.commit()

    rc = run(["reclassify", "--db", str(db_path)])
    assert rc == 0
    capsys.readouterr()  # drain reclassify output before render

    rc = run(["render", "--db", str(db_path), "--vault-root", str(vault_root), "--json"])
    payload = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert payload["rendered"] >= 1

    # At least a raw provenance note must exist.
    raw_notes = list((vault_root / "raw").glob("*.md"))
    assert len(raw_notes) >= 1, "expected at least one raw provenance note"


def test_full_pipeline_with_explicit_task_keyword(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A message containing an explicit task keyword should produce a high-confidence task."""
    db_path = _make_db(tmp_path)
    vault_root = tmp_path / "vault"
    vault_root.mkdir()
    conn = connect(db_path)
    init_db(conn)
    _seed_source(conn)

    msg = _make_raw_message("om_e2e_kw_1", "TODO: update the deployment runbook")
    insert_raw_message(conn, msg)
    conn.commit()

    run(["reclassify", "--db", str(db_path)])
    capsys.readouterr()

    tasks = list_tasks(conn)
    assert any(
        t.task_class in ("task", "follow_up") for t in tasks
    ), "expected at least one task/follow_up from explicit keyword"


# ---------------------------------------------------------------------------
# Replay idempotency
# ---------------------------------------------------------------------------


def test_replay_is_idempotent(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Replaying the same JSONL twice produces identical task and note state."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    vault_root = tmp_path / "vault"
    vault_root.mkdir()
    record = {
        "message_id": "om_idem_1",
        "source_id": "dm:ou_idem",
        "source_type": "dm_user",
        "chat_id": "ou_chat",
        "chat_type": "p2p",
        "sender_id": "ou_sender",
        "sender_name": "Alice",
        "direction": "incoming",
        "created_at": "2026-05-01T10:00:00Z",
        "content": "Please send me the weekly report",
        "payload": {},
    }
    (raw_dir / "2026-05-01.jsonl").write_text(json.dumps(record) + "\n", encoding="utf-8")
    db_path = _make_db(tmp_path)

    run(["replay", "--db", str(db_path), "--raw-dir", str(raw_dir)])
    capsys.readouterr()
    run(["reclassify", "--db", str(db_path)])
    capsys.readouterr()
    run(["render", "--db", str(db_path), "--vault-root", str(vault_root)])
    capsys.readouterr()

    conn = connect(db_path)
    init_db(conn)
    tasks_after_first = list_tasks(conn)
    notes_after_first = list((vault_root / "raw").glob("*.md"))

    # Second pass: replay again (should insert 0 new records).
    run(["replay", "--db", str(db_path), "--raw-dir", str(raw_dir), "--json"])
    second_payload = json.loads(capsys.readouterr().out)
    assert second_payload["inserted_records"] == 0

    # Reclassify again: no new tasks.
    run(["reclassify", "--db", str(db_path)])
    capsys.readouterr()
    run(["render", "--db", str(db_path), "--vault-root", str(vault_root)])
    capsys.readouterr()

    tasks_after_second = list_tasks(conn)
    notes_after_second = list((vault_root / "raw").glob("*.md"))

    assert len(tasks_after_second) == len(tasks_after_first), "replay must not create duplicate tasks"
    assert len(notes_after_second) == len(notes_after_first), "re-render must not create duplicate notes"


# ---------------------------------------------------------------------------
# Multilingual content
# ---------------------------------------------------------------------------


def test_pipeline_handles_chinese_content(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Chinese-language messages are ingested and classified without errors."""
    db_path = _make_db(tmp_path)
    vault_root = tmp_path / "vault"
    vault_root.mkdir()
    conn = connect(db_path)
    init_db(conn)
    _seed_source(conn)

    msg = _make_raw_message("om_e2e_zh_1", "请帮我审核一下这份报告,谢谢")
    insert_raw_message(conn, msg)
    conn.commit()

    rc = run(["reclassify", "--db", str(db_path)])
    assert rc == 0

    rc = run(["render", "--db", str(db_path), "--vault-root", str(vault_root)])
    assert rc == 0

    raw_notes = list((vault_root / "raw").glob("*.md"))
    assert len(raw_notes) >= 1


def test_pipeline_handles_mixed_language_content(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Mixed English/Chinese messages are handled gracefully."""
    db_path = _make_db(tmp_path)
    vault_root = tmp_path / "vault"
    vault_root.mkdir()
    conn = connect(db_path)
    init_db(conn)
    _seed_source(conn)

    msg = _make_raw_message("om_e2e_mixed_1", "TODO: 请在本周五前完成 code review")
    insert_raw_message(conn, msg)
    conn.commit()

    rc = run(["reclassify", "--db", str(db_path)])
    assert rc == 0

    tasks = list_tasks(conn)
    assert len(tasks) >= 1


# ---------------------------------------------------------------------------
# Malformed payload handling
# ---------------------------------------------------------------------------


def test_replay_skips_malformed_jsonl_records(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Malformed JSONL records in a replay file are skipped; valid records still ingest."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    good_record = {
        "message_id": "om_good_1",
        "source_id": "dm:ou_malform",
        "source_type": "dm_user",
        "chat_id": "ou_chat",
        "chat_type": "p2p",
        "sender_id": "ou_sender",
        "sender_name": "Alice",
        "direction": "incoming",
        "created_at": "2026-05-01T10:00:00Z",
        "content": "Valid message",
        "payload": {},
    }
    content = (
        "this is not valid JSON\n"
        + json.dumps(good_record) + "\n"
        + '{"incomplete":\n'  # truncated JSON
    )
    (raw_dir / "mixed.jsonl").write_text(content, encoding="utf-8")
    db_path = _make_db(tmp_path)

    rc = run(["replay", "--db", str(db_path), "--raw-dir", str(raw_dir), "--json"])

    payload = json.loads(capsys.readouterr().out)
    # The replay should complete without raising an exception.
    assert rc == 0
    # The valid record should be inserted.
    assert payload["inserted_records"] >= 1


def test_reclassify_handles_empty_content(
    tmp_path: Path,
) -> None:
    """Messages with empty content are classified without crashing."""
    db_path = _make_db(tmp_path)
    conn = connect(db_path)
    init_db(conn)
    _seed_source(conn)

    msg = _make_raw_message("om_empty_1", "")
    insert_raw_message(conn, msg)
    conn.commit()

    rc = run(["reclassify", "--db", str(db_path)])
    assert rc == 0


# ---------------------------------------------------------------------------
# Feedback import end-to-end
# ---------------------------------------------------------------------------


def test_feedback_import_changes_task_class_and_rerenders(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Import feedback → task class changes → re-render reflects the override."""
    db_path = _make_db(tmp_path)
    vault_root = tmp_path / "vault"
    vault_root.mkdir()
    conn = connect(db_path)
    init_db(conn)
    _seed_source(conn)

    # Insert a message that classifier might call CONTEXT (no strong signal).
    msg = _make_raw_message("om_fb_e2e_1", "Just checking in, nothing urgent")
    insert_raw_message(conn, msg)
    conn.commit()

    run(["reclassify", "--db", str(db_path)])
    capsys.readouterr()

    tasks = list_tasks(conn)
    assert len(tasks) == 1
    task_id = tasks[0].task_id

    # Build a feedback artifact that overrides to "task".
    artifact_path = tmp_path / "feedback.yaml"
    artifact_path.write_text(
        render_feedback_artifact(
            FeedbackArtifact(
                tasks={
                    task_id: FeedbackDirective(
                        action=FeedbackAction.WRONG_CLASS,
                        task_class="task",
                    ),
                },
            )
        ),
        encoding="utf-8",
    )

    rc = run(["feedback", "import", str(artifact_path), "--db", str(db_path)])
    assert rc == 0
    capsys.readouterr()  # drain feedback import output before render

    updated = get_task(conn, task_id)
    assert updated is not None
    assert updated.task_class == "task"

    # Re-render with the overridden class — should complete without error.
    rc = run(
        ["render", "--db", str(db_path), "--vault-root", str(vault_root),
         "--status", "open", "--json"]
    )
    payload = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert payload["errors"] == []


# ---------------------------------------------------------------------------
# Provenance traceability
# ---------------------------------------------------------------------------


def test_task_provenance_links_back_to_raw_message(
    tmp_path: Path,
) -> None:
    """Every task created by reclassify carries a source_message_id pointing to its raw message."""
    db_path = _make_db(tmp_path)
    conn = connect(db_path)
    init_db(conn)
    _seed_source(conn)

    message_id = "om_prov_1"
    msg = _make_raw_message(message_id, "Please review this PR before EOD")
    insert_raw_message(conn, msg)
    conn.commit()

    run(["reclassify", "--db", str(db_path)])

    tasks = list_tasks(conn)
    assert len(tasks) >= 1
    for task in tasks:
        assert task.created_from_raw_record_id == message_id, (
            f"task {task.task_id} has no provenance link to source message"
        )


# ---------------------------------------------------------------------------
# Multi-source isolation
# ---------------------------------------------------------------------------


def test_reclassify_source_isolation(
    tmp_path: Path,
) -> None:
    """Reclassify with --source-id only processes messages from that source."""
    db_path = _make_db(tmp_path)
    conn = connect(db_path)
    init_db(conn)
    _seed_source(conn, "ou_src_a")
    _seed_source(conn, "ou_src_b")

    msg_a = _make_raw_message(
        "om_iso_a1", "Review the doc", source_id="dm:ou_src_a"
    )
    msg_b = _make_raw_message(
        "om_iso_b1", "Reply to the thread", source_id="dm:ou_src_b"
    )
    insert_raw_message(conn, msg_a)
    insert_raw_message(conn, msg_b)
    conn.commit()

    run(["reclassify", "--db", str(db_path), "--source-id", "dm:ou_src_a"])

    tasks = list_tasks(conn)
    # Only source A messages should produce tasks.
    for task in tasks:
        assert task.created_from_raw_record_id == "om_iso_a1", (
            "reclassify leaked tasks from source B"
        )


# ---------------------------------------------------------------------------
# Acceptance: raw-message count matches task count (no silent drops)
# ---------------------------------------------------------------------------


def test_no_silent_drops_for_classifiable_messages(
    tmp_path: Path,
) -> None:
    """Every raw message that the classifier processes produces exactly one task (no drops)."""
    db_path = _make_db(tmp_path)
    conn = connect(db_path)
    init_db(conn)
    _seed_source(conn)

    messages = [
        ("om_drop_1", "Please review the design doc"),
        ("om_drop_2", "TODO: schedule the retrospective"),
        ("om_drop_3", "Let me know when you're free"),
    ]
    for mid, content in messages:
        insert_raw_message(conn, _make_raw_message(mid, content))
    conn.commit()

    run(["reclassify", "--db", str(db_path)])

    raw_count = len(list_raw_messages(conn))
    task_count = len(list_tasks(conn))
    assert task_count == raw_count, (
        f"expected {raw_count} tasks (one per message), got {task_count}"
    )
