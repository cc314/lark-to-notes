"""Operator CLI tests for currently implemented commands."""

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
from lark_to_notes.storage.db import connect, init_db, upsert_watched_source
from lark_to_notes.tasks.registry import get_task, upsert_task

FIXTURE_CORPUS_ROOT = Path(__file__).resolve().parents[1] / "raw" / "lark-worker" / "fixture-corpus"


def test_sources_list_json_outputs_watched_sources(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "state.db"
    conn = connect(db_path)
    init_db(conn)
    source = WatchedSource(
        source_id=make_source_id(SourceType.DM, "ou_demo"),
        source_type=SourceType.DM,
        external_id="ou_demo",
        name="Demo DM",
    )
    upsert_watched_source(conn, source)

    exit_code = run(["sources", "list", "--db", str(db_path), "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["count"] == 1
    assert payload["sources"][0]["source_id"] == source.source_id
    assert payload["sources"][0]["source_type"] == "dm"


def test_replay_json_outputs_summary_and_is_repeatable(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    record = {
        "message_id": "om_cli_1",
        "source_id": "dm:ou_demo",
        "source_type": "dm_user",
        "chat_id": "ou_chat",
        "chat_type": "p2p",
        "sender_id": "ou_sender",
        "sender_name": "Alice",
        "direction": "incoming",
        "created_at": "2026-04-14 10:00",
        "content": "hello from cli replay",
        "payload": {"content": "hello from cli replay"},
    }
    (raw_dir / "2026-04-14.jsonl").write_text(json.dumps(record) + "\n", encoding="utf-8")
    db_path = tmp_path / "state.db"

    first_exit = run(["replay", "--db", str(db_path), "--raw-dir", str(raw_dir), "--json"])
    first_payload = json.loads(capsys.readouterr().out)
    second_exit = run(["replay", "--db", str(db_path), "--raw-dir", str(raw_dir), "--json"])
    second_payload = json.loads(capsys.readouterr().out)

    assert first_exit == 0
    assert first_payload["file_count"] == 1
    assert first_payload["total_records"] == 1
    assert first_payload["inserted_records"] == 1
    assert first_payload["files"][0]["filename"] == "2026-04-14.jsonl"
    assert second_exit == 0
    assert second_payload["inserted_records"] == 0


def test_doctor_json_reports_fixture_health(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = run(["doctor", "--fixture-corpus", str(FIXTURE_CORPUS_ROOT), "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["status"] == "ok"
    assert payload["fixture_corpus"]["record_count"] == payload["replay"]["total_records"]
    assert payload["replay"]["matches_manifest"] is True


def test_feedback_import_json_applies_artifact(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "state.db"
    conn = connect(db_path)
    init_db(conn)
    task_id, _ = upsert_task(
        conn,
        fingerprint="cli-feedback-0001",
        title="Ambiguous request",
        task_class="needs_review",
        confidence_band="low",
        reason_code="long_content_no_signal",
        promotion_rec="review",
    )
    conn.commit()
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

    exit_code = run(["feedback", "import", str(artifact_path), "--db", str(db_path), "--json"])

    payload = json.loads(capsys.readouterr().out)
    updated_task = get_task(conn, task_id)
    assert exit_code == 0
    assert payload["applied_task_count"] == 1
    assert payload["feedback_event_count"] == 1
    assert payload["applied_task_ids"] == [task_id]
    assert updated_task is not None
    assert updated_task.task_class == "task"
    assert updated_task.promotion_rec == "current_tasks"
