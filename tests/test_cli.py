"""Operator CLI tests for currently implemented commands."""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path
from typing import Any

import pytest

from lark_to_notes.cli import run
from lark_to_notes.config.sources import SourceType, WatchedSource, make_source_id
from lark_to_notes.feedback import (
    FeedbackAction,
    FeedbackArtifact,
    FeedbackDirective,
    render_feedback_artifact,
)
from lark_to_notes.intake.ledger import count_raw_messages
from lark_to_notes.storage.db import connect, init_db, upsert_watched_source
from lark_to_notes.tasks.registry import get_task, upsert_task

FIXTURE_CORPUS_ROOT = (
    Path(__file__).resolve().parent / "fixtures" / "lark-worker" / "fixture-corpus"
)


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


def test_feedback_draft_writes_review_lane_stub(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from lark_to_notes.feedback.draft import DRAFT_ACTION_PLACEHOLDER

    db_path = tmp_path / "state.db"
    conn = connect(db_path)
    init_db(conn)
    task_id, _ = upsert_task(
        conn,
        fingerprint="cli-draft-00000001",
        title="Ambiguous request",
        task_class="needs_review",
        confidence_band="low",
        reason_code="long_content_no_signal",
        promotion_rec="review",
    )
    conn.commit()
    out_path = tmp_path / "review-draft.yaml"
    exit_code = run(["feedback", "draft", "--db", str(db_path), "--out", str(out_path), "--json"])
    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["task_count"] == 1
    assert payload["task_ids"] == [task_id]
    text = out_path.read_text(encoding="utf-8")
    assert task_id in text
    assert DRAFT_ACTION_PLACEHOLDER in text


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


# ---------------------------------------------------------------------------
# sources validate
# ---------------------------------------------------------------------------


def test_sources_validate_ok_source(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "state.db"
    conn = connect(db_path)
    init_db(conn)
    source = WatchedSource(
        source_id=make_source_id(SourceType.DM, "ou_val1"),
        source_type=SourceType.DM,
        external_id="ou_val1",
        name="Valid DM",
    )
    upsert_watched_source(conn, source)

    exit_code = run(["sources", "validate", "--db", str(db_path), "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["overall"] == "ok"
    assert payload["count"] == 1
    assert payload["sources"][0]["status"] == "ok"
    assert payload["sources"][0]["issues"] == []


def test_sources_validate_reports_error_for_empty_external_id(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "state.db"
    conn = connect(db_path)
    init_db(conn)
    # Insert a source with an empty external_id directly via SQL to bypass Python validation.
    conn.execute(
        "INSERT INTO watched_sources (source_id, source_type, external_id, name, enabled, config_json)"
        " VALUES (?, ?, ?, ?, 1, '{}')",
        ("dm:bad", "dm", "", "Bad DM"),
    )
    conn.commit()

    exit_code = run(["sources", "validate", "--db", str(db_path), "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert payload["overall"] == "error"
    src = next(s for s in payload["sources"] if s["source_id"] == "dm:bad")
    assert src["status"] == "error"
    assert any("external_id" in issue for issue in src["issues"])


def test_sources_validate_human_readable(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "state.db"
    conn = connect(db_path)
    init_db(conn)
    source = WatchedSource(
        source_id=make_source_id(SourceType.DM, "ou_hr"),
        source_type=SourceType.DM,
        external_id="ou_hr",
        name="HR Source",
    )
    upsert_watched_source(conn, source)

    exit_code = run(["sources", "validate", "--db", str(db_path)])

    out = capsys.readouterr().out
    assert exit_code == 0
    assert "overall: ok" in out


# ---------------------------------------------------------------------------
# reclassify
# ---------------------------------------------------------------------------


def _insert_test_message(conn: object, message_id: str, content: str) -> None:
    from lark_to_notes.intake.ledger import insert_raw_message
    from lark_to_notes.intake.models import RawMessage

    msg = RawMessage(
        message_id=message_id,
        source_id="dm:ou_reclassify",
        source_type="dm_user",
        chat_id="ou_chat",
        chat_type="p2p",
        sender_id="ou_sender",
        sender_name="Alice",
        direction="incoming",
        created_at="2026-05-01T10:00:00Z",
        content=content,
        payload={},
        ingested_at="2026-05-01T10:00:00Z",
    )
    insert_raw_message(conn, msg)  # type: ignore[arg-type]


def test_reclassify_inserts_tasks(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "state.db"
    conn = connect(db_path)
    init_db(conn)
    _insert_test_message(conn, "om_rcl_1", "Please review this document by Friday")
    conn.commit()

    exit_code = run(["reclassify", "--db", str(db_path), "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["messages_processed"] == 1
    assert payload["tasks_inserted"] == 1
    assert payload["tasks_skipped"] == 0
    assert payload["dry_run"] is False
    assert payload["budget_run_id"].startswith("reclassify:")


def test_reclassify_dry_run_does_not_write(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "state.db"
    conn = connect(db_path)
    init_db(conn)
    _insert_test_message(conn, "om_rcl_dr1", "Todo: write unit tests")
    conn.commit()

    exit_code = run(["reclassify", "--db", str(db_path), "--dry-run", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["dry_run"] is True
    assert payload["messages_processed"] == 1
    # A new fingerprint → would insert 1 task.
    assert payload["tasks_inserted"] == 1
    assert payload["tasks_skipped"] == 0
    # No tasks written — DB still empty.
    rows = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()
    assert rows[0] == 0


def test_reclassify_dry_run_skips_existing_fingerprint(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "state.db"
    conn = connect(db_path)
    init_db(conn)
    _insert_test_message(conn, "om_rcl_dr2", "Todo: write docs")
    conn.commit()

    # Real run first to insert the task.
    run(["reclassify", "--db", str(db_path)])
    capsys.readouterr()

    # Dry-run should see the existing fingerprint and count it as skipped.
    exit_code = run(["reclassify", "--db", str(db_path), "--dry-run", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["tasks_inserted"] == 0
    assert payload["tasks_skipped"] == 1


def test_reclassify_skips_existing_tasks(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "state.db"
    conn = connect(db_path)
    init_db(conn)
    _insert_test_message(conn, "om_rcl_2", "Please review this document")
    conn.commit()

    run(["reclassify", "--db", str(db_path)])
    capsys.readouterr()

    exit_code = run(["reclassify", "--db", str(db_path), "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["tasks_skipped"] == 1
    assert payload["tasks_inserted"] == 0


def test_reclassify_source_filter(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "state.db"
    conn = connect(db_path)
    init_db(conn)
    _insert_test_message(conn, "om_rcl_3", "Ping me when done")
    conn.commit()

    exit_code = run(["reclassify", "--db", str(db_path), "--source-id", "dm:other", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["messages_processed"] == 0


def test_reclassify_accepts_custom_high_pattern(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "state.db"
    conn = connect(db_path)
    init_db(conn)
    _insert_test_message(conn, "om_rcl_custom_1", "Someday we revisit this workflow")
    conn.commit()

    exit_code = run(
        [
            "reclassify",
            "--db",
            str(db_path),
            "--extra-high-pattern",
            r"(?i)\bsomeday\b::custom_someday",
            "--json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    row = conn.execute("SELECT task_class, confidence_band, reason_code FROM tasks").fetchone()
    assert exit_code == 0
    assert payload["tasks_inserted"] == 1
    assert row is not None
    assert row[0] == "task"
    assert row[1] == "high"
    assert row[2] == "custom_someday"


def test_reclassify_invalid_pattern_format_returns_error(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "state.db"
    conn = connect(db_path)
    init_db(conn)
    _insert_test_message(conn, "om_rcl_bad_pattern", "Please review this")
    conn.commit()

    exit_code = run(
        [
            "reclassify",
            "--db",
            str(db_path),
            "--extra-high-pattern",
            "not-a-valid-pattern",
            "--json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert "REGEX::SIGNAL" in payload["error"]


# ---------------------------------------------------------------------------
# render
# ---------------------------------------------------------------------------


def test_render_renders_open_tasks(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "state.db"
    vault_root = tmp_path / "vault"
    vault_root.mkdir()
    conn = connect(db_path)
    init_db(conn)
    upsert_task(
        conn,
        fingerprint="renderfp0001",
        title="Write quarterly report",
        task_class="task",
        confidence_band="high",
        reason_code="explicit_task_keyword",
        promotion_rec="current_tasks",
    )
    conn.commit()

    exit_code = run(["render", "--db", str(db_path), "--vault-root", str(vault_root), "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["tasks_found"] == 1
    assert payload["rendered"] == 1
    assert payload["errors"] == []


def test_render_no_tasks_exits_zero(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "state.db"
    vault_root = tmp_path / "vault"
    vault_root.mkdir()
    conn = connect(db_path)
    init_db(conn)

    exit_code = run(["render", "--db", str(db_path), "--vault-root", str(vault_root), "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["tasks_found"] == 0
    assert payload["rendered"] == 0


# ---------------------------------------------------------------------------
# reconcile
# ---------------------------------------------------------------------------


def test_reconcile_no_sources_exits_zero(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "state.db"
    conn = connect(db_path)
    init_db(conn)

    exit_code = run(["reconcile", "--db", str(db_path), "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["sources_checked"] == 0
    assert payload["gaps_found"] == 0


def test_reconcile_with_checkpoint_reports_no_gap(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "state.db"
    conn = connect(db_path)
    init_db(conn)
    # Insert a watched source and a checkpoint.
    source = WatchedSource(
        source_id=make_source_id(SourceType.DM, "ou_recon"),
        source_type=SourceType.DM,
        external_id="ou_recon",
        name="Recon Source",
    )
    upsert_watched_source(conn, source)
    conn.execute(
        "INSERT INTO checkpoints (source_id, last_message_id, last_message_timestamp)"
        " VALUES (?, ?, ?)",
        (source.source_id, "om_last_123", "2026-05-01T10:00:00Z"),
    )
    conn.commit()

    exit_code = run(["reconcile", "--db", str(db_path), "--json"])

    payload = json.loads(capsys.readouterr().out)
    # When source_state == checkpoint cursor, no gap.
    assert exit_code == 0
    assert payload["sources_checked"] >= 1


def test_reconcile_human_readable(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "state.db"
    conn = connect(db_path)
    init_db(conn)

    exit_code = run(["reconcile", "--db", str(db_path)])

    out = capsys.readouterr().out
    assert exit_code == 0
    assert "checked" in out


# ---------------------------------------------------------------------------
# budget status
# ---------------------------------------------------------------------------


def test_budget_status_no_records(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "state.db"
    conn = connect(db_path)
    init_db(conn)

    exit_code = run(["budget", "status", "--db", str(db_path), "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["status"] == "no_records"
    assert payload["quality_metrics"]["total_events"] == 0


def test_budget_status_with_records(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import datetime

    from lark_to_notes.budget.store import record_usage

    db_path = tmp_path / "state.db"
    conn = connect(db_path)
    init_db(conn)
    today = datetime.date.today().isoformat()
    record_usage(
        conn,
        {
            "call_id": "cli-budget-test-1",
            "provider": "copilot",
            "model": "gpt-4o",
            "run_id": "run-budget-cli-1",
            "source_id": "dm:test",
            "prompt_tokens": 100,
            "completion_tokens": 50,
            "duration_ms": 200,
            "cached": False,
            "fallback": False,
            "fallback_reason": "not_applicable",
            "created_at": f"{today}T09:00:00Z",
        },
    )
    conn.commit()

    exit_code = run(["budget", "status", "--db", str(db_path), "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["status"] == "ok"
    assert payload["call_count"] == 1
    assert payload["prompt_tokens"] == 100
    assert payload["completion_tokens"] == 50
    assert payload["cache_hit_rate"] == 0.0


def test_budget_status_run_scope(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import datetime

    from lark_to_notes.budget.store import record_usage

    db_path = tmp_path / "state.db"
    conn = connect(db_path)
    init_db(conn)
    today = datetime.date.today().isoformat()
    record_usage(
        conn,
        {
            "call_id": "cli-scope-test-1",
            "provider": "copilot",
            "model": "gpt-4o",
            "run_id": "run-scope-test",
            "source_id": "dm:test",
            "prompt_tokens": 20,
            "completion_tokens": 10,
            "duration_ms": 100,
            "cached": False,
            "fallback": False,
            "fallback_reason": "not_applicable",
            "created_at": f"{today}T09:00:00Z",
        },
    )
    conn.commit()

    exit_code = run(
        ["budget", "status", "--db", str(db_path), "--run-id", "run-scope-test", "--json"]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["status"] == "ok"
    assert payload["call_count"] == 1


def test_budget_status_includes_quality_metrics(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from lark_to_notes.tasks.registry import upsert_task

    db_path = tmp_path / "state.db"
    conn = connect(db_path)
    init_db(conn)
    conn.execute(
        """
        INSERT INTO raw_messages (
            message_id, source_id, source_type, chat_id, chat_type,
            sender_id, sender_name, direction, created_at, content, payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "cli-msg-task-1",
            "dm:cli",
            "dm_user",
            "chat-cli",
            "dm",
            "ou_cli",
            "CLI User",
            "inbound",
            "2026-04-16T08:00:00Z",
            "Please send the revised budget update.",
            "{}",
        ),
    )
    task_id, _ = upsert_task(
        conn,
        fingerprint="cliquality000001",
        title="Send the revised budget update",
        task_class="task",
        confidence_band="high",
        reason_code="en_please_verb",
        promotion_rec="current_tasks",
        created_from_raw_record_id="cli-msg-task-1",
    )
    conn.execute(
        """
        INSERT INTO feedback_events (
            feedback_id, target_type, target_id, action, payload_json, artifact_path
        ) VALUES (?, ?, ?, ?, ?, '')
        """,
        (
            "feedback-confirm-1",
            "task",
            task_id,
            "confirm",
            '{"action":"confirm","policy_version":"policy-v1","promotion_rec":"current_tasks"}',
        ),
    )
    conn.execute(
        """
        INSERT INTO feedback_events (
            feedback_id, target_type, target_id, action, payload_json, artifact_path
        ) VALUES (?, ?, ?, ?, ?, '')
        """,
        ("feedback-dismiss-1", "task", "task-2", "dismiss", '{"action":"dismiss"}'),
    )
    conn.commit()

    exit_code = run(["budget", "status", "--db", str(db_path), "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["quality_metrics"]["total_events"] == 2
    assert payload["quality_metrics"]["confirm_count"] == 1
    assert payload["quality_metrics"]["dismiss_count"] == 1
    assert payload["quality_metrics_scopes"]["rolling_7d"]["total_events"] == 2
    assert payload["quality_metrics_scopes"]["rolling_30d"]["total_events"] == 2
    assert payload["quality_metrics_scopes"]["by_artifact_path"]["unknown"]["total_events"] == 2
    assert payload["quality_metrics_scopes"]["by_day"]
    assert payload["quality_metrics_breakdown"]["by_target_type"]["task"]["total_events"] == 2
    assert payload["quality_metrics_breakdown"]["by_source_type"]["dm_user"]["confirm_count"] == 1
    assert (
        payload["quality_metrics_breakdown"]["by_policy_version"]["policy-v1"]["confirm_count"] == 1
    )
    assert (
        payload["quality_metrics_breakdown"]["by_promotion_rec"]["current_tasks"]["confirm_count"]
        == 1
    )


# ---------------------------------------------------------------------------
# Live worker integration
# ---------------------------------------------------------------------------


def test_sync_once_json_runs_worker_service(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeConfig:
        def __init__(self) -> None:
            self.vault_root = tmp_path
            self.state_db = tmp_path / "worker.db"
            self.poll_interval_seconds = 5
            self.enabled_sources: list[Any] = []

    class FakeService:
        def __init__(self, config: FakeConfig) -> None:
            self.config = config
            self.initialized = 0

        def initialize(self) -> None:
            self.initialized += 1

        def poll_once(self, *, sync_notes: bool) -> dict[str, int]:
            assert sync_notes is True
            return {"inserted_messages": 2, "distilled_items": 1}

    fake_config = FakeConfig()
    fake_service = FakeService(fake_config)
    monkeypatch.setattr(
        "lark_to_notes.cli._load_worker_service", lambda _path, _conn: (fake_config, fake_service)
    )

    exit_code = run(
        [
            "sync-once",
            "--db",
            str(tmp_path / "state.db"),
            "--config",
            str(tmp_path / "worker.json"),
            "--json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert fake_service.initialized == 1
    assert payload["inserted_messages"] == 2
    assert payload["distilled_items"] == 1
    assert payload["sync_notes"] is True
    assert payload["runtime"]["run_count_total"] == 1
    assert payload["canonical_db_path"] == str((tmp_path / "state.db").resolve())
    assert payload["runtime_lock_path"] == str(
        (tmp_path / "var" / "lark-to-notes.runtime.lock").resolve()
    )
    assert payload["config_state_db"] == str((tmp_path / "worker.db").resolve())


def test_sync_once_in_repo_adapter_without_external_worker(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Live sync uses ``ChatLiveAdapter`` + JSON config (no ``automation.lark_worker``)."""

    from lark_to_notes.live.chat_live import ChatLiveAdapter

    vault = tmp_path / "vault"
    vault.mkdir()
    cfg = {
        "vault_root": str(vault),
        "state_db": str(tmp_path / "legacy-worker.db"),
        "poll_interval_seconds": 300,
        "poll_lookback_days": 7,
        "sources": [],
    }
    cfg_path = tmp_path / "live.json"
    cfg_path.write_text(json.dumps(cfg), encoding="utf-8")
    db_path = tmp_path / "runtime.db"

    calls: list[bool] = []

    def fake_poll(self: ChatLiveAdapter, *, sync_notes: bool) -> dict[str, int]:
        _ = self
        calls.append(sync_notes)
        return {"inserted_messages": 0, "distilled_items": 0, "sources_scanned": 0}

    monkeypatch.setattr(ChatLiveAdapter, "poll_once", fake_poll)

    exit_code = run(["sync-once", "--db", str(db_path), "--config", str(cfg_path), "--json"])
    assert exit_code == 0
    assert calls == [True]
    payload = json.loads(capsys.readouterr().out)
    assert payload["inserted_messages"] == 0
    assert payload["distilled_items"] == 0


def test_sync_events_stdin_ingests_receive_v1(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    envelope = {
        "header": {"event_type": "im.message.receive_v1"},
        "event": {
            "message": {
                "message_id": "om_cli_evt",
                "chat_id": "ou_chat",
                "create_time": "1713096000000",
                "body": {"content": json.dumps({"text": "stdin event"})},
                "sender": {"id": "ou_sender", "name": "Alice"},
            }
        },
    }
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(envelope) + "\n"))
    db_path = tmp_path / "evt.db"
    exit_code = run(["sync-events", "--db", str(db_path), "--source-id", "dm:ou_demo", "--json"])
    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["json_objects"] == 1
    assert payload["envelopes_ingested"] == 1
    assert payload["reaction_rows_inserted"] == 0
    assert payload["chat_receive_observation_exceptions"] == 0
    assert payload["reaction_validation_rejects"] == 0
    assert payload["reaction_insert_exceptions"] == 0
    assert payload["reaction_parse_none_after_validate"] == 0
    assert payload["reaction_benign_duplicate_replays"] == 0
    assert payload["reaction_cap_deferred"] == 0
    assert payload["last_reaction_cap_reason_code"] is None
    assert payload["reaction_intake_run_id"] is None
    assert payload["last_reaction_quarantine_payload_hash"] is None
    assert payload["chat_intake_drained"] == 0
    assert payload["drain_skipped"] is False
    assert "runtime" in payload
    assert payload["drain_batch"] is not None
    assert payload["drain_batch"]["items_total"] == 0


def test_sync_events_coalesce_zero_drains_ready_rows(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    envelope = {
        "header": {"event_type": "im.message.receive_v1"},
        "event": {
            "message": {
                "message_id": "om_cli_evt_drain",
                "chat_id": "ou_chat",
                "create_time": "1713096000000",
                "body": {"content": json.dumps({"text": "stdin event drain"})},
                "sender": {"id": "ou_sender", "name": "Alice"},
            }
        },
    }
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(envelope) + "\n"))
    db_path = tmp_path / "evt.db"
    exit_code = run(
        [
            "sync-events",
            "--db",
            str(db_path),
            "--source-id",
            "dm:ou_demo",
            "--coalesce-window-seconds",
            "0",
            "--json",
        ]
    )
    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["json_objects"] == 1
    assert payload["envelopes_ingested"] == 1
    assert payload["reaction_rows_inserted"] == 0
    assert payload["reaction_validation_rejects"] == 0
    assert payload["reaction_cap_deferred"] == 0
    assert payload["last_reaction_cap_reason_code"] is None
    assert payload["reaction_intake_run_id"] is None
    assert payload["chat_intake_drained"] == 1
    assert payload["drain_skipped"] is False
    assert payload["drain_batch"] is not None
    assert payload["drain_batch"]["items_processed"] == 1
    assert payload["drain_batch"]["items_failed"] == 0
    conn = connect(db_path)
    assert count_raw_messages(conn) == 1


def test_sync_events_reaction_cap_defers_and_sets_run_id(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lines = []
    for i in range(3):
        lines.append(
            json.dumps(
                {
                    "header": {
                        "event_type": "im.message.reaction.created_v1",
                        "event_id": f"rx-cli-cap-{i}",
                    },
                    "event": {
                        "message_id": f"om_cli_cap_{i}",
                        "reaction_type": {"emoji_type": "THUMBSUP"},
                        "operator_type": "user",
                        "user_id": {"open_id": "ou_x"},
                        "action_time": "1",
                    },
                }
            )
        )
    monkeypatch.setattr(sys, "stdin", io.StringIO("\n".join(lines) + "\n"))
    db_path = tmp_path / "evt_cap.db"
    exit_code = run(
        [
            "sync-events",
            "--db",
            str(db_path),
            "--source-id",
            "dm:ou_demo",
            "--max-reactions-per-run",
            "2",
            "--json",
        ]
    )
    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["json_objects"] == 3
    assert payload["reaction_rows_inserted"] == 2
    assert payload["reaction_benign_duplicate_replays"] == 0
    assert payload["reaction_cap_deferred"] == 1
    assert payload["last_reaction_cap_reason_code"] == "reaction_cap_per_run_exceeded"
    assert payload["reaction_intake_run_id"] is not None


def test_backfill_json_runs_worker_service(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeConfig:
        def __init__(self) -> None:
            self.vault_root = tmp_path
            self.state_db = tmp_path / "worker.db"
            self.poll_interval_seconds = 5
            self.enabled_sources: list[Any] = []

    class FakeService:
        def __init__(self, config: FakeConfig) -> None:
            self.config = config
            self.initialized = 0

        def initialize(self) -> None:
            self.initialized += 1

        def backfill_history(
            self,
            *,
            lookback_days: int | None,
            source_ids: set[str] | None,
            sync_notes: bool,
        ) -> dict[str, int]:
            assert lookback_days == 14
            assert source_ids == {"dm:one"}
            assert sync_notes is True
            return {"sources_scanned": 1, "inserted_messages": 4, "distilled_items": 3}

    fake_config = FakeConfig()
    fake_service = FakeService(fake_config)
    monkeypatch.setattr(
        "lark_to_notes.cli._load_worker_service", lambda _path, _conn: (fake_config, fake_service)
    )

    exit_code = run(
        [
            "backfill",
            "--db",
            str(tmp_path / "state.db"),
            "--config",
            str(tmp_path / "worker.json"),
            "--days",
            "14",
            "--source-id",
            "dm:one",
            "--sync-notes",
            "--json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert fake_service.initialized == 1
    assert payload["sources_scanned"] == 1
    assert payload["inserted_messages"] == 4
    assert payload["distilled_items"] == 3
    assert payload["sync_notes"] is True
    assert payload["runtime"]["run_count_total"] == 1


def test_sync_daemon_json_runs_multiple_cycles(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeConfig:
        def __init__(self) -> None:
            self.vault_root = tmp_path
            self.state_db = tmp_path / "worker.db"
            self.poll_interval_seconds = 5
            self.enabled_sources: list[Any] = []

    class FakeService:
        def __init__(self, config: FakeConfig) -> None:
            self.config = config
            self.initialized = 0
            self.calls = 0

        def initialize(self) -> None:
            self.initialized += 1

        def poll_once(self, *, sync_notes: bool) -> dict[str, int]:
            assert sync_notes is True
            self.calls += 1
            if self.calls == 1:
                return {"inserted_messages": 1, "distilled_items": 1}
            return {"inserted_messages": 0, "distilled_items": 0}

    fake_config = FakeConfig()
    fake_service = FakeService(fake_config)
    monkeypatch.setattr(
        "lark_to_notes.cli._load_worker_service", lambda _path, _conn: (fake_config, fake_service)
    )

    exit_code = run(
        [
            "sync-daemon",
            "--db",
            str(tmp_path / "state.db"),
            "--config",
            str(tmp_path / "worker.json"),
            "--max-cycles",
            "2",
            "--poll-interval",
            "0",
            "--json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert fake_service.initialized == 2
    assert payload["cycle_count"] == 2
    assert payload["idle_cycles"] == 1
    assert payload["inserted_messages"] == 1
    assert payload["distilled_items"] == 1
    assert len(payload["run_ids"]) == 2
    assert payload["runtime"]["run_count_total"] == 2


def test_sync_once_auth_error_prints_recovery_guidance(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "lark_to_notes.cli._load_worker_service",
        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("auth expired")),
    )

    exit_code = run(
        [
            "sync-once",
            "--db",
            str(tmp_path / "state.db"),
            "--config",
            str(tmp_path / "worker.json"),
        ]
    )

    out = capsys.readouterr().out
    assert exit_code == 1
    assert "lark-cli auth login --as user" in out


def test_sync_once_auth_error_json_includes_stage_and_kind(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "lark_to_notes.cli._load_worker_service",
        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("auth expired")),
    )

    exit_code = run(
        [
            "sync-once",
            "--db",
            str(tmp_path / "state.db"),
            "--config",
            str(tmp_path / "worker.json"),
            "--json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert payload["error_kind"] == "auth_scope"
    assert payload["auth_related"] is True
    assert payload["stage"] == "load_worker_service"
    assert payload["next_actions"]


def test_sync_once_non_auth_error_json_classifies_runtime_failure(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "lark_to_notes.cli._load_worker_service",
        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("network timeout")),
    )

    exit_code = run(
        [
            "sync-once",
            "--db",
            str(tmp_path / "state.db"),
            "--config",
            str(tmp_path / "worker.json"),
            "--json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert payload["error_kind"] == "runtime_failure"
    assert payload["auth_related"] is False


def test_reconcile_live_mode_repairs_and_verifies(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from lark_to_notes.runtime.reconcile import SourceState

    db_path = tmp_path / "state.db"
    conn = connect(db_path)
    init_db(conn)
    source = WatchedSource(
        source_id=make_source_id(SourceType.DM, "ou_live_recon"),
        source_type=SourceType.DM,
        external_id="ou_live_recon",
        name="Live Recon",
    )
    upsert_watched_source(conn, source)
    conn.execute(
        "INSERT INTO checkpoints (source_id, last_message_id, last_message_timestamp)"
        " VALUES (?, ?, ?)",
        (source.source_id, "om_old", "2026-05-01T10:00:00Z"),
    )
    conn.commit()

    class FakeConfig:
        def __init__(self) -> None:
            self.vault_root = tmp_path
            self.state_db = tmp_path / "worker.db"
            self.poll_interval_seconds = 5
            self.enabled_sources: list[Any] = []

    class FakeService:
        def __init__(self, config: FakeConfig, db: Any, source_id: str) -> None:
            self.config = config
            self._db = db
            self._source_id = source_id
            self.poll_calls = 0

        def poll_once(self, *, sync_notes: bool) -> dict[str, int]:
            assert sync_notes is True
            self.poll_calls += 1
            self._db.execute(
                """
                UPDATE checkpoints
                SET last_message_id = ?, last_message_timestamp = ?
                WHERE source_id = ?
                """,
                ("om_new", "2026-05-01T11:00:00Z", self._source_id),
            )
            self._db.commit()
            return {"inserted_messages": 1, "distilled_items": 1}

    fake_config = FakeConfig()
    fake_service = FakeService(fake_config, conn, source.source_id)
    monkeypatch.setattr(
        "lark_to_notes.cli._load_worker_service", lambda _path, _conn: (fake_config, fake_service)
    )

    monkeypatch.setattr(
        "lark_to_notes.cli._collect_live_source_states",
        lambda _service, _conn: {
            source.source_id: SourceState(
                source_id=source.source_id,
                latest_message_id="om_new",
                latest_message_timestamp="2026-05-01T11:00:00Z",
            )
        },
    )

    exit_code = run(
        [
            "reconcile",
            "--db",
            str(db_path),
            "--config",
            str(tmp_path / "worker.json"),
            "--json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert fake_service.poll_calls == 1
    assert payload["mode"] == "live"
    assert payload["repairs_attempted"] == 1
    assert payload["repairs_succeeded"] == 1
    assert payload["gaps_found"] == 0


# ---------------------------------------------------------------------------
# lw-tst.6: CLI error-path and coverage gap tests
# ---------------------------------------------------------------------------


def test_feedback_import_bad_yaml_returns_rc1(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Bad YAML syntax should cause feedback import to return rc=1, not raise."""
    db_path = tmp_path / "state.db"
    artifact_path = tmp_path / "bad.yaml"
    artifact_path.write_text(": broken: yaml: [unclosed", encoding="utf-8")

    exit_code = run(["feedback", "import", str(artifact_path), "--db", str(db_path), "--json"])

    out = capsys.readouterr().out
    payload = json.loads(out)
    assert exit_code == 1
    assert "error" in payload
    assert payload["artifact_path"] == str(artifact_path)


def test_feedback_import_bad_yaml_human_readable_returns_rc1(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Bad YAML in human-readable mode prints 'error:' line and returns rc=1."""
    db_path = tmp_path / "state.db"
    artifact_path = tmp_path / "bad.yaml"
    artifact_path.write_text(": broken: yaml: [unclosed", encoding="utf-8")

    exit_code = run(["feedback", "import", str(artifact_path), "--db", str(db_path)])

    out = capsys.readouterr().out
    assert exit_code == 1
    assert "error" in out.lower()


def test_feedback_import_invalid_version_returns_rc1(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Artifact with unsupported version field returns rc=1 (ValueError path)."""
    db_path = tmp_path / "state.db"
    artifact_path = tmp_path / "ver99.yaml"
    artifact_path.write_text("version: 99\ntasks: {}\nsource_items: {}\n", encoding="utf-8")

    exit_code = run(["feedback", "import", str(artifact_path), "--db", str(db_path), "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert "error" in payload


def test_feedback_import_unknown_task_id_applies_zero_tasks(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Valid artifact referencing a non-existent task_id returns rc=1 (LookupError)."""
    db_path = tmp_path / "state.db"
    conn = connect(db_path)
    init_db(conn)
    conn.commit()

    artifact_path = tmp_path / "feedback.yaml"
    artifact_path.write_text(
        render_feedback_artifact(
            FeedbackArtifact(
                tasks={
                    "nonexistent-task-id-00001": FeedbackDirective(
                        action=FeedbackAction.CONFIRM,
                    ),
                },
            )
        ),
        encoding="utf-8",
    )

    exit_code = run(["feedback", "import", str(artifact_path), "--db", str(db_path), "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert "error" in payload
    assert "nonexistent-task-id-00001" in payload["error"]


def test_render_unwritable_vault_exits_nonzero(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """render returns rc=1 when vault root is not writable."""
    import os

    db_path = tmp_path / "state.db"
    vault_root = tmp_path / "vault"
    vault_root.mkdir()
    conn = connect(db_path)
    init_db(conn)
    upsert_task(
        conn,
        fingerprint="render-err-fp0001",
        title="Deliverable by end of week",
        task_class="task",
        confidence_band="high",
        reason_code="explicit_task_keyword",
        promotion_rec="current_tasks",
    )
    conn.commit()

    os.chmod(vault_root, 0o555)
    try:
        exit_code = run(["render", "--db", str(db_path), "--vault-root", str(vault_root), "--json"])
        payload = json.loads(capsys.readouterr().out)
    finally:
        os.chmod(vault_root, 0o755)

    assert exit_code == 1
    assert payload["errors"] != []
    assert payload["rendered"] == 0


def test_render_human_readable_includes_counts(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Human-readable render output mentions task counts."""
    db_path = tmp_path / "state.db"
    vault_root = tmp_path / "vault"
    vault_root.mkdir()
    conn = connect(db_path)
    init_db(conn)
    upsert_task(
        conn,
        fingerprint="render-hr-fp0001",
        title="Schedule kick-off meeting",
        task_class="task",
        confidence_band="high",
        reason_code="explicit_task_keyword",
        promotion_rec="current_tasks",
    )
    conn.commit()

    exit_code = run(["render", "--db", str(db_path), "--vault-root", str(vault_root)])

    out = capsys.readouterr().out
    assert exit_code == 0
    # Human-readable output should mention vault_root and task counts.
    assert str(vault_root) in out
    assert "1" in out  # rendered count


def test_reconcile_multi_source_checks_all(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """reconcile reports sources_checked == number of watched sources with checkpoints."""
    db_path = tmp_path / "state.db"
    conn = connect(db_path)
    init_db(conn)

    for i in range(2):
        src = WatchedSource(
            source_id=make_source_id(SourceType.DM, f"ou_multi_{i}"),
            source_type=SourceType.DM,
            external_id=f"ou_multi_{i}",
            name=f"Multi Source {i}",
        )
        upsert_watched_source(conn, src)
        conn.execute(
            "INSERT INTO checkpoints (source_id, last_message_id, last_message_timestamp)"
            " VALUES (?, ?, ?)",
            (src.source_id, f"om_multi_{i}", "2026-05-01T10:00:00Z"),
        )
    conn.commit()

    exit_code = run(["reconcile", "--db", str(db_path), "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["sources_checked"] == 2


def test_sources_list_human_readable_shows_source_name(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """sources list without --json prints the source name in plain text."""
    db_path = tmp_path / "state.db"
    conn = connect(db_path)
    init_db(conn)
    source = WatchedSource(
        source_id=make_source_id(SourceType.DM, "ou_hr_list"),
        source_type=SourceType.DM,
        external_id="ou_hr_list",
        name="Human Readable Source",
    )
    upsert_watched_source(conn, source)
    conn.commit()

    exit_code = run(["sources", "list", "--db", str(db_path)])

    out = capsys.readouterr().out
    assert exit_code == 0
    assert "Human Readable Source" in out


def test_doctor_json_has_all_expected_keys(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """doctor --json output contains all top-level schema keys."""
    db_path = tmp_path / "state.db"

    exit_code = run(
        [
            "doctor",
            "--fixture-corpus",
            str(FIXTURE_CORPUS_ROOT),
            "--db",
            str(db_path),
            "--json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    for key in (
        "status",
        "schema_version",
        "db_path",
        "fixture_corpus",
        "replay",
        "runtime",
        "runtime_diagnostics",
        "chat_intake_ledger",
        "message_reaction_events",
        "supervised_live",
    ):
        assert key in payload, f"missing key: {key!r}"
    for key in (
        "file_count",
        "total_records",
        "inserted_records",
        "db_record_count",
        "matches_manifest",
    ):
        assert key in payload["replay"], f"missing replay key: {key!r}"
    for key in ("record_count", "scenario_count", "missing_scenarios", "source_access_surfaces"):
        assert key in payload["fixture_corpus"], f"missing fixture_corpus key: {key!r}"
    for key in ("recent_failed_runs", "recent_dead_letters"):
        assert key in payload["runtime_diagnostics"], f"missing runtime_diagnostics key: {key!r}"
    for key in ("pending_ready", "pending_coalescing", "processed"):
        assert key in payload["chat_intake_ledger"], f"missing chat_intake_ledger key: {key!r}"
    mrx = payload["message_reaction_events"]
    assert mrx["row_count"] == 0
    assert mrx["orphan_row_count"] == 0
    assert mrx["linked_row_count"] == 0
    assert "orphan_backlog" in mrx
    assert mrx["orphan_backlog"]["queue_depth"] == 0
    assert "attach_reconcile_latency_ms" in mrx
    assert mrx["attach_reconcile_latency_ms"]["attach_reconcile_sample_count"] == 0
    ob = mrx["orphan_backlog"]
    for key in (
        "queue_depth",
        "timestamp_parse_skips",
        "oldest_first_queued_at",
        "oldest_age_seconds",
        "age_bucket_counts",
        "dwell_seconds_p50",
        "dwell_seconds_p90",
    ):
        assert key in ob, f"missing orphan_backlog key: {key!r}"
    assert ob["queue_depth"] == 0
    ar = mrx["attach_reconcile_latency_ms"]
    for key in ("attach_reconcile_sample_count", "attach_reconcile_ms_p50", "attach_reconcile_ms_p90"):
        assert key in ar, f"missing attach_reconcile_latency_ms key: {key!r}"
    assert ar["attach_reconcile_sample_count"] == 0


def test_doctor_human_readable_output(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """doctor without --json prints status, schema_version, and runtime summary."""
    db_path = tmp_path / "state.db"

    exit_code = run(
        [
            "doctor",
            "--fixture-corpus",
            str(FIXTURE_CORPUS_ROOT),
            "--db",
            str(db_path),
        ]
    )

    out = capsys.readouterr().out
    assert exit_code == 0
    assert "status:" in out
    assert "schema_version:" in out
    assert "runtime:" in out
    assert "chat_intake:" in out
    assert "reaction_events:" in out
    assert "reaction_orphan_queue:" in out
    assert "supervised_live:" in out
