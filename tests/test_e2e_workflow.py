"""End-to-end CLI workflow tests."""

from __future__ import annotations

import json
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
from lark_to_notes.storage.db import connect, init_db, upsert_watched_source
from lark_to_notes.tasks import derive_fingerprint
from lark_to_notes.tasks.registry import get_task_by_fingerprint

FIXTURE_CORPUS_ROOT = (
    Path(__file__).resolve().parent / "fixtures" / "lark-worker" / "fixture-corpus"
)
REVIEW_CONTENT = "context context context " * 40
REVIEW_CREATED_AT = "2026-05-01T10:03:00Z"


def _run_json(
    capsys: pytest.CaptureFixture[str],
    argv: list[str],
) -> tuple[int, dict[str, Any]]:
    exit_code = run(argv)
    payload = json.loads(capsys.readouterr().out)
    return exit_code, payload


def _message_record(
    *,
    message_id: str,
    source_id: str,
    created_at: str,
    content: str,
) -> dict[str, Any]:
    return {
        "message_id": message_id,
        "source_id": source_id,
        "source_type": "dm_user",
        "chat_id": "ou_chat_e2e",
        "chat_type": "p2p",
        "sender_id": "ou_sender_e2e",
        "sender_name": "Alice",
        "direction": "incoming",
        "created_at": created_at,
        "content": content,
        "payload": {"content": content},
    }


def _write_workflow_raw_log(raw_dir: Path, source_id: str) -> list[dict[str, Any]]:
    records = [
        _message_record(
            message_id="om_e2e_1",
            source_id=source_id,
            created_at="2026-05-01T10:00:00Z",
            content="Please review the launch checklist by Friday",
        ),
        _message_record(
            message_id="om_e2e_2",
            source_id=source_id,
            created_at="2026-05-01T10:01:00Z",
            content="Let's follow up with ops tomorrow morning",
        ),
        _message_record(
            message_id="om_e2e_3",
            source_id=source_id,
            created_at="2026-05-01T10:02:00Z",
            content="FYI: deployment finished successfully.",
        ),
        _message_record(
            message_id="om_e2e_4",
            source_id=source_id,
            created_at=REVIEW_CREATED_AT,
            content=REVIEW_CONTENT,
        ),
        _message_record(
            message_id="om_e2e_5",
            source_id=source_id,
            created_at="2026-05-01T10:04:00Z",
            content="TODO: update the changelog and notify Alice",
        ),
    ]
    raw_path = raw_dir / "2026-05-01.jsonl"
    raw_path.write_text(
        "\n".join(json.dumps(record, ensure_ascii=False) for record in records) + "\n",
        encoding="utf-8",
    )
    return records


def _setup_workflow_environment(
    tmp_path: Path,
) -> tuple[Path, Path, Path, WatchedSource, list[dict[str, Any]]]:
    db_path = tmp_path / "state.db"
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    vault_root = tmp_path / "vault"
    vault_root.mkdir()

    conn = connect(db_path)
    init_db(conn)
    source = WatchedSource(
        source_id=make_source_id(SourceType.DM, "ou_e2e"),
        source_type=SourceType.DM,
        external_id="ou_e2e",
        name="E2E Demo DM",
    )
    upsert_watched_source(conn, source)
    conn.execute(
        """
        INSERT INTO checkpoints (
            source_id, last_message_id, last_message_timestamp, page_token, updated_at
        ) VALUES (?, ?, ?, '', ?)
        """,
        (
            source.source_id,
            "om_e2e_5",
            "2026-05-01T10:04:00Z",
            "2026-05-01T10:04:00Z",
        ),
    )
    conn.commit()
    records = _write_workflow_raw_log(raw_dir, source.source_id)
    return db_path, raw_dir, vault_root, source, records


def _make_feedback_artifact(
    tmp_path: Path,
    *,
    task_id: str,
) -> Path:
    artifact_path = tmp_path / "feedback.yaml"
    artifact_path.write_text(
        render_feedback_artifact(
            FeedbackArtifact(
                tasks={
                    task_id: FeedbackDirective(
                        action=FeedbackAction.WRONG_CLASS,
                        task_class="task",
                    ),
                }
            )
        ),
        encoding="utf-8",
    )
    return artifact_path


def _execute_full_workflow(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    db_path, raw_dir, vault_root, source, records = _setup_workflow_environment(tmp_path)
    payloads: dict[str, dict[str, Any]] = {}

    commands = [
        ("sources_validate", ["sources", "validate", "--db", str(db_path), "--json"]),
        ("sources_list", ["sources", "list", "--db", str(db_path), "--json"]),
        ("replay", ["replay", "--db", str(db_path), "--raw-dir", str(raw_dir), "--json"]),
        ("reclassify", ["reclassify", "--db", str(db_path), "--json"]),
        (
            "render_before_feedback",
            ["render", "--db", str(db_path), "--vault-root", str(vault_root), "--json"],
        ),
        ("reconcile", ["reconcile", "--db", str(db_path), "--json"]),
        (
            "doctor",
            [
                "doctor",
                "--db",
                str(db_path),
                "--fixture-corpus",
                str(FIXTURE_CORPUS_ROOT),
                "--json",
            ],
        ),
    ]
    for name, argv in commands:
        exit_code, payload = _run_json(capsys, argv)
        assert exit_code == 0, name
        payloads[name] = payload

    reclassify_payload = payloads["reclassify"]
    budget_run_id = str(reclassify_payload["budget_run_id"])
    exit_code, payloads["budget_status"] = _run_json(
        capsys,
        ["budget", "status", "--db", str(db_path), "--run-id", budget_run_id, "--json"],
    )
    assert exit_code == 0

    conn = connect(db_path)
    review_fingerprint = derive_fingerprint(REVIEW_CONTENT, source.source_id, REVIEW_CREATED_AT)
    review_task = get_task_by_fingerprint(conn, review_fingerprint)
    assert review_task is not None
    artifact_path = _make_feedback_artifact(tmp_path, task_id=review_task.task_id)

    exit_code, payloads["feedback_import"] = _run_json(
        capsys,
        ["feedback", "import", str(artifact_path), "--db", str(db_path), "--json"],
    )
    assert exit_code == 0
    exit_code, payloads["render_after_feedback"] = _run_json(
        capsys,
        ["render", "--db", str(db_path), "--vault-root", str(vault_root), "--json"],
    )
    assert exit_code == 0

    return {
        "db_path": db_path,
        "raw_dir": raw_dir,
        "vault_root": vault_root,
        "source": source,
        "records": records,
        "review_task_id": review_task.task_id,
        "review_task_fingerprint": review_task.fingerprint,
        "review_task_title": review_task.title,
    }, payloads


def test_full_cli_workflow_all_steps_succeed(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    context, payloads = _execute_full_workflow(tmp_path, capsys)

    assert payloads["sources_validate"]["overall"] == "ok"
    assert payloads["sources_list"]["count"] == 1
    assert payloads["replay"]["inserted_records"] == len(context["records"])
    assert payloads["reclassify"]["messages_processed"] == len(context["records"])
    assert payloads["reclassify"]["tasks_inserted"] == len(context["records"])
    assert payloads["render_before_feedback"]["tasks_found"] == len(context["records"]) - 1
    assert (
        payloads["render_before_feedback"]["rendered"]
        == payloads["render_before_feedback"]["tasks_found"]
    )
    assert payloads["render_before_feedback"]["errors"] == []
    assert payloads["reconcile"]["sources_checked"] == 1
    assert payloads["reconcile"]["gaps_found"] == 0
    assert payloads["doctor"]["status"] == "ok"
    assert payloads["budget_status"]["status"] == "ok"
    assert payloads["budget_status"]["call_count"] >= 1
    assert payloads["feedback_import"]["applied_task_count"] == 1
    assert payloads["render_after_feedback"]["rendered"] == len(context["records"])
    assert payloads["render_after_feedback"]["errors"] == []


def test_cli_workflow_json_schema_completeness(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _context, payloads = _execute_full_workflow(tmp_path, capsys)

    expected_keys = {
        "sources_validate": {"db_path", "overall", "count", "sources"},
        "sources_list": {"db_path", "count", "sources"},
        "replay": {
            "db_path",
            "raw_dir",
            "glob",
            "file_count",
            "total_records",
            "inserted_records",
            "files",
        },
        "reclassify": {
            "db_path",
            "dry_run",
            "messages_processed",
            "tasks_inserted",
            "tasks_skipped",
            "budget_run_id",
        },
        "render_before_feedback": {
            "db_path",
            "vault_root",
            "status_filter",
            "tasks_found",
            "rendered",
            "errors",
        },
        "reconcile": {
            "db_path",
            "mode",
            "worker_config",
            "runtime_run_id",
            "sources_checked",
            "gaps_found",
            "repairs_attempted",
            "repairs_succeeded",
            "repair_sync_triggered",
            "gap_details",
            "initial_gap_details",
        },
        "doctor": {
            "status",
            "schema_version",
            "migrations",
            "db_path",
            "fixture_corpus",
            "replay",
            "runtime",
        },
        "budget_status": {
            "db_path",
            "scope",
            "cache_hit_rate",
            "quality_metrics",
            "status",
            "call_count",
        },
        "feedback_import": {
            "db_path",
            "artifact_path",
            "applied_task_count",
            "feedback_event_count",
            "applied_task_ids",
            "feedback_event_ids",
        },
        "render_after_feedback": {
            "db_path",
            "vault_root",
            "status_filter",
            "tasks_found",
            "rendered",
            "errors",
        },
    }

    for command_name, required_keys in expected_keys.items():
        assert required_keys.issubset(payloads[command_name].keys()), command_name


def test_cli_workflow_idempotent_replay_reclassify(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path, raw_dir, _vault_root, _source, records = _setup_workflow_environment(tmp_path)

    first_replay_exit, first_replay = _run_json(
        capsys,
        ["replay", "--db", str(db_path), "--raw-dir", str(raw_dir), "--json"],
    )
    first_reclassify_exit, first_reclassify = _run_json(
        capsys,
        ["reclassify", "--db", str(db_path), "--json"],
    )
    second_replay_exit, second_replay = _run_json(
        capsys,
        ["replay", "--db", str(db_path), "--raw-dir", str(raw_dir), "--json"],
    )
    second_reclassify_exit, second_reclassify = _run_json(
        capsys,
        ["reclassify", "--db", str(db_path), "--json"],
    )

    assert first_replay_exit == 0
    assert first_reclassify_exit == 0
    assert second_replay_exit == 0
    assert second_reclassify_exit == 0
    assert first_replay["inserted_records"] == len(records)
    assert first_reclassify["tasks_inserted"] == len(records)
    assert second_replay["inserted_records"] == 0
    assert second_reclassify["tasks_inserted"] == 0
    assert second_reclassify["tasks_skipped"] == len(records)


def test_cli_workflow_render_after_feedback_reflects_override(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    context, payloads = _execute_full_workflow(tmp_path, capsys)

    review_task_id = str(context["review_task_id"])
    review_task_fingerprint = str(context["review_task_fingerprint"])
    review_task_title = str(context["review_task_title"])
    vault_root = Path(context["vault_root"])
    raw_note = next(
        path
        for path in (vault_root / "raw").glob("*.md")
        if review_task_id in path.read_text(encoding="utf-8")
    )
    raw_text = raw_note.read_text(encoding="utf-8")
    current_tasks_path = vault_root / "area" / "current tasks" / "index.md"
    assert current_tasks_path.exists()
    current_tasks_text = current_tasks_path.read_text(encoding="utf-8")

    assert payloads["feedback_import"]["applied_task_ids"] == [review_task_id]
    assert "| promotion | current_tasks |" in raw_text
    assert review_task_id in raw_text
    assert review_task_fingerprint in current_tasks_text
    assert review_task_title in current_tasks_text
