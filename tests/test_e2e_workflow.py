"""End-to-end CLI workflow tests."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from lark_to_notes.cli import run
from lark_to_notes.config.sources import SourceType, WatchedSource, make_source_id
from lark_to_notes.feedback import (
    FeedbackAction,
    FeedbackArtifact,
    FeedbackDirective,
    render_feedback_artifact,
)
from lark_to_notes.feedback.draft import DRAFT_ACTION_PLACEHOLDER
from lark_to_notes.storage.db import connect, init_db, upsert_watched_source
from lark_to_notes.tasks import derive_fingerprint
from lark_to_notes.tasks.registry import get_task, get_task_by_fingerprint

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
    review_fingerprint = derive_fingerprint(
        REVIEW_CONTENT,
        source.source_id,
        REVIEW_CREATED_AT,
        source_type="dm_user",
    )
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
            "runtime_diagnostics",
            "chat_intake_ledger",
            "supervised_live",
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


def test_live_like_same_body_two_source_types_yield_distinct_tasks(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Cross-surface fingerprinting must not merge chat vs DM bodies that normalize the same."""
    db_path = tmp_path / "state.db"
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    conn = connect(db_path)
    init_db(conn)
    source = WatchedSource(
        source_id=make_source_id(SourceType.DM, "ou_split"),
        source_type=SourceType.DM,
        external_id="ou_split",
        name="Split fingerprint demo",
    )
    upsert_watched_source(conn, source)
    conn.commit()

    shared_body = (
        "unique shared collision body zeta eta theta iota kappa lambda mu nu xi omicron pi rho"
    )
    records = [
        _message_record(
            message_id="om_split_dm",
            source_id=source.source_id,
            created_at="2026-05-02T11:00:00Z",
            content=shared_body,
        )
        | {"source_type": "dm_user"},
        _message_record(
            message_id="om_split_grp",
            source_id=source.source_id,
            created_at="2026-05-02T11:00:05Z",
            content=shared_body,
        )
        | {"source_type": "group_user"},
    ]
    (raw_dir / "2026-05-02.jsonl").write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in records) + "\n",
        encoding="utf-8",
    )

    fp_dm = derive_fingerprint(
        shared_body,
        source.source_id,
        "2026-05-02T11:00:00Z",
        source_type="dm_user",
    )
    fp_grp = derive_fingerprint(
        shared_body,
        source.source_id,
        "2026-05-02T11:00:05Z",
        source_type="group_user",
    )
    assert fp_dm != fp_grp

    assert (
        _run_json(capsys, ["replay", "--db", str(db_path), "--raw-dir", str(raw_dir), "--json"])[0]
        == 0
    )
    assert _run_json(capsys, ["reclassify", "--db", str(db_path), "--json"])[0] == 0

    conn = connect(db_path)
    t_dm = get_task_by_fingerprint(conn, fp_dm)
    t_grp = get_task_by_fingerprint(conn, fp_grp)
    assert t_dm is not None and t_grp is not None
    assert t_dm.task_id != t_grp.task_id


def test_feedback_override_stable_after_idempotent_replay_reclassify(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Operator feedback must survive the same replay+reclassify path live sync repeats."""
    context, _payloads = _execute_full_workflow(tmp_path, capsys)
    db_path = Path(context["db_path"])
    raw_dir = Path(context["raw_dir"])
    review_task_id = str(context["review_task_id"])

    conn = connect(db_path)
    before = get_task(conn, review_task_id)
    assert before is not None
    assert before.manual_override_state is not None

    assert (
        _run_json(capsys, ["replay", "--db", str(db_path), "--raw-dir", str(raw_dir), "--json"])[0]
        == 0
    )
    assert _run_json(capsys, ["reclassify", "--db", str(db_path), "--json"])[0] == 0

    conn = connect(db_path)
    after = get_task(conn, review_task_id)
    assert after is not None
    assert after.task_id == before.task_id
    assert after.fingerprint == before.fingerprint
    assert after.task_class == before.task_class
    assert after.status == before.status
    assert after.promotion_rec == before.promotion_rec
    assert after.manual_override_state == before.manual_override_state


def test_feedback_draft_yaml_round_trips_through_import_after_operator_edit(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Review-lane tasks from replay appear in ``feedback draft`` and accept import after edits."""
    db_path, raw_dir, _vault_root, _source, _records = _setup_workflow_environment(tmp_path)
    assert (
        _run_json(capsys, ["replay", "--db", str(db_path), "--raw-dir", str(raw_dir), "--json"])[0]
        == 0
    )
    assert _run_json(capsys, ["reclassify", "--db", str(db_path), "--json"])[0] == 0

    conn = connect(db_path)
    review_fingerprint = derive_fingerprint(
        REVIEW_CONTENT,
        _source.source_id,
        REVIEW_CREATED_AT,
        source_type="dm_user",
    )
    review_task = get_task_by_fingerprint(conn, review_fingerprint)
    assert review_task is not None
    task_id = review_task.task_id

    draft_path = tmp_path / "draft.yaml"
    assert (
        _run_json(
            capsys,
            ["feedback", "draft", "--db", str(db_path), "--out", str(draft_path), "--json"],
        )[0]
        == 0
    )
    root = yaml.safe_load(draft_path.read_text(encoding="utf-8"))
    assert root["tasks"][task_id]["action"] == DRAFT_ACTION_PLACEHOLDER
    root["tasks"][task_id]["action"] = "confirm"
    root["tasks"][task_id]["task_class"] = "task"
    fixed_path = tmp_path / "fixed.yaml"
    fixed_path.write_text(
        yaml.safe_dump(root, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )

    exit_code, payload = _run_json(
        capsys,
        ["feedback", "import", str(fixed_path), "--db", str(db_path), "--json"],
    )
    assert exit_code == 0
    assert payload["applied_task_count"] == 1

    conn = connect(db_path)
    updated = get_task(conn, task_id)
    assert updated is not None
    assert updated.task_class == "task"
    assert updated.status == "open"
    assert updated.promotion_rec == "current_tasks"
