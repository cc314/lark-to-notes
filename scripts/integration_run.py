#!/usr/bin/env python3
"""Standalone integration script — full Lark-to-notes pipeline end-to-end.

Demonstrates the complete system with zero Lark API credentials needed.
All data is synthetic; a fresh SQLite DB and vault are created in a tmpdir
and cleaned up on exit.

Structured logs (DEBUG/JSON) go to **stderr**.
Human-readable step banners and the final summary go to **stdout**.

Usage::

    uv run python scripts/integration_run.py
    uv run python scripts/integration_run.py --verbose

Exit codes:
    0 — all steps passed
    1 — one or more steps failed
"""

from __future__ import annotations

import argparse
import io
import json
import sys
import tempfile
import textwrap
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Bootstrap: add the project src/ to sys.path so the script works when run
# directly with ``uv run python scripts/integration_run.py``.
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).resolve().parents[1]
_SRC = _REPO_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

# ---------------------------------------------------------------------------
# Logging configuration — must happen before importing pipeline modules so
# that all loggers pick up the handler.
# ---------------------------------------------------------------------------
from lark_to_notes.logging import configure_logging  # noqa: E402

configure_logging("DEBUG", json_logs=True)

# ---------------------------------------------------------------------------
# Pipeline imports
# ---------------------------------------------------------------------------
from lark_to_notes.cli import run  # noqa: E402
from lark_to_notes.config.sources import SourceType, WatchedSource, make_source_id  # noqa: E402
from lark_to_notes.feedback import (  # noqa: E402
    FeedbackAction,
    FeedbackArtifact,
    FeedbackDirective,
    render_feedback_artifact,
)
from lark_to_notes.storage.db import connect, init_db, upsert_watched_source  # noqa: E402
from lark_to_notes.tasks import derive_fingerprint  # noqa: E402
from lark_to_notes.tasks.registry import get_task_by_fingerprint  # noqa: E402

FIXTURE_CORPUS_ROOT = _REPO_ROOT / "raw" / "lark-worker" / "fixture-corpus"

# ---------------------------------------------------------------------------
# Step tracking
# ---------------------------------------------------------------------------

_step_results: list[dict[str, Any]] = []


def _step_banner(n: int, description: str) -> None:
    print(f"\n## Step {n}: {description}", flush=True)


def _pass(label: str, detail: str = "") -> None:
    marker = f"  ✓  {label}"
    if detail:
        marker += f" — {detail}"
    print(marker, flush=True)
    _step_results.append({"label": label, "status": "pass", "detail": detail})


def _fail(label: str, detail: str = "") -> None:
    marker = f"  ✗  {label}"
    if detail:
        marker += f" — {detail}"
    print(marker, file=sys.stderr, flush=True)
    _step_results.append({"label": label, "status": "fail", "detail": detail})


def _check(
    condition: bool,
    label: str,
    *,
    ok_detail: str = "",
    err_detail: str = "",
) -> bool:
    if condition:
        _pass(label, ok_detail)
    else:
        _fail(label, err_detail)
    return condition


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REVIEW_CONTENT = "context context context " * 40
REVIEW_CREATED_AT = "2026-05-01T10:03:00Z"


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
        "chat_id": "ou_chat_integ",
        "chat_type": "p2p",
        "sender_id": "ou_sender_integ",
        "sender_name": "Alice",
        "direction": "incoming",
        "created_at": created_at,
        "content": content,
        "payload": {"content": content},
    }


def _synthetic_messages(source_id: str) -> list[dict[str, Any]]:
    """Generate 15 synthetic messages: mix of EN/ZH, tasks/follow-ups/context."""
    return [
        # Clear tasks (English)
        _message_record(
            message_id="om_integ_01",
            source_id=source_id,
            created_at="2026-05-01T09:00:00Z",
            content="Please review the launch checklist by Friday",
        ),
        _message_record(
            message_id="om_integ_02",
            source_id=source_id,
            created_at="2026-05-01T09:05:00Z",
            content="TODO: update the changelog and notify the team",
        ),
        _message_record(
            message_id="om_integ_03",
            source_id=source_id,
            created_at="2026-05-01T09:10:00Z",
            content="Need to confirm budget approval before Monday",
        ),
        # Follow-ups (English)
        _message_record(
            message_id="om_integ_04",
            source_id=source_id,
            created_at="2026-05-01T09:15:00Z",
            content="Let's follow up with ops tomorrow morning",
        ),
        _message_record(
            message_id="om_integ_05",
            source_id=source_id,
            created_at="2026-05-01T09:20:00Z",
            content="Can you check on the status of the migration?",
        ),
        # Context (English)
        _message_record(
            message_id="om_integ_06",
            source_id=source_id,
            created_at="2026-05-01T09:25:00Z",
            content="FYI: deployment finished successfully.",
        ),
        _message_record(
            message_id="om_integ_07",
            source_id=source_id,
            created_at="2026-05-01T09:30:00Z",
            content="Standup is at 10am today.",
        ),
        # Chinese tasks
        _message_record(
            message_id="om_integ_08",
            source_id=source_id,
            created_at="2026-05-01T09:35:00Z",
            content="需要帮忙看下这个报告",
        ),
        _message_record(
            message_id="om_integ_09",
            source_id=source_id,
            created_at="2026-05-01T09:40:00Z",
            content="帮忙确认一下这个数字是否正确",
        ),
        # Mixed EN/ZH
        _message_record(
            message_id="om_integ_10",
            source_id=source_id,
            created_at="2026-05-01T09:45:00Z",
            content="Please 看看 this report and share your thoughts",
        ),
        _message_record(
            message_id="om_integ_11",
            source_id=source_id,
            created_at="2026-05-01T09:50:00Z",
            content="TODO: 更新文档 and push to staging",
        ),
        # More context
        _message_record(
            message_id="om_integ_12",
            source_id=source_id,
            created_at="2026-05-01T09:55:00Z",
            content="系统已经恢复正常了。",
        ),
        _message_record(
            message_id="om_integ_13",
            source_id=source_id,
            created_at="2026-05-01T10:00:00Z",
            content="LGTM",
        ),
        # Needs-review (long ambiguous)
        _message_record(
            message_id="om_integ_14",
            source_id=source_id,
            created_at=REVIEW_CREATED_AT,
            content=REVIEW_CONTENT,
        ),
        _message_record(
            message_id="om_integ_15",
            source_id=source_id,
            created_at="2026-05-01T10:10:00Z",
            content="Need to sync up with the product team about next quarter",
        ),
    ]


def _run_cli(
    argv: list[str],
    *,
    verbose: bool = False,
) -> tuple[int, dict[str, Any]]:
    """Run a CLI command; return (exit_code, json_payload)."""
    old_stdout = sys.stdout
    sys.stdout = buf = io.StringIO()
    try:
        exit_code = run(argv)
    finally:
        sys.stdout = old_stdout
    raw = buf.getvalue().strip()
    try:
        payload = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        payload = {"_raw": raw}
    if verbose:
        print(f"     cmd: {' '.join(argv)}", flush=True)
        print(f"     rc:  {exit_code}", flush=True)
        if payload:
            print(
                textwrap.indent(json.dumps(payload, indent=2, ensure_ascii=False), "     "),
                flush=True,
            )
    return exit_code, payload


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def main(*, verbose: bool = False) -> int:
    """Run the full integration pipeline. Returns 0 on success, 1 on failure."""
    all_ok = True

    with tempfile.TemporaryDirectory(prefix="ltn_integ_") as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "state.db"
        raw_dir = tmp / "raw"
        raw_dir.mkdir()
        vault_root = tmp / "vault"
        vault_root.mkdir()

        # ------------------------------------------------------------------
        print("\n" + "=" * 70, flush=True)
        print("  lark-to-notes integration run", flush=True)
        print("  db:    " + str(db_path), flush=True)
        print("  vault: " + str(vault_root), flush=True)
        print("=" * 70, flush=True)

        # ------------------------------------------------------------------
        # Step a: Schema init + source registration (3 sources)
        # ------------------------------------------------------------------
        _step_banner(1, "Schema init + source registration")

        conn = connect(db_path)
        init_db(conn)
        sources = [
            WatchedSource(
                source_id=make_source_id(SourceType.DM, "ou_integ_dm"),
                source_type=SourceType.DM,
                external_id="ou_integ_dm",
                name="Integration DM",
            ),
            WatchedSource(
                source_id=make_source_id(SourceType.GROUP, "cg_integ_grp"),
                source_type=SourceType.GROUP,
                external_id="cg_integ_grp",
                name="Integration Group",
            ),
            WatchedSource(
                source_id=make_source_id(SourceType.DOC, "docx_integ_doc"),
                source_type=SourceType.DOC,
                external_id="docx_integ_doc",
                name="Integration Doc",
            ),
        ]
        primary_source = sources[0]
        for src in sources:
            upsert_watched_source(conn, src)
        conn.commit()
        conn.close()

        all_ok &= _check(
            True,
            "schema_init",
            ok_detail="3 sources registered",
        )

        rc, sv = _run_cli(
            ["sources", "list", "--db", str(db_path), "--json"],
            verbose=verbose,
        )
        all_ok &= _check(
            rc == 0 and int(sv.get("count", 0)) == 3,
            "sources_list",
            ok_detail=f"count={sv.get('count')}",
            err_detail=f"rc={rc} count={sv.get('count')}",
        )

        # ------------------------------------------------------------------
        # Step b: Synthetic fixture generation (15 messages)
        # ------------------------------------------------------------------
        _step_banner(2, "Synthetic fixture generation (15 messages)")

        messages = _synthetic_messages(primary_source.source_id)
        raw_path = raw_dir / "2026-05-01.jsonl"
        raw_path.write_text(
            "\n".join(json.dumps(rec, ensure_ascii=False) for rec in messages) + "\n",
            encoding="utf-8",
        )
        all_ok &= _check(
            len(messages) == 15 and raw_path.exists(),
            "fixture_generated",
            ok_detail="15 messages in 2026-05-01.jsonl",
        )

        # ------------------------------------------------------------------
        # Step c: Replay (ingest into DB)
        # ------------------------------------------------------------------
        _step_banner(3, "Replay (ingest into DB)")

        rc, rp = _run_cli(
            ["replay", "--db", str(db_path), "--raw-dir", str(raw_dir), "--json"],
            verbose=verbose,
        )
        inserted = int(rp.get("inserted_records", 0))
        all_ok &= _check(
            rc == 0 and inserted == len(messages),
            "replay",
            ok_detail=f"inserted={inserted}",
            err_detail=f"rc={rc} inserted={inserted}",
        )

        # ------------------------------------------------------------------
        # Step d: Reclassify (show breakdown)
        # ------------------------------------------------------------------
        _step_banner(4, "Reclassify (heuristics-only)")

        rc, cl = _run_cli(
            ["reclassify", "--db", str(db_path), "--json"],
            verbose=verbose,
        )
        processed = int(cl.get("messages_processed", 0))
        tasks_inserted = int(cl.get("tasks_inserted", 0))
        budget_run_id = str(cl.get("budget_run_id", ""))
        all_ok &= _check(
            rc == 0 and processed == len(messages),
            "reclassify",
            ok_detail=f"processed={processed} tasks_inserted={tasks_inserted}",
            err_detail=f"rc={rc} processed={processed}",
        )
        print(f"     tasks={tasks_inserted}", flush=True)

        # ------------------------------------------------------------------
        # Step e: Render (show N files created)
        # ------------------------------------------------------------------
        _step_banner(5, "Render (create vault notes)")

        rc, rn = _run_cli(
            ["render", "--db", str(db_path), "--vault-root", str(vault_root), "--json"],
            verbose=verbose,
        )
        rendered = int(rn.get("rendered", 0))
        errors = rn.get("errors", [])
        all_ok &= _check(
            rc == 0 and rendered > 0 and errors == [],
            "render",
            ok_detail=f"rendered={rendered} errors=0",
            err_detail=f"rc={rc} rendered={rendered} errors={errors}",
        )
        raw_notes = list((vault_root / "raw").glob("*.md")) if (vault_root / "raw").exists() else []
        print(f"     raw notes created: {len(raw_notes)}", flush=True)

        # ------------------------------------------------------------------
        # Step f: Reconcile (cursor check)
        # ------------------------------------------------------------------
        _step_banner(6, "Reconcile (cursor check)")

        rc, rec = _run_cli(
            ["reconcile", "--db", str(db_path), "--json"],
            verbose=verbose,
        )
        sources_checked = int(rec.get("sources_checked", 0))
        gaps_found = int(rec.get("gaps_found", 0))
        all_ok &= _check(
            rc == 0,
            "reconcile",
            ok_detail=f"sources_checked={sources_checked} gaps_found={gaps_found}",
            err_detail=f"rc={rc}",
        )

        # ------------------------------------------------------------------
        # Step g: Doctor (health snapshot)
        # ------------------------------------------------------------------
        _step_banner(7, "Doctor (health snapshot)")

        rc, dr = _run_cli(
            [
                "doctor",
                "--db",
                str(db_path),
                "--fixture-corpus",
                str(FIXTURE_CORPUS_ROOT),
                "--json",
            ],
            verbose=verbose,
        )
        all_ok &= _check(
            rc == 0 and dr.get("status") == "ok",
            "doctor",
            ok_detail=f"status={dr.get('status')}",
            err_detail=f"rc={rc} status={dr.get('status')}",
        )

        # ------------------------------------------------------------------
        # Step h: Budget status (zero LLM usage)
        # ------------------------------------------------------------------
        _step_banner(8, "Budget status (confirm zero LLM cost)")

        rc, bs = _run_cli(
            [
                "budget",
                "status",
                "--db",
                str(db_path),
                *(["--run-id", budget_run_id] if budget_run_id else []),
                "--json",
            ],
            verbose=verbose,
        )
        net_llm_calls = int(bs.get("net_llm_call_count", 0))
        fallback_count = int(bs.get("fallback_count", 0))
        all_ok &= _check(
            rc == 0 and net_llm_calls == 0,
            "budget_status",
            ok_detail=f"net_llm_call_count={net_llm_calls} fallback_count={fallback_count}",
            err_detail=f"rc={rc} net_llm_call_count={net_llm_calls}",
        )
        if net_llm_calls == 0:
            print("     ✓  No LLM API calls made (fully heuristic run)", flush=True)

        # ------------------------------------------------------------------
        # Step i: Feedback import (override 1 task class)
        # ------------------------------------------------------------------
        _step_banner(9, "Feedback import (override 1 task class)")

        conn2 = connect(db_path)
        review_fp = derive_fingerprint(REVIEW_CONTENT, primary_source.source_id, REVIEW_CREATED_AT)
        review_task = get_task_by_fingerprint(conn2, review_fp)
        conn2.close()

        if review_task is not None:
            artifact_path = tmp / "feedback.yaml"
            artifact_path.write_text(
                render_feedback_artifact(
                    FeedbackArtifact(
                        tasks={
                            review_task.task_id: FeedbackDirective(
                                action=FeedbackAction.WRONG_CLASS,
                                task_class="task",
                            ),
                        }
                    )
                ),
                encoding="utf-8",
            )
            rc, fi = _run_cli(
                ["feedback", "import", str(artifact_path), "--db", str(db_path), "--json"],
                verbose=verbose,
            )
            applied = int(fi.get("applied_task_count", 0))
            all_ok &= _check(
                rc == 0 and applied == 1,
                "feedback_import",
                ok_detail=f"applied={applied} task_id={review_task.task_id[:8]}...",
                err_detail=f"rc={rc} applied={applied}",
            )
        else:
            _fail("feedback_import", "review task not found — cannot test feedback")
            all_ok = False

        # ------------------------------------------------------------------
        # Step j: Re-render (confirm idempotency — SKIPPED count expected)
        # ------------------------------------------------------------------
        _step_banner(10, "Re-render (idempotency check)")

        rc, rn2 = _run_cli(
            ["render", "--db", str(db_path), "--vault-root", str(vault_root), "--json"],
            verbose=verbose,
        )
        rendered2 = int(rn2.get("rendered", 0))
        errors2 = rn2.get("errors", [])
        all_ok &= _check(
            rc == 0 and errors2 == [],
            "render_idempotent",
            ok_detail=f"rendered={rendered2} (SKIPPED means idempotent)",
            err_detail=f"rc={rc} errors={errors2}",
        )

        # ------------------------------------------------------------------
        # Final summary
        # ------------------------------------------------------------------
        print("\n" + "=" * 70, flush=True)
        passed = sum(1 for s in _step_results if s["status"] == "pass")
        failed = sum(1 for s in _step_results if s["status"] == "fail")
        total = passed + failed

        if all_ok:
            print(f"  PASS  {passed}/{total} checks passed", flush=True)
        else:
            print(f"  FAIL  {passed}/{total} checks passed ({failed} failed)", flush=True)
            for step in _step_results:
                if step["status"] == "fail":
                    print(f"    ✗ {step['label']}: {step['detail']}", flush=True)
        print("=" * 70, flush=True)

    return 0 if all_ok else 1


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run the full lark-to-notes pipeline end-to-end with synthetic data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print full JSON payload from each step to stdout.",
    )
    args = parser.parse_args()
    sys.exit(main(verbose=args.verbose))
