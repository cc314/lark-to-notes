"""Multi-source isolation tests.

These tests verify that the pipeline correctly segregates data from multiple
WatchedSources, that per-source filters are honoured, and that concurrent
writes to the same database do not corrupt each other.

No mocks are used — every test exercises real SQLite and real filesystem I/O.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest

from lark_to_notes.cli import run
from lark_to_notes.config.sources import SourceType, WatchedSource, make_source_id
from lark_to_notes.intake.ledger import count_raw_messages, insert_raw_message
from lark_to_notes.intake.models import RawMessage
from lark_to_notes.render.blocks import list_block_ids
from lark_to_notes.storage.db import connect, init_db, upsert_watched_source
from lark_to_notes.tasks.registry import list_tasks

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DM_EXT = "ou_dm_multi"
_GRP_EXT = "group_grp_multi"
_DOC_EXT = "doc_doc_multi"


def _sid(source_type: SourceType, ext: str) -> str:
    return make_source_id(source_type, ext)


def _seed_three_sources(conn: object) -> tuple[WatchedSource, WatchedSource, WatchedSource]:
    src_dm = WatchedSource(
        source_id=_sid(SourceType.DM, _DM_EXT),
        source_type=SourceType.DM,
        external_id=_DM_EXT,
        name="Multi-Test DM",
    )
    src_grp = WatchedSource(
        source_id=_sid(SourceType.GROUP, _GRP_EXT),
        source_type=SourceType.GROUP,
        external_id=_GRP_EXT,
        name="Multi-Test Group",
    )
    src_doc = WatchedSource(
        source_id=_sid(SourceType.DOC, _DOC_EXT),
        source_type=SourceType.DOC,
        external_id=_DOC_EXT,
        name="Multi-Test Doc",
    )
    for src in (src_dm, src_grp, src_doc):
        upsert_watched_source(conn, src)  # type: ignore[arg-type]
    return src_dm, src_grp, src_doc


def _msg(
    message_id: str,
    content: str,
    source_id: str,
    created_at: str = "2026-05-01T10:00:00Z",
) -> dict[str, object]:
    """Return a JSONL-compatible message dict."""
    return {
        "message_id": message_id,
        "source_id": source_id,
        "source_type": "dm_user",
        "chat_id": "ou_chat_multi",
        "chat_type": "p2p",
        "sender_id": "ou_sender_multi",
        "sender_name": "Bob",
        "direction": "incoming",
        "created_at": created_at,
        "content": content,
        "payload": {"content": content},
        "ingested_at": created_at,
    }


def _raw_msg(
    message_id: str,
    content: str,
    source_id: str,
    created_at: str = "2026-05-01T10:00:00Z",
) -> RawMessage:
    return RawMessage(
        message_id=message_id,
        source_id=source_id,
        source_type="dm_user",
        chat_id="ou_chat_multi",
        chat_type="p2p",
        sender_id="ou_sender_multi",
        sender_name="Bob",
        direction="incoming",
        created_at=created_at,
        content=content,
        payload={},
        ingested_at=created_at,
    )


def _write_jsonl(path: Path, records: list[dict[str, object]]) -> None:
    path.write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in records) + "\n",
        encoding="utf-8",
    )


def _run_json(
    capsys: pytest.CaptureFixture[str],
    argv: list[str],
) -> tuple[int, dict[str, object]]:
    exit_code = run(argv)
    payload = json.loads(capsys.readouterr().out)
    return exit_code, payload


# ---------------------------------------------------------------------------
# Test 1: Replay isolation across 3 sources in a shared JSONL file
# ---------------------------------------------------------------------------


def test_three_source_replay_isolation(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """After replaying a mixed JSONL file, raw_message counts are exact per source."""
    db_path = tmp_path / "state.db"
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()

    conn = connect(db_path)
    init_db(conn)
    src_dm, src_grp, src_doc = _seed_three_sources(conn)
    conn.close()

    # 5 DM, 3 GROUP, 4 DOC messages in one shared JSONL
    records: list[dict[str, object]] = []
    for i in range(1, 6):
        records.append(_msg(f"om_dm_{i}", f"DM task {i}: review item {i}", src_dm.source_id))
    for i in range(1, 4):
        records.append(_msg(f"om_grp_{i}", f"Group follow-up {i}", src_grp.source_id))
    for i in range(1, 5):
        records.append(_msg(f"om_doc_{i}", f"Doc TODO item {i}", src_doc.source_id))

    _write_jsonl(raw_dir / "2026-05-01.jsonl", records)

    exit_code, payload = _run_json(
        capsys,
        ["replay", "--db", str(db_path), "--raw-dir", str(raw_dir), "--json"],
    )
    assert exit_code == 0
    assert payload["total_records"] == 12
    assert payload["inserted_records"] == 12

    # Verify per-source segregation in the DB
    conn2 = connect(db_path)
    assert count_raw_messages(conn2, source_id=src_dm.source_id) == 5
    assert count_raw_messages(conn2, source_id=src_grp.source_id) == 3
    assert count_raw_messages(conn2, source_id=src_doc.source_id) == 4
    assert count_raw_messages(conn2) == 12


# ---------------------------------------------------------------------------
# Test 2: reclassify --source-id produces tasks only for that source
# ---------------------------------------------------------------------------


def test_reclassify_per_source_separate_tasks(tmp_path: Path) -> None:
    """reclassify --source-id=<id> produces tasks only for that source's messages."""
    db_path = tmp_path / "state.db"
    conn = connect(db_path)
    init_db(conn)
    src_dm, src_grp, _src_doc = _seed_three_sources(conn)

    # Insert 3 messages per source, all action-item-style
    for i in range(1, 4):
        insert_raw_message(
            conn, _raw_msg(f"om_dm_{i}", f"Please review document {i}", src_dm.source_id)
        )
        insert_raw_message(
            conn, _raw_msg(f"om_grp_{i}", f"Follow up with team about item {i}", src_grp.source_id)
        )
    conn.commit()

    # Reclassify only the DM source
    exit_code = run(["reclassify", "--db", str(db_path), "--source-id", src_dm.source_id])
    assert exit_code == 0

    tasks = list_tasks(conn)
    # Every task must come from a DM message
    for task in tasks:
        assert task.created_from_raw_record_id is not None
        assert task.created_from_raw_record_id.startswith("om_dm_"), (
            f"reclassify leaked task from non-DM source: {task.created_from_raw_record_id}"
        )

    # GROUP messages must still be unclassified
    grp_msg_ids = {f"om_grp_{i}" for i in range(1, 4)}
    task_raw_ids = {t.created_from_raw_record_id for t in tasks}
    assert grp_msg_ids.isdisjoint(task_raw_ids), (
        "GROUP messages appeared as task source records after DM-only reclassify"
    )


# ---------------------------------------------------------------------------
# Test 3: render notes from multi-source pipeline have non-overlapping block_ids
# ---------------------------------------------------------------------------


def test_render_per_source_notes_no_overlap(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Block IDs written into vault notes are unique across sources — no cross-contamination."""
    db_path = tmp_path / "state.db"
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    vault_root = tmp_path / "vault"
    vault_root.mkdir()

    conn = connect(db_path)
    init_db(conn)
    src_dm, src_grp, _src_doc = _seed_three_sources(conn)
    conn.close()

    # Identical content across two sources — fingerprints must still differ
    content = "Please review the quarterly plan"
    records = [
        _msg("om_shared_dm_1", content, src_dm.source_id, "2026-05-01T10:00:00Z"),
        _msg(
            "om_shared_dm_2",
            "TODO: update the sprint board",
            src_dm.source_id,
            "2026-05-01T10:01:00Z",
        ),
        _msg("om_shared_grp_1", content, src_grp.source_id, "2026-05-01T10:00:00Z"),
        _msg(
            "om_shared_grp_2",
            "Follow up with Alice tomorrow",
            src_grp.source_id,
            "2026-05-01T10:02:00Z",
        ),
    ]
    _write_jsonl(raw_dir / "2026-05-01.jsonl", records)

    for argv in [
        ["replay", "--db", str(db_path), "--raw-dir", str(raw_dir), "--json"],
        ["reclassify", "--db", str(db_path), "--json"],
        ["render", "--db", str(db_path), "--vault-root", str(vault_root), "--json"],
    ]:
        exit_code, _ = _run_json(capsys, argv)
        assert exit_code == 0

    # Gather all block_ids from every rendered note
    all_block_ids: list[str] = []
    for note_file in vault_root.rglob("*.md"):
        all_block_ids.extend(list_block_ids(note_file.read_text(encoding="utf-8")))

    assert len(all_block_ids) > 0, "no blocks were rendered into vault"
    # All block_ids must be unique (no block from one source overwrites another)
    assert len(all_block_ids) == len(set(all_block_ids)), (
        f"duplicate block_ids found across sources: {all_block_ids}"
    )


# ---------------------------------------------------------------------------
# Test 4: reconcile checks all 3 sources when all have checkpoints
# ---------------------------------------------------------------------------


def test_multi_source_reconcile_checks_all(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """sources_checked == 3 when all 3 sources have checkpoint records."""
    db_path = tmp_path / "state.db"
    conn = connect(db_path)
    init_db(conn)
    src_dm, src_grp, src_doc = _seed_three_sources(conn)

    # Insert a checkpoint for each source
    for src, last_id, ts in [
        (src_dm, "om_dm_last", "2026-05-01T10:05:00Z"),
        (src_grp, "om_grp_last", "2026-05-01T10:06:00Z"),
        (src_doc, "om_doc_last", "2026-05-01T10:07:00Z"),
    ]:
        conn.execute(
            """
            INSERT INTO checkpoints
                (source_id, last_message_id, last_message_timestamp, page_token, updated_at)
            VALUES (?, ?, ?, '', ?)
            """,
            (src.source_id, last_id, ts, ts),
        )
    conn.commit()

    exit_code, payload = _run_json(
        capsys,
        ["reconcile", "--db", str(db_path), "--json"],
    )
    assert exit_code == 0, "reconcile should succeed (no gaps when stored == latest)"
    assert payload["sources_checked"] == 3, (
        f"expected 3 sources checked, got {payload['sources_checked']}"
    )
    assert payload["gaps_found"] == 0


# ---------------------------------------------------------------------------
# Test 5: health aggregates across 3 sources — dead_letter_count stays zero
# ---------------------------------------------------------------------------


def test_multi_source_health_aggregates(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Full pipeline on 3 sources produces dead_letter_count==0 in the doctor report."""
    db_path = tmp_path / "state.db"
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    vault_root = tmp_path / "vault"
    vault_root.mkdir()
    fixture_corpus = Path(__file__).resolve().parent / "fixtures" / "lark-worker" / "fixture-corpus"

    conn = connect(db_path)
    init_db(conn)
    src_dm, src_grp, src_doc = _seed_three_sources(conn)
    conn.close()

    # 3 messages per source, all clearly actionable
    records: list[dict[str, object]] = [
        _msg(
            "om_h_dm_1",
            "Review the onboarding guide by Thursday",
            src_dm.source_id,
            "2026-05-01T09:00:00Z",
        ),
        _msg(
            "om_h_dm_2",
            "Please follow up with legal team",
            src_dm.source_id,
            "2026-05-01T09:01:00Z",
        ),
        _msg(
            "om_h_dm_3",
            "TODO: push the hotfix to staging",
            src_dm.source_id,
            "2026-05-01T09:02:00Z",
        ),
        _msg(
            "om_h_grp_1",
            "Action: update sprint board before standup",
            src_grp.source_id,
            "2026-05-01T09:03:00Z",
        ),
        _msg(
            "om_h_grp_2", "Can you review the PR by EOD?", src_grp.source_id, "2026-05-01T09:04:00Z"
        ),
        _msg(
            "om_h_grp_3",
            "Follow up with Alice on the blockers",
            src_grp.source_id,
            "2026-05-01T09:05:00Z",
        ),
        _msg(
            "om_h_doc_1",
            "TODO: finalize the design spec",
            src_doc.source_id,
            "2026-05-01T09:06:00Z",
        ),
        _msg(
            "om_h_doc_2",
            "Please review the architecture diagram",
            src_doc.source_id,
            "2026-05-01T09:07:00Z",
        ),
        _msg(
            "om_h_doc_3",
            "Follow up with the infra team next week",
            src_doc.source_id,
            "2026-05-01T09:08:00Z",
        ),
    ]
    _write_jsonl(raw_dir / "2026-05-01.jsonl", records)

    for argv in [
        ["replay", "--db", str(db_path), "--raw-dir", str(raw_dir), "--json"],
        ["reclassify", "--db", str(db_path), "--json"],
        ["render", "--db", str(db_path), "--vault-root", str(vault_root), "--json"],
    ]:
        exit_code, _ = _run_json(capsys, argv)
        assert exit_code == 0

    exit_code, doctor_payload = _run_json(
        capsys,
        [
            "doctor",
            "--db",
            str(db_path),
            "--fixture-corpus",
            str(fixture_corpus),
            "--json",
        ],
    )
    assert exit_code == 0
    runtime: dict[str, object] = doctor_payload["runtime"]  # type: ignore[assignment]
    assert runtime["dead_letter_count"] == 0, (
        f"unexpected dead letters: {runtime['dead_letter_count']}"
    )
    assert runtime["error_rate"] == 0.0


# ---------------------------------------------------------------------------
# Test 6: reclassify without --source-id classifies all sources together
# ---------------------------------------------------------------------------


def test_reclassify_all_sources_without_filter(tmp_path: Path) -> None:
    """Omitting --source-id causes reclassify to process messages from all sources."""
    db_path = tmp_path / "state.db"
    conn = connect(db_path)
    init_db(conn)
    src_dm, src_grp, src_doc = _seed_three_sources(conn)

    # 2 action-item messages per source = 6 total
    messages = [
        _raw_msg("om_all_dm_1", "Please review the roadmap draft", src_dm.source_id),
        _raw_msg("om_all_dm_2", "Follow up with marketing team", src_dm.source_id),
        _raw_msg("om_all_grp_1", "TODO: write the meeting summary", src_grp.source_id),
        _raw_msg("om_all_grp_2", "Review the deployment checklist by Friday", src_grp.source_id),
        _raw_msg("om_all_doc_1", "Update the changelog and notify the team", src_doc.source_id),
        _raw_msg("om_all_doc_2", "Please review the spec comments", src_doc.source_id),
    ]
    for msg in messages:
        insert_raw_message(conn, msg)
    conn.commit()

    # Reclassify all sources (no --source-id filter)
    exit_code = run(["reclassify", "--db", str(db_path)])
    assert exit_code == 0

    tasks = list_tasks(conn)
    task_raw_ids = {t.created_from_raw_record_id for t in tasks}
    expected_raw_ids = {m.message_id for m in messages}

    # Every message must appear as a task source — none silently dropped
    assert expected_raw_ids == task_raw_ids, (
        f"messages not covered by tasks: {expected_raw_ids - task_raw_ids}"
    )

    # All three sources are represented in the tasks
    source_ids_in_tasks: set[str] = set()
    for task in tasks:
        rid = task.created_from_raw_record_id or ""
        if rid.startswith("om_all_dm_"):
            source_ids_in_tasks.add(src_dm.source_id)
        elif rid.startswith("om_all_grp_"):
            source_ids_in_tasks.add(src_grp.source_id)
        elif rid.startswith("om_all_doc_"):
            source_ids_in_tasks.add(src_doc.source_id)

    assert source_ids_in_tasks == {
        src_dm.source_id,
        src_grp.source_id,
        src_doc.source_id,
    }, f"not all sources covered by tasks: {source_ids_in_tasks}"


# ---------------------------------------------------------------------------
# Test 7: concurrent replay from separate threads — no cross-contamination
# ---------------------------------------------------------------------------


def test_concurrent_replay_separate_connections(tmp_path: Path) -> None:
    """Two threads replaying different JSONL files into the same DB are safe.

    Each thread opens its own SQLite connection.  After both complete, the
    per-source message counts must be exact and the total must equal the sum.
    No message must appear under the wrong source_id.
    """
    db_path = tmp_path / "state.db"

    # Initialise schema once on the main thread
    conn_main = connect(db_path)
    init_db(conn_main)
    src_a_ext = "ou_thread_a"
    src_b_ext = "ou_thread_b"
    src_a_id = _sid(SourceType.DM, src_a_ext)
    src_b_id = _sid(SourceType.DM, src_b_ext)
    upsert_watched_source(
        conn_main,
        WatchedSource(
            source_id=src_a_id,
            source_type=SourceType.DM,
            external_id=src_a_ext,
            name="Thread A source",
        ),
    )
    upsert_watched_source(
        conn_main,
        WatchedSource(
            source_id=src_b_id,
            source_type=SourceType.DM,
            external_id=src_b_ext,
            name="Thread B source",
        ),
    )
    conn_main.commit()
    conn_main.close()

    raw_a = tmp_path / "raw_a"
    raw_b = tmp_path / "raw_b"
    raw_a.mkdir()
    raw_b.mkdir()

    # 10 messages for source A, 8 for source B — no overlap in IDs
    recs_a = [_msg(f"om_ta_{i}", f"Thread-A task item {i}", src_a_id) for i in range(1, 11)]
    recs_b = [_msg(f"om_tb_{i}", f"Thread-B task item {i}", src_b_id) for i in range(1, 9)]
    _write_jsonl(raw_a / "2026-05-01.jsonl", recs_a)
    _write_jsonl(raw_b / "2026-05-01.jsonl", recs_b)

    errors: list[Exception] = []

    def _replay_thread(raw_dir: Path) -> None:
        from lark_to_notes.intake.replay import replay_jsonl_dir

        try:
            # Each thread uses its own connection — SQLite serialises writes via WAL/lock
            tconn = connect(db_path)
            replay_jsonl_dir(tconn, raw_dir=raw_dir)
            tconn.commit()
            tconn.close()
        except Exception as exc:
            errors.append(exc)

    t_a = threading.Thread(target=_replay_thread, args=(raw_a,))
    t_b = threading.Thread(target=_replay_thread, args=(raw_b,))
    t_a.start()
    t_b.start()
    t_a.join(timeout=30)
    t_b.join(timeout=30)

    assert not errors, f"concurrent replay raised exceptions: {errors}"
    assert not t_a.is_alive(), "thread A did not finish in time"
    assert not t_b.is_alive(), "thread B did not finish in time"

    # Verify counts on a fresh connection
    verify_conn = connect(db_path)
    count_a = count_raw_messages(verify_conn, source_id=src_a_id)
    count_b = count_raw_messages(verify_conn, source_id=src_b_id)
    total = count_raw_messages(verify_conn)

    assert count_a == 10, f"expected 10 messages for source A, got {count_a}"
    assert count_b == 8, f"expected 8 messages for source B, got {count_b}"
    assert total == 18, f"expected 18 total messages, got {total}"

    # No message from source A should be stored under source B's ID and vice versa
    cross_a_under_b = verify_conn.execute(
        "SELECT COUNT(*) FROM raw_messages WHERE source_id = ? AND message_id LIKE 'om_ta_%'",
        (src_b_id,),
    ).fetchone()[0]
    cross_b_under_a = verify_conn.execute(
        "SELECT COUNT(*) FROM raw_messages WHERE source_id = ? AND message_id LIKE 'om_tb_%'",
        (src_a_id,),
    ).fetchone()[0]
    assert cross_a_under_b == 0, "source A messages leaked into source B's slot"
    assert cross_b_under_a == 0, "source B messages leaked into source A's slot"
