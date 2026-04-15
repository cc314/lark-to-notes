"""Performance smoke tests.

Each test verifies that a core operation completes within a generous but
meaningful wall-clock budget.  The budgets are intentionally loose enough to
pass on a typical developer laptop under normal load, but tight enough to
catch catastrophic regressions (e.g. an O(n²) loop, an accidental full-table
scan, or blocking I/O on the hot path).

Run only these tests with::

    uv run pytest -m slow

Deselect them from the normal test run with::

    uv run pytest -m "not slow"
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from lark_to_notes.budget.chunking import chunk_text
from lark_to_notes.cli import run
from lark_to_notes.config.sources import SourceType, WatchedSource, make_source_id
from lark_to_notes.intake.ledger import insert_raw_message
from lark_to_notes.intake.models import RawMessage
from lark_to_notes.storage.db import connect, init_db, upsert_watched_source
from lark_to_notes.tasks import derive_fingerprint

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_source(conn: object, ext: str = "ou_perf") -> WatchedSource:
    src = WatchedSource(
        source_id=make_source_id(SourceType.DM, ext),
        source_type=SourceType.DM,
        external_id=ext,
        name=f"Perf source ({ext})",
    )
    upsert_watched_source(conn, src)  # type: ignore[arg-type]
    return src


def _raw_msg(message_id: str, content: str, source_id: str) -> RawMessage:
    return RawMessage(
        message_id=message_id,
        source_id=source_id,
        source_type="dm_user",
        chat_id="ou_chat_perf",
        chat_type="p2p",
        sender_id="ou_sender_perf",
        sender_name="Alice",
        direction="incoming",
        created_at="2026-05-01T10:00:00Z",
        content=content,
        payload={},
        ingested_at="2026-05-01T10:00:00Z",
    )


def _jsonl_record(message_id: str, content: str, source_id: str) -> str:
    rec = {
        "message_id": message_id,
        "source_id": source_id,
        "source_type": "dm_user",
        "chat_id": "ou_chat_perf",
        "chat_type": "p2p",
        "sender_id": "ou_sender_perf",
        "sender_name": "Alice",
        "direction": "incoming",
        "created_at": "2026-05-01T10:00:00Z",
        "content": content,
        "payload": {"content": content},
        "ingested_at": "2026-05-01T10:00:00Z",
    }
    return json.dumps(rec, ensure_ascii=False)


def _run_cli(
    capsys: pytest.CaptureFixture[str],
    argv: list[str],
) -> int:
    rc = run(argv)
    capsys.readouterr()  # discard output so it does not pollute subsequent reads
    return rc


# ---------------------------------------------------------------------------
# Test 1: inserting 500 raw messages must complete in under 3 s
# ---------------------------------------------------------------------------


@pytest.mark.slow
def test_insert_500_raw_messages_under_3s(tmp_path: Path) -> None:
    """500 raw-message inserts into SQLite complete in < 3 s."""
    db_path = tmp_path / "state.db"
    conn = connect(db_path)
    init_db(conn)
    src = _make_source(conn)

    t0 = time.monotonic()
    for i in range(500):
        insert_raw_message(conn, _raw_msg(f"om_perf_{i}", f"Review task item {i}", src.source_id))
    conn.commit()
    elapsed = time.monotonic() - t0

    assert elapsed < 3.0, f"500 inserts took {elapsed:.3f}s — expected < 3.0s"

    # Sanity: all 500 are in the DB
    row = conn.execute("SELECT COUNT(*) FROM raw_messages").fetchone()
    assert row[0] == 500


# ---------------------------------------------------------------------------
# Test 2: reclassify 100 messages must complete in under 5 s
# ---------------------------------------------------------------------------


@pytest.mark.slow
def test_reclassify_100_messages_under_5s(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Reclassifying 100 pre-loaded messages completes in < 5 s."""
    db_path = tmp_path / "state.db"
    conn = connect(db_path)
    init_db(conn)
    src = _make_source(conn)

    for i in range(100):
        content = (
            f"Please review item {i} before the deadline"
            if i % 2 == 0
            else f"TODO: follow up on ticket {i}"
        )
        insert_raw_message(conn, _raw_msg(f"om_rcl_{i}", content, src.source_id))
    conn.commit()

    t0 = time.monotonic()
    rc = _run_cli(capsys, ["reclassify", "--db", str(db_path)])
    elapsed = time.monotonic() - t0

    assert rc == 0, "reclassify returned non-zero exit code"
    assert elapsed < 5.0, f"reclassify 100 messages took {elapsed:.3f}s — expected < 5.0s"


# ---------------------------------------------------------------------------
# Test 3: render pipeline for 50 tasks must complete in under 5 s
# ---------------------------------------------------------------------------


@pytest.mark.slow
def test_render_pipeline_50_tasks_under_5s(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Rendering 50 tasks to vault notes completes in < 5 s."""
    db_path = tmp_path / "state.db"
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    vault_root = tmp_path / "vault"
    vault_root.mkdir()

    conn = connect(db_path)
    init_db(conn)
    src = _make_source(conn)
    conn.close()

    lines = [
        _jsonl_record(
            f"om_rnd_{i}",
            f"Please review the document {i} by end of day",
            src.source_id,
        )
        for i in range(50)
    ]
    (raw_dir / "2026-05-01.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")

    _run_cli(capsys, ["replay", "--db", str(db_path), "--raw-dir", str(raw_dir)])
    _run_cli(capsys, ["reclassify", "--db", str(db_path)])

    t0 = time.monotonic()
    rc = _run_cli(
        capsys,
        ["render", "--db", str(db_path), "--vault-root", str(vault_root)],
    )
    elapsed = time.monotonic() - t0

    assert rc == 0, "render returned non-zero exit code"
    assert elapsed < 5.0, f"render 50 tasks took {elapsed:.3f}s — expected < 5.0s"

    # Sanity: at least one vault note was created
    notes = list(vault_root.rglob("*.md"))
    assert len(notes) >= 1, "render produced no vault notes"


# ---------------------------------------------------------------------------
# Test 4: chunk_text on 1 MB must complete in under 1 s
# ---------------------------------------------------------------------------


@pytest.mark.slow
def test_chunk_text_1mb_under_1s() -> None:
    """chunk_text on a 1 MB string completes in < 1 s."""
    one_mb = "Lorem ipsum dolor sit amet. " * 37450  # ≈ 1 048 600 chars
    assert len(one_mb) >= 1_000_000

    t0 = time.monotonic()
    chunks = chunk_text(one_mb, max_chars=500)
    elapsed = time.monotonic() - t0

    assert elapsed < 1.0, f"chunk_text 1 MB took {elapsed:.3f}s — expected < 1.0s"
    assert len(chunks) >= 1_000_000 // 500, "too few chunks for 1 MB text"


# ---------------------------------------------------------------------------
# Test 5: fingerprinting 10 000 strings must complete in under 1 s
# ---------------------------------------------------------------------------


@pytest.mark.slow
def test_fingerprint_10k_strings_under_1s() -> None:
    """derive_fingerprint on 10 000 distinct strings completes in < 1 s."""
    source_id = "dm:ou_fp_perf"
    ts = "2026-05-01T10:00:00Z"

    t0 = time.monotonic()
    fingerprints = [derive_fingerprint(f"content {i}", source_id, ts) for i in range(10_000)]
    elapsed = time.monotonic() - t0

    assert elapsed < 1.0, f"10k fingerprints took {elapsed:.3f}s — expected < 1.0s"
    # All fingerprints must be distinct (no hash collisions in this range)
    assert len(set(fingerprints)) == 10_000, "unexpected fingerprint collision"


# ---------------------------------------------------------------------------
# Test 6: init_db 100 times on in-memory databases must complete in under 2 s
# ---------------------------------------------------------------------------


@pytest.mark.slow
def test_init_db_100x_under_2s() -> None:
    """init_db on 100 separate in-memory connections completes in < 2 s."""
    t0 = time.monotonic()
    for _ in range(100):
        conn = connect(":memory:")
        init_db(conn)
        conn.close()
    elapsed = time.monotonic() - t0

    assert elapsed < 2.0, f"init_db 100x took {elapsed:.3f}s — expected < 2.0s"


# ---------------------------------------------------------------------------
# Test 7: full pipeline (100 messages, all steps) must complete in under 15 s
# ---------------------------------------------------------------------------


@pytest.mark.slow
def test_full_pipeline_100_messages_e2e(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """End-to-end pipeline with 100 messages (all steps) completes in < 15 s."""
    db_path = tmp_path / "state.db"
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    vault_root = tmp_path / "vault"
    vault_root.mkdir()
    fixture_corpus = Path(__file__).resolve().parents[1] / "raw" / "lark-worker" / "fixture-corpus"

    conn = connect(db_path)
    init_db(conn)
    src = _make_source(conn)
    conn.commit()
    conn.close()

    # 100 messages split across 4 dates for a realistic daily-note spread
    records: dict[str, list[str]] = {
        "2026-05-01.jsonl": [],
        "2026-05-02.jsonl": [],
        "2026-05-03.jsonl": [],
        "2026-05-04.jsonl": [],
    }
    files = list(records.keys())
    for i in range(100):
        fname = files[i % 4]
        content = (
            f"Please review item {i}"
            if i % 3 == 0
            else f"TODO: follow up on task {i}"
            if i % 3 == 1
            else f"FYI: status update for item {i}"
        )
        records[fname].append(_jsonl_record(f"om_e2e100_{i}", content, src.source_id))
    for fname, lines in records.items():
        (raw_dir / fname).write_text("\n".join(lines) + "\n", encoding="utf-8")

    t0 = time.monotonic()

    # Step 1: replay
    rc = _run_cli(capsys, ["replay", "--db", str(db_path), "--raw-dir", str(raw_dir)])
    assert rc == 0, "replay failed"

    # Step 2: reclassify
    rc = _run_cli(capsys, ["reclassify", "--db", str(db_path)])
    assert rc == 0, "reclassify failed"

    # Step 3: render
    rc = _run_cli(capsys, ["render", "--db", str(db_path), "--vault-root", str(vault_root)])
    assert rc == 0, "render failed"

    # Step 4: doctor
    rc = _run_cli(
        capsys,
        [
            "doctor",
            "--db",
            str(db_path),
            "--fixture-corpus",
            str(fixture_corpus),
        ],
    )
    assert rc == 0, "doctor failed"

    elapsed = time.monotonic() - t0
    assert elapsed < 15.0, f"full pipeline 100 messages took {elapsed:.3f}s — expected < 15.0s"

    # Sanity checks
    verify_conn = connect(db_path)
    raw_count = verify_conn.execute("SELECT COUNT(*) FROM raw_messages").fetchone()[0]
    task_count = verify_conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
    assert raw_count == 100, f"expected 100 raw messages, got {raw_count}"
    assert task_count == 100, f"expected 100 tasks (one per message), got {task_count}"
    assert len(list(vault_root.rglob("*.md"))) >= 1, "no vault notes produced"
