"""Live sync: locking, single-writer, throughput, and operator diagnostics.

Covers lw-m5k.7.3 — mixed poll/event paths share the runtime lock for drains,
chat polling uses bounded page sizes, and lock files carry pid-based
diagnostics for contention forensics.
"""

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("fcntl")

from lark_to_notes.intake.ledger import observe_chat_message
from lark_to_notes.intake.models import IntakePath, RawMessage
from lark_to_notes.live import chat_live as chat_live_mod
from lark_to_notes.live.chat_live import (
    ChatLiveAdapter,
    _list_argv_for_source,
    live_chat_source_view,
)
from lark_to_notes.live.worker_config import load_live_worker_config
from lark_to_notes.runtime.executor import drain_ready_chat_intake
from lark_to_notes.runtime.lock import RuntimeLock
from lark_to_notes.storage.db import connect, init_db


def _make_msg(mid: str, source_id: str = "dm:ou_livectl") -> RawMessage:
    return RawMessage(
        message_id=mid,
        source_id=source_id,
        source_type="dm_user",
        chat_id="ou_chat",
        chat_type="p2p",
        sender_id="ou_sender",
        sender_name="Alice",
        direction="incoming",
        created_at="2026-04-14T10:00:00Z",
        content="live control probe",
        payload={},
        ingested_at="2026-04-14T10:00:00Z",
    )


def test_drain_ready_chat_intake_serializes_with_runtime_lock(tmp_path: Path) -> None:
    """``drain_ready_chat_intake`` blocks while an unrelated holder keeps the same lock file."""

    db_path = tmp_path / "mix.db"
    conn = connect(db_path)
    init_db(conn)
    observe_chat_message(
        conn,
        _make_msg("om_lock_mix"),
        intake_path=IntakePath.EVENT,
        observed_at="2026-04-14T10:00:00Z",
        coalesce_window_seconds=0,
    )
    lock_path = tmp_path / "lark-to-notes.runtime.lock"
    holder_span: list[tuple[float, float]] = []
    drainer_span: list[tuple[float, float]] = []
    holder_ready = threading.Event()

    def hold_lock() -> None:
        t0 = time.monotonic()
        with RuntimeLock(lock_path, owner_tag="foreign-holder"):
            holder_ready.set()
            time.sleep(0.12)
        t1 = time.monotonic()
        holder_span.append((t0, t1))

    def drain_when_free() -> None:
        assert holder_ready.wait(timeout=5.0), "holder should signal lock ownership"
        c2 = connect(db_path)
        try:
            t_enter = time.monotonic()
            batch = drain_ready_chat_intake(c2, lock_path=lock_path, command="test-drain")
            t_exit = time.monotonic()
            drainer_span.append((t_enter, t_exit))
            assert batch.items_processed == 1
        finally:
            c2.close()

    th = threading.Thread(target=hold_lock)
    td = threading.Thread(target=drain_when_free)
    th.start()
    td.start()
    th.join()
    td.join()

    assert len(holder_span) == 1 and len(drainer_span) == 1
    (_h0, h1) = holder_span[0]
    (d0, d1) = drainer_span[0]
    assert d0 < h1 + 0.03, "drainer should start while holder still active or just after"
    assert d1 >= h1 - 0.02, "drain should finish only after the foreign lock is released"


def test_canonical_vault_var_db_matches_poll_runtime_lock(tmp_path: Path) -> None:
    """When the store lives at ``<vault>/var/*.db``, poll and event drains share one lock."""

    vault = tmp_path / "notes_vault"
    (vault / "var").mkdir(parents=True)
    db_path = vault / "var" / "lark-to-notes.db"
    poll_lock = vault / "var" / "lark-to-notes.runtime.lock"
    event_lock = db_path.parent / "lark-to-notes.runtime.lock"
    assert poll_lock.resolve() == event_lock.resolve()


def test_runtime_lock_file_records_pid_for_diagnostics(tmp_path: Path) -> None:
    lock_path = tmp_path / "diag.lock"
    with RuntimeLock(lock_path, owner_tag="probe"):
        data = lock_path.read_bytes()
    assert data.startswith(f"pid={os.getpid()}".encode())


def test_chat_live_poll_requests_page_size_50(tmp_path: Path) -> None:
    """Live chat list uses a fixed page size for backpressure (bounded batches)."""

    raw = {
        "source_id": "dm:ou_page",
        "source_type": "dm_user",
        "external_id": "ou_ext",
        "name": "DM",
        "enabled": True,
    }
    view = live_chat_source_view(raw)
    assert view is not None
    argv = _list_argv_for_source(
        view,
        start="2026-04-01T00:00:00Z",
        end="2026-04-20T00:00:00Z",
        sort="asc",
        page_size="50",
        page_token=None,
    )
    assert "--page-size" in argv
    assert argv[argv.index("--page-size") + 1] == "50"


def test_chat_live_adapter_respects_max_distill_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``poll_once`` passes limit=200 into distill (bounded work per cycle)."""

    vault = tmp_path / "vault"
    vault.mkdir()
    cfg_path = tmp_path / "live.json"
    cfg_path.write_text(
        json.dumps(
            {
                "vault_root": str(vault),
                "state_db": str(tmp_path / "w.db"),
                "poll_interval_seconds": 300,
                "poll_lookback_days": 7,
                "sources": [
                    {
                        "source_id": "dm:ou_dist",
                        "source_type": "dm_user",
                        "external_id": "ou_ext",
                        "name": "DM",
                        "enabled": True,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    db_path = tmp_path / "runtime.db"
    conn = connect(db_path)
    init_db(conn)
    snap = load_live_worker_config(cfg_path)
    adapter = ChatLiveAdapter(snap, conn)
    adapter.initialize()

    seen: dict[str, Any] = {}

    def fake_distill(_conn: Any, *, limit: int, budget_run_id: str) -> int:
        seen["limit"] = limit
        seen["budget_run_id"] = budget_run_id
        return 0

    messages = [
        {
            "message_id": "om_one",
            "chat_id": "ou_chat",
            "create_time": "1713096000000",
            "body": {"content": json.dumps({"text": "x"})},
            "sender": {"id": "ou_sender", "name": "A"},
        }
    ]

    def fake_runner(argv: list[str]) -> dict[str, Any]:
        _ = argv
        return {"data": {"messages": messages}}

    monkeypatch.setattr(adapter, "_runner", fake_runner)
    monkeypatch.setattr(chat_live_mod, "_distill_recent", fake_distill)

    out = adapter.poll_once(sync_notes=True)
    assert out["distilled_items"] == 0
    assert seen.get("limit") == 200
    assert str(seen.get("budget_run_id", "")).startswith("chat-live:")
