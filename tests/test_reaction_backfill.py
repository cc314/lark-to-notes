"""Tests for reaction REST backfill checkpoints and batching (lw-pzj.6.2)."""

from __future__ import annotations

import json
import sqlite3
from typing import Any

from lark_to_notes.config.sources import ReactionBackfillCheckpoint, SourceType, WatchedSource
from lark_to_notes.intake.reaction_backfill import (
    count_raw_messages_after_watermark,
    execute_reaction_backfill,
    make_lark_cli_reactions_list_fetcher,
    reaction_list_item_to_normalized,
    reaction_rest_backfill_doctor_block,
)
from lark_to_notes.intake.reaction_model import NormalizedReactionEvent, ReactionKind
from lark_to_notes.storage.db import (
    connect,
    get_reaction_backfill_checkpoint,
    init_db,
    upsert_reaction_backfill_checkpoint,
    upsert_watched_source,
)


def _conn() -> sqlite3.Connection:
    c = connect(":memory:")
    init_db(c)
    return c


def _seed_source(conn: sqlite3.Connection, *, source_id: str = "dm:ou_demo") -> None:
    upsert_watched_source(
        conn,
        WatchedSource(
            source_id=source_id,
            source_type=SourceType.DM,
            external_id="ou_demo",
            name="Demo",
        ),
    )


def _insert_raw(
    conn: sqlite3.Connection,
    *,
    message_id: str,
    source_id: str = "dm:ou_demo",
    created_at: str,
) -> None:
    conn.execute(
        """
        INSERT INTO raw_messages (
            message_id, source_id, source_type, chat_id, chat_type,
            sender_id, sender_name, direction, created_at, content, payload_json
        )
        VALUES (?, ?, 'dm_user', 'oc_x', 'p2p', 'u1', 'A', 'incoming', ?, 'x', '{}')
        """,
        (message_id, source_id, created_at),
    )
    conn.commit()


def test_reaction_list_item_to_normalized_maps_list_record() -> None:
    item = {
        "reaction_id": "rid-1",
        "operator": {"operator_type": "user", "operator_id": "ou_u1"},
        "action_time": "99",
        "reaction_type": {"emoji_type": "THUMBSUP"},
    }
    ev = reaction_list_item_to_normalized(item, source_id="dm:ou_demo", message_id="om_z")
    assert ev.reaction_event_id == "rid-1"
    assert ev.message_id == "om_z"
    assert ev.source_id == "dm:ou_demo"
    assert ev.reaction_kind is ReactionKind.ADD
    assert ev.emoji_type == "THUMBSUP"
    assert ev.operator_open_id == "ou_u1"


def test_execute_reaction_backfill_inserts_and_advances_watermark() -> None:
    conn = _conn()
    _seed_source(conn)
    _insert_raw(conn, message_id="om_a", created_at="2026-01-01T10:00:00Z")
    _insert_raw(conn, message_id="om_b", created_at="2026-01-01T11:00:00Z")

    def fetch(mid: str, tok: str | None) -> tuple[list[NormalizedReactionEvent], str | None]:
        assert tok is None
        rid = f"rx-{mid}"
        ev = NormalizedReactionEvent(
            reaction_event_id=rid,
            source_id="dm:ou_demo",
            message_id=mid,
            reaction_kind=ReactionKind.ADD,
            emoji_type="THUMBSUP",
            operator_type="user",
            operator_open_id="ou_x",
            operator_user_id="",
            operator_union_id="",
            action_time="1",
            payload={"header": {"event_id": rid}, "event": {"message_id": mid}},
        )
        return [ev], None

    sleeps: list[float] = []

    def fake_sleep(d: float) -> None:
        sleeps.append(d)

    out = execute_reaction_backfill(
        conn,
        source_id="dm:ou_demo",
        fetch_page=fetch,
        batch_size=10,
        min_interval_s=0.05,
        max_messages=2,
        sleep_fn=fake_sleep,
    )
    assert out["messages_processed"] == 2
    assert out["rows_inserted"] == 2
    assert out["api_calls"] == 2
    assert out["watermark_message_id"] == "om_b"
    assert out["last_error"] is None
    assert len(sleeps) == 2
    cp = get_reaction_backfill_checkpoint(conn, "dm:ou_demo")
    assert cp is not None
    assert cp.watermark_message_id == "om_b"
    assert cp.inflight_message_id is None


def test_execute_reaction_backfill_paginates_per_message() -> None:
    conn = _conn()
    _seed_source(conn)
    _insert_raw(conn, message_id="om_p", created_at="2026-02-01T10:00:00Z")

    def fetch(mid: str, tok: str | None) -> tuple[list[NormalizedReactionEvent], str | None]:
        if tok is None:
            ev = NormalizedReactionEvent(
                reaction_event_id="r1",
                source_id="dm:ou_demo",
                message_id=mid,
                reaction_kind=ReactionKind.ADD,
                emoji_type="THUMBSUP",
                operator_type="user",
                operator_open_id="ou_a",
                operator_user_id="",
                operator_union_id="",
                action_time="1",
                payload={"p": 1},
            )
            return [ev], "next"
        ev2 = NormalizedReactionEvent(
            reaction_event_id="r2",
            source_id="dm:ou_demo",
            message_id=mid,
            reaction_kind=ReactionKind.ADD,
            emoji_type="SMILE",
            operator_type="user",
            operator_open_id="ou_b",
            operator_user_id="",
            operator_union_id="",
            action_time="2",
            payload={"p": 2},
        )
        return [ev2], None

    out = execute_reaction_backfill(
        conn,
        source_id="dm:ou_demo",
        fetch_page=fetch,
        batch_size=5,
        min_interval_s=0.0,
        max_messages=1,
    )
    assert out["rows_inserted"] == 2
    assert out["api_calls"] == 2


def test_execute_reaction_backfill_resumes_inflight_page_token() -> None:
    conn = _conn()
    _seed_source(conn)
    _insert_raw(conn, message_id="om_resume", created_at="2026-03-01T10:00:00Z")

    upsert_reaction_backfill_checkpoint(
        conn,
        ReactionBackfillCheckpoint(
            source_id="dm:ou_demo",
            watermark_created_at=None,
            watermark_message_id=None,
            inflight_message_id="om_resume",
            inflight_created_at="2026-03-01T10:00:00Z",
            inflight_page_token="PAGE1",
        ),
    )

    calls: list[str | None] = []

    def fetch(mid: str, tok: str | None) -> tuple[list[NormalizedReactionEvent], str | None]:
        calls.append(tok)
        assert mid == "om_resume"
        assert tok == "PAGE1"
        ev = NormalizedReactionEvent(
            reaction_event_id="r_after_resume",
            source_id="dm:ou_demo",
            message_id=mid,
            reaction_kind=ReactionKind.ADD,
            emoji_type="THUMBSUP",
            operator_type="user",
            operator_open_id="ou_z",
            operator_user_id="",
            operator_union_id="",
            action_time="3",
            payload={},
        )
        return [ev], None

    execute_reaction_backfill(
        conn,
        source_id="dm:ou_demo",
        fetch_page=fetch,
        batch_size=5,
        min_interval_s=0.0,
        max_messages=1,
    )
    assert calls == ["PAGE1"]


def test_count_raw_messages_after_watermark() -> None:
    conn = _conn()
    _seed_source(conn)
    assert (
        count_raw_messages_after_watermark(
            conn, source_id="dm:ou_demo", watermark_created_at=None, watermark_message_id=None
        )
        == 0
    )
    _insert_raw(conn, message_id="om_a", created_at="2026-01-01T00:00:00Z")
    _insert_raw(conn, message_id="om_b", created_at="2026-01-02T00:00:00Z")
    _insert_raw(conn, message_id="om_c", created_at="2026-01-02T00:00:00Z")
    assert (
        count_raw_messages_after_watermark(
            conn, source_id="dm:ou_demo", watermark_created_at=None, watermark_message_id=None
        )
        == 3
    )
    assert (
        count_raw_messages_after_watermark(
            conn,
            source_id="dm:ou_demo",
            watermark_created_at="2026-01-01T00:00:00Z",
            watermark_message_id="om_a",
        )
        == 2
    )
    assert (
        count_raw_messages_after_watermark(
            conn,
            source_id="dm:ou_demo",
            watermark_created_at="2026-01-02T00:00:00Z",
            watermark_message_id="om_b",
        )
        == 1
    )


def test_reaction_rest_backfill_doctor_block_per_source() -> None:
    conn = _conn()
    _seed_source(conn)
    _insert_raw(conn, message_id="om_1", created_at="2026-02-01T00:00:00Z")
    upsert_reaction_backfill_checkpoint(
        conn,
        ReactionBackfillCheckpoint(
            source_id="dm:ou_demo",
            watermark_created_at="2026-02-01T00:00:00Z",
            watermark_message_id="om_1",
            api_calls=3,
            rows_inserted=1,
            batches_completed=2,
            last_error=None,
        ),
    )
    blk = reaction_rest_backfill_doctor_block(conn)
    assert len(blk["sources"]) == 1
    s0 = blk["sources"][0]
    assert s0["source_id"] == "dm:ou_demo"
    assert s0["pending_raw_messages_after_watermark"] == 0
    assert s0["api_calls_checkpointed_total"] == 3
    assert s0["rows_inserted_checkpointed_total"] == 1
    assert s0["batches_completed"] == 2


def test_make_lark_cli_reactions_list_fetcher_builds_argv() -> None:
    captured: dict[str, Any] = {}

    def fake_run(argv: list[str]) -> dict[str, Any]:
        captured["argv"] = argv
        return {
            "ok": True,
            "data": {
                "items": [
                    {
                        "reaction_id": "rid-x",
                        "operator": {"operator_type": "user", "operator_id": "ou_u"},
                        "action_time": "9",
                        "reaction_type": {"emoji_type": "LAUGH"},
                    }
                ],
                "has_more": False,
            },
        }

    fetch = make_lark_cli_reactions_list_fetcher("dm:ou_demo", page_size=12, runner=fake_run)
    evs, nxt = fetch("om_cli", None)
    assert nxt is None
    assert len(evs) == 1
    assert evs[0].reaction_event_id == "rid-x"
    assert "im" in captured["argv"]
    assert "reactions" in captured["argv"]
    params = json.loads(captured["argv"][captured["argv"].index("--params") + 1])
    assert params["message_id"] == "om_cli"
    assert params["page_size"] == 12
