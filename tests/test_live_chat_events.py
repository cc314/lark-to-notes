"""Tests for Lark chat event envelopes into the mixed chat-intake ledger."""

from __future__ import annotations

import json
import sqlite3

import pytest

from lark_to_notes.intake.ledger import chat_ingest_key, get_chat_intake_item
from lark_to_notes.intake.models import IntakePath
from lark_to_notes.intake.reaction_caps import (
    REACTION_INTAKE_GOVERNANCE_VERSION,
    ReactionIntakeCaps,
    ReactionIntakeCapState,
)
from lark_to_notes.intake.reaction_deferrals import count_reaction_intake_deferrals
from lark_to_notes.live.chat_events import (
    event_type_from_envelope,
    extract_im_message_from_envelope,
    ingest_chat_event_ndjson_lines,
    ingest_receive_message_v1_envelope,
    iter_chat_event_envelopes_from_ndjson,
    payload_hash_for_chat_event,
)
from lark_to_notes.storage.db import connect, init_db


def _conn() -> sqlite3.Connection:
    c = connect(":memory:")
    init_db(c)
    return c


def _receive_v1_envelope(*, message_id: str = "om_evt_1") -> dict[str, object]:
    return {
        "header": {"event_type": "im.message.receive_v1"},
        "event": {
            "message": {
                "message_id": message_id,
                "chat_id": "ou_chat",
                "create_time": "1713096000000",
                "body": {"content": json.dumps({"text": "event body"})},
                "sender": {"id": "ou_sender", "name": "Alice"},
            }
        },
    }


def test_ingest_receive_message_v1_envelope_writes_ledger() -> None:
    conn = _conn()
    env = _receive_v1_envelope()
    item = ingest_receive_message_v1_envelope(
        conn,
        env,
        source_id="dm:ou_demo",
        worker_source_type="dm_user",
        chat_type="p2p",
        observed_at="2026-05-01T10:00:00Z",
        coalesce_window_seconds=120,
    )
    assert item is not None
    key = chat_ingest_key("dm:ou_demo", "om_evt_1")
    row = get_chat_intake_item(conn, key)
    assert row is not None
    assert row.last_intake_path is IntakePath.EVENT
    assert row.event_seen_count >= 1


def test_ingest_receive_message_v1_wrong_event_type_is_skipped() -> None:
    conn = _conn()
    env = {"header": {"event_type": "im.chat.access_event.bot_p2p_chat_enter_v1"}, "event": {}}
    assert event_type_from_envelope(env) == "im.chat.access_event.bot_p2p_chat_enter_v1"
    assert extract_im_message_from_envelope(env) is None
    item = ingest_receive_message_v1_envelope(
        conn,
        env,
        source_id="dm:ou_demo",
        worker_source_type="dm_user",
        chat_type="p2p",
    )
    assert item is None


def test_ingest_chat_event_ndjson_lines_counts_and_skips_garbage() -> None:
    conn = _conn()
    lines = [
        "",
        "not-json",
        json.dumps(_receive_v1_envelope(message_id="om_a")),
        json.dumps({"header": {"event_type": "im.message.receive_v1"}, "event": {}}),
    ]
    out = ingest_chat_event_ndjson_lines(
        conn,
        lines,
        source_id="dm:ou_demo",
        worker_source_type="dm_user",
        chat_type="p2p",
        observed_at="2026-05-01T10:00:00Z",
    )
    assert out.json_objects == 2
    assert out.chat_envelopes_ingested == 1
    assert out.reaction_rows_inserted == 0
    assert out.reaction_validation_rejects == 0
    assert out.reaction_cap_deferred == 0
    assert out.last_reaction_cap_reason_code is None
    assert out.reaction_benign_duplicate_replays == 0


def test_ingest_chat_event_ndjson_lines_skips_invalid_reaction_envelope() -> None:
    conn = _conn()
    bad = {
        "header": {"event_type": "im.message.reaction.created_v1", "event_id": "rx-bad"},
        "event": {"message_id": "om_z"},
    }
    out = ingest_chat_event_ndjson_lines(
        conn,
        [json.dumps(bad)],
        source_id="dm:ou_demo",
        worker_source_type="dm_user",
        chat_type="p2p",
    )
    assert out.json_objects == 1
    assert out.chat_envelopes_ingested == 0
    assert out.reaction_rows_inserted == 0
    assert out.reaction_validation_rejects == 1
    assert out.last_reaction_quarantine_event_id == "rx-bad"
    assert out.last_reaction_quarantine_payload_hash == payload_hash_for_chat_event(bad)
    assert out.last_reaction_quarantine_reason_code is not None
    assert out.last_reaction_quarantine_reason_code.startswith("reaction_envelope_invalid:")


def test_ingest_chat_event_ndjson_lines_inserts_reaction_event() -> None:
    conn = _conn()
    react = {
        "header": {"event_type": "im.message.reaction.created_v1", "event_id": "rx-e2e-1"},
        "event": {
            "message_id": "om_rx_1",
            "reaction_type": {"emoji_type": "THUMBSUP"},
            "operator_type": "user",
            "user_id": {"open_id": "ou_x"},
            "action_time": "1",
        },
    }
    lines = [json.dumps(react)]
    out = ingest_chat_event_ndjson_lines(
        conn,
        lines,
        source_id="dm:ou_demo",
        worker_source_type="dm_user",
        chat_type="p2p",
    )
    assert out.json_objects == 1
    assert out.chat_envelopes_ingested == 0
    assert out.reaction_rows_inserted == 1
    assert out.reaction_validation_rejects == 0
    assert out.reaction_benign_duplicate_replays == 0
    row = conn.execute(
        "SELECT message_id, reaction_kind, governance_version FROM message_reaction_events "
        "WHERE reaction_event_id = ?",
        ("rx-e2e-1",),
    ).fetchone()
    assert row is not None
    assert row["message_id"] == "om_rx_1"
    assert row["reaction_kind"] == "add"
    assert row["governance_version"] == REACTION_INTAKE_GOVERNANCE_VERSION


def test_ingest_chat_event_ndjson_lines_reaction_insert_exception_is_quarantined(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import lark_to_notes.live.chat_events as chat_events_mod

    def boom(*_a: object, **_k: object) -> None:
        raise RuntimeError("simulated db failure")

    monkeypatch.setattr(chat_events_mod, "insert_message_reaction_event", boom)
    conn = _conn()
    react = {
        "header": {"event_type": "im.message.reaction.created_v1", "event_id": "rx-exc-1"},
        "event": {
            "message_id": "om_rx_exc",
            "reaction_type": {"emoji_type": "THUMBSUP"},
            "operator_type": "user",
            "user_id": {"open_id": "ou_x"},
            "action_time": "1",
        },
    }
    out = ingest_chat_event_ndjson_lines(
        conn,
        [json.dumps(react)],
        source_id="dm:ou_demo",
        worker_source_type="dm_user",
        chat_type="p2p",
    )
    assert out.reaction_rows_inserted == 0
    assert out.reaction_insert_exceptions == 1
    assert out.last_reaction_quarantine_event_id == "rx-exc-1"
    assert out.last_reaction_quarantine_reason_code == "reaction_insert_exception:RuntimeError"


def test_ingest_chat_event_ndjson_lines_receive_v1_exception_is_quarantined(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import lark_to_notes.live.chat_events as chat_events_mod

    def boom(*_a: object, **_k: object) -> None:
        raise ValueError("simulated ledger failure")

    monkeypatch.setattr(chat_events_mod, "observe_chat_message", boom)
    conn = _conn()
    env = _receive_v1_envelope(message_id="om_exc_1")
    out = ingest_chat_event_ndjson_lines(
        conn,
        [json.dumps(env)],
        source_id="dm:ou_demo",
        worker_source_type="dm_user",
        chat_type="p2p",
    )
    assert out.json_objects == 1
    assert out.chat_envelopes_ingested == 0
    assert out.chat_receive_observation_exceptions == 1
    assert out.last_chat_quarantine_reason_code == "chat_receive_v1_exception:ValueError"


def test_iter_chat_event_envelopes_from_ndjson_yields_objects() -> None:
    lines = ["", json.dumps({"a": 1}), json.dumps({"b": 2})]
    out = list(iter_chat_event_envelopes_from_ndjson(lines))
    assert out == [{"a": 1}, {"b": 2}]


def _reaction_envelope(*, event_id: str, message_id: str = "om_rx_cap") -> dict[str, object]:
    return {
        "header": {"event_type": "im.message.reaction.created_v1", "event_id": event_id},
        "event": {
            "message_id": message_id,
            "reaction_type": {"emoji_type": "THUMBSUP"},
            "operator_type": "user",
            "user_id": {"open_id": "ou_x"},
            "action_time": "1",
        },
    }


def test_ingest_reaction_caps_active_requires_run_id() -> None:
    conn = _conn()
    caps = ReactionIntakeCaps(max_reaction_envelopes_per_run=1)
    react = _reaction_envelope(event_id="rx-cap-run")
    with pytest.raises(ValueError, match="reaction_intake_run_id"):
        ingest_chat_event_ndjson_lines(
            conn,
            [json.dumps(react)],
            source_id="dm:ou_demo",
            worker_source_type="dm_user",
            chat_type="p2p",
            caps=caps,
            cap_state=ReactionIntakeCapState(),
            reaction_intake_run_id=None,
        )


def test_ingest_reaction_per_run_cap_defers_with_explicit_row() -> None:
    conn = _conn()
    caps = ReactionIntakeCaps(max_reaction_envelopes_per_run=2)
    lines = [
        json.dumps(_reaction_envelope(event_id="rx-a")),
        json.dumps(_reaction_envelope(event_id="rx-b")),
        json.dumps(_reaction_envelope(event_id="rx-c")),
    ]
    out = ingest_chat_event_ndjson_lines(
        conn,
        lines,
        source_id="dm:ou_demo",
        worker_source_type="dm_user",
        chat_type="p2p",
        caps=caps,
        cap_state=ReactionIntakeCapState(),
        reaction_intake_run_id="run-cap-test",
    )
    assert out.reaction_rows_inserted == 2
    assert out.reaction_cap_deferred == 1
    assert out.last_reaction_cap_reason_code == "reaction_cap_per_run_exceeded"
    assert count_reaction_intake_deferrals(conn, run_id="run-cap-test") == 1


def test_ingest_reaction_cap_replay_duplicate_does_not_consume_cap_slot() -> None:
    conn = _conn()
    caps = ReactionIntakeCaps(max_reaction_envelopes_per_run=2)
    react = _reaction_envelope(event_id="rx-dup")
    line = json.dumps(react)
    out = ingest_chat_event_ndjson_lines(
        conn,
        [line, line],
        source_id="dm:ou_demo",
        worker_source_type="dm_user",
        chat_type="p2p",
        caps=caps,
        cap_state=ReactionIntakeCapState(),
        reaction_intake_run_id="run-dup",
    )
    assert out.reaction_rows_inserted == 1
    assert out.reaction_cap_deferred == 0
    assert out.reaction_benign_duplicate_replays == 1


def test_ingest_reaction_cap_converges_when_limit_raised() -> None:
    conn = _conn()
    lines = [
        json.dumps(_reaction_envelope(event_id="rx-c1")),
        json.dumps(_reaction_envelope(event_id="rx-c2")),
        json.dumps(_reaction_envelope(event_id="rx-c3")),
    ]
    out_tight = ingest_chat_event_ndjson_lines(
        conn,
        lines,
        source_id="dm:ou_demo",
        worker_source_type="dm_user",
        chat_type="p2p",
        caps=ReactionIntakeCaps(max_reaction_envelopes_per_run=2),
        cap_state=ReactionIntakeCapState(),
        reaction_intake_run_id="run-tight",
    )
    assert out_tight.reaction_rows_inserted == 2
    assert out_tight.reaction_cap_deferred == 1
    out_replay = ingest_chat_event_ndjson_lines(
        conn,
        lines,
        source_id="dm:ou_demo",
        worker_source_type="dm_user",
        chat_type="p2p",
        caps=ReactionIntakeCaps(max_reaction_envelopes_per_run=5),
        cap_state=ReactionIntakeCapState(),
        reaction_intake_run_id="run-wide",
    )
    assert out_replay.reaction_rows_inserted == 1
    assert out_replay.reaction_cap_deferred == 0
    assert out_replay.reaction_benign_duplicate_replays == 2
    total = conn.execute("SELECT COUNT(*) AS c FROM message_reaction_events").fetchone()
    assert int(total["c"]) == 3


def test_ingest_reaction_cap_retry_storm_many_duplicates_respects_tight_cap() -> None:
    """Under per-run cap=1, many identical replays must not defer after the first insert."""

    conn = _conn()
    caps = ReactionIntakeCaps(max_reaction_envelopes_per_run=1)
    react = _reaction_envelope(event_id="rx-storm-dup")
    line = json.dumps(react)
    lines = [line] * 50
    out = ingest_chat_event_ndjson_lines(
        conn,
        lines,
        source_id="dm:ou_demo",
        worker_source_type="dm_user",
        chat_type="p2p",
        caps=caps,
        cap_state=ReactionIntakeCapState(),
        reaction_intake_run_id="run-storm-dup",
    )
    assert out.reaction_rows_inserted == 1
    assert out.reaction_cap_deferred == 0
    assert out.reaction_benign_duplicate_replays == 49
    total = conn.execute("SELECT COUNT(*) AS c FROM message_reaction_events").fetchone()
    assert int(total["c"]) == 1


def test_ingest_reaction_cap_repeated_batch_no_row_growth_beyond_policy() -> None:
    """Replaying the same capped NDJSON converges: total rows never exceed unique reactions."""

    conn = _conn()
    caps = ReactionIntakeCaps(max_reaction_envelopes_per_run=2)
    lines = [
        json.dumps(_reaction_envelope(event_id="rx-rp-a")),
        json.dumps(_reaction_envelope(event_id="rx-rp-b")),
        json.dumps(_reaction_envelope(event_id="rx-rp-c")),
    ]
    for i in range(10):
        out = ingest_chat_event_ndjson_lines(
            conn,
            lines,
            source_id="dm:ou_demo",
            worker_source_type="dm_user",
            chat_type="p2p",
            caps=caps,
            cap_state=ReactionIntakeCapState(),
            reaction_intake_run_id=f"run-repeat-{i}",
        )
        if i == 0:
            assert out.reaction_rows_inserted == 2
            assert out.reaction_cap_deferred == 1
            assert out.reaction_benign_duplicate_replays == 0
        elif i == 1:
            assert out.reaction_rows_inserted == 1
            assert out.reaction_cap_deferred == 0
            assert out.reaction_benign_duplicate_replays == 2
        else:
            assert out.reaction_rows_inserted == 0
            assert out.reaction_cap_deferred == 0
            assert out.reaction_benign_duplicate_replays == 3
    total = conn.execute("SELECT COUNT(*) AS c FROM message_reaction_events").fetchone()
    assert int(total["c"]) == 3


def test_ingest_reaction_cap_unique_stream_each_pass_respects_per_run_cap() -> None:
    """A large unique batch is split by the cap; a replay cannot insert more than cap in one pass."""

    conn = _conn()
    cap_n = 5
    caps = ReactionIntakeCaps(max_reaction_envelopes_per_run=cap_n)
    lines = [json.dumps(_reaction_envelope(event_id=f"rx-uniq-{i}")) for i in range(20)]
    out1 = ingest_chat_event_ndjson_lines(
        conn,
        lines,
        source_id="dm:ou_demo",
        worker_source_type="dm_user",
        chat_type="p2p",
        caps=caps,
        cap_state=ReactionIntakeCapState(),
        reaction_intake_run_id="run-uniq-1",
    )
    assert out1.reaction_rows_inserted == cap_n
    assert out1.reaction_cap_deferred == 20 - cap_n
    assert out1.reaction_benign_duplicate_replays == 0
    out2 = ingest_chat_event_ndjson_lines(
        conn,
        lines,
        source_id="dm:ou_demo",
        worker_source_type="dm_user",
        chat_type="p2p",
        caps=caps,
        cap_state=ReactionIntakeCapState(),
        reaction_intake_run_id="run-uniq-2",
    )
    assert out2.reaction_rows_inserted == cap_n
    assert out2.reaction_cap_deferred == 10
    assert out2.reaction_benign_duplicate_replays == cap_n
    total = conn.execute("SELECT COUNT(*) AS c FROM message_reaction_events").fetchone()
    assert int(total["c"]) == 2 * cap_n
