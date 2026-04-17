"""Failure-path and operator-log coverage for reaction NDJSON ingest (lw-pzj.10.10)."""

from __future__ import annotations

import json
import logging
import sqlite3

import pytest

from lark_to_notes.intake.reaction_caps import ReactionIntakeCaps, ReactionIntakeCapState
from lark_to_notes.live.chat_events import ingest_chat_event_ndjson_lines
from lark_to_notes.storage.db import connect, init_db


def _conn() -> sqlite3.Connection:
    c = connect(":memory:")
    init_db(c)
    return c


def test_truncated_json_line_emits_skip_reason(caplog: pytest.LogCaptureFixture) -> None:
    """Partial NDJSON must not abort the ingest loop; skip is logged."""

    caplog.set_level(logging.INFO, logger="lark_to_notes.live.chat_events")
    conn = _conn()
    out = ingest_chat_event_ndjson_lines(
        conn,
        ['{"header":'],
        source_id="dm:ou_demo",
        worker_source_type="dm_user",
        chat_type="p2p",
    )
    assert out.json_objects == 0
    assert any(
        getattr(r, "reason", None) == "json_decode_error" and r.msg == "chat_event_ndjson_skip"
        for r in caplog.records
    )


def test_reaction_envelope_quarantine_log_includes_operator_fields(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Validation quarantine logs stable reason_code, payload_hash, and bounded excerpt."""

    caplog.set_level(logging.INFO, logger="lark_to_notes.live.chat_events")
    bad = {
        "header": {"event_type": "im.message.reaction.created_v1", "event_id": "rx-log-1"},
        "event": {"message_id": "om_log"},
    }
    conn = _conn()
    ingest_chat_event_ndjson_lines(
        conn,
        [json.dumps(bad)],
        source_id="dm:ou_demo",
        worker_source_type="dm_user",
        chat_type="p2p",
    )
    hit = [r for r in caplog.records if r.msg == "reaction_envelope_quarantined"]
    assert len(hit) == 1
    r = hit[0]
    assert getattr(r, "reason_code", "").startswith("reaction_envelope_invalid:")
    assert len(getattr(r, "payload_hash", "")) == 16
    assert getattr(r, "event_id", "") == "rx-log-1"
    assert getattr(r, "source_id", "") == "dm:ou_demo"
    excerpt = getattr(r, "payload_excerpt", "")
    assert isinstance(excerpt, str) and len(excerpt) <= 257


def test_reaction_cap_deferral_log_includes_operator_fields(caplog: pytest.LogCaptureFixture) -> None:
    """Cap deferral logs reason_code, payload_hash, run_id, and excerpt for triage."""

    caplog.set_level(logging.INFO, logger="lark_to_notes.live.chat_events")
    def _env(eid: str) -> dict[str, object]:
        return {
            "header": {"event_type": "im.message.reaction.created_v1", "event_id": eid},
            "event": {
                "message_id": "om_cap_log",
                "reaction_type": {"emoji_type": "THUMBSUP"},
                "operator_type": "user",
                "user_id": {"open_id": "ou_x"},
                "action_time": "1",
            },
        }

    lines = [
        json.dumps(_env("rx-cap-log-a")),
        json.dumps(_env("rx-cap-log-b")),
        json.dumps(_env("rx-cap-log-c")),
    ]
    conn = _conn()
    ingest_chat_event_ndjson_lines(
        conn,
        lines,
        source_id="dm:ou_demo",
        worker_source_type="dm_user",
        chat_type="p2p",
        caps=ReactionIntakeCaps(max_reaction_envelopes_per_run=2),
        cap_state=ReactionIntakeCapState(),
        reaction_intake_run_id="run-cap-log",
    )
    hits = [r for r in caplog.records if r.msg == "reaction_intake_cap_deferred"]
    assert len(hits) == 1
    r = hits[0]
    assert getattr(r, "reason_code", "") == "reaction_cap_per_run_exceeded"
    assert len(getattr(r, "payload_hash", "")) == 16
    assert getattr(r, "event_id", "") == "rx-cap-log-c"
    assert getattr(r, "run_id", "") == "run-cap-log"
    excerpt = getattr(r, "payload_excerpt", "")
    assert isinstance(excerpt, str) and len(excerpt) <= 257
