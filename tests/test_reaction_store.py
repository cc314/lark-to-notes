"""Tests for :mod:`lark_to_notes.intake.reaction_store`."""

from __future__ import annotations

import logging
import sqlite3
from typing import Any

import pytest

from lark_to_notes.intake.ledger import chat_ingest_key, insert_raw_message
from lark_to_notes.intake.models import RawMessage
from lark_to_notes.intake.reaction_model import NormalizedReactionEvent, ReactionKind
from lark_to_notes.intake.reaction_store import (
    canonical_reaction_event_id,
    count_reaction_orphan_queue,
    insert_message_reaction_event,
    reaction_attach_reconcile_latency_ms,
    reaction_correlation_counts,
    reaction_ledger_governance_sample_for_doctor,
    reaction_orphan_backlog_metrics,
    surrogate_reaction_event_id,
)
from lark_to_notes.storage.db import connect, init_db


def _event(
    *,
    eid: str = "",
    emoji: str = "OK",
    message_id: str = "om_1",
    action_time: str = "1",
    operator_open_id: str = "ou_a",
) -> NormalizedReactionEvent:
    return NormalizedReactionEvent(
        reaction_event_id=eid,
        source_id="dm:test",
        message_id=message_id,
        reaction_kind=ReactionKind.ADD,
        emoji_type=emoji,
        operator_type="user",
        operator_open_id=operator_open_id,
        operator_user_id="",
        operator_union_id="",
        action_time=action_time,
        payload={"header": {}, "event": {"message_id": message_id}},
    )


@pytest.fixture
def mem() -> sqlite3.Connection:
    c = connect(":memory:")
    init_db(c)
    return c


def test_surrogate_stable() -> None:
    ev = _event(eid="")
    s1 = surrogate_reaction_event_id(ev)
    s2 = surrogate_reaction_event_id(ev)
    assert s1 == s2
    assert s1.startswith("rxs_")


def test_canonical_prefers_upstream_id() -> None:
    ev = _event(eid="  feishu-id  ")
    assert canonical_reaction_event_id(ev) == "feishu-id"


def test_insert_stamps_governance_and_policy_versions(mem: sqlite3.Connection) -> None:
    ev = _event(eid="ev-gov")
    insert_message_reaction_event(mem, ev, governance_version="gov_x", policy_version="pol_y")
    row = mem.execute(
        "SELECT governance_version, policy_version FROM message_reaction_events WHERE reaction_event_id = ?",
        ("ev-gov",),
    ).fetchone()
    assert row is not None
    assert row["governance_version"] == "gov_x"
    assert row["policy_version"] == "pol_y"


def test_governance_sample_for_doctor_detects_drift(mem: sqlite3.Connection) -> None:
    insert_message_reaction_event(mem, _event(eid="a1"), governance_version="1", policy_version="")
    insert_message_reaction_event(
        mem, _event(eid="a2"), governance_version="legacy-x", policy_version=""
    )
    sample = reaction_ledger_governance_sample_for_doctor(mem, limit=10)
    assert sample["hints"]["drift_from_builtin_governance"] is True
    assert sample["compare_as_of"]["mismatch_vs_runtime_intake_caps"] is True
    dom = sample["compare_as_of"]["dominant_ledger_tuple"]
    assert dom is not None
    assert dom["row_count"] == 1
    versions = {t["governance_version"] for t in sample["tuples"]}
    assert "1" in versions
    assert "legacy-x" in versions


def test_governance_sample_compare_as_of_no_mismatch_when_builtin_aligned(
    mem: sqlite3.Connection,
) -> None:
    from lark_to_notes.intake.reaction_caps import (
        REACTION_INTAKE_GOVERNANCE_VERSION,
        REACTION_INTAKE_POLICY_VERSION,
    )

    insert_message_reaction_event(
        mem,
        _event(eid="b1"),
        governance_version=REACTION_INTAKE_GOVERNANCE_VERSION,
        policy_version=REACTION_INTAKE_POLICY_VERSION,
    )
    sample = reaction_ledger_governance_sample_for_doctor(mem, limit=10)
    assert sample["compare_as_of"]["mismatch_vs_runtime_intake_caps"] is False
    assert sample["hints"]["drift_from_builtin_governance"] is False


def test_governance_sample_policy_drift_triggers_mismatch(mem: sqlite3.Connection) -> None:
    from lark_to_notes.intake.reaction_caps import REACTION_INTAKE_GOVERNANCE_VERSION

    insert_message_reaction_event(
        mem,
        _event(eid="c1"),
        governance_version=REACTION_INTAKE_GOVERNANCE_VERSION,
        policy_version="pinned-policy-x",
    )
    sample = reaction_ledger_governance_sample_for_doctor(mem, limit=10)
    assert sample["compare_as_of"]["mismatch_vs_runtime_intake_caps"] is True


def test_insert_idempotent(mem: sqlite3.Connection) -> None:
    ev = _event(eid="evdup")
    r1 = insert_message_reaction_event(mem, ev)
    assert r1.inserted is True
    assert r1.raw_message_present is False
    assert r1.chat_ingest_fingerprint == chat_ingest_key(ev.source_id, ev.message_id)
    r2 = insert_message_reaction_event(mem, ev)
    assert r2.inserted is False
    assert r2.reaction_event_id == "evdup"
    n = mem.execute("SELECT COUNT(*) AS c FROM message_reaction_events").fetchone()
    assert int(n[0]) == 1


def test_insert_surrogate_idempotent(mem: sqlite3.Connection) -> None:
    ev = _event(eid="")
    rid = canonical_reaction_event_id(ev)
    assert insert_message_reaction_event(mem, ev).inserted is True
    assert insert_message_reaction_event(mem, ev).inserted is False
    row = mem.execute(
        "SELECT reaction_event_id FROM message_reaction_events WHERE reaction_event_id = ?",
        (rid,),
    ).fetchone()
    assert row is not None


def test_raw_message_present_when_raw_exists(mem: sqlite3.Connection) -> None:
    mem.execute(
        """
        INSERT INTO raw_messages (
            message_id, source_id, source_type, chat_id, chat_type,
            sender_id, sender_name, direction, created_at, content, payload_json
        )
        VALUES (?, ?, 'dm', 'c1', 'p2p', '', '', 'incoming', '2026-01-01T00:00:00Z', '', '{}')
        """,
        ("om_1", "dm:test"),
    )
    mem.commit()
    ev = _event(eid="ev-has-raw")
    res = insert_message_reaction_event(mem, ev)
    assert res.inserted is True
    assert res.raw_message_present is True
    row = mem.execute(
        "SELECT raw_message_present, chat_ingest_fingerprint FROM message_reaction_events "
        "WHERE reaction_event_id = ?",
        ("ev-has-raw",),
    ).fetchone()
    assert int(row[0]) == 1
    assert row[1] == chat_ingest_key("dm:test", "om_1")


def test_message_reaction_events_table_has_expected_columns(mem: sqlite3.Connection) -> None:
    """Migrations must materialize the reaction ledger with v10 correlation columns."""

    rows = mem.execute("PRAGMA table_info(message_reaction_events)").fetchall()
    names = {str(r["name"]) for r in rows}
    assert "reaction_event_id" in names
    assert "payload_json" in names
    assert "first_seen_at" in names
    assert "chat_ingest_fingerprint" in names
    assert "raw_message_present" in names


def test_reaction_render_order_action_time_then_event_id(mem: sqlite3.Connection) -> None:
    """Stable timeline for vault/render: tie-break on ``reaction_event_id`` when ``action_time`` ties."""

    insert_message_reaction_event(mem, _event(eid="ev_z", action_time="9"))
    insert_message_reaction_event(mem, _event(eid="ev_a", action_time="9"))
    insert_message_reaction_event(mem, _event(eid="ev_m", action_time="9"))
    ordered = mem.execute(
        """
        SELECT reaction_event_id FROM message_reaction_events
        WHERE source_id = ? AND message_id = ?
        ORDER BY action_time ASC, reaction_event_id ASC
        """,
        ("dm:test", "om_1"),
    ).fetchall()
    assert [r["reaction_event_id"] for r in ordered] == ["ev_a", "ev_m", "ev_z"]


def test_reaction_render_order_scoped_per_message(mem: sqlite3.Connection) -> None:
    """Ordering query must not bleed reactions across ``message_id``."""

    insert_message_reaction_event(mem, _event(eid="e1", message_id="om_a", action_time="1"))
    insert_message_reaction_event(mem, _event(eid="e2", message_id="om_b", action_time="1"))
    insert_message_reaction_event(mem, _event(eid="e3", message_id="om_a", action_time="2"))
    ordered = mem.execute(
        """
        SELECT reaction_event_id FROM message_reaction_events
        WHERE source_id = ? AND message_id = ?
        ORDER BY action_time ASC, reaction_event_id ASC
        """,
        ("dm:test", "om_a"),
    ).fetchall()
    assert [r["reaction_event_id"] for r in ordered] == ["e1", "e3"]


def test_upstream_idempotency_ignores_volatile_payload_header(mem: sqlite3.Connection) -> None:
    """Same ``header.event_id`` must dedupe even when envelope copy in ``payload_json`` differs."""

    base = _event(eid="feishu-stable")
    alt = NormalizedReactionEvent(
        reaction_event_id="feishu-stable",
        source_id=base.source_id,
        message_id=base.message_id,
        reaction_kind=base.reaction_kind,
        emoji_type=base.emoji_type,
        operator_type=base.operator_type,
        operator_open_id=base.operator_open_id,
        operator_user_id=base.operator_user_id,
        operator_union_id=base.operator_union_id,
        action_time=base.action_time,
        payload={
            "header": {"event_id": "feishu-stable", "create_time": "999"},
            "event": {"message_id": "om_1"},
        },
    )
    assert insert_message_reaction_event(mem, base).inserted is True
    assert insert_message_reaction_event(mem, alt).inserted is False
    n = mem.execute("SELECT COUNT(*) AS c FROM message_reaction_events").fetchone()
    assert int(n["c"]) == 1


def test_reaction_correlation_counts_join(mem: sqlite3.Connection) -> None:
    assert reaction_correlation_counts(mem) == {
        "total": 0,
        "linked_to_raw_message": 0,
        "orphan": 0,
    }
    insert_message_reaction_event(mem, _event(eid="orph1"))
    assert reaction_correlation_counts(mem) == {
        "total": 1,
        "linked_to_raw_message": 0,
        "orphan": 1,
    }
    mem.execute(
        """
        INSERT INTO raw_messages (
            message_id, source_id, source_type, chat_id, chat_type,
            sender_id, sender_name, direction, created_at, content, payload_json
        )
        VALUES (?, ?, 'dm', 'c1', 'p2p', '', '', 'incoming', '2026-01-01T00:00:00Z', '', '{}')
        """,
        ("om_1", "dm:test"),
    )
    mem.commit()
    assert reaction_correlation_counts(mem) == {
        "total": 1,
        "linked_to_raw_message": 1,
        "orphan": 0,
    }


def test_insert_raw_message_reconciliation_log_extra_fields(
    mem: sqlite3.Connection, caplog: pytest.LogCaptureFixture
) -> None:
    """``insert_raw_message`` emits structured ``reaction_orphan_reconciled`` (lw-pzj.15.2)."""

    caplog.set_level(logging.INFO, "lark_to_notes.intake.ledger")
    insert_message_reaction_event(mem, _event(eid="ev-orph-log"))
    msg = RawMessage(
        message_id="om_1",
        source_id="dm:test",
        source_type="dm_user",
        chat_id="ou_c",
        chat_type="p2p",
        sender_id="ou_s",
        sender_name="Bob",
        direction="incoming",
        created_at="2026-01-01T00:00:00Z",
        content="hi",
        payload={"content": "hi"},
    )
    assert insert_raw_message(mem, msg) is True
    hits = [r for r in caplog.records if r.msg == "reaction_orphan_reconciled"]
    assert len(hits) == 1
    rec: Any = hits[0]
    assert len(getattr(rec, "orphan_batch_id", "")) == 32
    assert getattr(rec, "message_id", None) == "om_1"
    assert getattr(rec, "source_id", None) == "dm:test"
    assert getattr(rec, "attached_count", None) == 1
    assert getattr(rec, "still_orphan", None) == 0
    assert getattr(rec, "still_orphan_pair", None) == 0
    assert isinstance(getattr(rec, "elapsed_ms", None), int | float)


def test_insert_raw_message_records_reconcile_observation(mem: sqlite3.Connection) -> None:
    """Successful orphan attach persists one ``reaction_reconcile_observations`` row (lw-pzj.15.3)."""

    insert_message_reaction_event(mem, _event(eid="ev-orph-obs"))
    msg = RawMessage(
        message_id="om_1",
        source_id="dm:test",
        source_type="dm_user",
        chat_id="ou_c",
        chat_type="p2p",
        sender_id="ou_s",
        sender_name="Bob",
        direction="incoming",
        created_at="2026-01-01T00:00:00Z",
        content="hi",
        payload={"content": "hi"},
    )
    assert insert_raw_message(mem, msg) is True
    n = mem.execute("SELECT COUNT(*) AS c FROM reaction_reconcile_observations").fetchone()
    assert int(n["c"]) == 1
    lat = reaction_attach_reconcile_latency_ms(mem)
    assert lat["attach_reconcile_sample_count"] == 1
    assert lat["attach_reconcile_ms_p50"] is not None
    bl = reaction_orphan_backlog_metrics(mem)
    assert bl["queue_depth"] == 0


def test_insert_raw_message_links_preexisting_orphan_reactions(mem: sqlite3.Connection) -> None:
    """Orphan reactions (no raw row at insert time) attach when ``insert_raw_message`` runs."""

    insert_message_reaction_event(mem, _event(eid="ev-orph-link"))
    assert count_reaction_orphan_queue(mem) == 1
    row = mem.execute(
        "SELECT raw_message_present FROM message_reaction_events WHERE reaction_event_id = ?",
        ("ev-orph-link",),
    ).fetchone()
    assert int(row[0]) == 0
    msg = RawMessage(
        message_id="om_1",
        source_id="dm:test",
        source_type="dm_user",
        chat_id="ou_c",
        chat_type="p2p",
        sender_id="ou_s",
        sender_name="Bob",
        direction="incoming",
        created_at="2026-01-01T00:00:00Z",
        content="hi",
        payload={"content": "hi"},
    )
    assert insert_raw_message(mem, msg) is True
    row2 = mem.execute(
        "SELECT raw_message_present FROM message_reaction_events WHERE reaction_event_id = ?",
        ("ev-orph-link",),
    ).fetchone()
    assert int(row2[0]) == 1
    assert count_reaction_orphan_queue(mem) == 0


def test_insert_raw_message_idempotent_still_links_orphans(mem: sqlite3.Connection) -> None:
    """Second ``insert_raw_message`` (INSERT OR IGNORE) still runs linkage for the pair."""

    insert_message_reaction_event(mem, _event(eid="ev-orph-idem"))
    msg = RawMessage(
        message_id="om_1",
        source_id="dm:test",
        source_type="dm_user",
        chat_id="ou_c",
        chat_type="p2p",
        sender_id="ou_s",
        sender_name="Bob",
        direction="incoming",
        created_at="2026-01-01T00:00:00Z",
        content="hi",
        payload={"content": "hi"},
    )
    assert insert_raw_message(mem, msg) is True
    assert insert_raw_message(mem, msg) is False
    row = mem.execute(
        "SELECT raw_message_present FROM message_reaction_events WHERE reaction_event_id = ?",
        ("ev-orph-idem",),
    ).fetchone()
    assert int(row[0]) == 1
    assert count_reaction_orphan_queue(mem) == 0


def test_orphan_queue_replay_updates_last_seen(mem: sqlite3.Connection) -> None:
    """Duplicate inserts while still orphan bump ``last_seen_at`` only."""

    ev = _event(eid="ev-orph-replay")
    insert_message_reaction_event(mem, ev)
    first = mem.execute(
        "SELECT first_queued_at, last_seen_at FROM reaction_orphan_queue WHERE reaction_event_id = ?",
        ("ev-orph-replay",),
    ).fetchone()
    assert first is not None
    insert_message_reaction_event(mem, ev)
    second = mem.execute(
        "SELECT first_queued_at, last_seen_at FROM reaction_orphan_queue WHERE reaction_event_id = ?",
        ("ev-orph-replay",),
    ).fetchone()
    assert second is not None
    assert second["first_queued_at"] == first["first_queued_at"]
    assert second["last_seen_at"] >= first["last_seen_at"]


def test_linked_reaction_never_queues_orphan(mem: sqlite3.Connection) -> None:
    """When ``raw_messages`` already has the parent, no orphan-queue row is created."""

    mem.execute(
        """
        INSERT INTO raw_messages (
            message_id, source_id, source_type, chat_id, chat_type,
            sender_id, sender_name, direction, created_at, content, payload_json
        )
        VALUES (?, ?, 'dm', 'c1', 'p2p', '', '', 'incoming', '2026-01-01T00:00:00Z', '', '{}')
        """,
        ("om_1", "dm:test"),
    )
    mem.commit()
    insert_message_reaction_event(mem, _event(eid="ev-linked"))
    assert count_reaction_orphan_queue(mem) == 0
    row = mem.execute(
        "SELECT raw_message_present FROM message_reaction_events WHERE reaction_event_id = ?",
        ("ev-linked",),
    ).fetchone()
    assert int(row[0]) == 1


def test_orphan_backlog_metrics_age_buckets(mem: sqlite3.Connection) -> None:
    """Age histogram buckets reflect ``first_queued_at`` (lw-pzj.15.3)."""

    from datetime import UTC, datetime, timedelta

    now = datetime.now(UTC)
    rows_spec: list[tuple[str, str, str, str]] = [
        ("q-b1", "dm:x", "m1", (now - timedelta(seconds=30)).strftime("%Y-%m-%dT%H:%M:%SZ")),
        ("q-b2", "dm:x", "m2", (now - timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ")),
        ("q-b3", "dm:x", "m3", (now - timedelta(hours=5)).strftime("%Y-%m-%dT%H:%M:%SZ")),
        ("q-b4", "dm:x", "m4", (now - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")),
    ]
    for rid, src, mid, ts in rows_spec:
        mem.execute(
            """
            INSERT INTO reaction_orphan_queue (
                reaction_event_id, source_id, message_id, first_queued_at, last_seen_at
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (rid, src, mid, ts, ts),
        )
    mem.commit()
    m = reaction_orphan_backlog_metrics(mem)
    assert m["queue_depth"] == 4
    assert m["timestamp_parse_skips"] == 0
    bc = m["age_bucket_counts"]
    assert bc["lt_1m"] == 1
    assert bc["1m_to_1h"] == 1
    assert bc["1h_to_24h"] == 1
    assert bc["gte_24h"] == 1
    assert m["dwell_seconds_p50"] is not None
    assert m["dwell_seconds_p90"] is not None
