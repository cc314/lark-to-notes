"""Tests for :mod:`lark_to_notes.intake.reaction_replay`."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from lark_to_notes.cli import run
from lark_to_notes.intake.ledger import count_raw_messages
from lark_to_notes.intake.reaction_model import NormalizedReactionEvent, ReactionKind
from lark_to_notes.intake.reaction_replay import (
    REPLAY_STAGE_REACTION_ORPHAN_RELINK,
    replay_orphan_reactions,
)
from lark_to_notes.intake.reaction_store import (
    count_reaction_orphan_queue,
    insert_message_reaction_event,
)
from lark_to_notes.storage.db import connect, init_db


def _event(
    *,
    eid: str,
    message_id: str = "om_1",
    source_id: str = "dm:test",
) -> NormalizedReactionEvent:
    return NormalizedReactionEvent(
        reaction_event_id=eid,
        source_id=source_id,
        message_id=message_id,
        reaction_kind=ReactionKind.ADD,
        emoji_type="OK",
        operator_type="user",
        operator_open_id="ou_a",
        operator_user_id="",
        operator_union_id="",
        action_time="1",
        payload={"header": {}, "event": {"message_id": message_id}},
    )


@pytest.fixture
def mem() -> sqlite3.Connection:
    c = connect(":memory:")
    init_db(c)
    return c


def test_replay_links_orphan_when_raw_row_exists_later(mem: sqlite3.Connection) -> None:
    insert_message_reaction_event(
        mem,
        _event(eid="ev-replay-1"),
        governance_version="g1",
        policy_version="p1",
    )
    assert count_reaction_orphan_queue(mem) == 1
    n_raw_before = count_raw_messages(mem)
    mem.execute(
        """
        INSERT INTO raw_messages (
            message_id, source_id, source_type, chat_id, chat_type,
            sender_id, sender_name, direction, created_at, content, payload_json
        )
        VALUES (?, ?, 'dm_user', 'c1', 'p2p', '', '', 'incoming',
                '2026-01-01T00:00:00Z', 'hi', '{}')
        """,
        ("om_1", "dm:test"),
    )
    mem.commit()
    row = mem.execute(
        "SELECT raw_message_present FROM message_reaction_events WHERE reaction_event_id = ?",
        ("ev-replay-1",),
    ).fetchone()
    assert int(row["raw_message_present"]) == 0

    summary = replay_orphan_reactions(mem)
    assert summary.pairs_processed == 1
    assert summary.rows_attached == 1
    assert summary.pairs_idempotent_noop == 0

    assert count_raw_messages(mem) == n_raw_before + 1
    row2 = mem.execute(
        "SELECT raw_message_present FROM message_reaction_events WHERE reaction_event_id = ?",
        ("ev-replay-1",),
    ).fetchone()
    assert int(row2["raw_message_present"]) == 1
    assert count_reaction_orphan_queue(mem) == 0


def test_replay_idempotent_second_pass(mem: sqlite3.Connection) -> None:
    insert_message_reaction_event(mem, _event(eid="ev-replay-2"))
    mem.execute(
        """
        INSERT INTO raw_messages (
            message_id, source_id, source_type, chat_id, chat_type,
            sender_id, sender_name, direction, created_at, content, payload_json
        )
        VALUES (?, ?, 'dm_user', 'c1', 'p2p', '', '', 'incoming',
                '2026-01-01T00:00:00Z', 'hi', '{}')
        """,
        ("om_1", "dm:test"),
    )
    mem.commit()
    first = replay_orphan_reactions(mem)
    assert first.rows_attached == 1
    second = replay_orphan_reactions(mem)
    assert second.pairs_processed == 0
    assert second.rows_attached == 0


def test_replay_progress_contract(mem: sqlite3.Connection) -> None:
    insert_message_reaction_event(
        mem, _event(eid="ev-replay-3"), governance_version="gv", policy_version="pv"
    )
    mem.execute(
        """
        INSERT INTO raw_messages (
            message_id, source_id, source_type, chat_id, chat_type,
            sender_id, sender_name, direction, created_at, content, payload_json
        )
        VALUES (?, ?, 'dm_user', 'c1', 'p2p', '', '', 'incoming',
                '2026-01-01T00:00:00Z', 'hi', '{}')
        """,
        ("om_1", "dm:test"),
    )
    mem.commit()
    lines: list[dict[str, object]] = []

    replay_orphan_reactions(mem, progress=lines.append)

    assert len(lines) == 1
    p = lines[0]
    expected_keys = {
        "stage",
        "source_id",
        "message_id",
        "rows_processed",
        "rows_skipped_idempotent",
        "cursor_high_water",
        "governance_version",
        "policy_version",
        "duration_ms",
    }
    assert set(p.keys()) == expected_keys
    assert p["stage"] == REPLAY_STAGE_REACTION_ORPHAN_RELINK
    assert p["source_id"] == "dm:test"
    assert p["message_id"] == "om_1"
    assert p["governance_version"] == "gv"
    assert p["policy_version"] == "pv"
    assert p["rows_processed"] == 1
    assert p["rows_skipped_idempotent"] == 0
    assert isinstance(p["duration_ms"], int | float)


def test_replay_source_id_filter_skips_non_matching(mem: sqlite3.Connection) -> None:
    insert_message_reaction_event(mem, _event(eid="ev-replay-4", source_id="dm:test"))
    mem.execute(
        """
        INSERT INTO raw_messages (
            message_id, source_id, source_type, chat_id, chat_type,
            sender_id, sender_name, direction, created_at, content, payload_json
        )
        VALUES (?, ?, 'dm_user', 'c1', 'p2p', '', '', 'incoming',
                '2026-01-01T00:00:00Z', 'hi', '{}')
        """,
        ("om_1", "dm:test"),
    )
    mem.commit()
    summary = replay_orphan_reactions(mem, source_ids=frozenset(["dm:other"]))
    assert summary.pairs_processed == 0
    row = mem.execute(
        "SELECT raw_message_present FROM message_reaction_events WHERE reaction_event_id = ?",
        ("ev-replay-4",),
    ).fetchone()
    assert int(row["raw_message_present"]) == 0


def test_replay_does_not_mutate_existing_raw_rows(mem: sqlite3.Connection) -> None:
    """Replay only updates reaction rows; raw_messages content stays byte-stable for the PK."""

    insert_message_reaction_event(mem, _event(eid="ev-replay-5"))
    mem.execute(
        """
        INSERT INTO raw_messages (
            message_id, source_id, source_type, chat_id, chat_type,
            sender_id, sender_name, direction, created_at, content, payload_json
        )
        VALUES (?, ?, 'dm_user', 'c1', 'p2p', 's1', 'Bob', 'incoming',
                '2026-01-01T00:00:00Z', 'hello', '{"k":1}')
        """,
        ("om_1", "dm:test"),
    )
    mem.commit()
    before = mem.execute(
        "SELECT content, payload_json FROM raw_messages WHERE message_id = ?", ("om_1",)
    ).fetchone()
    replay_orphan_reactions(mem)
    after = mem.execute(
        "SELECT content, payload_json FROM raw_messages WHERE message_id = ?", ("om_1",)
    ).fetchone()
    assert dict(before) == dict(after)


def test_cli_replay_reactions_json_and_ndjson(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "rx.db"
    conn = connect(db_path)
    init_db(conn)
    insert_message_reaction_event(conn, _event(eid="ev-cli-rx"))
    conn.execute(
        """
        INSERT INTO raw_messages (
            message_id, source_id, source_type, chat_id, chat_type,
            sender_id, sender_name, direction, created_at, content, payload_json
        )
        VALUES (?, ?, 'dm_user', 'c1', 'p2p', '', '', 'incoming',
                '2026-01-01T00:00:00Z', 'hi', '{}')
        """,
        ("om_1", "dm:test"),
    )
    conn.commit()
    conn.close()

    exit_code = run(
        [
            "replay-reactions",
            "--db",
            str(db_path),
            "--json",
            "--progress-ndjson",
        ],
    )
    captured = capsys.readouterr()
    assert exit_code == 0
    summary = json.loads(captured.out)
    assert summary["pairs_processed"] == 1
    assert summary["rows_attached"] == 1
    prog = json.loads(captured.err.strip().splitlines()[0])
    assert prog["stage"] == REPLAY_STAGE_REACTION_ORPHAN_RELINK
