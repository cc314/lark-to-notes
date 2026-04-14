"""Tests for watched-source governance: source model, schema, and storage."""

from __future__ import annotations

import sqlite3

import lark_to_notes.config.sources as sources_mod
import lark_to_notes.storage.db as db_mod
import lark_to_notes.storage.schema as schema_mod
from lark_to_notes.config.sources import (
    Checkpoint,
    SourceType,
    WatchedSource,
    make_source_id,
)
from lark_to_notes.storage.db import (
    connect,
    get_checkpoint,
    get_watched_source,
    init_db,
    list_watched_sources,
    upsert_checkpoint,
    upsert_watched_source,
)
from lark_to_notes.storage.schema import SCHEMA_VERSION, all_versions

# ---------------------------------------------------------------------------
# Source model unit tests
# ---------------------------------------------------------------------------


def test_make_source_id_format() -> None:
    sid = make_source_id(SourceType.DM, "abc123")
    assert sid == "dm:abc123"


def test_make_source_id_all_types() -> None:
    assert make_source_id(SourceType.GROUP, "g1").startswith("group:")
    assert make_source_id(SourceType.DOC, "d1").startswith("doc:")


def test_watched_source_defaults() -> None:
    src = WatchedSource(
        source_id="dm:x",
        source_type=SourceType.DM,
        external_id="x",
        name="Test DM",
    )
    assert src.enabled is True
    assert src.config == {}
    assert src.created_at != ""


def test_watched_source_config_json_roundtrip() -> None:
    src = WatchedSource(
        source_id="dm:x",
        source_type=SourceType.DM,
        external_id="x",
        name="Test",
        config={"fetch_days": 7, "label": "inbox"},
    )
    serialised = src.config_json()
    assert '"fetch_days": 7' in serialised
    assert '"label": "inbox"' in serialised


def test_watched_source_from_row_roundtrip() -> None:
    original = WatchedSource(
        source_id="group:grp1",
        source_type=SourceType.GROUP,
        external_id="grp1",
        name="Engineering Sync",
    )
    row = {
        "source_id": original.source_id,
        "source_type": str(original.source_type),
        "external_id": original.external_id,
        "name": original.name,
        "enabled": 1,
        "config_json": "{}",
        "created_at": original.created_at,
        "updated_at": original.updated_at,
    }
    restored = WatchedSource.from_row(row)
    assert restored == original


def test_checkpoint_defaults() -> None:
    cp = Checkpoint(source_id="dm:x")
    assert cp.last_message_id is None
    assert cp.last_message_timestamp is None
    assert cp.page_token is None
    assert cp.updated_at != ""


def test_checkpoint_from_row_roundtrip() -> None:
    original = Checkpoint(
        source_id="dm:x",
        last_message_id="msg42",
        last_message_timestamp="2026-04-14T10:00:00Z",
        page_token="tok_abc",
    )
    row = {
        "source_id": original.source_id,
        "last_message_id": original.last_message_id,
        "last_message_timestamp": original.last_message_timestamp,
        "page_token": original.page_token,
        "updated_at": original.updated_at,
    }
    restored = Checkpoint.from_row(row)
    assert restored == original


# ---------------------------------------------------------------------------
# Schema unit tests
# ---------------------------------------------------------------------------


def test_schema_version_constant() -> None:
    assert max(all_versions()) == SCHEMA_VERSION


def test_all_versions_returns_sorted_list() -> None:
    versions = all_versions()
    assert versions == sorted(versions)
    assert 1 in versions


# ---------------------------------------------------------------------------
# Storage integration tests (in-memory SQLite)
# ---------------------------------------------------------------------------


def _mem_conn() -> sqlite3.Connection:
    conn = connect(":memory:")
    init_db(conn)
    return conn


def test_init_db_creates_tables() -> None:
    conn = _mem_conn()
    tables = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    assert "watched_sources" in tables
    assert "checkpoints" in tables
    assert "schema_versions" in tables


def test_init_db_is_idempotent() -> None:
    conn = _mem_conn()
    init_db(conn)  # second call should not raise
    rows = conn.execute("SELECT COUNT(*) FROM schema_versions").fetchone()
    assert rows is not None
    assert rows[0] >= 1


def test_upsert_and_get_watched_source() -> None:
    conn = _mem_conn()
    src = WatchedSource(
        source_id=make_source_id(SourceType.DM, "u999"),
        source_type=SourceType.DM,
        external_id="u999",
        name="Alice",
    )
    upsert_watched_source(conn, src)
    fetched = get_watched_source(conn, src.source_id)
    assert fetched is not None
    assert fetched.source_id == src.source_id
    assert fetched.name == "Alice"
    assert fetched.enabled is True


def test_get_watched_source_missing_returns_none() -> None:
    conn = _mem_conn()
    assert get_watched_source(conn, "dm:nonexistent") is None


def test_upsert_watched_source_updates_on_conflict() -> None:
    conn = _mem_conn()
    src = WatchedSource(
        source_id="dm:u1",
        source_type=SourceType.DM,
        external_id="u1",
        name="Old Name",
    )
    upsert_watched_source(conn, src)
    updated = WatchedSource(
        source_id="dm:u1",
        source_type=SourceType.DM,
        external_id="u1",
        name="New Name",
        enabled=False,
    )
    upsert_watched_source(conn, updated)
    fetched = get_watched_source(conn, "dm:u1")
    assert fetched is not None
    assert fetched.name == "New Name"
    assert fetched.enabled is False


def test_list_watched_sources_enabled_only() -> None:
    conn = _mem_conn()
    active = WatchedSource(
        source_id="group:g1",
        source_type=SourceType.GROUP,
        external_id="g1",
        name="Active",
    )
    inactive = WatchedSource(
        source_id="group:g2",
        source_type=SourceType.GROUP,
        external_id="g2",
        name="Inactive",
        enabled=False,
    )
    upsert_watched_source(conn, active)
    upsert_watched_source(conn, inactive)
    results = list_watched_sources(conn, enabled_only=True)
    ids = [s.source_id for s in results]
    assert "group:g1" in ids
    assert "group:g2" not in ids


def test_list_watched_sources_all() -> None:
    conn = _mem_conn()
    for i in range(3):
        src = WatchedSource(
            source_id=f"doc:d{i}",
            source_type=SourceType.DOC,
            external_id=f"d{i}",
            name=f"Doc {i}",
            enabled=(i % 2 == 0),
        )
        upsert_watched_source(conn, src)
    all_sources = list_watched_sources(conn, enabled_only=False)
    assert len(all_sources) == 3


def test_upsert_and_get_checkpoint() -> None:
    conn = _mem_conn()
    src = WatchedSource(
        source_id="dm:u1",
        source_type=SourceType.DM,
        external_id="u1",
        name="Alice",
    )
    upsert_watched_source(conn, src)
    cp = Checkpoint(
        source_id="dm:u1",
        last_message_id="msg99",
        last_message_timestamp="2026-04-14T10:00:00Z",
        page_token="tok_xyz",
    )
    upsert_checkpoint(conn, cp)
    fetched = get_checkpoint(conn, "dm:u1")
    assert fetched is not None
    assert fetched.last_message_id == "msg99"
    assert fetched.page_token == "tok_xyz"


def test_get_checkpoint_missing_returns_none() -> None:
    conn = _mem_conn()
    assert get_checkpoint(conn, "dm:nobody") is None


def test_checkpoint_cascade_delete() -> None:
    conn = _mem_conn()
    src = WatchedSource(
        source_id="dm:u2",
        source_type=SourceType.DM,
        external_id="u2",
        name="Bob",
    )
    upsert_watched_source(conn, src)
    cp = Checkpoint(source_id="dm:u2", last_message_id="m1")
    upsert_checkpoint(conn, cp)
    # Delete the source; the checkpoint should cascade-delete.
    conn.execute("DELETE FROM watched_sources WHERE source_id = 'dm:u2'")
    conn.commit()
    assert get_checkpoint(conn, "dm:u2") is None


# Ensure the public re-exports in the module are accessible.
def test_module_public_api() -> None:
    assert hasattr(sources_mod, "WatchedSource")
    assert hasattr(sources_mod, "SourceType")
    assert hasattr(sources_mod, "Checkpoint")
    assert hasattr(schema_mod, "SCHEMA_VERSION")
    assert hasattr(db_mod, "connect")
    assert hasattr(db_mod, "init_db")
