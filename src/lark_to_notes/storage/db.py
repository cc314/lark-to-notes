"""SQLite database connection and CRUD operations for watched-source governance.

All public functions accept a :class:`sqlite3.Connection` as their first
argument so callers control transaction boundaries.  Row factory is set to
``sqlite3.Row`` inside :func:`connect` so rows are accessible by column name.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from lark_to_notes.config.sources import Checkpoint, WatchedSource
from lark_to_notes.storage.schema import all_versions, applied_versions_sql, migration_sql


def connect(db_path: Path | str) -> sqlite3.Connection:
    """Open a SQLite connection with sensible defaults.

    Sets ``row_factory = sqlite3.Row`` and enables WAL journal mode and
    foreign-key enforcement.

    Args:
        db_path: Filesystem path to the database file.  Pass ``":memory:"``
            for an in-memory database (useful in tests).

    Returns:
        An open :class:`sqlite3.Connection`.
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    """Apply all pending schema migrations to *conn*.

    Safe to call on an already-initialised database — only unapplied
    migrations are executed.

    Args:
        conn: An open database connection (see :func:`connect`).
    """
    # Bootstrap: the schema_versions table may not exist yet.
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_versions ("
        "  version    INTEGER PRIMARY KEY,"
        "  applied_at TEXT NOT NULL"
        "             DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))"
        ")"
    )
    conn.commit()

    applied: set[int] = {row[0] for row in conn.execute(applied_versions_sql()).fetchall()}

    for version in all_versions():
        if version in applied:
            continue
        conn.executescript(migration_sql(version))
        conn.execute("INSERT OR IGNORE INTO schema_versions (version) VALUES (?)", (version,))
        conn.commit()


# ---------------------------------------------------------------------------
# WatchedSource CRUD
# ---------------------------------------------------------------------------


def upsert_watched_source(conn: sqlite3.Connection, source: WatchedSource) -> None:
    """Insert or replace a :class:`WatchedSource` record.

    Args:
        conn: An open database connection.
        source: The source to persist.
    """
    conn.execute(
        """
        INSERT INTO watched_sources
            (source_id, source_type, external_id, name, enabled, config_json,
             created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_id) DO UPDATE SET
            source_type = excluded.source_type,
            external_id = excluded.external_id,
            name        = excluded.name,
            enabled     = excluded.enabled,
            config_json = excluded.config_json,
            updated_at  = excluded.updated_at
        """,
        (
            source.source_id,
            str(source.source_type),
            source.external_id,
            source.name,
            int(source.enabled),
            source.config_json(),
            source.created_at,
            source.updated_at,
        ),
    )
    conn.commit()


def get_watched_source(conn: sqlite3.Connection, source_id: str) -> WatchedSource | None:
    """Fetch a single :class:`WatchedSource` by its stable identifier.

    Args:
        conn: An open database connection.
        source_id: The stable source identifier to look up.

    Returns:
        A :class:`WatchedSource` if found, otherwise ``None``.
    """
    row = conn.execute("SELECT * FROM watched_sources WHERE source_id = ?", (source_id,)).fetchone()
    if row is None:
        return None
    return WatchedSource.from_row(dict(row))


def list_watched_sources(
    conn: sqlite3.Connection, *, enabled_only: bool = True
) -> list[WatchedSource]:
    """Return all watched sources, optionally filtered to enabled ones.

    Args:
        conn: An open database connection.
        enabled_only: When ``True`` (the default) only sources with
            ``enabled=1`` are returned.

    Returns:
        A list of :class:`WatchedSource` instances ordered by ``source_id``.
    """
    sql = "SELECT * FROM watched_sources"
    params: tuple[Any, ...] = ()
    if enabled_only:
        sql += " WHERE enabled = 1"
    sql += " ORDER BY source_id"
    rows = conn.execute(sql, params).fetchall()
    return [WatchedSource.from_row(dict(row)) for row in rows]


# ---------------------------------------------------------------------------
# Checkpoint CRUD
# ---------------------------------------------------------------------------


def upsert_checkpoint(conn: sqlite3.Connection, checkpoint: Checkpoint) -> None:
    """Insert or replace a :class:`Checkpoint` record.

    Args:
        conn: An open database connection.
        checkpoint: The checkpoint state to persist.
    """
    conn.execute(
        """
        INSERT INTO checkpoints
            (source_id, last_message_id, last_message_timestamp,
             page_token, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(source_id) DO UPDATE SET
            last_message_id        = excluded.last_message_id,
            last_message_timestamp = excluded.last_message_timestamp,
            page_token             = excluded.page_token,
            updated_at             = excluded.updated_at
        """,
        (
            checkpoint.source_id,
            checkpoint.last_message_id,
            checkpoint.last_message_timestamp,
            checkpoint.page_token,
            checkpoint.updated_at,
        ),
    )
    conn.commit()


def get_checkpoint(conn: sqlite3.Connection, source_id: str) -> Checkpoint | None:
    """Fetch the :class:`Checkpoint` for a source, if one exists.

    Args:
        conn: An open database connection.
        source_id: The stable source identifier to look up.

    Returns:
        A :class:`Checkpoint` if a checkpoint row exists, otherwise ``None``.
    """
    row = conn.execute("SELECT * FROM checkpoints WHERE source_id = ?", (source_id,)).fetchone()
    if row is None:
        return None
    return Checkpoint.from_row(dict(row))
