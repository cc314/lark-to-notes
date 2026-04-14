"""SQLite schema DDL for the lark-to-notes local store.

The schema is versioned via a ``schema_versions`` table.  Each migration
is a plain SQL string identified by an integer version number.  Applying
``init_schema`` is idempotent — running it on an already-initialised
database is a no-op.
"""

from __future__ import annotations

SCHEMA_VERSION = 2

# ---------------------------------------------------------------------------
# Version 1 — core watched-source governance tables
# ---------------------------------------------------------------------------

_V1_DDL = """
CREATE TABLE IF NOT EXISTS schema_versions (
    version   INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS watched_sources (
    source_id   TEXT PRIMARY KEY,
    source_type TEXT NOT NULL CHECK(source_type IN ('dm', 'group', 'doc')),
    external_id TEXT NOT NULL,
    name        TEXT NOT NULL,
    enabled     INTEGER NOT NULL DEFAULT 1,
    config_json TEXT NOT NULL DEFAULT '{}',
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_watched_sources_enabled
    ON watched_sources (enabled);

CREATE TABLE IF NOT EXISTS checkpoints (
    source_id              TEXT PRIMARY KEY
                               REFERENCES watched_sources(source_id)
                               ON DELETE CASCADE,
    last_message_id        TEXT,
    last_message_timestamp TEXT,
    page_token             TEXT,
    updated_at             TEXT NOT NULL
                               DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
"""

# ---------------------------------------------------------------------------
# Version 2 — raw-capture ledger and intake-run audit log
# ---------------------------------------------------------------------------

_V2_DDL = """
CREATE TABLE IF NOT EXISTS raw_messages (
    message_id   TEXT PRIMARY KEY,
    source_id    TEXT NOT NULL,
    source_type  TEXT NOT NULL,
    chat_id      TEXT NOT NULL,
    chat_type    TEXT NOT NULL DEFAULT '',
    sender_id    TEXT NOT NULL DEFAULT '',
    sender_name  TEXT NOT NULL DEFAULT '',
    direction    TEXT NOT NULL DEFAULT 'incoming',
    created_at   TEXT NOT NULL,
    content      TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{}',
    ingested_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_raw_messages_source_id
    ON raw_messages (source_id);

CREATE INDEX IF NOT EXISTS idx_raw_messages_created_at
    ON raw_messages (created_at);

CREATE TABLE IF NOT EXISTS intake_runs (
    run_id           TEXT PRIMARY KEY,
    source_id        TEXT NOT NULL,
    started_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    finished_at      TEXT,
    messages_fetched INTEGER NOT NULL DEFAULT 0,
    messages_new     INTEGER NOT NULL DEFAULT 0,
    status           TEXT NOT NULL DEFAULT 'running'
                         CHECK(status IN ('running', 'done', 'error')),
    error_detail     TEXT
);

CREATE INDEX IF NOT EXISTS idx_intake_runs_source_id
    ON intake_runs (source_id);
"""

_MIGRATIONS: dict[int, str] = {
    1: _V1_DDL,
    2: _V2_DDL,
}


def applied_versions_sql() -> str:
    """Return the SQL to query applied schema versions."""
    return "SELECT version FROM schema_versions ORDER BY version"


def migration_sql(version: int) -> str:
    """Return the DDL for a specific schema *version*.

    Args:
        version: The integer schema version to retrieve.

    Returns:
        A SQL string containing all DDL statements for that version.

    Raises:
        KeyError: If *version* is not a known migration.
    """
    return _MIGRATIONS[version]


def all_versions() -> list[int]:
    """Return all known migration version numbers in ascending order."""
    return sorted(_MIGRATIONS.keys())
