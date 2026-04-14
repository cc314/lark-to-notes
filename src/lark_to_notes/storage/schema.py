"""SQLite schema DDL for the lark-to-notes local store.

The schema is versioned via a ``schema_versions`` table.  Each migration
is a plain SQL string identified by an integer version number.  Applying
``init_schema`` is idempotent — running it on an already-initialised
database is a no-op.
"""

from __future__ import annotations

SCHEMA_VERSION = 1

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

_MIGRATIONS: dict[int, str] = {
    1: _V1_DDL,
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
