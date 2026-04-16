"""SQLite schema DDL for the lark-to-notes local store.

The schema is versioned via a ``schema_versions`` table.  Each migration
is a plain SQL string identified by an integer version number.  Applying
``init_schema`` is idempotent — running it on an already-initialised
database is a no-op.
"""

from __future__ import annotations

SCHEMA_VERSION = 8

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

# ---------------------------------------------------------------------------
# Version 3 — task registry, lifecycle, and evidence tables
# ---------------------------------------------------------------------------

_V3_DDL = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id         TEXT PRIMARY KEY,
    fingerprint     TEXT NOT NULL UNIQUE,
    title           TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'open'
                        CHECK(status IN (
                            'open', 'needs_review', 'snoozed', 'dismissed',
                            'completed', 'merged', 'superseded'
                        )),
    task_class      TEXT NOT NULL,
    confidence_band TEXT NOT NULL,
    summary         TEXT NOT NULL DEFAULT '',
    reason_code     TEXT NOT NULL DEFAULT '',
    promotion_rec   TEXT NOT NULL DEFAULT 'review',
    assignee_refs   TEXT NOT NULL DEFAULT '[]',
    due_at          TEXT,
    manual_override_state TEXT,
    created_from_raw_record_id TEXT,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    last_updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_tasks_fingerprint
    ON tasks (fingerprint);

CREATE INDEX IF NOT EXISTS idx_tasks_status
    ON tasks (status);

CREATE TABLE IF NOT EXISTS task_evidence (
    evidence_id      TEXT PRIMARY KEY,
    task_id          TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
    raw_record_id    TEXT,
    source_item_id   TEXT NOT NULL DEFAULT '',
    excerpt          TEXT NOT NULL DEFAULT '',
    confidence_delta REAL NOT NULL DEFAULT 0.0,
    evidence_role    TEXT NOT NULL DEFAULT 'primary',
    created_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_task_evidence_task_id
    ON task_evidence (task_id);
"""

# ---------------------------------------------------------------------------
# Version 4 — runtime operations: run tracking and dead-letter quarantine
# ---------------------------------------------------------------------------

_V4_DDL = """
CREATE TABLE IF NOT EXISTS runtime_runs (
    run_id           TEXT PRIMARY KEY,
    command          TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'running'
                         CHECK(status IN ('running', 'completed', 'failed', 'cancelled')),
    started_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    finished_at      TEXT,
    items_processed  INTEGER NOT NULL DEFAULT 0,
    items_failed     INTEGER NOT NULL DEFAULT 0,
    error            TEXT
);

CREATE INDEX IF NOT EXISTS idx_runtime_runs_status
    ON runtime_runs (status);

CREATE INDEX IF NOT EXISTS idx_runtime_runs_started_at
    ON runtime_runs (started_at);

CREATE TABLE IF NOT EXISTS dead_letters (
    dl_id            TEXT PRIMARY KEY,
    source_id        TEXT NOT NULL,
    raw_message_id   TEXT,
    attempt_count    INTEGER NOT NULL DEFAULT 0,
    first_failed_at  TEXT NOT NULL,
    last_failed_at   TEXT NOT NULL,
    last_error       TEXT NOT NULL,
    quarantined_at   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dead_letters_source_id
    ON dead_letters (source_id);

CREATE INDEX IF NOT EXISTS idx_dead_letters_quarantined_at
    ON dead_letters (quarantined_at);
"""

# ---------------------------------------------------------------------------
# Version 5 — structured feedback events and review artifact imports
# ---------------------------------------------------------------------------

_V5_DDL = """
CREATE TABLE IF NOT EXISTS feedback_events (
    feedback_id    TEXT PRIMARY KEY,
    target_type    TEXT NOT NULL CHECK(target_type IN ('task', 'source_item')),
    target_id      TEXT NOT NULL,
    action         TEXT NOT NULL CHECK(action IN (
                        'confirm', 'dismiss', 'merge', 'snooze',
                        'wrong_class', 'missed_task'
                    )),
    payload_json   TEXT NOT NULL DEFAULT '{}',
    comment        TEXT NOT NULL DEFAULT '',
    actor_ref      TEXT NOT NULL DEFAULT '',
    created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    artifact_path  TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_feedback_events_target
    ON feedback_events (target_type, target_id);

CREATE INDEX IF NOT EXISTS idx_feedback_events_created_at
    ON feedback_events (created_at);
"""

# ---------------------------------------------------------------------------
# Version 6 — LLM budget tracking and content-result cache
# ---------------------------------------------------------------------------

_V6_DDL = """
CREATE TABLE IF NOT EXISTS llm_usage_records (
    call_id           TEXT PRIMARY KEY,
    provider          TEXT NOT NULL DEFAULT '',
    model             TEXT NOT NULL DEFAULT '',
    prompt_tokens     INTEGER NOT NULL DEFAULT 0,
    completion_tokens INTEGER NOT NULL DEFAULT 0,
    duration_ms       INTEGER NOT NULL DEFAULT 0,
    cached            INTEGER NOT NULL DEFAULT 0,
    fallback          INTEGER NOT NULL DEFAULT 0,
    fallback_reason   TEXT NOT NULL DEFAULT 'not_applicable',
    run_id            TEXT NOT NULL DEFAULT '',
    source_id         TEXT NOT NULL DEFAULT '',
    created_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_llm_usage_run_id
    ON llm_usage_records (run_id);

CREATE INDEX IF NOT EXISTS idx_llm_usage_created_at
    ON llm_usage_records (created_at);

CREATE TABLE IF NOT EXISTS content_cache (
    cache_key   TEXT PRIMARY KEY,
    result_json TEXT NOT NULL DEFAULT '{}',
    expires_at  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_content_cache_expires_at
    ON content_cache (expires_at);
"""

# ---------------------------------------------------------------------------
# Version 7 — mixed poll/event chat intake ledger
# ---------------------------------------------------------------------------

_V7_DDL = """
CREATE TABLE IF NOT EXISTS chat_intake_ledger (
    ingest_key        TEXT PRIMARY KEY,
    message_id        TEXT NOT NULL,
    source_id         TEXT NOT NULL,
    source_type       TEXT NOT NULL,
    chat_id           TEXT NOT NULL,
    chat_type         TEXT NOT NULL DEFAULT '',
    sender_id         TEXT NOT NULL DEFAULT '',
    sender_name       TEXT NOT NULL DEFAULT '',
    direction         TEXT NOT NULL DEFAULT 'incoming',
    created_at        TEXT NOT NULL,
    content           TEXT NOT NULL DEFAULT '',
    payload_json      TEXT NOT NULL DEFAULT '{}',
    first_seen_at     TEXT NOT NULL,
    last_seen_at      TEXT NOT NULL,
    first_intake_path TEXT NOT NULL CHECK(first_intake_path IN ('poll', 'event')),
    last_intake_path  TEXT NOT NULL CHECK(last_intake_path IN ('poll', 'event')),
    poll_seen_count   INTEGER NOT NULL DEFAULT 0,
    event_seen_count  INTEGER NOT NULL DEFAULT 0,
    coalesce_until    TEXT,
    processing_state  TEXT NOT NULL DEFAULT 'pending'
                           CHECK(processing_state IN ('pending', 'processed')),
    processed_at      TEXT,
    last_error        TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_chat_intake_source_message
    ON chat_intake_ledger (source_id, message_id);

CREATE INDEX IF NOT EXISTS idx_chat_intake_processing_state
    ON chat_intake_ledger (processing_state, coalesce_until, first_seen_at);
"""

# ---------------------------------------------------------------------------
# Version 8 — document body, comment, and reply intake (revision-bearing)
# ---------------------------------------------------------------------------
#
# Chat messages use ``chat_intake_ledger`` + ``raw_messages``.  Document
# surfaces are first-class ``record_type`` values with their own revision
# and lifecycle columns so comments and replies are not forced into the
# chat-shaped ``chat_id`` / ``message_id`` model.

_V8_DDL = """
CREATE TABLE IF NOT EXISTS document_intake_ledger (
    ingest_key         TEXT PRIMARY KEY,
    record_type        TEXT NOT NULL CHECK(record_type IN (
                            'doc_body', 'doc_comment', 'doc_reply'
                        )),
    source_id          TEXT NOT NULL
                           REFERENCES watched_sources(source_id)
                           ON DELETE CASCADE,
    document_token     TEXT NOT NULL,
    source_stream_id   TEXT NOT NULL,
    source_item_id     TEXT NOT NULL,
    parent_item_id     TEXT NOT NULL DEFAULT '',
    revision_id        TEXT NOT NULL DEFAULT '',
    lifecycle_state    TEXT NOT NULL DEFAULT 'active' CHECK(lifecycle_state IN (
                            'active', 'edited', 'deleted', 'superseded'
                        )),
    content_hash       TEXT NOT NULL DEFAULT '',
    normalized_text    TEXT NOT NULL DEFAULT '',
    payload_json       TEXT NOT NULL DEFAULT '{}',
    canonical_link     TEXT NOT NULL DEFAULT '',
    first_seen_at      TEXT NOT NULL,
    last_seen_at       TEXT NOT NULL,
    first_intake_path  TEXT NOT NULL CHECK(first_intake_path IN ('poll', 'event')),
    last_intake_path   TEXT NOT NULL CHECK(last_intake_path IN ('poll', 'event')),
    poll_seen_count    INTEGER NOT NULL DEFAULT 0,
    event_seen_count   INTEGER NOT NULL DEFAULT 0,
    coalesce_until     TEXT,
    processing_state   TEXT NOT NULL DEFAULT 'pending' CHECK(processing_state IN (
                            'pending', 'processed'
                        )),
    processed_at       TEXT,
    last_error         TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_doc_intake_source_document
    ON document_intake_ledger (source_id, document_token);

CREATE INDEX IF NOT EXISTS idx_doc_intake_stream_item
    ON document_intake_ledger (source_stream_id, source_item_id);

CREATE INDEX IF NOT EXISTS idx_doc_intake_processing_state
    ON document_intake_ledger (processing_state, coalesce_until, first_seen_at);
"""

_MIGRATIONS: dict[int, str] = {
    1: _V1_DDL,
    2: _V2_DDL,
    3: _V3_DDL,
    4: _V4_DDL,
    5: _V5_DDL,
    6: _V6_DDL,
    7: _V7_DDL,
    8: _V8_DDL,
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
