from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from .config import SourceConfig
from .models import DistilledItem, RawMessage


SCHEMA = """
CREATE TABLE IF NOT EXISTS watched_sources (
    source_id TEXT PRIMARY KEY,
    source_type TEXT NOT NULL,
    external_id TEXT NOT NULL,
    name TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS checkpoints (
    source_id TEXT PRIMARY KEY,
    last_message_timestamp TEXT,
    last_message_id TEXT,
    page_token TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_messages (
    message_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    source_type TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    chat_type TEXT NOT NULL,
    sender_id TEXT,
    sender_name TEXT,
    direction TEXT NOT NULL,
    created_at TEXT NOT NULL,
    content TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS distilled_items (
    source_message_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    title TEXT NOT NULL,
    summary TEXT NOT NULL,
    task_status TEXT NOT NULL,
    confidence REAL NOT NULL,
    needs_current_tasks INTEGER NOT NULL DEFAULT 0,
    related_people_json TEXT NOT NULL DEFAULT '[]',
    note_date TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


def connect(db_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def init_db(db_path: Path) -> None:
    with connect(db_path) as connection:
        connection.executescript(SCHEMA)


def sync_sources(connection: sqlite3.Connection, sources: list[SourceConfig]) -> None:
    connection.executemany(
        """
        INSERT INTO watched_sources (source_id, source_type, external_id, name, enabled, updated_at)
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(source_id) DO UPDATE SET
            source_type = excluded.source_type,
            external_id = excluded.external_id,
            name = excluded.name,
            enabled = excluded.enabled,
            updated_at = CURRENT_TIMESTAMP
        """,
        [
            (source.source_id, source.source_type, source.lark_id, source.name, 1 if source.enabled else 0)
            for source in sources
        ],
    )


def get_checkpoint(connection: sqlite3.Connection, source_id: str) -> sqlite3.Row | None:
    return connection.execute(
        "SELECT * FROM checkpoints WHERE source_id = ?",
        (source_id,),
    ).fetchone()


def upsert_checkpoint(
    connection: sqlite3.Connection,
    source_id: str,
    last_message_timestamp: str | None,
    last_message_id: str | None,
    page_token: str | None = None,
) -> None:
    connection.execute(
        """
        INSERT INTO checkpoints (source_id, last_message_timestamp, last_message_id, page_token, updated_at)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(source_id) DO UPDATE SET
            last_message_timestamp = excluded.last_message_timestamp,
            last_message_id = excluded.last_message_id,
            page_token = excluded.page_token,
            updated_at = CURRENT_TIMESTAMP
        """,
        (source_id, last_message_timestamp, last_message_id, page_token),
    )


def insert_raw_message(connection: sqlite3.Connection, message: RawMessage) -> bool:
    cursor = connection.execute(
        """
        INSERT OR IGNORE INTO raw_messages (
            message_id,
            source_id,
            source_type,
            chat_id,
            chat_type,
            sender_id,
            sender_name,
            direction,
            created_at,
            content,
            payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            message.message_id,
            message.source_id,
            message.source_type,
            message.chat_id,
            message.chat_type,
            message.sender_id,
            message.sender_name,
            message.direction,
            message.created_at,
            message.content,
            message.payload_json,
        ),
    )
    return cursor.rowcount > 0


def upsert_distilled_item(connection: sqlite3.Connection, item: DistilledItem) -> None:
    connection.execute(
        """
        INSERT INTO distilled_items (
            source_message_id,
            source_id,
            kind,
            title,
            summary,
            task_status,
            confidence,
            needs_current_tasks,
            related_people_json,
            note_date,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(source_message_id) DO UPDATE SET
            kind = excluded.kind,
            title = excluded.title,
            summary = excluded.summary,
            task_status = excluded.task_status,
            confidence = excluded.confidence,
            needs_current_tasks = excluded.needs_current_tasks,
            related_people_json = excluded.related_people_json,
            note_date = excluded.note_date,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            item.source_message_id,
            item.source_id,
            item.kind,
            item.title,
            item.summary,
            item.task_status,
            item.confidence,
            1 if item.needs_current_tasks else 0,
            json.dumps(item.related_people, ensure_ascii=False),
            item.note_date,
        ),
    )


def get_note_dates(connection: sqlite3.Connection) -> list[str]:
    rows = connection.execute(
        "SELECT DISTINCT note_date FROM distilled_items ORDER BY note_date"
    ).fetchall()
    return [row["note_date"] for row in rows]


def get_distilled_items_for_date(connection: sqlite3.Connection, note_date: str) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT *
        FROM distilled_items
        WHERE note_date = ?
        ORDER BY source_message_id
        """,
        (note_date,),
    ).fetchall()


def get_open_task_items(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT *
        FROM distilled_items
        WHERE needs_current_tasks = 1 AND task_status = 'open'
        ORDER BY note_date DESC, updated_at DESC
        """
    ).fetchall()
