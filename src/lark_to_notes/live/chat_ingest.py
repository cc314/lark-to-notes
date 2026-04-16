"""Canonical chat ingestion into the SQLite intake ledger.

Records must match the JSONL schema understood by
:class:`~lark_to_notes.intake.models.RawMessage` (the same shape produced by the
legacy worker collector and replayed by :mod:`lark_to_notes.intake.replay`).
Future in-repo ``lark-cli`` transports should normalize upstream chat payloads
into this shape and stream them through :func:`ingest_chat_records` so live and
replay share one durable path into ``raw_messages``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Iterable

from lark_to_notes.intake.ledger import insert_raw_message
from lark_to_notes.intake.models import RawMessage


def ingest_chat_records(
    conn: sqlite3.Connection,
    records: Iterable[dict[str, Any]],
) -> tuple[int, int]:
    """Insert chat records into ``raw_messages`` with replay-identical semantics.

    Each mapping must decode like a JSONL log line: at minimum a ``message_id``
    and the fields required by :meth:`RawMessage.from_jsonl_record`. Malformed
    rows are skipped without raising so transports can filter upstream.

    Args:
        conn:     Open SQLite connection with schema applied.
        records:  Iterable of dicts in worker JSONL shape.

    Returns:
        ``(total_seen, inserted)`` where *total_seen* counts dicts with a
        ``message_id`` key and *inserted* counts rows actually inserted
        (``INSERT OR IGNORE`` misses do not increment inserted).
    """
    total = 0
    inserted = 0
    for record in records:
        if not isinstance(record, dict):
            continue
        if "message_id" not in record:
            continue
        total += 1
        msg = RawMessage.from_jsonl_record(record)
        if insert_raw_message(conn, msg):
            inserted += 1
    return total, inserted
