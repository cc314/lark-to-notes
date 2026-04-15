"""Replay semantics: ingest historical JSONL files into the SQLite ledger.

Replay is the mechanism that makes the intake pipeline safe to re-run.
Any JSONL file in the vault's ``raw/lark-worker/`` directory can be replayed
into SQLite without creating duplicates, because
:func:`~lark_to_notes.intake.ledger.insert_raw_message` uses
``INSERT OR IGNORE`` on the stable ``message_id`` primary key.

Typical usage::

    from lark_to_notes.intake.replay import replay_jsonl_dir
    stats = replay_jsonl_dir(conn, raw_dir=vault_root / "raw" / "lark-worker")

"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from lark_to_notes.intake.ledger import insert_raw_message
from lark_to_notes.intake.models import RawMessage


def replay_jsonl_file(
    conn: sqlite3.Connection,
    jsonl_path: Path,
) -> tuple[int, int]:
    """Replay one JSONL file into the raw-messages ledger.

    Each line is expected to be a JSON object with at least a
    ``message_id`` field (the format produced by the existing
    ``automation/lark_worker/`` collector).  Lines that cannot be parsed
    or that lack a ``message_id`` are silently skipped.

    Args:
        conn: An open database connection with the v2 schema applied.
        jsonl_path: Path to the JSONL file to replay.

    Returns:
        A ``(total_lines, inserted)`` tuple where *total_lines* is the
        number of parseable records and *inserted* is how many were new.
    """
    total = 0
    inserted = 0

    with jsonl_path.open(encoding="utf-8") as fh:
        for raw_line in fh:
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            try:
                record: dict[str, object] = json.loads(raw_line)
            except json.JSONDecodeError:
                continue

            if "message_id" not in record:
                continue

            total += 1
            msg = RawMessage.from_jsonl_record(record)
            if insert_raw_message(conn, msg):
                inserted += 1

    return total, inserted


def replay_jsonl_dir(
    conn: sqlite3.Connection,
    raw_dir: Path,
    *,
    glob: str = "*.jsonl",
) -> dict[str, tuple[int, int]]:
    """Replay all JSONL files in *raw_dir* into the raw-messages ledger.

    Files are processed in lexicographic order (which matches the
    ``YYYY-MM-DD.jsonl`` naming convention used by the collector).

    Args:
        conn: An open database connection with the v2 schema applied.
        raw_dir: Directory containing the JSONL log files.
        glob: Glob pattern used to find JSONL files.  Defaults to
            ``"*.jsonl"``.

    Returns:
        A mapping of ``{filename: (total, inserted)}`` for every file
        processed.
    """
    results: dict[str, tuple[int, int]] = {}
    for path in sorted(raw_dir.glob(glob)):
        total, inserted = replay_jsonl_file(conn, path)
        results[path.name] = (total, inserted)
    return results
