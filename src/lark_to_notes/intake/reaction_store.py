"""Append-only persistence for normalized IM reaction events.

Maps :class:`~lark_to_notes.intake.reaction_model.NormalizedReactionEvent` rows
into ``message_reaction_events``. Each call uses one insert + ``commit``, like
:mod:`lark_to_notes.intake.ledger`. Deterministic surrogate ids cover missing
``header.event_id`` so ``INSERT OR IGNORE`` replays stay idempotent.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from lark_to_notes.intake.ledger import chat_ingest_key
from lark_to_notes.intake.reaction_model import NormalizedReactionEvent

ReactionIntakePath = Literal["event", "poll", "backfill"]


def _utcnow_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def count_reaction_orphan_queue(conn: sqlite3.Connection) -> int:
    """Return row count in ``reaction_orphan_queue`` (reactions awaiting parent raw)."""

    row = conn.execute("SELECT COUNT(*) AS c FROM reaction_orphan_queue").fetchone()
    return int(row["c"] if row is not None else 0)


def latest_message_reaction_event_seen_at(conn: sqlite3.Connection) -> str | None:
    """Latest ``first_seen_at`` across ``message_reaction_events`` (doctor / lw-pzj.9.1)."""

    row = conn.execute("SELECT MAX(first_seen_at) AS m FROM message_reaction_events").fetchone()
    if row is None or row["m"] is None:
        return None
    return str(row["m"])


def _percentile_int(sorted_seconds: list[int], p: float) -> int | None:
    if not sorted_seconds:
        return None
    idx = min(len(sorted_seconds) - 1, max(0, round((len(sorted_seconds) - 1) * p)))
    return sorted_seconds[idx]


def _percentile_float(sorted_vals: list[float], p: float) -> float | None:
    if not sorted_vals:
        return None
    idx = min(len(sorted_vals) - 1, max(0, round((len(sorted_vals) - 1) * p)))
    return sorted_vals[idx]


def reaction_orphan_backlog_metrics(conn: sqlite3.Connection) -> dict[str, Any]:
    """Doctor-facing orphan-queue depth, age buckets, and dwell percentiles (lw-pzj.15.3).

    ``dwell`` is wall time since ``first_queued_at`` for rows **still** in the
    queue (stale-wait signal). Batch attach latency is summarized separately via
    :func:`reaction_attach_reconcile_latency_ms` (``reaction_reconcile_observations``).
    """

    rows = conn.execute("SELECT first_queued_at FROM reaction_orphan_queue").fetchall()
    depth = len(rows)
    now = datetime.now(UTC)
    buckets = {"lt_1m": 0, "1m_to_1h": 0, "1h_to_24h": 0, "gte_24h": 0}
    ages: list[int] = []
    oldest_at: str | None = None
    oldest_age: int | None = None
    parse_skips = 0
    for r in rows:
        raw = str(r["first_queued_at"])
        try:
            ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            parse_skips += 1
            continue
        age_sec = max(0, int((now - ts.astimezone(UTC)).total_seconds()))
        ages.append(age_sec)
        if oldest_age is None or age_sec > oldest_age:
            oldest_age = age_sec
            oldest_at = raw
        if age_sec < 60:
            buckets["lt_1m"] += 1
        elif age_sec < 3600:
            buckets["1m_to_1h"] += 1
        elif age_sec < 86400:
            buckets["1h_to_24h"] += 1
        else:
            buckets["gte_24h"] += 1
    ages.sort()
    return {
        "queue_depth": depth,
        "timestamp_parse_skips": parse_skips,
        "oldest_first_queued_at": oldest_at,
        "oldest_age_seconds": oldest_age,
        "age_bucket_counts": buckets,
        "dwell_seconds_p50": _percentile_int(ages, 0.50),
        "dwell_seconds_p90": _percentile_int(ages, 0.90),
    }


def reaction_attach_reconcile_latency_ms(
    conn: sqlite3.Connection, *, sample_limit: int = 200
) -> dict[str, Any]:
    """Percentiles of recent per-batch linkage ``elapsed_ms`` (lw-pzj.15.3).

    Populated when :func:`insert_raw_message` attaches one or more orphan
    reactions and records a row in ``reaction_reconcile_observations``.
    """

    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        ("reaction_reconcile_observations",),
    ).fetchone()
    if row is None:
        return {
            "attach_reconcile_sample_count": 0,
            "attach_reconcile_ms_p50": None,
            "attach_reconcile_ms_p90": None,
        }
    rows = conn.execute(
        """
        SELECT elapsed_ms FROM reaction_reconcile_observations
        ORDER BY observation_id DESC
        LIMIT ?
        """,
        (sample_limit,),
    ).fetchall()
    vals = sorted(float(r["elapsed_ms"]) for r in rows)
    return {
        "attach_reconcile_sample_count": len(vals),
        "attach_reconcile_ms_p50": _percentile_float(vals, 0.50),
        "attach_reconcile_ms_p90": _percentile_float(vals, 0.90),
    }


@dataclass(frozen=True)
class ReactionInsertResult:
    """Outcome of one reaction insert attempt."""

    reaction_event_id: str
    inserted: bool
    chat_ingest_fingerprint: str
    raw_message_present: bool


def reaction_correlation_counts(conn: sqlite3.Connection) -> dict[str, int]:
    """Count reaction rows versus rows in ``raw_messages`` for the same chat pair.

    **Linked** means a ``raw_messages`` row exists with the same ``message_id``
    and ``source_id`` as the reaction row (matches :func:`insert_message_reaction_event`
    presence detection). **Orphan** is ``total - linked`` (reconcile / backfill may
    change linkage over time even if ``raw_message_present`` was snapshotted at insert).
    """

    total_row = conn.execute("SELECT COUNT(*) AS c FROM message_reaction_events").fetchone()
    total = int(total_row["c"] if total_row is not None else 0)
    linked_row = conn.execute(
        """
        SELECT COUNT(*) AS c FROM message_reaction_events r
        WHERE EXISTS (
            SELECT 1 FROM raw_messages m
            WHERE m.message_id = r.message_id AND m.source_id = r.source_id
        )
        """
    ).fetchone()
    linked = int(linked_row["c"] if linked_row is not None else 0)
    return {
        "total": total,
        "linked_to_raw_message": linked,
        "orphan": max(0, total - linked),
    }


def reaction_ledger_governance_sample_for_doctor(
    conn: sqlite3.Connection,
    *,
    limit: int = 8,
) -> dict[str, Any]:
    """Aggregate ``(governance_version, policy_version)`` counts for ``doctor --json`` (lw-pzj.9.4).

    Uses **GROUP BY** only (no message ids or payloads) so the snapshot stays
    privacy-safe. Ordering is lexicographic for deterministic output.
    """

    from lark_to_notes.intake.reaction_caps import (
        REACTION_INTAKE_GOVERNANCE_VERSION,
        REACTION_INTAKE_POLICY_VERSION,
    )

    lim = max(1, min(int(limit), 50))
    rows = conn.execute(
        """
        SELECT governance_version AS gv, policy_version AS pv, COUNT(*) AS n
        FROM message_reaction_events
        GROUP BY governance_version, policy_version
        ORDER BY governance_version ASC, policy_version ASC, n DESC
        LIMIT ?
        """,
        (lim,),
    ).fetchall()
    tuple_counts: list[dict[str, str | int]] = [
        {
            "governance_version": str(r["gv"] or ""),
            "policy_version": str(r["pv"] or ""),
            "row_count": int(r["n"]),
        }
        for r in rows
    ]
    builtin_gv = REACTION_INTAKE_GOVERNANCE_VERSION
    builtin_pv = REACTION_INTAKE_POLICY_VERSION
    rows_with_explicit_gv = sum(
        int(t["row_count"]) for t in tuple_counts if str(t["governance_version"]) != ""
    )
    rows_matching_builtin = sum(
        int(t["row_count"]) for t in tuple_counts if str(t["governance_version"]) == builtin_gv
    )
    drift_from_builtin_governance = False
    for t in tuple_counts:
        gv = str(t["governance_version"])
        rc = int(t["row_count"])
        if gv not in ("", builtin_gv) and rc > 0:
            drift_from_builtin_governance = True
            break
    policy_drift_from_builtin = any(
        str(t["policy_version"]) != builtin_pv for t in tuple_counts if int(t["row_count"]) > 0
    )
    mismatch_vs_runtime_intake_caps = drift_from_builtin_governance or policy_drift_from_builtin
    dominant_ledger_tuple: dict[str, Any] | None
    if tuple_counts:
        dom = max(tuple_counts, key=lambda t: int(t["row_count"]))
        dominant_ledger_tuple = {
            "governance_version": str(dom["governance_version"]),
            "policy_version": str(dom["policy_version"]),
            "row_count": int(dom["row_count"]),
        }
    else:
        dominant_ledger_tuple = None
    return {
        "sampling": {
            "method": "group_by_governance_policy_lexicographic",
            "limit": lim,
            "note": (
                "Deterministic SQL ORDER BY (governance_version, policy_version, count); "
                "counts only — no message identifiers or reaction payloads."
            ),
        },
        "tuples": tuple_counts,
        "runtime_builtin_governance_version": builtin_gv,
        "runtime_builtin_policy_version": builtin_pv,
        "row_counts": {
            "explicit_governance_version": rows_with_explicit_gv,
            "matching_builtin_governance_version": rows_matching_builtin,
        },
        "hints": {
            "drift_from_builtin_governance": drift_from_builtin_governance,
        },
        "compare_as_of": {
            "expected_tuple": {
                "governance_version": builtin_gv,
                "policy_version": builtin_pv,
            },
            "dominant_ledger_tuple": dominant_ledger_tuple,
            "mismatch_vs_runtime_intake_caps": mismatch_vs_runtime_intake_caps,
        },
    }


def surrogate_reaction_event_id(event: NormalizedReactionEvent) -> str:
    """Build a deterministic surrogate primary key when ``event_id`` is absent.

    The digest inputs are restricted to fields that should repeat across
    redeliveries of the same logical platform event; volatile header noise is
    intentionally omitted.
    """

    key = json.dumps(
        {
            "source_id": event.source_id,
            "message_id": event.message_id,
            "kind": event.reaction_kind.value,
            "emoji_type": event.emoji_type,
            "operator_type": event.operator_type,
            "operator_open_id": event.operator_open_id,
            "operator_user_id": event.operator_user_id,
            "operator_union_id": event.operator_union_id,
            "action_time": event.action_time,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:40]
    return f"rxs_{digest}"


def canonical_reaction_event_id(event: NormalizedReactionEvent) -> str:
    """Return upstream ``event_id`` when set, otherwise :func:`surrogate_reaction_event_id`."""
    head = event.reaction_event_id.strip()
    return head if head else surrogate_reaction_event_id(event)


def reaction_event_row_exists(conn: sqlite3.Connection, reaction_event_id: str) -> bool:
    """Return True when ``message_reaction_events`` already holds this primary key."""

    row = conn.execute(
        "SELECT 1 FROM message_reaction_events WHERE reaction_event_id = ? LIMIT 1",
        (reaction_event_id,),
    ).fetchone()
    return row is not None


def insert_message_reaction_event(
    conn: sqlite3.Connection,
    event: NormalizedReactionEvent,
    *,
    intake_path: ReactionIntakePath = "event",
    governance_version: str = "",
    policy_version: str = "",
) -> ReactionInsertResult:
    """Insert one reaction row when its primary key is new.

    Args:
        conn: Open SQLite connection (``sqlite3.Row`` factory recommended).
        event: Parsed reaction envelope.
        intake_path: Which intake path produced the row.
        governance_version: Governance tuple component for replay/doctor (lw-pzj.12.2).
        policy_version: Policy tuple component for replay/doctor (lw-pzj.12.2).

    Returns:
        :class:`ReactionInsertResult` with the effective id and whether a new
        row was stored.
    """
    rid = canonical_reaction_event_id(event)
    ingest_fp = chat_ingest_key(event.source_id, event.message_id)
    raw_hit = conn.execute(
        "SELECT 1 FROM raw_messages WHERE message_id = ? AND source_id = ? LIMIT 1",
        (event.message_id, event.source_id),
    ).fetchone()
    raw_present = 1 if raw_hit is not None else 0
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO message_reaction_events (
            reaction_event_id, source_id, message_id, reaction_kind, emoji_type,
            operator_type, operator_open_id, operator_user_id, operator_union_id,
            action_time, intake_path, payload_json, governance_version, policy_version,
            chat_ingest_fingerprint, raw_message_present
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            rid,
            event.source_id,
            event.message_id,
            event.reaction_kind.value,
            event.emoji_type,
            event.operator_type,
            event.operator_open_id,
            event.operator_user_id,
            event.operator_union_id,
            event.action_time,
            intake_path,
            event.payload_json(),
            governance_version,
            policy_version,
            ingest_fp,
            raw_present,
        ),
    )
    inserted = cur.rowcount == 1
    if raw_present == 0:
        ts = _utcnow_iso()
        conn.execute(
            """
            INSERT INTO reaction_orphan_queue (
                reaction_event_id, source_id, message_id, first_queued_at, last_seen_at
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(reaction_event_id) DO UPDATE SET
                last_seen_at = excluded.last_seen_at
            """,
            (rid, event.source_id, event.message_id, ts, ts),
        )
    conn.commit()
    return ReactionInsertResult(
        reaction_event_id=rid,
        inserted=inserted,
        chat_ingest_fingerprint=ingest_fp,
        raw_message_present=raw_present == 1,
    )
