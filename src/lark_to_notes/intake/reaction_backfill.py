"""REST reaction list backfill over ``raw_messages`` with SQLite checkpoints (lw-pzj.6.2).

Iterates messages in stable ``(created_at, message_id)`` order, pages
``im.reactions.list``-shaped fetchers, inserts via :func:`insert_message_reaction_event`
with ``intake_path="backfill"``, and persists resume state in
``reaction_backfill_checkpoints``.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from lark_to_notes.config.sources import ReactionBackfillCheckpoint
from lark_to_notes.intake.reaction_caps import REACTION_INTAKE_GOVERNANCE_VERSION
from lark_to_notes.intake.reaction_model import NormalizedReactionEvent, ReactionKind
from lark_to_notes.intake.reaction_store import insert_message_reaction_event
from lark_to_notes.storage.db import (
    get_reaction_backfill_checkpoint,
    upsert_reaction_backfill_checkpoint,
)

if TYPE_CHECKING:
    import sqlite3

logger = logging.getLogger(__name__)

FetchPageFn = Callable[[str, str | None], tuple[list[NormalizedReactionEvent], str | None]]
SleepFn = Callable[[float], None]


def _utc_now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _extract_reactions_list_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Normalize ``lark-cli im reactions list`` JSON envelope to a body dict."""

    data = payload.get("data")
    if isinstance(data, dict):
        return data
    return payload


def reaction_list_item_to_normalized(
    item: dict[str, Any],
    *,
    source_id: str,
    message_id: str,
) -> NormalizedReactionEvent:
    """Map one ``im.reactions.list`` ``items[]`` record to :class:`NormalizedReactionEvent`."""

    raw_op = item.get("operator")
    op: dict[str, Any] = raw_op if isinstance(raw_op, dict) else {}
    op_type = str(op.get("operator_type") or "").strip()
    op_id = str(op.get("operator_id") or "").strip()
    raw_rt = item.get("reaction_type")
    rt: dict[str, Any] = raw_rt if isinstance(raw_rt, dict) else {}
    emoji = str(rt.get("emoji_type") or "").strip()
    rid = str(item.get("reaction_id") or "").strip()
    header = {"event_type": "im.message.reaction.created_v1", "event_id": rid}
    event: dict[str, Any] = {
        "message_id": message_id,
        "reaction_type": dict(rt) if isinstance(rt, dict) else {"emoji_type": emoji},
        "operator_type": op_type,
        "action_time": str(item.get("action_time") or "").strip(),
    }
    if op_id:
        event["user_id"] = {"open_id": op_id}
    payload = {"header": header, "event": event, "source": "im.reactions.list"}
    return NormalizedReactionEvent(
        reaction_event_id=rid,
        source_id=source_id,
        message_id=message_id,
        reaction_kind=ReactionKind.ADD,
        emoji_type=emoji,
        operator_type=op_type,
        operator_open_id=op_id if op_type == "user" else "",
        operator_user_id="",
        operator_union_id="",
        action_time=str(item.get("action_time") or "").strip(),
        payload=payload,
    )


def make_lark_cli_reactions_list_fetcher(
    source_id: str,
    *,
    page_size: int = 50,
    user_id_type: str = "open_id",
    runner: Callable[[list[str]], dict[str, Any]] | None = None,
) -> FetchPageFn:
    """Build a fetcher that calls ``lark-cli im reactions list`` (``lw-pzj.6.1`` API)."""

    from lark_to_notes.live.lark_cli import run_lark_cli_json_retryable

    run = runner or run_lark_cli_json_retryable

    def fetch(
        message_id: str,
        page_token: str | None,
    ) -> tuple[list[NormalizedReactionEvent], str | None]:
        params: dict[str, Any] = {
            "message_id": message_id,
            "page_size": page_size,
            "user_id_type": user_id_type,
        }
        if page_token:
            params["page_token"] = page_token
        argv = [
            "im",
            "reactions",
            "list",
            "--params",
            json.dumps(params, ensure_ascii=False, separators=(",", ":")),
            "--format",
            "json",
        ]
        payload = run(argv)
        body = _extract_reactions_list_payload(payload)
        items = body.get("items")
        if not isinstance(items, list):
            items = []
        events = [
            reaction_list_item_to_normalized(i, source_id=source_id, message_id=message_id)
            for i in items
            if isinstance(i, dict)
        ]
        has_more = bool(body.get("has_more"))
        next_tok = str(body.get("page_token") or "").strip() if has_more else ""
        return events, (next_tok or None)

    return fetch


def _list_next_raw_messages(
    conn: sqlite3.Connection,
    *,
    source_id: str,
    watermark_created_at: str | None,
    watermark_message_id: str | None,
    limit: int,
    since_created_at: str | None = None,
    chat_id: str | None = None,
) -> list[tuple[str, str]]:
    """Return up to *limit* ``(message_id, created_at)`` rows after the watermark."""

    clauses = ["source_id = ?"]
    params: list[Any] = [source_id]
    if watermark_created_at is not None and watermark_message_id is not None:
        clauses.append("(created_at > ? OR (created_at = ? AND message_id > ?))")
        params.extend([watermark_created_at, watermark_created_at, watermark_message_id])
    if since_created_at:
        clauses.append("created_at >= ?")
        params.append(since_created_at)
    if chat_id:
        clauses.append("chat_id = ?")
        params.append(chat_id)
    where_sql = " AND ".join(clauses)
    sql = f"""
        SELECT message_id, created_at FROM raw_messages
        WHERE {where_sql}
        ORDER BY created_at ASC, message_id ASC
        LIMIT ?
    """
    params.append(limit)
    rows = conn.execute(sql, params).fetchall()
    return [(str(r["message_id"]), str(r["created_at"])) for r in rows]


def _dry_run_reaction_backfill(
    conn: sqlite3.Connection,
    *,
    source_id: str,
    batch_size: int,
    max_messages: int | None,
    since_created_at: str | None,
    chat_id: str | None,
) -> dict[str, Any]:
    """Simulate message scan order without API calls or checkpoint writes (lw-pzj.6.4)."""

    base = get_reaction_backfill_checkpoint(conn, source_id)
    if base is None:
        cp = ReactionBackfillCheckpoint(
            source_id=source_id,
            watermark_created_at=None,
            watermark_message_id=None,
        )
    else:
        cp = base
    preview_ids: list[str] = []
    truncated = False
    wca, wmid = cp.watermark_created_at, cp.watermark_message_id
    if cp.inflight_message_id and cp.inflight_created_at:
        preview_ids.append(str(cp.inflight_message_id))
        wca, wmid = cp.inflight_created_at, str(cp.inflight_message_id)
        if max_messages is not None and len(preview_ids) >= max_messages:
            more = _list_next_raw_messages(
                conn,
                source_id=source_id,
                watermark_created_at=wca,
                watermark_message_id=wmid,
                limit=1,
                since_created_at=since_created_at,
                chat_id=chat_id,
            )
            truncated = bool(more)
            return {
                "source_id": source_id,
                "dry_run": True,
                "preview_message_ids": preview_ids,
                "preview_truncated": truncated,
                "filters": {"since_created_at": since_created_at, "chat_id": chat_id},
                "messages_processed": 0,
                "api_calls": 0,
                "rows_inserted": 0,
                "batches_completed": 0,
                "watermark_created_at": cp.watermark_created_at,
                "watermark_message_id": cp.watermark_message_id,
                "inflight_message_id": cp.inflight_message_id,
                "inflight_page_token": cp.inflight_page_token,
                "last_error": None,
            }

    while True:
        if max_messages is not None and len(preview_ids) >= max_messages:
            more = _list_next_raw_messages(
                conn,
                source_id=source_id,
                watermark_created_at=wca,
                watermark_message_id=wmid,
                limit=1,
                since_created_at=since_created_at,
                chat_id=chat_id,
            )
            truncated = bool(more)
            break
        lim = batch_size
        if max_messages is not None:
            lim = min(lim, max_messages - len(preview_ids))
        if lim <= 0:
            break
        rows = _list_next_raw_messages(
            conn,
            source_id=source_id,
            watermark_created_at=wca,
            watermark_message_id=wmid,
            limit=lim,
            since_created_at=since_created_at,
            chat_id=chat_id,
        )
        if not rows:
            break
        for mid, mca in rows:
            preview_ids.append(mid)
            wca, wmid = mca, mid
            if max_messages is not None and len(preview_ids) >= max_messages:
                more = _list_next_raw_messages(
                    conn,
                    source_id=source_id,
                    watermark_created_at=wca,
                    watermark_message_id=wmid,
                    limit=1,
                    since_created_at=since_created_at,
                    chat_id=chat_id,
                )
                truncated = bool(more)
                return {
                    "source_id": source_id,
                    "dry_run": True,
                    "preview_message_ids": preview_ids,
                    "preview_truncated": truncated,
                    "filters": {"since_created_at": since_created_at, "chat_id": chat_id},
                    "messages_processed": 0,
                    "api_calls": 0,
                    "rows_inserted": 0,
                    "batches_completed": 0,
                    "watermark_created_at": cp.watermark_created_at,
                    "watermark_message_id": cp.watermark_message_id,
                    "inflight_message_id": cp.inflight_message_id,
                    "inflight_page_token": cp.inflight_page_token,
                    "last_error": None,
                }
    return {
        "source_id": source_id,
        "dry_run": True,
        "preview_message_ids": preview_ids,
        "preview_truncated": truncated,
        "filters": {"since_created_at": since_created_at, "chat_id": chat_id},
        "messages_processed": 0,
        "api_calls": 0,
        "rows_inserted": 0,
        "batches_completed": 0,
        "watermark_created_at": cp.watermark_created_at,
        "watermark_message_id": cp.watermark_message_id,
        "inflight_message_id": cp.inflight_message_id,
        "inflight_page_token": cp.inflight_page_token,
        "last_error": None,
    }


def _drain_message_pages(
    conn: sqlite3.Connection,
    *,
    source_id: str,
    message_id: str,
    created_at: str,
    fetch_page: FetchPageFn,
    governance_version: str,
    policy_version: str,
    min_interval_s: float,
    sleep_fn: SleepFn,
    start_page_token: str | None,
    cp: ReactionBackfillCheckpoint,
) -> tuple[ReactionBackfillCheckpoint, int, int]:
    """Fetch all pages for one message; persist inflight state between pages."""

    api_calls = 0
    inserted_here = 0
    tok: str | None = start_page_token
    while True:
        if min_interval_s > 0:
            sleep_fn(min_interval_s)
        page, next_tok = fetch_page(message_id, tok)
        api_calls += 1
        ins = 0
        for ev in page:
            res = insert_message_reaction_event(
                conn,
                ev,
                intake_path="backfill",
                governance_version=governance_version,
                policy_version=policy_version,
            )
            if res.inserted:
                ins += 1
        inserted_here += ins
        tok = next_tok
        cp = replace(
            cp,
            inflight_message_id=message_id,
            inflight_created_at=created_at,
            inflight_page_token=tok,
            api_calls=cp.api_calls + 1,
            rows_inserted=cp.rows_inserted + ins,
            last_error=None,
            updated_at=_utc_now_iso(),
        )
        upsert_reaction_backfill_checkpoint(conn, cp)
        if tok is None:
            cp = replace(
                cp,
                inflight_message_id=None,
                inflight_created_at=None,
                inflight_page_token=None,
                watermark_created_at=created_at,
                watermark_message_id=message_id,
                updated_at=_utc_now_iso(),
            )
            upsert_reaction_backfill_checkpoint(conn, cp)
            break
    return cp, api_calls, inserted_here


def execute_reaction_backfill(
    conn: sqlite3.Connection,
    *,
    source_id: str,
    fetch_page: FetchPageFn | None = None,
    batch_size: int = 25,
    min_interval_s: float = 0.2,
    governance_version: str = REACTION_INTAKE_GOVERNANCE_VERSION,
    policy_version: str = "",
    max_messages: int | None = None,
    sleep_fn: SleepFn | None = None,
    dry_run: bool = False,
    since_created_at: str | None = None,
    chat_id: str | None = None,
) -> dict[str, Any]:
    """Run backfill until *max_messages* reached or there is nothing left to do."""

    if dry_run:
        return _dry_run_reaction_backfill(
            conn,
            source_id=source_id,
            batch_size=batch_size,
            max_messages=max_messages,
            since_created_at=since_created_at,
            chat_id=chat_id,
        )
    if fetch_page is None:
        raise ValueError("fetch_page is required when dry_run is False")

    sleeper: SleepFn = sleep_fn or time.sleep
    base = get_reaction_backfill_checkpoint(conn, source_id)
    if base is None:
        cp = ReactionBackfillCheckpoint(
            source_id=source_id,
            watermark_created_at=None,
            watermark_message_id=None,
        )
    else:
        cp = base

    messages_processed = 0
    api_calls_total = 0
    rows_inserted_total = 0
    batches_completed = cp.batches_completed

    try:
        if cp.inflight_message_id and cp.inflight_created_at:
            cp, ac, ins = _drain_message_pages(
                conn,
                source_id=source_id,
                message_id=cp.inflight_message_id,
                created_at=cp.inflight_created_at,
                fetch_page=fetch_page,
                governance_version=governance_version,
                policy_version=policy_version,
                min_interval_s=min_interval_s,
                sleep_fn=sleeper,
                start_page_token=cp.inflight_page_token,
                cp=cp,
            )
            api_calls_total += ac
            rows_inserted_total += ins
            messages_processed += 1
            batches_completed += 1
            cp = replace(cp, batches_completed=batches_completed, updated_at=_utc_now_iso())
            upsert_reaction_backfill_checkpoint(conn, cp)
            if max_messages is not None and messages_processed >= max_messages:
                return {
                    "source_id": source_id,
                    "messages_processed": messages_processed,
                    "api_calls": api_calls_total,
                    "rows_inserted": rows_inserted_total,
                    "batches_completed": batches_completed,
                    "watermark_created_at": cp.watermark_created_at,
                    "watermark_message_id": cp.watermark_message_id,
                    "inflight_message_id": cp.inflight_message_id,
                    "inflight_page_token": cp.inflight_page_token,
                    "last_error": None,
                }

        while max_messages is None or messages_processed < max_messages:
            remaining = batch_size
            if max_messages is not None:
                remaining = min(remaining, max_messages - messages_processed)
            if remaining <= 0:
                break
            rows = _list_next_raw_messages(
                conn,
                source_id=source_id,
                watermark_created_at=cp.watermark_created_at,
                watermark_message_id=cp.watermark_message_id,
                limit=remaining,
                since_created_at=since_created_at,
                chat_id=chat_id,
            )
            if not rows:
                break
            for mid, mca in rows:
                if max_messages is not None and messages_processed >= max_messages:
                    break
                cp, ac, ins = _drain_message_pages(
                    conn,
                    source_id=source_id,
                    message_id=mid,
                    created_at=mca,
                    fetch_page=fetch_page,
                    governance_version=governance_version,
                    policy_version=policy_version,
                    min_interval_s=min_interval_s,
                    sleep_fn=sleeper,
                    start_page_token=None,
                    cp=cp,
                )
                api_calls_total += ac
                rows_inserted_total += ins
                messages_processed += 1
            batches_completed += 1
            cp = replace(cp, batches_completed=batches_completed, updated_at=_utc_now_iso())
            upsert_reaction_backfill_checkpoint(conn, cp)

    except Exception as exc:
        last_err = f"{type(exc).__name__}:{exc}"
        logger.exception("reaction_backfill_failed", extra={"source_id": source_id})
        cp = replace(cp, last_error=last_err, updated_at=_utc_now_iso())
        upsert_reaction_backfill_checkpoint(conn, cp)
        return {
            "source_id": source_id,
            "messages_processed": messages_processed,
            "api_calls": api_calls_total,
            "rows_inserted": rows_inserted_total,
            "batches_completed": batches_completed,
            "watermark_created_at": cp.watermark_created_at,
            "watermark_message_id": cp.watermark_message_id,
            "inflight_message_id": cp.inflight_message_id,
            "inflight_page_token": cp.inflight_page_token,
            "last_error": last_err,
        }

    return {
        "source_id": source_id,
        "messages_processed": messages_processed,
        "api_calls": api_calls_total,
        "rows_inserted": rows_inserted_total,
        "batches_completed": batches_completed,
        "watermark_created_at": cp.watermark_created_at,
        "watermark_message_id": cp.watermark_message_id,
        "inflight_message_id": cp.inflight_message_id,
        "inflight_page_token": cp.inflight_page_token,
        "last_error": None,
    }


def count_raw_messages_after_watermark(
    conn: sqlite3.Connection,
    *,
    source_id: str,
    watermark_created_at: str | None,
    watermark_message_id: str | None,
) -> int:
    """Count ``raw_messages`` rows strictly after the backfill watermark (or all if unset)."""

    if watermark_created_at is None or watermark_message_id is None:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM raw_messages WHERE source_id = ?",
            (source_id,),
        ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT COUNT(*) AS c FROM raw_messages
            WHERE source_id = ?
              AND (
                created_at > ?
                OR (created_at = ? AND message_id > ?)
              )
            """,
            (source_id, watermark_created_at, watermark_created_at, watermark_message_id),
        ).fetchone()
    return int(row["c"]) if row is not None else 0


def reaction_rest_backfill_doctor_block(conn: sqlite3.Connection) -> dict[str, Any]:
    """Doctor JSON fragment for REST reaction backfill / WS gap triage (lw-pzj.6.3)."""

    rows = conn.execute(
        "SELECT * FROM reaction_backfill_checkpoints ORDER BY source_id",
    ).fetchall()
    sources: list[dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        sid = str(d["source_id"])
        wca = d.get("watermark_created_at")
        wmid = d.get("watermark_message_id")
        pending = count_raw_messages_after_watermark(
            conn,
            source_id=sid,
            watermark_created_at=wca,
            watermark_message_id=wmid,
        )
        sources.append(
            {
                "source_id": sid,
                "watermark_created_at": wca,
                "watermark_message_id": wmid,
                "pending_raw_messages_after_watermark": pending,
                "inflight_message_id": d.get("inflight_message_id"),
                "inflight_rest_page_pending": bool(d.get("inflight_page_token")),
                "api_calls_checkpointed_total": int(d.get("api_calls") or 0),
                "rows_inserted_checkpointed_total": int(d.get("rows_inserted") or 0),
                "batches_completed": int(d.get("batches_completed") or 0),
                "last_error": d.get("last_error"),
                "checkpoint_updated_at": d.get("updated_at"),
            },
        )
    return {
        "policy": (
            "REST ``im.reactions.list`` snapshots supplement append-only WS reaction "
            "events; use pending_raw_messages_after_watermark plus event ledger per "
            "message_id to reason about gaps (lw-pzj.6.3)."
        ),
        "sources": sources,
        "notes": (
            "No row for a source means ``reaction-backfill`` has never checkpointed "
            "that source_id; pending counts treat an empty watermark as the full "
            "``raw_messages`` tail for that source."
        ),
    }
