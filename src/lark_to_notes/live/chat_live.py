"""In-repo DM/group chat polling via ``lark-cli`` into the canonical intake ledger."""

from __future__ import annotations

import json
import logging
import sqlite3  # noqa: TC003
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from lark_to_notes.budget import BudgetEnforcer, BudgetPolicy
from lark_to_notes.config.sources import Checkpoint, SourceType, WatchedSource
from lark_to_notes.distill.heuristics import HeuristicClassifier
from lark_to_notes.distill.models import DistillInput
from lark_to_notes.distill.routing import classify_with_routing
from lark_to_notes.intake.ledger import list_raw_messages_recent, observe_chat_message
from lark_to_notes.intake.models import IntakePath, RawMessage
from lark_to_notes.live.lark_cli import LarkCliApiError, run_lark_cli_json_retryable
from lark_to_notes.live.worker_config import LiveWorkerConfigSnapshot  # noqa: TC001
from lark_to_notes.runtime.executor import drain_ready_chat_intake
from lark_to_notes.runtime.reconcile import SourceState
from lark_to_notes.storage.db import get_checkpoint, upsert_checkpoint, upsert_watched_source
from lark_to_notes.tasks import derive_fingerprint
from lark_to_notes.tasks.registry import upsert_task

logger = logging.getLogger(__name__)

type LarkCliJsonRunner = Callable[[list[str]], dict[str, Any]]


class ChatLiveConfigError(ValueError):
    """Raised when a worker JSON source row cannot be used for chat transport."""


@dataclass(frozen=True, slots=True)
class LiveChatSourceView:
    """Minimal live-source view consumed by reconcile and chat polling."""

    source_id: str
    worker_source_type: str
    external_id: str
    name: str
    enabled: bool
    chat_id: str


def _utcnow_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _live_start_date(timestamp: str | None, *, lookback_days: int) -> str:
    if timestamp:
        return timestamp.split("T", 1)[0].split(" ", 1)[0]
    return (date.today() - timedelta(days=lookback_days)).isoformat()


def _map_worker_source_type(source_type: str) -> SourceType:
    mapping = {
        "dm_user": SourceType.DM,
        "dm": SourceType.DM,
        "chat": SourceType.GROUP,
        "group": SourceType.GROUP,
        "doc": SourceType.DOC,
    }
    key = source_type.lower()
    if key not in mapping:
        raise ChatLiveConfigError(f"unsupported worker source_type: {source_type!r}")
    return mapping[key]


def watched_source_from_raw(raw: dict[str, Any]) -> WatchedSource | None:
    """Build a :class:`WatchedSource` from one worker ``sources[]`` object."""

    source_id = str(raw.get("source_id") or "").strip()
    external_id = str(raw.get("external_id") or "").strip()
    if not source_id or not external_id:
        return None
    st_raw = str(raw.get("source_type") or "").strip()
    if not st_raw:
        return None
    return WatchedSource(
        source_id=source_id,
        source_type=_map_worker_source_type(st_raw),
        external_id=external_id,
        name=str(raw.get("name") or source_id).strip() or source_id,
        enabled=bool(raw.get("enabled", True)),
        config={
            "worker_source_type": st_raw,
            **{
                k: v
                for k, v in raw.items()
                if k
                not in {
                    "source_id",
                    "source_type",
                    "external_id",
                    "name",
                    "enabled",
                }
            },
        },
    )


def live_chat_source_view(raw: dict[str, Any]) -> LiveChatSourceView | None:
    try:
        ws = watched_source_from_raw(raw)
    except ChatLiveConfigError:
        return None
    if ws is None:
        return None
    chat_id = str(raw.get("chat_id") or "").strip()
    st = str(raw.get("source_type") or "").strip().lower()
    return LiveChatSourceView(
        source_id=ws.source_id,
        worker_source_type=st,
        external_id=ws.external_id,
        name=ws.name,
        enabled=ws.enabled,
        chat_id=chat_id,
    )


def _lark_ts_to_created_at(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if text.isdigit() and len(text) >= 13:
        ms = int(text)
        return datetime.fromtimestamp(ms / 1000.0, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    if text.isdigit() and len(text) == 10:
        sec = int(text)
        return datetime.fromtimestamp(sec, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    return text


def _extract_plain_text_from_body(body: object) -> str:
    if not isinstance(body, dict):
        return ""
    raw = body.get("content")
    if not isinstance(raw, str) or not raw.strip():
        return ""
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return raw.strip()
    if isinstance(data, dict) and "text" in data:
        return str(data.get("text", "")).strip()
    return raw.strip()


def raw_message_from_lark_im_api(
    api_msg: dict[str, Any],
    *,
    source_id: str,
    source_type: str,
    chat_id: str,
    chat_type: str,
) -> RawMessage | None:
    """Project a Lark IM ``messages`` resource into :class:`RawMessage`."""

    message_id = str(api_msg.get("message_id") or "").strip()
    if not message_id:
        return None
    created_at = _lark_ts_to_created_at(api_msg.get("create_time") or api_msg.get("update_time"))
    body = api_msg.get("body")
    content = _extract_plain_text_from_body(body)
    sender = api_msg.get("sender")
    sender_id = ""
    sender_name = ""
    if isinstance(sender, dict):
        sender_id = str(sender.get("id") or sender.get("sender_id") or "").strip()
        sender_name = str(sender.get("name") or sender.get("sender_name") or "").strip()
    direction = "incoming"
    payload = dict(api_msg)
    return RawMessage(
        message_id=message_id,
        source_id=source_id,
        source_type=source_type,
        chat_id=str(api_msg.get("chat_id") or chat_id),
        chat_type=chat_type,
        sender_id=sender_id,
        sender_name=sender_name,
        direction=direction,
        created_at=created_at or _utcnow_iso(),
        content=content,
        payload=payload,
    )


def _messages_from_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = payload.get("data")
    if not isinstance(data, dict):
        return []
    items = data.get("messages")
    if items is None:
        items = data.get("items")
    if not isinstance(items, list):
        return []
    return [m for m in items if isinstance(m, dict)]


def _list_argv_for_source(
    src: LiveChatSourceView,
    *,
    start: str | None,
    end: str | None,
    sort: str,
    page_size: str,
    page_token: str | None,
) -> list[str]:
    st = src.worker_source_type
    argv: list[str] = [
        "im",
        "+chat-messages-list",
        "--format",
        "json",
        "--page-size",
        page_size,
        "--sort",
        sort,
    ]
    if st in {"dm_user", "dm"}:
        argv += ["--user-id", src.external_id]
    elif st in {"chat", "group"}:
        chat = src.chat_id or src.external_id
        if not str(chat).startswith("oc_"):
            raise ChatLiveConfigError(
                f"group source {src.source_id!r} needs an oc_ chat id "
                f"(set chat_id or external_id to the group chat id)"
            )
        argv += ["--chat-id", str(chat)]
    else:
        raise ChatLiveConfigError(f"unsupported chat transport source_type: {st!r}")
    if start:
        argv += ["--start", start]
    if end:
        argv += ["--end", end]
    if page_token:
        argv += ["--page-token", page_token]
    return argv


def _default_runner(argv: list[str]) -> dict[str, Any]:
    return run_lark_cli_json_retryable(argv)


def _distill_recent(conn: sqlite3.Connection, *, limit: int, budget_run_id: str) -> int:
    classifier = HeuristicClassifier()
    budget_enforcer = BudgetEnforcer(conn, BudgetPolicy())
    inserted = 0
    for msg in list_raw_messages_recent(conn, limit=limit):
        dinput = DistillInput(
            message_id=msg.message_id,
            source_id=msg.source_id,
            source_type=msg.source_type,
            content=msg.content,
            sender_name=msg.sender_name,
            direction=msg.direction,
            created_at=msg.created_at,
        )
        result = classify_with_routing(
            dinput,
            classifier=classifier,
            llm_provider=None,
            budget_enforcer=budget_enforcer,
            run_id=budget_run_id,
        )
        fp = derive_fingerprint(
            msg.content,
            msg.source_id,
            msg.created_at,
            source_type=msg.source_type,
        )
        _task_id, is_new = upsert_task(
            conn,
            fingerprint=fp,
            title=msg.content[:80].strip(),
            task_class=str(result.task_class),
            confidence_band=str(result.confidence_band),
            summary=result.excerpt or "",
            reason_code=result.reason_code,
            promotion_rec=str(result.promotion_rec),
            created_from_raw_record_id=msg.message_id,
        )
        if is_new:
            inserted += 1
    conn.commit()
    return inserted


@dataclass(frozen=True, slots=True)
class LiveWorkerBridgeConfig:
    """Config surface historically read from ``automation.lark_worker`` objects."""

    vault_root: Path
    state_db: Path
    poll_interval_seconds: int
    poll_lookback_days: int
    enabled_sources: tuple[LiveChatSourceView, ...]


class ChatLiveAdapter:
    """Canonical chat live adapter: ``lark-cli`` transport + SQLite intake ledger."""

    def __init__(
        self,
        snapshot: LiveWorkerConfigSnapshot,
        runtime_conn: sqlite3.Connection,
        *,
        runner: LarkCliJsonRunner | None = None,
    ) -> None:
        views = tuple(
            v
            for raw in snapshot.raw_sources
            if isinstance(raw, dict) and (v := live_chat_source_view(raw)) is not None
        )
        self._snapshot = snapshot
        self._conn = runtime_conn
        self._runner = runner or _default_runner
        self.config = LiveWorkerBridgeConfig(
            vault_root=snapshot.vault_root,
            state_db=snapshot.state_db,
            poll_interval_seconds=snapshot.poll_interval_seconds,
            poll_lookback_days=snapshot.poll_lookback_days,
            enabled_sources=views,
        )

    def initialize(self) -> None:
        """Upsert watched sources from the worker JSON into the canonical database."""

        for raw in self._snapshot.raw_sources:
            if not isinstance(raw, dict):
                continue
            try:
                ws = watched_source_from_raw(raw)
            except ChatLiveConfigError as exc:
                logger.warning("skip_worker_source", extra={"raw": raw, "error": str(exc)})
                continue
            if ws is None:
                logger.warning("skip_invalid_worker_source", extra={"raw": raw})
                continue
            upsert_watched_source(self._conn, ws)

    def poll_once(self, *, sync_notes: bool) -> dict[str, int]:
        return self._poll_or_backfill(
            lookback_days=None,
            source_ids=None,
            sync_notes=sync_notes,
        )

    def backfill_history(
        self,
        *,
        lookback_days: int | None,
        source_ids: set[str] | None,
        sync_notes: bool,
    ) -> dict[str, int]:
        return self._poll_or_backfill(
            lookback_days=lookback_days,
            source_ids=source_ids,
            sync_notes=sync_notes,
        )

    def collect_live_source_states(
        self, runtime_conn: sqlite3.Connection
    ) -> dict[str, SourceState]:
        """Mirror the legacy worker reconcile probe using ``lark-cli``."""

        end_date = (date.today() + timedelta(days=1)).isoformat()
        states: dict[str, SourceState] = {}
        for src in self.config.enabled_sources:
            if not src.enabled:
                continue
            checkpoint = get_checkpoint(runtime_conn, src.source_id)
            if src.worker_source_type not in {"dm_user", "dm", "chat", "group"}:
                if checkpoint is None:
                    continue
                states[src.source_id] = SourceState(
                    source_id=src.source_id,
                    latest_message_id=checkpoint.last_message_id or "",
                    latest_message_timestamp=checkpoint.last_message_timestamp or "",
                )
                continue
            try:
                argv = _list_argv_for_source(
                    src,
                    start=None,
                    end=end_date,
                    sort="desc",
                    page_size="1",
                    page_token=None,
                )
                payload = self._runner(argv)
            except (LarkCliApiError, ChatLiveConfigError) as exc:
                logger.info(
                    "live_chat_peek_failed",
                    extra={"source_id": src.source_id, "error": str(exc)},
                )
                if checkpoint is None:
                    continue
                states[src.source_id] = SourceState(
                    source_id=src.source_id,
                    latest_message_id=checkpoint.last_message_id or "",
                    latest_message_timestamp=checkpoint.last_message_timestamp or "",
                )
                continue

            items = _messages_from_payload(payload)
            if items:
                latest = items[0]
                states[src.source_id] = SourceState(
                    source_id=src.source_id,
                    latest_message_id=str(latest.get("message_id", "")),
                    latest_message_timestamp=_lark_ts_to_created_at(
                        latest.get("create_time") or latest.get("update_time")
                    ),
                )
                continue
            if checkpoint is None:
                continue
            states[src.source_id] = SourceState(
                source_id=src.source_id,
                latest_message_id=checkpoint.last_message_id or "",
                latest_message_timestamp=checkpoint.last_message_timestamp or "",
            )
        return states

    def _poll_or_backfill(
        self,
        *,
        lookback_days: int | None,
        source_ids: set[str] | None,
        sync_notes: bool,
    ) -> dict[str, int]:
        lock_path = Path(self._snapshot.vault_root) / "var" / "lark-to-notes.runtime.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)

        inserted_messages = 0
        sources_scanned = 0
        end_date = (date.today() + timedelta(days=1)).isoformat()

        for src in self.config.enabled_sources:
            if not src.enabled:
                continue
            if source_ids is not None and src.source_id not in source_ids:
                continue
            if src.worker_source_type not in {"dm_user", "dm", "chat", "group"}:
                continue

            sources_scanned += 1
            checkpoint = get_checkpoint(self._conn, src.source_id)
            if lookback_days is not None:
                start_day = (date.today() - timedelta(days=lookback_days)).isoformat()
                start = f"{start_day}T00:00:00Z"
            elif checkpoint and checkpoint.last_message_timestamp:
                start = checkpoint.last_message_timestamp
            else:
                start_day = _live_start_date(None, lookback_days=self._snapshot.poll_lookback_days)
                start = f"{start_day}T00:00:00Z"

            chat_type = "p2p" if src.worker_source_type in {"dm_user", "dm"} else "group"
            chat_container = src.chat_id or src.external_id
            page_token: str | None = None
            latest_id = ""
            latest_ts = ""
            while True:
                argv = _list_argv_for_source(
                    src,
                    start=start,
                    end=end_date,
                    sort="asc",
                    page_size="50",
                    page_token=page_token,
                )
                payload = self._runner(argv)
                items = _messages_from_payload(payload)
                if not items:
                    break
                for api_msg in items:
                    rm = raw_message_from_lark_im_api(
                        api_msg,
                        source_id=src.source_id,
                        source_type=src.worker_source_type,
                        chat_id=str(api_msg.get("chat_id") or chat_container),
                        chat_type=chat_type,
                    )
                    if rm is None:
                        continue
                    observe_chat_message(
                        self._conn,
                        rm,
                        intake_path=IntakePath.POLL,
                    )
                last = items[-1]
                latest_id = str(last.get("message_id") or "").strip()
                latest_ts = _lark_ts_to_created_at(
                    last.get("create_time") or last.get("update_time")
                )
                data = payload.get("data")
                page_token = None
                if isinstance(data, dict):
                    pt = data.get("page_token")
                    if isinstance(pt, str) and pt.strip():
                        page_token = pt.strip()
                if not page_token:
                    break

            batch = drain_ready_chat_intake(
                self._conn,
                lock_path=lock_path,
                command="chat-live",
                as_of=_utcnow_iso(),
            )
            inserted_messages += batch.items_processed

            if latest_id and latest_ts:
                upsert_checkpoint(
                    self._conn,
                    Checkpoint(
                        source_id=src.source_id,
                        last_message_id=latest_id,
                        last_message_timestamp=latest_ts,
                        page_token=None,
                    ),
                )

        distilled = 0
        if sync_notes:
            distilled = _distill_recent(
                self._conn,
                limit=200,
                budget_run_id=f"chat-live:{uuid4()}",
            )

        return {
            "inserted_messages": inserted_messages,
            "distilled_items": distilled,
            "sources_scanned": sources_scanned,
        }
