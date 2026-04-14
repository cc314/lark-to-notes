from __future__ import annotations

import json
import threading
import time
from pathlib import Path

from .config import WorkerConfig, ensure_directories
from .db import (
    connect,
    get_checkpoint,
    get_distilled_items_for_date,
    get_note_dates,
    get_open_task_items,
    init_db,
    insert_raw_message,
    sync_sources,
    upsert_checkpoint,
    upsert_distilled_item,
)
from .distill import distill_message
from .lark import LarkCliClient
from .models import RawMessage
from .notes import sync_current_tasks_note, sync_daily_note
from .people import build_people_index


class WorkerService:
    def __init__(self, config: WorkerConfig) -> None:
        self.config = config
        self.client = LarkCliClient(config)
        self._note_lock = threading.Lock()

    def initialize(self) -> None:
        ensure_directories(self.config)
        init_db(self.config.state_db)
        with connect(self.config.state_db) as connection:
            sync_sources(connection, self.config.user_sources)

    def poll_once(self, sync_notes: bool = True) -> dict[str, int]:
        self.initialize()
        people_index = build_people_index(self.config.vault_root)
        inserted_messages = 0
        distilled_items = 0
        touched_dates: set[str] = set()

        with connect(self.config.state_db) as connection:
            for source in self.config.enabled_sources:
                checkpoint = get_checkpoint(connection, source.source_id)
                messages = self.client.poll_source(source, dict(checkpoint) if checkpoint else None)
                last_message: RawMessage | None = None
                for message in messages:
                    last_message = message
                    if not insert_raw_message(connection, message):
                        continue
                    inserted_messages += 1
                    self._append_raw_log(message)
                    item = distill_message(message, self.config, people_index)
                    if item is None:
                        continue
                    upsert_distilled_item(connection, item)
                    distilled_items += 1
                    touched_dates.add(item.note_date)

                if last_message is not None:
                    upsert_checkpoint(
                        connection,
                        source.source_id,
                        last_message.created_at,
                        last_message.message_id,
                    )

        if sync_notes:
            self.sync_notes(touched_dates=touched_dates or None)

        return {
            "inserted_messages": inserted_messages,
            "distilled_items": distilled_items,
        }

    def backfill_history(
        self,
        lookback_days: int | None = None,
        source_ids: set[str] | None = None,
        sync_notes: bool = False,
    ) -> dict[str, int]:
        self.initialize()
        people_index = build_people_index(self.config.vault_root)
        inserted_messages = 0
        distilled_items = 0
        touched_dates: set[str] = set()
        source_count = 0

        with connect(self.config.state_db) as connection:
            for source in self.config.enabled_sources:
                if source_ids and source.source_id not in source_ids:
                    continue
                source_count += 1
                messages = self.client.poll_source(
                    source,
                    checkpoint=None,
                    lookback_days=lookback_days or self.config.history_lookback_days,
                    use_checkpoint=False,
                )
                for message in messages:
                    if not insert_raw_message(connection, message):
                        continue
                    inserted_messages += 1
                    self._append_raw_log(message)
                    item = distill_message(message, self.config, people_index)
                    if item is None:
                        continue
                    upsert_distilled_item(connection, item)
                    distilled_items += 1
                    touched_dates.add(item.note_date)

        if sync_notes:
            self.sync_notes(touched_dates=touched_dates or None)

        return {
            "sources_scanned": source_count,
            "inserted_messages": inserted_messages,
            "distilled_items": distilled_items,
        }

    def sync_notes(self, touched_dates: set[str] | None = None) -> None:
        self.initialize()
        with self._note_lock, connect(self.config.state_db) as connection:
            note_dates = sorted(touched_dates or set(get_note_dates(connection)))
            for note_date in note_dates:
                items = [dict(row) for row in get_distilled_items_for_date(connection, note_date)]
                sync_daily_note(self.config, note_date, items)
            task_items = [dict(row) for row in get_open_task_items(connection)]
            sync_current_tasks_note(self.config, task_items)

    def listen_events(self, sync_notes: bool = True) -> None:
        self.initialize()
        people_index = build_people_index(self.config.vault_root)
        for message in self.client.iter_event_messages():
            with connect(self.config.state_db) as connection:
                if not insert_raw_message(connection, message):
                    continue
                self._append_raw_log(message)
                item = distill_message(message, self.config, people_index)
                touched_dates = set()
                if item is not None:
                    upsert_distilled_item(connection, item)
                    touched_dates.add(item.note_date)
            if sync_notes:
                self.sync_notes(touched_dates=touched_dates or None)

    def run_forever(self, with_events: bool = False) -> None:
        self.initialize()
        event_thread: threading.Thread | None = None
        if with_events:
            event_thread = threading.Thread(target=self.listen_events, daemon=True)
            event_thread.start()

        while True:
            self.poll_once(sync_notes=True)
            time.sleep(self.config.poll_interval_seconds)

    def _append_raw_log(self, message: RawMessage) -> None:
        raw_log_path = self.config.raw_dir / f"{message.note_date}.jsonl"
        raw_log_path.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "source_id": message.source_id,
            "source_type": message.source_type,
            "message_id": message.message_id,
            "chat_id": message.chat_id,
            "chat_type": message.chat_type,
            "sender_id": message.sender_id,
            "sender_name": message.sender_name,
            "direction": message.direction,
            "created_at": message.created_at,
            "content": message.content,
            "payload": json.loads(message.payload_json),
        }
        with raw_log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False))
            handle.write("\n")
