from __future__ import annotations

import json
import subprocess
from datetime import date, datetime, timedelta

from .config import SourceConfig, WorkerConfig
from .models import RawMessage


class LarkCliError(RuntimeError):
    """Raised when a lark-cli command fails."""


class LarkCliClient:
    def __init__(self, config: WorkerConfig) -> None:
        self.config = config

    def run_json(self, args: list[str]) -> dict:
        return self._run_json(args)

    def list_chat_messages(
        self,
        *,
        source: SourceConfig,
        start_date: str,
        end_date: str,
        page_size: int = 50,
        sort: str = "asc",
        page_token: str | None = None,
    ) -> dict:
        args = [
            "lark-cli",
            "im",
            "+chat-messages-list",
            "--sort",
            sort,
            "--page-size",
            str(page_size),
            "--format",
            "json",
            "--as",
            "user",
            "--start",
            start_date,
            "--end",
            end_date,
        ]
        if source.source_type == "dm_user":
            args.extend(["--user-id", source.lark_id])
        else:
            args.extend(["--chat-id", source.lark_id])
        if page_token:
            args.extend(["--page-token", page_token])
        return self._run_json(args)

    def search_docs(self, query: str, page_size: int = 1) -> dict:
        args = [
            "lark-cli",
            "docs",
            "+search",
            "--as",
            "user",
            "--query",
            query,
            "--page-size",
            str(page_size),
            "--format",
            "json",
        ]
        return self._run_json(args)

    def fetch_doc(self, doc: str, limit: int | None = None, offset: int | None = None) -> dict:
        args = [
            "lark-cli",
            "docs",
            "+fetch",
            "--as",
            "user",
            "--doc",
            doc,
            "--format",
            "json",
        ]
        if limit is not None:
            args.extend(["--limit", str(limit)])
        if offset is not None:
            args.extend(["--offset", str(offset)])
        return self._run_json(args)

    def list_file_comments(self, *, file_token: str, file_type: str = "docx", page_size: int = 1) -> dict:
        args = [
            "lark-cli",
            "drive",
            "file.comments",
            "list",
            "--as",
            "user",
            "--format",
            "json",
            "--params",
            json.dumps(
                {
                    "file_token": file_token,
                    "file_type": file_type,
                    "page_size": page_size,
                },
                ensure_ascii=False,
            ),
        ]
        return self._run_json(args)

    def list_comment_replies(
        self,
        *,
        file_token: str,
        comment_id: str,
        file_type: str = "docx",
        page_size: int = 1,
    ) -> dict:
        args = [
            "lark-cli",
            "drive",
            "file.comment.replys",
            "list",
            "--as",
            "user",
            "--format",
            "json",
            "--params",
            json.dumps(
                {
                    "file_token": file_token,
                    "comment_id": comment_id,
                    "file_type": file_type,
                    "page_size": page_size,
                },
                ensure_ascii=False,
            ),
        ]
        return self._run_json(args)

    def poll_source(
        self,
        source: SourceConfig,
        checkpoint: dict[str, str] | None,
        lookback_days: int | None = None,
        use_checkpoint: bool = True,
    ) -> list[RawMessage]:
        messages: list[RawMessage] = []
        page_token: str | None = None

        start_date = self._start_date(
            checkpoint,
            lookback_days=lookback_days,
            use_checkpoint=use_checkpoint,
        )
        end_date = (date.today() + timedelta(days=1)).isoformat()

        while True:
            response = self.list_chat_messages(
                source=source,
                start_date=start_date,
                end_date=end_date,
                page_size=50,
                sort="asc",
                page_token=page_token,
            )
            payload = response.get("data", {})
            for item in payload.get("messages", []):
                message = self._to_raw_message(source, item)
                messages.append(message)

            if not payload.get("has_more"):
                break
            page_token = payload.get("page_token") or None
            if not page_token:
                break

        return messages

    def iter_event_messages(self) -> list[RawMessage]:
        args = [
            "lark-cli",
            "event",
            "+subscribe",
            "--event-types",
            ",".join(self.config.bot_event_types),
            "--compact",
            "--quiet",
        ]

        process = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        assert process.stdout is not None
        try:
            for line in process.stdout:
                stripped = line.strip()
                if not stripped:
                    continue
                event = json.loads(stripped)
                message = self._event_to_raw_message(event)
                if message:
                    yield message
        finally:
            process.kill()

    def _run_json(self, args: list[str]) -> dict:
        completed = subprocess.run(
            args,
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            raise LarkCliError(completed.stderr.strip() or completed.stdout.strip())

        stdout = completed.stdout.strip()
        if not stdout:
            raise LarkCliError("lark-cli returned no JSON output")
        return json.loads(stdout)

    def _start_date(
        self,
        checkpoint: dict[str, str] | None,
        lookback_days: int | None = None,
        use_checkpoint: bool = True,
    ) -> str:
        if use_checkpoint and checkpoint and checkpoint.get("last_message_timestamp"):
            return checkpoint["last_message_timestamp"].split(" ", 1)[0]
        days = self.config.poll_lookback_days if lookback_days is None else lookback_days
        return (date.today() - timedelta(days=days)).isoformat()

    def _to_raw_message(self, source: SourceConfig, item: dict) -> RawMessage:
        sender = item.get("sender", {})
        sender_id = sender.get("id", "")
        sender_name = sender.get("name", "")
        return RawMessage(
            source_id=source.source_id,
            source_type=source.source_type,
            message_id=item["message_id"],
            chat_id=item.get("chat_id", source.lark_id),
            chat_type=item.get("chat_type", "unknown"),
            sender_id=sender_id,
            sender_name=sender_name,
            direction=self._direction(sender_id, sender_name),
            created_at=item.get("create_time", ""),
            content=item.get("content", ""),
            payload_json=json.dumps(item, ensure_ascii=False),
        )

    def _event_to_raw_message(self, event: dict) -> RawMessage | None:
        if event.get("type") != "im.message.receive_v1":
            return None

        chat_id = event.get("chat_id", "")
        chat_type = event.get("chat_type", "unknown")
        sender_id = event.get("sender_id", "")
        sender_name = event.get("sender_name", sender_id)
        source = self._resolve_event_source(chat_id, chat_type, sender_id)
        if source is None:
            return None

        timestamp = event.get("timestamp") or event.get("create_time")
        created_at = self._format_timestamp(timestamp)
        return RawMessage(
            source_id=source.source_id,
            source_type=source.source_type,
            message_id=event.get("message_id", event.get("id", "")),
            chat_id=chat_id,
            chat_type=chat_type,
            sender_id=sender_id,
            sender_name=sender_name,
            direction=self._direction(sender_id, sender_name),
            created_at=created_at,
            content=event.get("content", ""),
            payload_json=json.dumps(event, ensure_ascii=False),
        )

    def _resolve_event_source(
        self,
        chat_id: str,
        chat_type: str,
        sender_id: str,
    ) -> SourceConfig | None:
        for source in self.config.enabled_sources:
            if source.source_type == "chat" and source.lark_id == chat_id:
                return source
            if source.source_type == "dm_user" and chat_type == "p2p" and source.lark_id == sender_id:
                return source
        return None

    def _direction(self, sender_id: str, sender_name: str) -> str:
        if sender_id in self.config.self_sender_ids or sender_name in self.config.self_sender_names:
            return "outgoing"
        return "incoming"

    @staticmethod
    def _format_timestamp(raw_timestamp: str | int | None) -> str:
        if raw_timestamp is None:
            return ""
        try:
            value = int(raw_timestamp)
        except (TypeError, ValueError):
            return str(raw_timestamp)
        return datetime.fromtimestamp(value / 1000).strftime("%Y-%m-%d %H:%M")
