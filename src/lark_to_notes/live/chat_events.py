"""Lark chat *event* envelopes into the mixed poll/event chat-intake ledger.

`lark-cli event +subscribe` streams NDJSON objects; each line that decodes to a
Feishu/Lark ``im.message.receive_v1`` style envelope can be passed through
:func:`ingest_receive_message_v1_envelope` so **event** observations share the
same :func:`~lark_to_notes.intake.ledger.observe_chat_message` path as polling
(:class:`~lark_to_notes.intake.models.IntakePath` ``EVENT``).
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from lark_to_notes.intake.ledger import observe_chat_message
from lark_to_notes.intake.models import ChatIntakeItem, IntakePath
from lark_to_notes.live.chat_live import raw_message_from_lark_im_api

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Iterable, Iterator

logger = logging.getLogger(__name__)

_RECEIVE_MESSAGE_V1 = "im.message.receive_v1"


def event_type_from_envelope(envelope: dict[str, Any]) -> str:
    """Return ``header.event_type`` when present."""

    header = envelope.get("header")
    if isinstance(header, dict):
        return str(header.get("event_type") or "").strip()
    return ""


def extract_im_message_from_envelope(envelope: dict[str, Any]) -> dict[str, Any] | None:
    """Return ``event.message`` when it looks like an IM message resource."""

    event = envelope.get("event")
    if not isinstance(event, dict):
        return None
    msg = event.get("message")
    if not isinstance(msg, dict):
        return None
    if not str(msg.get("message_id") or "").strip():
        return None
    return msg


def iter_chat_event_envelopes_from_ndjson(lines: Iterable[str]) -> Iterator[dict[str, Any]]:
    """Yield each non-empty line that decodes to a JSON object."""

    for raw_line in lines:
        text = raw_line.strip()
        if not text:
            continue
        try:
            obj = json.loads(text)
        except json.JSONDecodeError:
            logger.info("chat_event_ndjson_skip", extra={"reason": "json_decode_error"})
            continue
        if isinstance(obj, dict):
            yield obj


def ingest_receive_message_v1_envelope(
    conn: sqlite3.Connection,
    envelope: dict[str, Any],
    *,
    source_id: str,
    worker_source_type: str,
    chat_type: str,
    observed_at: str | None = None,
    coalesce_window_seconds: int = 60,
    chat_id_override: str | None = None,
) -> ChatIntakeItem | None:
    """Record one ``im.message.receive_v1`` event into ``chat_intake_ledger``.

    *source_id* and *worker_source_type* must match the watched source used for
    polling so poll and event paths coalesce on the same :func:`chat ingest key
    <lark_to_notes.intake.ledger.chat_ingest_key>`.
    """

    if event_type_from_envelope(envelope) != _RECEIVE_MESSAGE_V1:
        return None
    api_msg = extract_im_message_from_envelope(envelope)
    if api_msg is None:
        return None
    chat_id = (chat_id_override or str(api_msg.get("chat_id") or "")).strip()
    rm = raw_message_from_lark_im_api(
        api_msg,
        source_id=source_id,
        source_type=worker_source_type,
        chat_id=chat_id,
        chat_type=chat_type,
    )
    if rm is None:
        return None
    return observe_chat_message(
        conn,
        rm,
        intake_path=IntakePath.EVENT,
        observed_at=observed_at,
        coalesce_window_seconds=coalesce_window_seconds,
    )


def ingest_chat_event_ndjson_lines(
    conn: sqlite3.Connection,
    lines: Iterable[str],
    *,
    source_id: str,
    worker_source_type: str,
    chat_type: str,
    observed_at: str | None = None,
    coalesce_window_seconds: int = 60,
    chat_id_override: str | None = None,
) -> tuple[int, int]:
    """Ingest each NDJSON line that is a handled ``im.message.receive_v1`` envelope.

    Returns ``(json_objects, envelopes_ingested)`` where *json_objects* counts
    lines that decoded to a JSON object (blank lines skipped) and
    *envelopes_ingested* counts envelopes that produced a ledger observation
    (including duplicates that update an existing row).
    """

    objects = 0
    ingested = 0
    for envelope in iter_chat_event_envelopes_from_ndjson(lines):
        objects += 1
        item = ingest_receive_message_v1_envelope(
            conn,
            envelope,
            source_id=source_id,
            worker_source_type=worker_source_type,
            chat_type=chat_type,
            observed_at=observed_at,
            coalesce_window_seconds=coalesce_window_seconds,
            chat_id_override=chat_id_override,
        )
        if item is not None:
            ingested += 1
    return objects, ingested
