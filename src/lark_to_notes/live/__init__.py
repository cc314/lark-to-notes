"""In-repo live adapter building blocks (chat transport and canonical intake).

``sync-once``, ``sync-daemon``, and ``backfill`` use ``ChatLiveAdapter`` in
``lark_to_notes.live.chat_live`` (worker-style JSON + ``lark-cli``) instead of
importing ``automation.lark_worker``.  ``lark_to_notes.live.chat_ingest`` offers a
replay-identical path for raw ``dict`` payloads into ``raw_messages``.
"""

from __future__ import annotations

from lark_to_notes.live.chat_events import (
    event_type_from_envelope,
    extract_im_message_from_envelope,
    ingest_chat_event_ndjson_lines,
    ingest_receive_message_v1_envelope,
    iter_chat_event_envelopes_from_ndjson,
)
from lark_to_notes.live.chat_ingest import ingest_chat_records
from lark_to_notes.live.doc_adapters import (
    DocumentAdapterBlockedError,
    DocumentPollSummary,
    poll_document_surfaces_to_ledger,
)
from lark_to_notes.live.lark_cli import (
    LarkCliApiError,
    LarkCliError,
    LarkCliInvocationError,
    LarkCliNotFoundError,
    resolve_lark_cli_binary,
    run_lark_cli_json,
)
from lark_to_notes.live.worker_config import (
    LiveWorkerConfigError,
    LiveWorkerConfigSnapshot,
    load_live_worker_config,
    parse_live_worker_config_mapping,
)

__all__ = [
    "DocumentAdapterBlockedError",
    "DocumentPollSummary",
    "LarkCliApiError",
    "LarkCliError",
    "LarkCliInvocationError",
    "LarkCliNotFoundError",
    "LiveWorkerConfigError",
    "LiveWorkerConfigSnapshot",
    "event_type_from_envelope",
    "extract_im_message_from_envelope",
    "ingest_chat_event_ndjson_lines",
    "ingest_chat_records",
    "ingest_receive_message_v1_envelope",
    "iter_chat_event_envelopes_from_ndjson",
    "load_live_worker_config",
    "parse_live_worker_config_mapping",
    "poll_document_surfaces_to_ledger",
    "resolve_lark_cli_binary",
    "run_lark_cli_json",
]
