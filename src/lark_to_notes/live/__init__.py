"""In-repo live adapter building blocks (chat transport and canonical intake).

``sync-once``, ``sync-daemon``, and ``backfill`` use ``ChatLiveAdapter`` in
``lark_to_notes.live.chat_live`` (worker-style JSON + ``lark-cli``) instead of
importing ``automation.lark_worker``.  ``lark_to_notes.live.chat_ingest`` offers a
replay-identical path for raw ``dict`` payloads into ``raw_messages``.
"""

from __future__ import annotations

from lark_to_notes.live.chat_events import (
    ChatEventNdjsonIngestOutcome,
    bounded_envelope_excerpt,
    emit_sync_event_stage_log,
    envelope_event_id,
    event_type_from_envelope,
    extract_im_message_from_envelope,
    ingest_chat_event_ndjson_lines,
    ingest_receive_message_v1_envelope,
    iter_chat_event_envelopes_from_ndjson,
    payload_hash_for_chat_event,
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
    run_lark_cli_json_retryable,
)
from lark_to_notes.live.reaction_envelope_validation import (
    reaction_envelope_is_valid,
    reaction_envelope_validation_errors,
)
from lark_to_notes.live.reaction_envelopes import (
    ReactionEnvelopeValidation,
    im_message_reaction_event_types,
    is_im_message_reaction_event_type,
    validate_im_message_reaction_envelope,
)
from lark_to_notes.live.reaction_preflight import (
    PRIMARY_REACTION_READ_SCOPE,
    reaction_scope_preflight_check,
)
from lark_to_notes.live.worker_config import (
    LiveWorkerConfigError,
    LiveWorkerConfigSnapshot,
    load_live_worker_config,
    parse_live_worker_config_mapping,
)

__all__ = [
    "PRIMARY_REACTION_READ_SCOPE",
    "ChatEventNdjsonIngestOutcome",
    "DocumentAdapterBlockedError",
    "DocumentPollSummary",
    "LarkCliApiError",
    "LarkCliError",
    "LarkCliInvocationError",
    "LarkCliNotFoundError",
    "LiveWorkerConfigError",
    "LiveWorkerConfigSnapshot",
    "ReactionEnvelopeValidation",
    "bounded_envelope_excerpt",
    "emit_sync_event_stage_log",
    "envelope_event_id",
    "event_type_from_envelope",
    "extract_im_message_from_envelope",
    "im_message_reaction_event_types",
    "ingest_chat_event_ndjson_lines",
    "ingest_chat_records",
    "ingest_receive_message_v1_envelope",
    "is_im_message_reaction_event_type",
    "iter_chat_event_envelopes_from_ndjson",
    "load_live_worker_config",
    "parse_live_worker_config_mapping",
    "payload_hash_for_chat_event",
    "poll_document_surfaces_to_ledger",
    "reaction_envelope_is_valid",
    "reaction_envelope_validation_errors",
    "reaction_scope_preflight_check",
    "resolve_lark_cli_binary",
    "run_lark_cli_json",
    "run_lark_cli_json_retryable",
    "validate_im_message_reaction_envelope",
]
