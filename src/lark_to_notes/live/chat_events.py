"""Lark chat *event* envelopes into the mixed poll/event chat-intake ledger.

`lark-cli event +subscribe` streams NDJSON objects; each line that decodes to a
Feishu/Lark ``im.message.receive_v1`` style envelope can be passed through
:func:`ingest_receive_message_v1_envelope` so **event** observations share the
same :func:`~lark_to_notes.intake.ledger.observe_chat_message` path as polling
(:class:`~lark_to_notes.intake.models.IntakePath` ``EVENT``).
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from lark_to_notes.intake.ledger import observe_chat_message
from lark_to_notes.intake.models import ChatIntakeItem, IntakePath
from lark_to_notes.intake.reaction_caps import (
    ReactionIntakeCaps,
    ReactionIntakeCapState,
    reaction_cap_block_reason,
    reaction_cap_consume_slot,
    reaction_cap_release_slot,
)
from lark_to_notes.intake.reaction_deferrals import insert_reaction_intake_deferral
from lark_to_notes.intake.reaction_model import ReactionKind, parse_reaction_envelope
from lark_to_notes.intake.reaction_store import (
    canonical_reaction_event_id,
    insert_message_reaction_event,
    reaction_event_row_exists,
)
from lark_to_notes.live.chat_live import raw_message_from_lark_im_api
from lark_to_notes.live.reaction_envelopes import (
    is_im_message_reaction_event_type,
    validate_im_message_reaction_envelope,
)

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable, Iterable, Iterator

logger = logging.getLogger(__name__)

_SYNC_EVENT_STAGE_LOG_RESULTS = frozenset(
    {"accepted", "quarantined", "deferred", "idempotent_skip"}
)


def emit_sync_event_stage_log(
    sink: Callable[[dict[str, Any]], None],
    *,
    run_id: str,
    stage: str,
    event_type: str,
    event_id: str | None,
    source_id: str,
    message_id: str | None,
    result: str,
    reason_code: str,
    duration_ms: int,
) -> None:
    """Emit one structured record for ``sync-events`` / NDJSON integration (lw-pzj.10.6)."""

    if result not in _SYNC_EVENT_STAGE_LOG_RESULTS:
        raise ValueError(f"invalid stage log result: {result!r}")
    sink(
        {
            "ts": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "stage": stage,
            "event_type": event_type,
            "event_id": event_id,
            "source_id": source_id,
            "message_id": message_id,
            "result": result,
            "reason_code": reason_code,
            "duration_ms": int(duration_ms),
            "run_id": run_id,
        }
    )

_RECEIVE_MESSAGE_V1 = "im.message.receive_v1"
_EXCERPT_MAX = 256
_HASH_PREFIX_LEN = 16
_HASH_INPUT_MAX = 8192


@dataclass(frozen=True)
class ChatEventNdjsonIngestOutcome:
    """Counters and last-quarantine fingerprints from :func:`ingest_chat_event_ndjson_lines`.

    ``last_*`` fields retain only the **most recent** quarantine row (bounded memory);
    totals live in the ``*_rejects`` / ``*_exceptions`` counters for doctor JSON (lw-pzj.9.*).

    ``reaction_cap_deferred`` counts validated reaction envelopes skipped because
    intake caps were already exhausted; ``last_reaction_cap_reason_code`` mirrors
    quarantine-style operator hints.

    ``reaction_benign_duplicate_replays`` counts parsed reactions whose canonical
    ``reaction_event_id`` was already stored (retry storm; no cap slot, no insert).

    ``reaction_rows_inserted_add`` / ``reaction_rows_inserted_remove`` split successful
    inserts by ``reaction_kind``. ``reaction_quarantined`` is the sum of validation
    rejects, parse-none-after-validate, and insert exceptions (stable ``sync-events --json`` / CI).
    """

    json_objects: int
    chat_envelopes_ingested: int
    reaction_rows_inserted: int
    reaction_rows_inserted_add: int
    reaction_rows_inserted_remove: int
    reaction_quarantined: int
    chat_receive_observation_exceptions: int
    reaction_validation_rejects: int
    reaction_insert_exceptions: int
    reaction_parse_none_after_validate: int
    reaction_benign_duplicate_replays: int
    reaction_cap_deferred: int
    last_reaction_cap_reason_code: str | None
    last_chat_quarantine_event_id: str | None
    last_chat_quarantine_payload_hash: str | None
    last_chat_quarantine_reason_code: str | None
    last_reaction_quarantine_event_id: str | None
    last_reaction_quarantine_payload_hash: str | None
    last_reaction_quarantine_reason_code: str | None


def envelope_event_id(envelope: dict[str, Any]) -> str:
    """Return ``header.event_id`` when present (empty string otherwise)."""

    header = envelope.get("header")
    if isinstance(header, dict):
        return str(header.get("event_id") or "").strip()
    return ""


def bounded_envelope_excerpt(envelope: dict[str, Any], *, limit: int = _EXCERPT_MAX) -> str:
    """Short operator-safe excerpt of the envelope for structured logs."""

    try:
        raw = json.dumps(envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError):
        raw = repr(envelope)
    if len(raw) > limit:
        return f"{raw[:limit]}…"
    return raw


def payload_hash_for_chat_event(envelope: dict[str, Any]) -> str:
    """Stable SHA-256 prefix over bounded canonical JSON (quarantine / doctor parity)."""

    try:
        raw = json.dumps(envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError):
        raw = repr(envelope)
    if len(raw) > _HASH_INPUT_MAX:
        raw = raw[:_HASH_INPUT_MAX]
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:_HASH_PREFIX_LEN]


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


def _receive_v1_message_id_hint(envelope: dict[str, Any]) -> str | None:
    """Best-effort ``message_id`` for ``im.message.receive_v1`` envelopes."""

    if event_type_from_envelope(envelope) != _RECEIVE_MESSAGE_V1:
        return None
    msg = extract_im_message_from_envelope(envelope)
    if msg is None:
        return None
    mid = str(msg.get("message_id") or "").strip()
    return mid or None


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
    caps: ReactionIntakeCaps | None = None,
    cap_state: ReactionIntakeCapState | None = None,
    reaction_intake_run_id: str | None = None,
    stage_log: Callable[[dict[str, Any]], None] | None = None,
) -> ChatEventNdjsonIngestOutcome:
    """Ingest NDJSON lines for chat message events and IM reaction events.

    When *caps* limits are active, provide *reaction_intake_run_id* (typically a
    ``runtime_runs.run_id``) so cap hits persist :func:`insert_reaction_intake_deferral`
    rows keyed by ``(run_id, source_id, event_id / payload_hash)``.

    Malformed or failing rows **do not abort** the iterator: exceptions on the
    ``im.message.receive_v1`` ledger path and the reaction insert path are caught,
    logged with ``reason_code`` / ``payload_hash`` / ``event_id``, and counted for
    operator surfaces (``lw-pzj.9.*`` / ``sync-events --json``).

    When *stage_log* is set, each decoded envelope emits exactly one structured
    record (``lw-pzj.10.6``): ``ts``, ``stage``, ``event_type``, ``event_id``,
    ``source_id``, ``message_id``, ``result`` (``accepted`` / ``quarantined`` /
    ``deferred`` / ``idempotent_skip``), ``reason_code``, ``duration_ms``,
    ``run_id``.

    Returns :class:`ChatEventNdjsonIngestOutcome` with *json_objects* (decoded
    dict lines), *chat_envelopes_ingested* (``im.message.receive_v1`` ledger
    observations), *reaction_rows_inserted* (new ``message_reaction_events``
    rows), *reaction_rows_inserted_add* / *reaction_rows_inserted_remove* (split
    by ``reaction_kind``), *reaction_quarantined* (validation + parse-none +
    insert-exception totals), *reaction_benign_duplicate_replays* (canonical id
    already present), plus quarantine counters and last-seen fingerprints (bounded memory).
    """
    eff_caps = caps or ReactionIntakeCaps()
    eff_state = cap_state or ReactionIntakeCapState()
    if eff_caps.limits_active and not (reaction_intake_run_id or "").strip():
        msg = "reaction_intake_run_id is required when reaction intake caps are active"
        raise ValueError(msg)

    objects = 0
    ingested = 0
    reactions_inserted = 0
    reactions_inserted_add = 0
    reactions_inserted_remove = 0
    chat_exc = 0
    rx_val_reject = 0
    rx_ins_exc = 0
    rx_parse_none = 0
    rx_benign_dup = 0
    rx_cap_deferred = 0
    last_cap_rc: str | None = None
    last_chat_eid: str | None = None
    last_chat_ph: str | None = None
    last_chat_rc: str | None = None
    last_rx_eid: str | None = None
    last_rx_ph: str | None = None
    last_rx_rc: str | None = None

    log_run_id = (reaction_intake_run_id or "").strip() or f"ndjson-{uuid.uuid4().hex[:12]}"

    def _emit(
        *,
        stage: str,
        event_type: str,
        event_id: str | None,
        message_id: str | None,
        result: str,
        reason_code: str,
        start_ns: int,
    ) -> None:
        if stage_log is None:
            return
        duration_ms = max(0, int((time.perf_counter_ns() - start_ns) // 1_000_000))
        emit_sync_event_stage_log(
            stage_log,
            run_id=log_run_id,
            stage=stage,
            event_type=event_type,
            event_id=event_id,
            source_id=source_id,
            message_id=message_id,
            result=result,
            reason_code=reason_code,
            duration_ms=duration_ms,
        )

    for envelope in iter_chat_event_envelopes_from_ndjson(lines):
        objects += 1
        t0 = time.perf_counter_ns()
        eid0 = envelope_event_id(envelope) or None
        try:
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
        except Exception as exc:
            chat_exc += 1
            ph = payload_hash_for_chat_event(envelope)
            eid = envelope_event_id(envelope)
            et = event_type_from_envelope(envelope)
            rc = f"chat_receive_v1_exception:{type(exc).__name__}"
            last_chat_eid, last_chat_ph, last_chat_rc = eid or None, ph, rc
            logger.exception(
                "chat_receive_v1_quarantined",
                extra={
                    "reason_code": rc,
                    "payload_hash": ph,
                    "event_id": eid,
                    "event_type": et,
                    "source_id": source_id,
                    "payload_excerpt": bounded_envelope_excerpt(envelope),
                },
            )
            _emit(
                stage="chat_receive_v1",
                event_type=et,
                event_id=eid or None,
                message_id=_receive_v1_message_id_hint(envelope),
                result="quarantined",
                reason_code=rc,
                start_ns=t0,
            )
            continue
        if item is not None:
            ingested += 1
            _emit(
                stage="chat_receive_v1",
                event_type=_RECEIVE_MESSAGE_V1,
                event_id=eid0,
                message_id=item.message_id,
                result="accepted",
                reason_code="chat_receive_v1_observed",
                start_ns=t0,
            )
            continue
        et = event_type_from_envelope(envelope)
        if not is_im_message_reaction_event_type(et):
            _emit(
                stage="ndjson_routing",
                event_type=et,
                event_id=eid0,
                message_id=_receive_v1_message_id_hint(envelope),
                result="idempotent_skip",
                reason_code="event_type_not_ingested",
                start_ns=t0,
            )
            continue
        ph = payload_hash_for_chat_event(envelope)
        eid = envelope_event_id(envelope)
        ev_payload = envelope.get("event")
        rx_msg_hint: str | None = None
        if isinstance(ev_payload, dict):
            xm = str(ev_payload.get("message_id") or "").strip()
            rx_msg_hint = xm or None
        check = validate_im_message_reaction_envelope(envelope)
        if not check.ok:
            rx_val_reject += 1
            rc = "reaction_envelope_invalid:" + ",".join(check.errors)
            last_rx_eid, last_rx_ph, last_rx_rc = eid or None, ph, rc
            logger.info(
                "reaction_envelope_quarantined",
                extra={
                    "reason_code": rc,
                    "payload_hash": ph,
                    "event_id": eid,
                    "event_type": et,
                    "source_id": source_id,
                    "payload_excerpt": bounded_envelope_excerpt(envelope),
                },
            )
            _emit(
                stage="reaction_envelope_validate",
                event_type=et,
                event_id=eid or None,
                message_id=rx_msg_hint,
                result="quarantined",
                reason_code=rc,
                start_ns=t0,
            )
            continue
        rev = parse_reaction_envelope(envelope, source_id=source_id)
        if rev is None:
            rx_parse_none += 1
            rc = "reaction_parse_none_after_validate"
            last_rx_eid, last_rx_ph, last_rx_rc = eid or None, ph, rc
            logger.warning(
                "reaction_pipeline_quarantined",
                extra={
                    "reason_code": rc,
                    "payload_hash": ph,
                    "event_id": eid,
                    "event_type": et,
                    "source_id": source_id,
                    "payload_excerpt": bounded_envelope_excerpt(envelope),
                },
            )
            _emit(
                stage="reaction_parse",
                event_type=et,
                event_id=eid or None,
                message_id=rx_msg_hint,
                result="quarantined",
                reason_code=rc,
                start_ns=t0,
            )
            continue
        rid = canonical_reaction_event_id(rev)
        if reaction_event_row_exists(conn, rid):
            rx_benign_dup += 1
            _emit(
                stage="reaction_dedupe",
                event_type=et,
                event_id=eid or None,
                message_id=rev.message_id,
                result="idempotent_skip",
                reason_code="reaction_row_already_present",
                start_ns=t0,
            )
            continue
        cap_rc = reaction_cap_block_reason(eff_caps, eff_state, source_id=source_id)
        if cap_rc is not None:
            rx_cap_deferred += 1
            last_cap_rc = cap_rc
            assert reaction_intake_run_id is not None
            insert_reaction_intake_deferral(
                conn,
                run_id=reaction_intake_run_id,
                source_id=source_id,
                cursor_event_id=eid,
                cursor_payload_hash=ph,
                reason_code=cap_rc,
                governance_version=eff_caps.governance_version,
                policy_version=eff_caps.policy_version,
                payload_extra={
                    "event_type": et,
                    "run_total": eff_state.run_total,
                    "source_total": eff_state.by_source.get(source_id, 0),
                    "max_per_run": eff_caps.max_reaction_envelopes_per_run,
                    "max_per_source": eff_caps.max_reaction_envelopes_per_source_per_run,
                    "payload_excerpt": bounded_envelope_excerpt(envelope),
                },
            )
            logger.info(
                "reaction_intake_cap_deferred",
                extra={
                    "reason_code": cap_rc,
                    "payload_hash": ph,
                    "event_id": eid,
                    "event_type": et,
                    "source_id": source_id,
                    "run_id": reaction_intake_run_id,
                    "payload_excerpt": bounded_envelope_excerpt(envelope),
                },
            )
            _emit(
                stage="reaction_intake_cap",
                event_type=et,
                event_id=eid or None,
                message_id=rev.message_id,
                result="deferred",
                reason_code=cap_rc,
                start_ns=t0,
            )
            continue
        reaction_cap_consume_slot(eff_state, source_id=source_id)
        try:
            res = insert_message_reaction_event(
                conn,
                rev,
                governance_version=eff_caps.governance_version,
                policy_version=eff_caps.policy_version,
            )
        except Exception as exc:
            reaction_cap_release_slot(eff_state, source_id=source_id)
            rx_ins_exc += 1
            rc = f"reaction_insert_exception:{type(exc).__name__}"
            last_rx_eid, last_rx_ph, last_rx_rc = eid or None, ph, rc
            logger.exception(
                "reaction_insert_quarantined",
                extra={
                    "reason_code": rc,
                    "payload_hash": ph,
                    "event_id": eid,
                    "event_type": et,
                    "source_id": source_id,
                    "payload_excerpt": bounded_envelope_excerpt(envelope),
                },
            )
            _emit(
                stage="reaction_insert",
                event_type=et,
                event_id=eid or None,
                message_id=rev.message_id,
                result="quarantined",
                reason_code=rc,
                start_ns=t0,
            )
            continue
        if res.inserted:
            reactions_inserted += 1
            if rev.reaction_kind is ReactionKind.ADD:
                reactions_inserted_add += 1
            else:
                reactions_inserted_remove += 1
            _emit(
                stage="reaction_insert",
                event_type=et,
                event_id=eid or None,
                message_id=rev.message_id,
                result="accepted",
                reason_code="reaction_row_inserted",
                start_ns=t0,
            )
        else:
            reaction_cap_release_slot(eff_state, source_id=source_id)
            _emit(
                stage="reaction_insert",
                event_type=et,
                event_id=eid or None,
                message_id=rev.message_id,
                result="idempotent_skip",
                reason_code="reaction_row_duplicate_replay",
                start_ns=t0,
            )
    rx_quarantined = rx_val_reject + rx_parse_none + rx_ins_exc
    return ChatEventNdjsonIngestOutcome(
        json_objects=objects,
        chat_envelopes_ingested=ingested,
        reaction_rows_inserted=reactions_inserted,
        reaction_rows_inserted_add=reactions_inserted_add,
        reaction_rows_inserted_remove=reactions_inserted_remove,
        reaction_quarantined=rx_quarantined,
        chat_receive_observation_exceptions=chat_exc,
        reaction_validation_rejects=rx_val_reject,
        reaction_insert_exceptions=rx_ins_exc,
        reaction_parse_none_after_validate=rx_parse_none,
        reaction_benign_duplicate_replays=rx_benign_dup,
        reaction_cap_deferred=rx_cap_deferred,
        last_reaction_cap_reason_code=last_cap_rc,
        last_chat_quarantine_event_id=last_chat_eid,
        last_chat_quarantine_payload_hash=last_chat_ph,
        last_chat_quarantine_reason_code=last_chat_rc,
        last_reaction_quarantine_event_id=last_rx_eid,
        last_reaction_quarantine_payload_hash=last_rx_ph,
        last_reaction_quarantine_reason_code=last_rx_rc,
    )
