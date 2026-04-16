"""Document, comment, and reply transport via ``lark-cli`` into the intake ledger.

Uses the same mixed poll/event ledger as chat intake
(:func:`~lark_to_notes.intake.ledger.observe_document_surface`) so downstream
processing can treat document surfaces as first-class revision-bearing records.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3  # noqa: TC003
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from lark_to_notes.intake.ledger import observe_document_surface
from lark_to_notes.intake.models import DocumentLifecycleState, DocumentRecordType, IntakePath
from lark_to_notes.live.lark_cli import LarkCliApiError, LarkCliError, run_lark_cli_json

logger = logging.getLogger(__name__)

type LarkCliJsonRunner = Callable[[list[str]], dict[str, Any]]


class DocumentAdapterBlockedError(LarkCliError):
    """Raised when a watched document surface cannot be ingested safely or is out of scope."""


@dataclass(frozen=True, slots=True)
class DocumentPollSummary:
    """Counts of ledger observations written during one poll cycle."""

    body_rows: int
    comment_rows: int
    reply_rows: int


def _stable_content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _extract_markdown_from_fetch(payload: dict[str, Any]) -> tuple[str, str]:
    """Return ``(normalized_text, revision_hint)`` from a ``docs +fetch`` JSON envelope."""

    data = payload.get("data", payload)
    if not isinstance(data, dict):
        return "", ""
    for key in ("markdown", "content", "text", "body"):
        val = data.get(key)
        if isinstance(val, str) and val.strip():
            rev = ""
            for rev_key in ("revision_id", "revision", "edit_time", "update_time"):
                r = data.get(rev_key)
                if r is not None and str(r).strip():
                    rev = str(r)
                    break
            return val, rev
    return "", ""


def _list_shape_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = payload.get("data")
    if not isinstance(data, dict):
        return []
    items = data.get("items")
    if isinstance(items, list):
        return [x for x in items if isinstance(x, dict)]
    return []


def _comment_text(comment: Mapping[str, Any]) -> str:
    for key in ("content", "text", "comment", "markdown"):
        v = comment.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _comment_id(comment: Mapping[str, Any]) -> str:
    for key in ("comment_id", "id"):
        v = comment.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


def _reply_id(reply: Mapping[str, Any]) -> str:
    for key in ("reply_id", "id"):
        v = reply.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


def _reply_text(reply: Mapping[str, Any]) -> str:
    return _comment_text(reply)


def _lifecycle_for_mapping(row: Mapping[str, Any]) -> DocumentLifecycleState:
    if row.get("deleted") is True or row.get("is_deleted") is True:
        return DocumentLifecycleState.DELETED
    status = str(row.get("status", "")).lower()
    if status in {"deleted", "removed"}:
        return DocumentLifecycleState.DELETED
    return DocumentLifecycleState.ACTIVE


def _default_lark_runner(argv: list[str]) -> dict[str, Any]:
    return run_lark_cli_json(argv)


def poll_document_surfaces_to_ledger(
    conn: sqlite3.Connection,
    *,
    source_id: str,
    document_token: str,
    file_type: str,
    intake_path: IntakePath,
    runner: LarkCliJsonRunner | None = None,
) -> DocumentPollSummary:
    """Poll document body, comments, and replies via ``lark-cli`` into ``document_intake_ledger``.

    Args:
        conn: Open SQLite connection (``document_intake_ledger`` must exist).
        source_id: Watched source id (must already exist in ``watched_sources``).
        document_token: Lark ``file_token`` / doc token used with drive APIs.
        file_type: ``doc`` or ``docx`` (and other drive ``file_type`` values supported by Lark).
        intake_path: Whether this observation came from poll or event transport.
        runner: Optional test hook; defaults to :func:`run_lark_cli_json`.

    Returns:
        Counts of rows observed this cycle.

    Raises:
        DocumentAdapterBlockedError: For unsupported tokens such as bare wiki URLs.
        LarkCliApiError: When ``lark-cli`` reports ``ok: false``.
    """

    if not document_token.strip():
        raise DocumentAdapterBlockedError("document_token must be non-empty")
    lowered = document_token.lower()
    if "wiki/" in lowered or lowered.startswith("wiki"):
        raise DocumentAdapterBlockedError(
            "wiki URLs must be resolved to a concrete file_token via wiki node lookup "
            "before document polling; refusing bare wiki tokens."
        )

    ft = file_type.strip().lower()
    if ft not in {"doc", "docx", "sheet", "slides", "file"}:
        raise DocumentAdapterBlockedError(
            f"unsupported file_type {file_type!r} for document comments API "
            f"(expected one of doc, docx, sheet, slides, file)"
        )

    run = runner or _default_lark_runner

    body_rows = 0
    comment_rows = 0
    reply_rows = 0

    try:
        fetch_payload = run(
            ["docs", "+fetch", "--doc", document_token, "--format", "json"],
        )
    except LarkCliApiError as exc:
        raise LarkCliApiError(
            f"docs +fetch failed: {exc}",
            code=exc.code,
            payload=exc.payload,
        ) from exc

    markdown, rev_hint = _extract_markdown_from_fetch(fetch_payload)
    body_hash = _stable_content_hash(markdown)
    observe_document_surface(
        conn,
        record_type=DocumentRecordType.DOC_BODY,
        source_id=source_id,
        document_token=document_token,
        source_stream_id=f"{document_token}:body",
        source_item_id="body",
        content_hash=body_hash,
        normalized_text=markdown,
        payload=fetch_payload,
        intake_path=intake_path,
        revision_id=rev_hint,
        lifecycle_state=DocumentLifecycleState.ACTIVE,
        canonical_link="",
    )
    body_rows = 1

    params_obj: dict[str, Any] = {
        "file_token": document_token,
        "file_type": ft,
        "page_size": 50,
    }
    try:
        comments_payload = run(
            [
                "drive",
                "file.comments",
                "list",
                "--params",
                json.dumps(params_obj, separators=(",", ":")),
                "--format",
                "json",
            ],
        )
    except LarkCliApiError as exc:
        raise LarkCliApiError(
            f"drive file.comments list failed: {exc}",
            code=exc.code,
            payload=exc.payload,
        ) from exc

    comments = _list_shape_items(comments_payload)
    for comment in comments:
        cid = _comment_id(comment)
        if not cid:
            logger.warning("skip_comment_without_id", extra={"source_id": source_id})
            continue
        text = _comment_text(comment)
        chash = _stable_content_hash(text)
        lifecycle = _lifecycle_for_mapping(comment)
        observe_document_surface(
            conn,
            record_type=DocumentRecordType.DOC_COMMENT,
            source_id=source_id,
            document_token=document_token,
            source_stream_id=f"{document_token}:comments",
            source_item_id=cid,
            content_hash=chash,
            normalized_text=text,
            payload=dict(comment),
            intake_path=intake_path,
            revision_id=str(comment.get("update_time") or comment.get("edit_time") or ""),
            lifecycle_state=lifecycle,
            canonical_link="",
        )
        comment_rows += 1

        reply_params = {
            "file_token": document_token,
            "file_type": ft,
            "comment_id": cid,
            "page_size": 50,
        }
        try:
            replies_payload = run(
                [
                    "drive",
                    "file.comment.replys",
                    "list",
                    "--params",
                    json.dumps(reply_params, separators=(",", ":")),
                    "--format",
                    "json",
                ],
            )
        except LarkCliApiError as exc:
            raise LarkCliApiError(
                f"drive file.comment.replys list failed for comment_id={cid}: {exc}",
                code=exc.code,
                payload=exc.payload,
            ) from exc

        for reply in _list_shape_items(replies_payload):
            rid = _reply_id(reply)
            if not rid:
                logger.warning("skip_reply_without_id", extra={"comment_id": cid})
                continue
            rtext = _reply_text(reply)
            rhash = _stable_content_hash(rtext)
            rlifecycle = _lifecycle_for_mapping(reply)
            observe_document_surface(
                conn,
                record_type=DocumentRecordType.DOC_REPLY,
                source_id=source_id,
                document_token=document_token,
                source_stream_id=f"{document_token}:comments:{cid}:replies",
                source_item_id=rid,
                parent_item_id=cid,
                content_hash=rhash,
                normalized_text=rtext,
                payload=dict(reply),
                intake_path=intake_path,
                revision_id=str(reply.get("update_time") or reply.get("edit_time") or ""),
                lifecycle_state=rlifecycle,
                canonical_link="",
            )
            reply_rows += 1

    return DocumentPollSummary(
        body_rows=body_rows,
        comment_rows=comment_rows,
        reply_rows=reply_rows,
    )
