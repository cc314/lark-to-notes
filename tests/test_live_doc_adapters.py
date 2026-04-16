"""Tests for ``lark-cli``-backed document surface adapters."""

from __future__ import annotations

import sqlite3
from typing import Any

import pytest

from lark_to_notes.config.sources import SourceType, WatchedSource
from lark_to_notes.intake.ledger import document_ingest_key, get_document_intake_item
from lark_to_notes.intake.models import DocumentRecordType, IntakePath
from lark_to_notes.live.doc_adapters import (
    DocumentAdapterBlockedError,
    poll_document_surfaces_to_ledger,
)
from lark_to_notes.live.lark_cli import LarkCliApiError
from lark_to_notes.storage.db import connect, init_db, upsert_watched_source


def _mem() -> sqlite3.Connection:
    conn = connect(":memory:")
    init_db(conn)
    upsert_watched_source(
        conn,
        WatchedSource(
            source_id="doc:tok1",
            source_type=SourceType.DOC,
            external_id="tok1",
            name="Doc",
        ),
    )
    return conn


_FETCH_OK: dict[str, Any] = {
    "ok": True,
    "data": {"markdown": "# Title\nhello", "revision_id": "rev-a"},
}

_COMMENTS_OK: dict[str, Any] = {
    "ok": True,
    "data": {
        "items": [
            {
                "comment_id": "c1",
                "content": "first thread",
                "update_time": "111",
            },
            {
                "comment_id": "c2",
                "content": "second thread",
                "update_time": "222",
            },
        ]
    },
}

_REPLIES_C1: dict[str, Any] = {
    "ok": True,
    "data": {"items": [{"reply_id": "r1", "content": "reply a", "update_time": "1"}]},
}

_REPLIES_C2: dict[str, Any] = {
    "ok": True,
    "data": {"items": []},
}


def test_poll_document_surfaces_writes_ledger_rows() -> None:
    conn = _mem()
    queue: list[dict[str, Any]] = [_FETCH_OK, _COMMENTS_OK, _REPLIES_C1, _REPLIES_C2]

    def fake_runner(argv: list[str]) -> dict[str, Any]:
        assert argv[0] in {"docs", "drive"}, argv
        return queue.pop(0)

    summary = poll_document_surfaces_to_ledger(
        conn,
        source_id="doc:tok1",
        document_token="doxcnREALTOKENFAKE1",
        file_type="docx",
        intake_path=IntakePath.POLL,
        runner=fake_runner,
    )
    assert summary.body_rows == 1
    assert summary.comment_rows == 2
    assert summary.reply_rows == 1

    body_key = document_ingest_key(
        "doc:tok1",
        DocumentRecordType.DOC_BODY,
        "doxcnREALTOKENFAKE1:body",
        "body",
    )
    row = get_document_intake_item(conn, body_key)
    assert row is not None
    assert row.normalized_text.startswith("# Title")
    assert row.record_type is DocumentRecordType.DOC_BODY


def test_wiki_token_is_blocked() -> None:
    conn = _mem()

    def _fail(_argv: list[str]) -> dict[str, Any]:
        raise AssertionError("runner should not be called")

    with pytest.raises(DocumentAdapterBlockedError, match="wiki"):
        poll_document_surfaces_to_ledger(
            conn,
            source_id="doc:tok1",
            document_token="wikiFAKE1234567890123456789012",
            file_type="docx",
            intake_path=IntakePath.POLL,
            runner=_fail,
        )


def test_unsupported_file_type_is_blocked() -> None:
    conn = _mem()

    def _fail(_argv: list[str]) -> dict[str, Any]:
        raise AssertionError("runner should not be called")

    with pytest.raises(DocumentAdapterBlockedError, match="unsupported file_type"):
        poll_document_surfaces_to_ledger(
            conn,
            source_id="doc:tok1",
            document_token="doxcnREALTOKENFAKE1",
            file_type="bitable",
            intake_path=IntakePath.POLL,
            runner=_fail,
        )


def test_lark_cli_api_error_surfaces() -> None:
    conn = _mem()

    def err_runner(_argv: list[str]) -> dict[str, Any]:
        raise LarkCliApiError("boom", code=123, payload={"ok": False})

    with pytest.raises(LarkCliApiError, match="docs \\+fetch failed"):
        poll_document_surfaces_to_ledger(
            conn,
            source_id="doc:tok1",
            document_token="doxcnREALTOKENFAKE1",
            file_type="docx",
            intake_path=IntakePath.POLL,
            runner=err_runner,
        )
