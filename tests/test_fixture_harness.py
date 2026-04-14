"""Tests for reusable fixture-corpus loading helpers."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from lark_to_notes.storage.db import connect, init_db
from lark_to_notes.testing import FixtureCorpusError, load_fixture_corpus

FIXTURE_CORPUS_ROOT = Path(__file__).resolve().parents[1] / "raw" / "lark-worker" / "fixture-corpus"


def test_load_fixture_corpus_from_directory() -> None:
    corpus = load_fixture_corpus(FIXTURE_CORPUS_ROOT)

    assert corpus.manifest_path == FIXTURE_CORPUS_ROOT / "manifest.json"
    assert corpus.record_count >= 1
    assert corpus.scenario_names == (
        "han",
        "mixed",
        "updated",
        "english_only",
        "threaded",
        "deleted",
    )
    assert corpus.missing_scenarios == ()


def test_load_fixture_corpus_from_manifest_path() -> None:
    corpus = load_fixture_corpus(FIXTURE_CORPUS_ROOT / "manifest.json")

    assert corpus.root == FIXTURE_CORPUS_ROOT


def test_load_named_scenario() -> None:
    corpus = load_fixture_corpus(FIXTURE_CORPUS_ROOT)

    scenario = corpus.load_scenario("deleted")
    payload = scenario["payload"]

    assert scenario["source_id"] == "dm-zhao-yuanlong"
    assert scenario["message_id"] == "om_x100b55a770069cace121eb0bf2fe31b"
    assert isinstance(payload, dict)
    assert payload["deleted"] is True


def test_unknown_scenario_raises_key_error() -> None:
    corpus = load_fixture_corpus(FIXTURE_CORPUS_ROOT)

    with pytest.raises(KeyError, match="Unknown fixture scenario"):
        corpus.load_scenario("missing")


def test_load_doc_surface_artifact() -> None:
    corpus = load_fixture_corpus(FIXTURE_CORPUS_ROOT)

    report = corpus.load_doc_surface_artifact("doc-surfaces/source-access-report.json")
    counts_by_status = report["counts_by_status"]

    assert report["probe_count"] == 8
    assert isinstance(counts_by_status, dict)
    assert counts_by_status["ok"] == 6


def test_fixture_coverage_summary() -> None:
    corpus = load_fixture_corpus(FIXTURE_CORPUS_ROOT)

    coverage = corpus.coverage(
        required_scenarios=("han", "mixed", "updated", "english_only", "threaded", "deleted"),
        required_surfaces=(
            "dm_chat_messages",
            "group_chat_messages",
            "doc_fetch",
            "doc_comments",
            "doc_comment_replies",
        ),
    )

    assert coverage.record_count == corpus.record_count
    assert coverage.missing_required_scenarios == ()
    assert coverage.missing_required_surfaces == ()
    assert coverage.source_access_surfaces == (
        "dm_chat_messages",
        "group_chat_messages",
        "doc_fetch",
        "doc_comments",
        "doc_comment_replies",
    )


def test_fixture_replay_summary_matches_checked_in_raw_logs() -> None:
    corpus = load_fixture_corpus(FIXTURE_CORPUS_ROOT)
    conn = connect(":memory:")
    init_db(conn)
    expected_files = corpus.raw_log_paths()

    summary = corpus.replay_summary(conn)

    assert summary.file_count == len(expected_files)
    assert summary.total_records == corpus.record_count
    assert summary.inserted_records == corpus.record_count
    assert summary.db_record_count == corpus.record_count
    assert summary.files[0].filename == expected_files[0].name
    assert summary.files[-1].filename == expected_files[-1].name


def test_fixture_replay_summary_is_idempotent() -> None:
    corpus = load_fixture_corpus(FIXTURE_CORPUS_ROOT)
    conn = connect(":memory:")
    init_db(conn)

    first = corpus.replay_summary(conn)
    second = corpus.replay_summary(conn)

    assert first.inserted_records == corpus.record_count
    assert second.total_records == corpus.record_count
    assert second.inserted_records == 0
    assert second.db_record_count == corpus.record_count


def test_fixture_coverage_reports_missing_items() -> None:
    corpus = load_fixture_corpus(FIXTURE_CORPUS_ROOT)

    coverage = corpus.coverage(
        required_scenarios=("han", "imaginary"),
        required_surfaces=("dm_chat_messages", "imaginary_surface"),
    )

    assert coverage.missing_required_scenarios == ("imaginary",)
    assert coverage.missing_required_surfaces == ("imaginary_surface",)


def test_fixture_coverage_treats_manifest_missing_scenarios_as_missing(tmp_path: Path) -> None:
    fixture_root = tmp_path / "fixture-corpus"
    doc_surfaces_dir = fixture_root / "doc-surfaces"
    doc_surfaces_dir.mkdir(parents=True)

    (doc_surfaces_dir / "source-access-report.json").write_text(
        json.dumps({"counts_by_surface": {"dm_chat_messages": 1}}),
        encoding="utf-8",
    )
    (fixture_root / "manifest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-04-14T00:00:00Z",
                "raw_summary": {"record_count": 1},
                "scenario_files": {"han": "han.json"},
                "missing_scenarios": ["deleted"],
                "doc_surface_artifacts": ["doc-surfaces/source-access-report.json"],
            }
        ),
        encoding="utf-8",
    )

    corpus = load_fixture_corpus(fixture_root)
    coverage = corpus.coverage(required_scenarios=("han", "deleted"))

    assert coverage.missing_required_scenarios == ("deleted",)


def test_unknown_doc_surface_artifact_raises_key_error() -> None:
    corpus = load_fixture_corpus(FIXTURE_CORPUS_ROOT)

    with pytest.raises(KeyError, match="Unknown doc-surface artifact"):
        corpus.load_doc_surface_artifact("doc-surfaces/does-not-exist.json")


def test_invalid_manifest_raises_fixture_error(tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps({"generated_at": "2026-04-14T00:00:00Z"}), encoding="utf-8")

    with pytest.raises(FixtureCorpusError, match="Expected object for raw_summary"):
        load_fixture_corpus(manifest_path)
