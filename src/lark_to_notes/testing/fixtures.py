"""Reusable loaders for the checked-in fixture corpus.

Also contains fixture-build helpers (``iter_raw_records``,
``select_fixture_examples``, ``copy_source_access_artifacts``) and source-
access probe utilities (``ProbeRecord``, ``summarize_probe``,
``build_report_manifest``, ``extract_doc_token``) that were used to
generate the checked-in fixture data.
"""

from __future__ import annotations

import json
import re
import shutil
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from lark_to_notes.intake.ledger import count_raw_messages
from lark_to_notes.intake.replay import replay_jsonl_dir

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Iterable


class FixtureCorpusError(ValueError):
    """Raised when the fixture corpus manifest is malformed."""


@dataclass(frozen=True, slots=True)
class FixtureCoverage:
    """Coverage summary for fixture scenarios and source-access surfaces."""

    record_count: int
    scenario_names: tuple[str, ...]
    source_access_surfaces: tuple[str, ...]
    missing_required_scenarios: tuple[str, ...]
    missing_required_surfaces: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class FixtureReplayFile:
    """Replay outcome for one raw JSONL file."""

    filename: str
    total_records: int
    inserted_records: int


@dataclass(frozen=True, slots=True)
class FixtureReplaySummary:
    """Replay outcome across the repository's checked-in raw JSONL corpus."""

    file_count: int
    total_records: int
    inserted_records: int
    db_record_count: int
    files: tuple[FixtureReplayFile, ...]


@dataclass(frozen=True, slots=True)
class FixtureCorpus:
    """A typed view over the repository's fixture corpus manifest."""

    root: Path
    manifest_path: Path
    generated_at: str
    raw_summary: dict[str, object]
    scenario_files: dict[str, str]
    missing_scenarios: tuple[str, ...]
    doc_surface_artifacts: tuple[str, ...]

    @classmethod
    def from_path(cls, path: str | Path) -> FixtureCorpus:
        """Load a fixture corpus from a directory or manifest path."""
        candidate = Path(path).expanduser().resolve()
        manifest_path = candidate if candidate.is_file() else candidate / "manifest.json"
        payload = _read_json_object(manifest_path)

        generated_at = _expect_string(payload, "generated_at")
        raw_summary = _expect_object(payload, "raw_summary")
        scenario_files = _expect_string_mapping(payload, "scenario_files")
        missing_scenarios = tuple(_expect_string_list(payload, "missing_scenarios"))
        doc_surface_artifacts = tuple(_expect_string_list(payload, "doc_surface_artifacts"))

        return cls(
            root=manifest_path.parent,
            manifest_path=manifest_path,
            generated_at=generated_at,
            raw_summary=raw_summary,
            scenario_files=scenario_files,
            missing_scenarios=missing_scenarios,
            doc_surface_artifacts=doc_surface_artifacts,
        )

    @property
    def scenario_names(self) -> tuple[str, ...]:
        """Return the manifest's scenario names in insertion order."""
        return tuple(self.scenario_files)

    @property
    def record_count(self) -> int:
        """Return the total record count declared in the manifest summary."""
        return _expect_int(self.raw_summary, "record_count")

    @property
    def raw_log_dir(self) -> Path:
        """Return the sibling directory holding append-only raw JSONL logs."""
        return self.root.parent

    def raw_log_paths(self, *, glob: str = "*.jsonl") -> tuple[Path, ...]:
        """Return sorted raw JSONL log paths adjacent to the fixture corpus."""
        return tuple(sorted(self.raw_log_dir.glob(glob)))

    def scenario_path(self, name: str) -> Path:
        """Return the filesystem path for a named scenario."""
        try:
            relpath = self.scenario_files[name]
        except KeyError as error:
            raise KeyError(f"Unknown fixture scenario: {name}") from error
        return (self.root / relpath).resolve()

    def load_scenario(self, name: str) -> dict[str, object]:
        """Load a named scenario JSON document."""
        return _read_json_object(self.scenario_path(name))

    def doc_surface_path(self, relpath: str) -> Path:
        """Return the filesystem path for a doc-surface artifact."""
        if relpath not in self.doc_surface_artifacts:
            raise KeyError(f"Unknown doc-surface artifact: {relpath}")
        return (self.root / relpath).resolve()

    def load_doc_surface_artifact(self, relpath: str) -> dict[str, object]:
        """Load a doc-surface JSON artifact referenced by the manifest."""
        return _read_json_object(self.doc_surface_path(relpath))

    def load_source_access_report(self) -> dict[str, object]:
        """Load the fixture corpus's copied source-access report."""
        return self.load_doc_surface_artifact("doc-surfaces/source-access-report.json")

    def replay_summary(
        self,
        conn: sqlite3.Connection,
        *,
        glob: str = "*.jsonl",
    ) -> FixtureReplaySummary:
        """Replay sibling raw JSONL logs into *conn* and summarize the result."""
        results = replay_jsonl_dir(conn, self.raw_log_dir, glob=glob)
        file_summaries = tuple(
            FixtureReplayFile(
                filename=filename,
                total_records=totals[0],
                inserted_records=totals[1],
            )
            for filename, totals in results.items()
        )
        total_records = sum(item.total_records for item in file_summaries)
        inserted_records = sum(item.inserted_records for item in file_summaries)
        return FixtureReplaySummary(
            file_count=len(file_summaries),
            total_records=total_records,
            inserted_records=inserted_records,
            db_record_count=count_raw_messages(conn),
            files=file_summaries,
        )

    def coverage(
        self,
        *,
        required_scenarios: Iterable[str] = (),
        required_surfaces: Iterable[str] = (),
    ) -> FixtureCoverage:
        """Summarize corpus coverage against required scenarios and surfaces."""
        report = self.load_source_access_report()
        counts_by_surface = _expect_object(report, "counts_by_surface")
        source_access_surfaces = tuple(str(surface) for surface in counts_by_surface)
        scenario_names = self.scenario_names

        missing_required_scenarios = tuple(
            scenario for scenario in required_scenarios if scenario not in self.scenario_files
        )
        missing_required_surfaces = tuple(
            surface for surface in required_surfaces if surface not in counts_by_surface
        )

        return FixtureCoverage(
            record_count=self.record_count,
            scenario_names=scenario_names,
            source_access_surfaces=source_access_surfaces,
            missing_required_scenarios=missing_required_scenarios,
            missing_required_surfaces=missing_required_surfaces,
        )


def load_fixture_corpus(path: str | Path) -> FixtureCorpus:
    """Load and validate a fixture corpus manifest."""
    return FixtureCorpus.from_path(path)


def _read_json_object(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise
    except json.JSONDecodeError as error:
        raise FixtureCorpusError(f"Invalid JSON in {path}") from error

    if not isinstance(payload, dict):
        raise FixtureCorpusError(f"Expected JSON object in {path}")
    return payload


def _expect_object(payload: dict[str, object], key: str) -> dict[str, object]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise FixtureCorpusError(f"Expected object for {key}")
    if not all(isinstance(item_key, str) for item_key in value):
        raise FixtureCorpusError(f"Expected string keys in {key}")
    return {item_key: value[item_key] for item_key in value}


def _expect_string(payload: dict[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str):
        raise FixtureCorpusError(f"Expected string for {key}")
    return value


def _expect_string_list(payload: dict[str, object], key: str) -> list[str]:
    value = payload.get(key)
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise FixtureCorpusError(f"Expected string list for {key}")
    return list(value)


def _expect_string_mapping(payload: dict[str, object], key: str) -> dict[str, str]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise FixtureCorpusError(f"Expected object for {key}")
    result: dict[str, str] = {}
    for item_key, item_value in value.items():
        if not isinstance(item_key, str) or not isinstance(item_value, str):
            raise FixtureCorpusError(f"Expected string mapping for {key}")
        result[item_key] = item_value
    return result


def _expect_int(payload: dict[str, object], key: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int):
        raise FixtureCorpusError(f"Expected int for {key}")
    return value


# ---------------------------------------------------------------------------
# Fixture corpus build helpers
# ---------------------------------------------------------------------------

SCENARIOS = (
    "english_only",
    "han",
    "mixed",
    "updated",
    "deleted",
    "threaded",
)


def iter_raw_records(raw_dir: Path) -> Iterable[dict[str, Any]]:
    """Yield every JSONL record from ``*.jsonl`` files in *raw_dir*.

    Each yielded dict has two extra keys injected for traceability:
    ``_fixture_source_path`` and ``_fixture_line_number``.
    """
    for path in sorted(raw_dir.glob("*.jsonl")):
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if not line.strip():
                continue
            payload = json.loads(line)
            payload["_fixture_source_path"] = str(path)
            payload["_fixture_line_number"] = line_number
            yield payload


def summarize_raw_records(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Return a statistical summary of *records* for use in corpus manifests."""
    from lark_to_notes.distill.heuristics import (
        has_han,
        is_english_only,
        is_mixed_language,
        is_threaded_record,
    )

    msg_types: Counter[str] = Counter()
    summary: dict[str, Any] = {
        "record_count": len(records),
        "updated": 0,
        "deleted": 0,
        "threaded": 0,
        "han": 0,
        "mixed": 0,
        "english_only": 0,
    }

    for record in records:
        content = record.get("content", "")
        payload = record.get("payload", {})
        msg_types[str(payload.get("msg_type", "unknown"))] += 1
        if payload.get("updated") is True:
            summary["updated"] += 1
        if payload.get("deleted") is True:
            summary["deleted"] += 1
        if is_threaded_record(record):
            summary["threaded"] += 1
        if has_han(content):
            summary["han"] += 1
        if is_mixed_language(content):
            summary["mixed"] += 1
        if is_english_only(content):
            summary["english_only"] += 1

    summary["msg_types"] = dict(sorted(msg_types.items()))
    return summary


def select_fixture_examples(
    records: Iterable[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Pick one representative record for each scenario in :data:`SCENARIOS`."""
    from lark_to_notes.distill.heuristics import (
        has_han,
        is_english_only,
        is_mixed_language,
        is_threaded_record,
    )

    selected: dict[str, dict[str, Any]] = {}
    for record in records:
        content = record.get("content", "")
        payload = record.get("payload", {})
        if "english_only" not in selected and is_english_only(content):
            selected["english_only"] = record
        if "han" not in selected and has_han(content):
            selected["han"] = record
        if "mixed" not in selected and is_mixed_language(content):
            selected["mixed"] = record
        if "updated" not in selected and payload.get("updated") is True:
            selected["updated"] = record
        if "deleted" not in selected and payload.get("deleted") is True:
            selected["deleted"] = record
        if "threaded" not in selected and is_threaded_record(record):
            selected["threaded"] = record
        if len(selected) == len(SCENARIOS):
            break
    return selected


def copy_source_access_artifacts(
    source_access_dir: Path | None,
    destination_dir: Path,
) -> list[str]:
    """Copy source-access JSON artifacts into *destination_dir*.

    Returns a list of relative paths (relative to ``destination_dir.parent``)
    of the copied files.
    """
    if source_access_dir is None or not source_access_dir.exists():
        return []

    destination_dir.mkdir(parents=True, exist_ok=True)
    copied: list[str] = []

    report_path = source_access_dir / "source-access-report.json"
    if not report_path.exists():
        return copied

    target = destination_dir / report_path.name
    shutil.copy2(report_path, target)
    copied.append(target.relative_to(destination_dir.parent).as_posix())

    report = json.loads(report_path.read_text(encoding="utf-8"))
    for probe in report.get("probes", []):
        artifact_relpath = probe.get("artifact_relpath")
        if not artifact_relpath:
            continue
        artifact_path = source_access_dir / artifact_relpath
        if not artifact_path.exists():
            continue
        probe_target = destination_dir / artifact_path.name
        shutil.copy2(artifact_path, probe_target)
        copied.append(probe_target.relative_to(destination_dir.parent).as_posix())

    return copied


# ---------------------------------------------------------------------------
# Source-access probe utilities
# ---------------------------------------------------------------------------

_CHAT_SURFACES = {"dm_chat_messages", "group_chat_messages"}
_DOC_TOKEN_PATTERN = re.compile(r"/docx?/([A-Za-z0-9]+)")


@dataclass(slots=True)
class ProbeRecord:
    """Result of probing one Lark API surface for source-access validation."""

    surface: str
    target_id: str
    target_name: str
    command: str
    status: str
    sample_count: int
    top_level_keys: list[str]
    artifact_relpath: str | None = None
    identity: str | None = None
    notes: str | None = None
    error_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def extract_doc_token(doc: str) -> str:
    """Extract a bare document token from a Lark document URL or raw token.

    If *doc* looks like a URL, the token is parsed from the path component.
    If it is already a bare token, it is returned as-is.

    Raises :exc:`ValueError` if a URL is given but no token can be extracted.
    """
    raw = doc.strip()
    if "://" not in raw:
        return raw
    match = _DOC_TOKEN_PATTERN.search(raw)
    if match is None:
        raise ValueError(f"Could not extract document token from {doc!r}")
    return match.group(1)


def summarize_probe(surface: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Summarise a raw Lark API response into a compact probe status dict."""
    data = payload.get("data", {}) if isinstance(payload, dict) else {}
    top_level_keys = sorted(data.keys()) if isinstance(data, dict) else []
    identity = payload.get("identity") if isinstance(payload, dict) else None

    if not _is_success_payload(payload):
        return {
            "status": "blocked",
            "sample_count": 0,
            "top_level_keys": top_level_keys,
            "identity": identity,
            "notes": payload.get("msg") or payload.get("message") or payload.get("error"),
        }

    if surface in _CHAT_SURFACES:
        items = data.get("messages", []) if isinstance(data, dict) else []
    elif surface in {"doc_comments", "doc_comment_replies"}:
        items = data.get("items", []) if isinstance(data, dict) else []
    elif surface == "doc_fetch":
        items = [data] if isinstance(data, dict) and data.get("doc_id") else []
    else:
        items = []

    return {
        "status": "ok" if items else "empty",
        "sample_count": len(items),
        "top_level_keys": top_level_keys,
        "identity": identity,
        "notes": None,
    }


def build_report_manifest(probes: list[ProbeRecord]) -> dict[str, Any]:
    """Build a source-access report manifest dict from a list of *probes*."""
    counts_by_status: dict[str, int] = {}
    counts_by_surface: dict[str, int] = {}
    for probe in probes:
        counts_by_status[probe.status] = counts_by_status.get(probe.status, 0) + 1
        counts_by_surface[probe.surface] = counts_by_surface.get(probe.surface, 0) + 1

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "probe_count": len(probes),
        "counts_by_status": counts_by_status,
        "counts_by_surface": counts_by_surface,
        "probes": [probe.to_dict() for probe in probes],
    }


def _is_success_payload(payload: dict[str, Any]) -> bool:
    if "ok" in payload:
        return bool(payload["ok"])
    if "code" in payload:
        return payload.get("code") == 0
    return True
