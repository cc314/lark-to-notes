"""Reusable loaders for the checked-in fixture corpus."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

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
