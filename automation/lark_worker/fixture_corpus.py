from __future__ import annotations

import json
import re
import shutil
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .config import WorkerConfig

HAN_RE = re.compile(r"[\u4e00-\u9fff]")
LATIN_RE = re.compile(r"[A-Za-z]")

SCENARIOS = (
    "english_only",
    "han",
    "mixed",
    "updated",
    "deleted",
    "threaded",
)


def build_fixture_corpus(
    config: WorkerConfig,
    *,
    output_dir: Path,
    source_access_dir: Path | None = None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)

    records = list(iter_raw_records(config.raw_dir))
    examples = select_fixture_examples(records)
    summary = summarize_raw_records(records)
    doc_surface_artifacts = copy_source_access_artifacts(source_access_dir, output_dir / "doc-surfaces")

    scenario_files: dict[str, str] = {}
    for scenario, sample in examples.items():
        target = output_dir / f"{scenario}.json"
        target.write_text(json.dumps(sample, ensure_ascii=False, indent=2), encoding="utf-8")
        scenario_files[scenario] = target.relative_to(output_dir).as_posix()

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "raw_summary": summary,
        "scenario_files": scenario_files,
        "missing_scenarios": [scenario for scenario in SCENARIOS if scenario not in scenario_files],
        "doc_surface_artifacts": doc_surface_artifacts,
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    manifest["manifest_path"] = str(manifest_path)
    return manifest


def iter_raw_records(raw_dir: Path) -> Iterable[dict[str, Any]]:
    for path in sorted(raw_dir.glob("*.jsonl")):
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if not line.strip():
                continue
            payload = json.loads(line)
            payload["_fixture_source_path"] = str(path)
            payload["_fixture_line_number"] = line_number
            yield payload


def summarize_raw_records(records: list[dict[str, Any]]) -> dict[str, Any]:
    msg_types: Counter[str] = Counter()
    summary = {
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


def select_fixture_examples(records: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
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


def copy_source_access_artifacts(source_access_dir: Path | None, destination_dir: Path) -> list[str]:
    if source_access_dir is None or not source_access_dir.exists():
        return []

    destination_dir.mkdir(parents=True, exist_ok=True)
    copied: list[str] = []

    report_path = source_access_dir / "source-access-report.json"
    if report_path.exists():
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
            target = destination_dir / artifact_path.name
            shutil.copy2(artifact_path, target)
            copied.append(target.relative_to(destination_dir.parent).as_posix())

    return copied


def has_han(content: str) -> bool:
    return bool(HAN_RE.search(content))


def is_mixed_language(content: str) -> bool:
    return has_han(content) and bool(LATIN_RE.search(content))


def is_english_only(content: str) -> bool:
    return bool(LATIN_RE.search(content)) and not has_han(content)


def is_threaded_record(record: dict[str, Any]) -> bool:
    payload = record.get("payload", {})
    if payload.get("thread_id"):
        return True
    thread_replies = payload.get("thread_replies")
    return bool(thread_replies)
