"""Load and save machine-parseable YAML feedback sidecars."""

from __future__ import annotations

from pathlib import Path
from typing import cast

import yaml

from lark_to_notes.feedback.models import FeedbackArtifact, FeedbackDirective, FeedbackTargetType


def parse_feedback_artifact(text: str) -> FeedbackArtifact:
    """Parse YAML feedback text into a typed artifact."""
    loaded = yaml.safe_load(text) or {}
    if not isinstance(loaded, dict):
        raise ValueError("feedback artifact root must be a mapping")

    root = cast("dict[str, object]", loaded)
    version = root.get("version", 1)
    if not isinstance(version, int):
        raise ValueError("feedback artifact version must be an integer")
    if version != 1:
        raise ValueError(f"unsupported feedback artifact version: {version}")

    return FeedbackArtifact(
        version=version,
        tasks=_parse_section(root.get("tasks"), target_type=FeedbackTargetType.TASK),
        source_items=_parse_section(
            root.get("source_items"),
            target_type=FeedbackTargetType.SOURCE_ITEM,
        ),
    )


def load_feedback_artifact(path: Path | str) -> FeedbackArtifact:
    """Read and parse a YAML feedback artifact from disk."""
    artifact_path = Path(path)
    return parse_feedback_artifact(artifact_path.read_text(encoding="utf-8"))


def render_feedback_artifact(artifact: FeedbackArtifact) -> str:
    """Render a typed feedback artifact back to YAML text."""
    document = {
        "version": artifact.version,
        "tasks": {
            target_id: directive.to_payload() for target_id, directive in artifact.tasks.items()
        },
        "source_items": {
            target_id: directive.to_payload()
            for target_id, directive in artifact.source_items.items()
        },
    }
    return yaml.safe_dump(document, sort_keys=False, allow_unicode=False)


def write_feedback_artifact(path: Path | str, artifact: FeedbackArtifact) -> None:
    """Write *artifact* to *path* as YAML, creating parents as needed."""
    artifact_path = Path(path)
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(render_feedback_artifact(artifact), encoding="utf-8")


def _parse_section(
    value: object,
    *,
    target_type: FeedbackTargetType,
) -> dict[str, FeedbackDirective]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("feedback artifact sections must be mappings keyed by stable IDs")

    directives: dict[str, FeedbackDirective] = {}
    section = cast("dict[object, object]", value)
    for raw_key, raw_value in section.items():
        if not isinstance(raw_key, str) or not raw_key.strip():
            raise ValueError("feedback artifact keys must be non-empty strings")
        directives[raw_key] = FeedbackDirective.from_mapping(
            raw_value,
            target_type=target_type,
        )
    return directives
