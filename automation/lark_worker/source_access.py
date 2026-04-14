from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .config import SourceConfig, WorkerConfig
from .lark import LarkCliClient, LarkCliError

CHAT_SURFACES = {"dm_chat_messages", "group_chat_messages"}
DOC_SURFACES = {"doc_fetch", "doc_comments", "doc_comment_replies"}
DOC_TOKEN_PATTERN = re.compile(r"/docx?/([A-Za-z0-9]+)")


@dataclass(slots=True)
class ProbeRecord:
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
    raw = doc.strip()
    if "://" not in raw:
        return raw
    match = DOC_TOKEN_PATTERN.search(raw)
    if match is None:
        raise ValueError(f"Could not extract document token from {doc!r}")
    return match.group(1)


def summarize_probe(surface: str, payload: dict[str, Any]) -> dict[str, Any]:
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

    if surface in CHAT_SURFACES:
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
    counts_by_status: dict[str, int] = {}
    counts_by_surface: dict[str, int] = {}
    for probe in probes:
        counts_by_status[probe.status] = counts_by_status.get(probe.status, 0) + 1
        counts_by_surface[probe.surface] = counts_by_surface.get(probe.surface, 0) + 1

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "probe_count": len(probes),
        "counts_by_status": counts_by_status,
        "counts_by_surface": counts_by_surface,
        "probes": [probe.to_dict() for probe in probes],
    }


def build_source_access_report(
    config: WorkerConfig,
    *,
    doc_targets: list[str],
    output_dir: Path,
    lookback_days: int | None = None,
    page_size: int = 1,
) -> dict[str, Any]:
    client = LarkCliClient(config)
    output_dir.mkdir(parents=True, exist_ok=True)
    probes: list[ProbeRecord] = []

    start_date = (date.today() - timedelta(days=lookback_days or config.poll_lookback_days)).isoformat()
    end_date = (date.today() + timedelta(days=1)).isoformat()

    for source in _select_probe_sources(config.enabled_sources):
        surface = "dm_chat_messages" if source.source_type == "dm_user" else "group_chat_messages"
        command = _chat_command(source, start_date=start_date, end_date=end_date, page_size=page_size)
        probes.append(
            _run_probe(
                output_dir=output_dir,
                surface=surface,
                target_id=source.source_id,
                target_name=source.name,
                command=command,
                runner=lambda source=source: client.list_chat_messages(
                    source=source,
                    start_date=start_date,
                    end_date=end_date,
                    page_size=page_size,
                    sort="asc",
                ),
            )
        )

    for doc in doc_targets:
        doc_label = extract_doc_token(doc)
        doc_command = f"lark-cli docs +fetch --as user --doc {json.dumps(doc)} --format json"
        doc_probe = _run_probe(
            output_dir=output_dir,
            surface="doc_fetch",
            target_id=doc_label,
            target_name=doc,
            command=doc_command,
            runner=lambda doc=doc: client.fetch_doc(doc),
        )
        probes.append(doc_probe)

        comments_command = (
            "lark-cli drive file.comments list --as user --format json --params "
            + json.dumps({"file_token": doc_label, "file_type": "docx", "page_size": page_size}, ensure_ascii=False)
        )
        comments_probe = _run_probe(
            output_dir=output_dir,
            surface="doc_comments",
            target_id=doc_label,
            target_name=doc,
            command=comments_command,
            runner=lambda token=doc_label: client.list_file_comments(file_token=token, page_size=page_size),
        )
        probes.append(comments_probe)

        comment_id = _first_comment_id(output_dir / comments_probe.artifact_relpath) if comments_probe.artifact_relpath else None
        if comment_id:
            replies_command = (
                "lark-cli drive file.comment.replys list --as user --format json --params "
                + json.dumps(
                    {"file_token": doc_label, "comment_id": comment_id, "file_type": "docx", "page_size": page_size},
                    ensure_ascii=False,
                )
            )
            probes.append(
                _run_probe(
                    output_dir=output_dir,
                    surface="doc_comment_replies",
                    target_id=comment_id,
                    target_name=doc,
                    command=replies_command,
                    runner=lambda token=doc_label, comment_id=comment_id: client.list_comment_replies(
                        file_token=token,
                        comment_id=comment_id,
                        page_size=page_size,
                    ),
                )
            )
        else:
            probes.append(
                ProbeRecord(
                    surface="doc_comment_replies",
                    target_id=doc_label,
                    target_name=doc,
                    command="lark-cli drive file.comment.replys list",
                    status="not_sampled",
                    sample_count=0,
                    top_level_keys=[],
                    notes="No comment_id available from the seeded comment probe",
                )
            )

    manifest = build_report_manifest(probes)
    manifest_path = output_dir / "source-access-report.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    manifest["manifest_path"] = str(manifest_path)
    return manifest


def _select_probe_sources(sources: list[SourceConfig]) -> list[SourceConfig]:
    selected: list[SourceConfig] = []
    seen_types: set[str] = set()
    for source in sources:
        if source.source_type in {"dm_user", "chat"} and source.source_type not in seen_types:
            selected.append(source)
            seen_types.add(source.source_type)
    return selected


def _chat_command(source: SourceConfig, *, start_date: str, end_date: str, page_size: int) -> str:
    target_flag = "--user-id" if source.source_type == "dm_user" else "--chat-id"
    return (
        "lark-cli im +chat-messages-list --as user --sort asc "
        f"--page-size {page_size} --format json --start {start_date} --end {end_date} "
        f"{target_flag} {source.lark_id}"
    )


def _run_probe(
    *,
    output_dir: Path,
    surface: str,
    target_id: str,
    target_name: str,
    command: str,
    runner,
) -> ProbeRecord:
    try:
        payload = runner()
    except LarkCliError as error:
        return ProbeRecord(
            surface=surface,
            target_id=target_id,
            target_name=target_name,
            command=command,
            status="blocked",
            sample_count=0,
            top_level_keys=[],
            error_message=str(error),
            notes=str(error),
        )

    summary = summarize_probe(surface, payload)
    artifact_relpath = _write_payload(output_dir, surface, target_id, payload)
    return ProbeRecord(
        surface=surface,
        target_id=target_id,
        target_name=target_name,
        command=command,
        status=summary["status"],
        sample_count=summary["sample_count"],
        top_level_keys=summary["top_level_keys"],
        artifact_relpath=artifact_relpath,
        identity=summary["identity"],
        notes=summary["notes"],
    )


def _write_payload(output_dir: Path, surface: str, target_id: str, payload: dict[str, Any]) -> str:
    safe_target = _slugify(target_id)
    filename = f"{surface}-{safe_target}.json"
    path = output_dir / filename
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path.relative_to(output_dir).as_posix()


def _first_comment_id(path: Path) -> str | None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    data = payload.get("data", {})
    if not isinstance(data, dict):
        return None
    items = data.get("items", [])
    if not items:
        return None
    first_item = items[0]
    if not isinstance(first_item, dict):
        return None
    comment_id = first_item.get("comment_id")
    return str(comment_id) if comment_id else None


def _is_success_payload(payload: dict[str, Any]) -> bool:
    if "ok" in payload:
        return bool(payload["ok"])
    if "code" in payload:
        return payload.get("code") == 0
    return True


def _slugify(value: str) -> str:
    lowered = value.casefold()
    slug = re.sub(r"[^a-z0-9]+", "-", lowered).strip("-")
    return slug or "sample"
