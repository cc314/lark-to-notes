from __future__ import annotations

import os
import plistlib
import re
import subprocess
import sys
import time
from pathlib import Path

from .config import WorkerConfig


DEFAULT_LAUNCHD_PATH = "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"


def default_label(config: WorkerConfig) -> str:
    vault_name = re.sub(r"[^a-z0-9]+", "-", config.vault_root.name.lower()).strip("-") or "vault"
    return f"com.local.{vault_name}.lark-worker"


def default_plist_path(label: str) -> Path:
    return Path.home() / "Library" / "LaunchAgents" / f"{label}.plist"


def default_log_paths(config: WorkerConfig, label: str) -> tuple[Path, Path]:
    log_dir = config.state_db.parent / "logs"
    return (
        log_dir / f"{label}.stdout.log",
        log_dir / f"{label}.stderr.log",
    )


def render_plist(
    *,
    config: WorkerConfig,
    config_path: Path,
    label: str,
    with_events: bool,
    python_executable: Path | None = None,
) -> bytes:
    python_path = (python_executable or Path(sys.executable)).expanduser().resolve()
    stdout_path, stderr_path = default_log_paths(config, label)
    raw_path_entries = os.environ.get("PATH", DEFAULT_LAUNCHD_PATH).split(":")
    local_bin = str((Path.home() / ".local" / "bin").resolve())
    path_entries: list[str] = []
    for entry in [local_bin, *raw_path_entries]:
        if entry and entry not in path_entries:
            path_entries.append(entry)

    program_arguments = [
        str(python_path),
        "-m",
        "automation.lark_worker",
        "run",
        "--config",
        str(config_path.expanduser().resolve()),
    ]
    if with_events:
        program_arguments.append("--with-events")

    payload = {
        "Label": label,
        "ProgramArguments": program_arguments,
        "WorkingDirectory": str(config.vault_root),
        "RunAtLoad": True,
        "KeepAlive": True,
        "ProcessType": "Background",
        "StandardOutPath": str(stdout_path),
        "StandardErrorPath": str(stderr_path),
        "EnvironmentVariables": {
            "PATH": ":".join(path_entries),
            "PYTHONPATH": str(config.vault_root),
            "PYTHONUNBUFFERED": "1",
        },
    }
    return plistlib.dumps(payload, sort_keys=False)


def install_launch_agent(
    *,
    config: WorkerConfig,
    config_path: Path,
    label: str | None = None,
    plist_path: Path | None = None,
    with_events: bool = False,
    start: bool = True,
) -> dict[str, object]:
    resolved_label = label or default_label(config)
    resolved_plist_path = (plist_path or default_plist_path(resolved_label)).expanduser().resolve()
    stdout_path, stderr_path = default_log_paths(config, resolved_label)

    resolved_plist_path.parent.mkdir(parents=True, exist_ok=True)
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_plist_path.write_bytes(
        render_plist(
            config=config,
            config_path=config_path,
            label=resolved_label,
            with_events=with_events,
        )
    )

    _run_bootout(resolved_plist_path)
    _run_launchctl("bootstrap", launchd_domain(), str(resolved_plist_path))
    _run_launchctl("enable", launchd_target(resolved_label), check=False)
    if start:
        _run_launchctl("kickstart", "-k", launchd_target(resolved_label))
        time.sleep(2)

    status = read_status(config=config, label=resolved_label, plist_path=resolved_plist_path)
    status.update(
        {
            "installed": True,
            "with_events": with_events,
            "start_requested": start,
        }
    )
    return status


def read_status(
    *,
    config: WorkerConfig,
    label: str | None = None,
    plist_path: Path | None = None,
) -> dict[str, object]:
    resolved_label = label or default_label(config)
    resolved_plist_path = (plist_path or default_plist_path(resolved_label)).expanduser().resolve()
    stdout_path, stderr_path = default_log_paths(config, resolved_label)
    target = launchd_target(resolved_label)
    result = subprocess.run(
        ["launchctl", "print", target],
        capture_output=True,
        text=True,
        check=False,
    )
    output = result.stdout or result.stderr
    return {
        "label": resolved_label,
        "target": target,
        "plist_path": str(resolved_plist_path),
        "stdout_log": str(stdout_path),
        "stderr_log": str(stderr_path),
        "loaded": result.returncode == 0,
        "pid": _extract_int(output, r"\bpid = (\d+)"),
        "last_exit_code": _extract_int(output, r"\blast exit code = (\d+)"),
        "state": _extract_string(output, r"\bstate = ([^\n]+)"),
    }


def launchd_domain() -> str:
    return f"gui/{os.getuid()}"


def launchd_target(label: str) -> str:
    return f"{launchd_domain()}/{label}"


def _run_bootout(plist_path: Path) -> None:
    result = subprocess.run(
        ["launchctl", "bootout", launchd_domain(), str(plist_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode == 0:
        return
    combined = "\n".join(part for part in [result.stdout.strip(), result.stderr.strip()] if part).lower()
    benign_markers = [
        "could not find service",
        "no such process",
        "service is disabled",
        "input/output error",
    ]
    if any(marker in combined for marker in benign_markers):
        return
    raise RuntimeError(combined or f"launchctl bootout failed with exit code {result.returncode}")


def _run_launchctl(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["launchctl", *args],
        capture_output=True,
        text=True,
        check=check,
    )


def _extract_int(text: str, pattern: str) -> int | None:
    match = re.search(pattern, text)
    if match is None:
        return None
    return int(match.group(1))


def _extract_string(text: str, pattern: str) -> str | None:
    match = re.search(pattern, text)
    if match is None:
        return None
    return match.group(1).strip()
