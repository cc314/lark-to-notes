"""IM reaction **scope preflight** via ``lark-cli auth`` (lw-pzj.14.3).

Operators pipe ``lark-cli event +subscribe`` into ``sync-events``; missing
``im:message.reactions:read`` yields empty reaction tables that look like “no
traffic”. Preflight surfaces that as an explicit capability outcome before work
starts when ``--require-reaction-scopes`` is set, or via ``preflight reactions``.
"""

from __future__ import annotations

import logging
import secrets
from typing import Any

from lark_to_notes.live.lark_cli import (
    LarkCliApiError,
    LarkCliInvocationError,
    LarkCliNotFoundError,
    run_lark_cli_json,
)

logger = logging.getLogger(__name__)

CHECK_NAME = "lark_cli_auth_check_im_message_reactions_read"
PRIMARY_REACTION_READ_SCOPE = "im:message.reactions:read"


def _argv_with_profile(profile: str | None, tail: list[str]) -> list[str]:
    if profile:
        return ["--profile", profile, *tail]
    return list(tail)


def _normalize_missing(raw: object) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x) for x in raw if str(x).strip()]
    s = str(raw).strip()
    return [s] if s else []


def _remediation(result: str) -> str:
    if result == "lark_cli_missing":
        return "Install lark-cli and ensure it is on PATH (see README live prerequisites)."
    if result == "scope_missing":
        return (
            "Grant im:message.reactions:read to the app / user token, then "
            "`lark-cli auth login --as user` (see README IM reactions scope matrix)."
        )
    if result == "auth_check_failed":
        return "lark-cli auth check reported ok=false; inspect Lark app scopes and token identity."
    if result == "lark_cli_error":
        return "lark-cli auth check failed; retry with network, or run `lark-cli auth status`."
    return "ok"


def reaction_scope_preflight_check(
    *,
    profile: str | None = None,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """Run ``lark-cli auth check`` for :data:`PRIMARY_REACTION_READ_SCOPE`.

    Returns a JSON-serializable dict including ``run_id`` for log correlation.
    On success, best-effort ``auth status`` adds ``tenant_app_id`` (``appId``)
    when present.
    """

    run_id = f"pf_rx_{secrets.token_hex(8)}"
    tenant_app_id: str | None = None
    identity: str | None = None

    check_argv = _argv_with_profile(
        profile,
        ["auth", "check", "--scope", PRIMARY_REACTION_READ_SCOPE],
    )
    try:
        raw = run_lark_cli_json(check_argv, timeout=timeout)
    except LarkCliNotFoundError:
        result = "lark_cli_missing"
        payload: dict[str, Any] = {
            "run_id": run_id,
            "check_name": CHECK_NAME,
            "required_scopes": [PRIMARY_REACTION_READ_SCOPE],
            "result": result,
            "remediation_hint": _remediation(result),
            "tenant_app_id": None,
            "identity": None,
            "auth_check": None,
        }
        logger.info(
            "reaction_scope_preflight",
            extra={
                "check_name": CHECK_NAME,
                "result": result,
                "remediation_hint": payload["remediation_hint"],
                "tenant_app_id": None,
                "run_id": run_id,
            },
        )
        return payload
    except (LarkCliApiError, LarkCliInvocationError) as exc:
        result = "auth_check_failed" if isinstance(exc, LarkCliApiError) else "lark_cli_error"
        payload = {
            "run_id": run_id,
            "check_name": CHECK_NAME,
            "required_scopes": [PRIMARY_REACTION_READ_SCOPE],
            "result": result,
            "remediation_hint": _remediation(result),
            "tenant_app_id": None,
            "identity": None,
            "auth_check": {"error": str(exc)},
        }
        logger.info(
            "reaction_scope_preflight",
            extra={
                "check_name": CHECK_NAME,
                "result": result,
                "remediation_hint": payload["remediation_hint"],
                "tenant_app_id": None,
                "run_id": run_id,
            },
        )
        return payload

    missing = _normalize_missing(raw.get("missing"))
    granted = raw.get("granted")
    ok = raw.get("ok") is True
    if ok and not missing:
        result = "pass"
    elif missing:
        result = "scope_missing"
    else:
        result = "auth_check_failed"

    status_argv = _argv_with_profile(profile, ["auth", "status"])
    try:
        st = run_lark_cli_json(status_argv, timeout=timeout)
        if isinstance(st.get("appId"), str):
            tenant_app_id = st["appId"]
        if isinstance(st.get("identity"), str):
            identity = st["identity"]
    except (LarkCliApiError, LarkCliInvocationError, LarkCliNotFoundError):
        pass

    payload = {
        "run_id": run_id,
        "check_name": CHECK_NAME,
        "required_scopes": [PRIMARY_REACTION_READ_SCOPE],
        "result": result,
        "remediation_hint": _remediation(result),
        "tenant_app_id": tenant_app_id,
        "identity": identity,
        "auth_check": {
            "ok": raw.get("ok"),
            "granted": granted,
            "missing": missing or None,
        },
    }
    logger.info(
        "reaction_scope_preflight",
        extra={
            "check_name": CHECK_NAME,
            "result": result,
            "remediation_hint": payload["remediation_hint"],
            "tenant_app_id": tenant_app_id,
            "run_id": run_id,
        },
    )
    return payload
