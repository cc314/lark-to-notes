"""Tests for :mod:`lark_to_notes.live.reaction_preflight`."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from lark_to_notes.live.lark_cli import LarkCliApiError, LarkCliNotFoundError
from lark_to_notes.live.reaction_preflight import (
    PRIMARY_REACTION_READ_SCOPE,
    reaction_scope_preflight_check,
)


def test_preflight_pass_parses_granted_and_status() -> None:
    def fake_run(argv: list[str], **kwargs: object) -> dict[str, object]:
        if "check" in argv:
            return {"ok": True, "granted": [PRIMARY_REACTION_READ_SCOPE], "missing": None}
        if argv[-1] == "status":
            return {"appId": "cli_test", "identity": "user"}
        raise AssertionError(argv)

    with patch("lark_to_notes.live.reaction_preflight.run_lark_cli_json", side_effect=fake_run):
        out = reaction_scope_preflight_check(profile=None, timeout=5.0)
    assert out["result"] == "pass"
    assert out["tenant_app_id"] == "cli_test"
    assert out["identity"] == "user"
    assert out["auth_check"]["missing"] is None


def test_preflight_scope_missing() -> None:
    def fake_run(argv: list[str], **kwargs: object) -> dict[str, object]:
        if "check" in argv:
            return {
                "ok": True,
                "granted": [],
                "missing": [PRIMARY_REACTION_READ_SCOPE],
            }
        if argv[-1] == "status":
            return {"appId": "cli_x"}
        raise AssertionError(argv)

    with patch("lark_to_notes.live.reaction_preflight.run_lark_cli_json", side_effect=fake_run):
        out = reaction_scope_preflight_check(profile=None, timeout=5.0)
    assert out["result"] == "scope_missing"
    assert out["tenant_app_id"] == "cli_x"


def test_preflight_lark_cli_missing() -> None:
    with patch(
        "lark_to_notes.live.reaction_preflight.run_lark_cli_json",
        side_effect=LarkCliNotFoundError("no lark-cli"),
    ):
        out = reaction_scope_preflight_check(profile=None, timeout=5.0)
    assert out["result"] == "lark_cli_missing"
    assert out["auth_check"] is None


def test_preflight_auth_api_error() -> None:
    with patch(
        "lark_to_notes.live.reaction_preflight.run_lark_cli_json",
        side_effect=LarkCliApiError("denied", code=403),
    ):
        out = reaction_scope_preflight_check(profile=None, timeout=5.0)
    assert out["result"] == "auth_check_failed"


def test_preflight_uses_profile_prefix() -> None:
    calls: list[list[str]] = []

    def fake_run(argv: list[str], **kwargs: object) -> dict[str, object]:
        calls.append(argv)
        if "check" in argv:
            assert argv[:2] == ["--profile", "p1"]
            return {"ok": True, "granted": [PRIMARY_REACTION_READ_SCOPE], "missing": None}
        if argv[-1] == "status":
            assert argv[:2] == ["--profile", "p1"]
            return {}
        raise AssertionError(argv)

    with patch("lark_to_notes.live.reaction_preflight.run_lark_cli_json", side_effect=fake_run):
        reaction_scope_preflight_check(profile="p1", timeout=5.0)
    assert len(calls) == 2


@pytest.mark.parametrize(
    ("missing_raw", "expected"),
    [
        (None, "pass"),
        ([], "pass"),
        ([PRIMARY_REACTION_READ_SCOPE], "scope_missing"),
    ],
)
def test_preflight_missing_normalization(missing_raw: object, expected: str) -> None:
    def fake_run(argv: list[str], **kwargs: object) -> dict[str, object]:
        if "check" in argv:
            return {"ok": True, "granted": [], "missing": missing_raw}
        return {}

    with patch("lark_to_notes.live.reaction_preflight.run_lark_cli_json", side_effect=fake_run):
        out = reaction_scope_preflight_check(profile=None, timeout=5.0)
    assert out["result"] == expected
