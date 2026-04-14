"""Pytest configuration and shared fixtures for lark-to-notes."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    import pathlib


@pytest.fixture()
def tmp_vault(tmp_path: pathlib.Path) -> pathlib.Path:
    """Return a temporary directory that mimics a minimal vault layout."""
    (tmp_path / "raw").mkdir()
    (tmp_path / "daily").mkdir()
    (tmp_path / "area" / "current tasks").mkdir(parents=True)
    return tmp_path
