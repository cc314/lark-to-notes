"""Smoke tests for the lark_to_notes package scaffold.

Verifies that the package is importable, version is set, and
all sub-packages resolve without errors.
"""

from __future__ import annotations

import lark_to_notes
import lark_to_notes.cli
import lark_to_notes.config
import lark_to_notes.distill
import lark_to_notes.feedback
import lark_to_notes.intake
import lark_to_notes.render
import lark_to_notes.runtime
import lark_to_notes.storage
import lark_to_notes.tasks
import lark_to_notes.testing


def test_version_is_set() -> None:
    assert lark_to_notes.__version__ == "0.1.0"


def test_all_subpackages_importable() -> None:
    packages = [
        lark_to_notes.cli,
        lark_to_notes.config,
        lark_to_notes.distill,
        lark_to_notes.feedback,
        lark_to_notes.intake,
        lark_to_notes.render,
        lark_to_notes.runtime,
        lark_to_notes.storage,
        lark_to_notes.tasks,
        lark_to_notes.testing,
    ]
    assert all(p is not None for p in packages)


def test_cli_entry_point_callable() -> None:
    from lark_to_notes.cli import main

    assert callable(main)


def test_get_logger_returns_bound_logger() -> None:
    from lark_to_notes.logging import get_logger

    log = get_logger(__name__)
    assert log is not None
