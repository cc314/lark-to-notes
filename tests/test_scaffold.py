"""Smoke tests for the lark_to_notes package scaffold.

Verifies that the package is importable, version is set, and
all sub-packages resolve without errors.
"""

from __future__ import annotations

from collections.abc import Generator

import pytest

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


# ---------------------------------------------------------------------------
# lw-tst.3: configure_logging tests
# ---------------------------------------------------------------------------


class TestConfigureLogging:
    """Tests for configure_logging() modes, handler reset, and level propagation.

    Each test saves and restores the root logger state and sys.stderr so
    tests cannot pollute each other.
    """

    @pytest.fixture(autouse=True)
    def _restore_root_logger(self) -> Generator[None, None, None]:
        """Save root logger handlers and level; restore after each test."""
        import logging

        root = logging.getLogger()
        saved_handlers = list(root.handlers)
        saved_level = root.level
        yield
        root.handlers.clear()
        root.handlers.extend(saved_handlers)
        root.setLevel(saved_level)
        # Reset structlog config to avoid cache pollution
        import structlog

        structlog.reset_defaults()

    def test_configure_logging_attaches_handler(self) -> None:
        import logging

        from lark_to_notes.logging import configure_logging

        configure_logging()
        root = logging.getLogger()
        assert len(root.handlers) >= 1

    def test_configure_logging_resets_handlers_no_duplicates(self) -> None:
        import logging

        from lark_to_notes.logging import configure_logging

        configure_logging()
        configure_logging()
        root = logging.getLogger()
        # root.handlers.clear() is called before addHandler, so exactly 1 handler
        assert len(root.handlers) == 1

    def test_configure_logging_json_lines_parseable(self) -> None:
        import io
        import json
        import sys

        import structlog

        from lark_to_notes.logging import configure_logging, get_logger

        buf = io.StringIO()
        old_err = sys.stderr
        sys.stderr = buf
        try:
            configure_logging("DEBUG", json_logs=True)
            log = get_logger("test.json")
            # structlog caches loggers; reset so the new config is used
            structlog.reset_defaults()
            configure_logging("DEBUG", json_logs=True)
            log = get_logger("test.json")
            log.info("hello from json", marker="test-marker")
        finally:
            sys.stderr = old_err

        output = buf.getvalue().strip()
        assert output, "no log output captured"
        first_line = output.splitlines()[0]
        parsed = json.loads(first_line)
        assert "level" in parsed or "log_level" in parsed
        assert "event" in parsed

    def test_configure_logging_console_mode_not_json(self) -> None:
        import io
        import json
        import sys

        import structlog

        from lark_to_notes.logging import configure_logging, get_logger

        buf = io.StringIO()
        old_err = sys.stderr
        sys.stderr = buf
        try:
            structlog.reset_defaults()
            configure_logging("DEBUG", json_logs=False)
            log = get_logger("test.console")
            log.info("console output test", marker="console-marker")
        finally:
            sys.stderr = old_err

        output = buf.getvalue().strip()
        assert output, "no log output captured"
        # Console renderer output is NOT valid JSON
        try:
            json.loads(output.splitlines()[0])
            # If it parses as JSON, the test should fail
            raise AssertionError("expected non-JSON console output but got valid JSON")
        except json.JSONDecodeError:
            pass  # Expected: console output is not JSON

    def test_configure_logging_debug_level(self) -> None:
        import logging

        from lark_to_notes.logging import configure_logging

        configure_logging("DEBUG")
        root = logging.getLogger()
        assert root.level == logging.DEBUG

    def test_get_logger_has_info_method(self) -> None:
        from lark_to_notes.logging import get_logger

        log = get_logger(__name__)
        assert callable(getattr(log, "info", None))
