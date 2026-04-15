"""Structured logging setup for lark-to-notes.

Call ``configure_logging()`` once at application startup to get
timestamped, level-filtered, key-value structured log output via
structlog.  In tests, the default stdlib renderer is used so output
remains readable without a terminal.
"""

from __future__ import annotations

import logging
import sys

import structlog


def configure_logging(
    level: str = "INFO",
    *,
    json_logs: bool = False,
) -> None:
    """Configure structlog for the application.

    Args:
        level: Minimum log level as a string (e.g. ``"DEBUG"``, ``"INFO"``).
        json_logs: When ``True``, emit machine-readable JSON lines instead of
            the human-friendly console renderer.  Defaults to ``False``.
    """
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.stdlib.ExtraAdder(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
    ]

    if json_logs:
        renderer: structlog.types.Processor = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer()

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level)),
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Return a bound structlog logger for *name*.

    Args:
        name: Typically ``__name__`` of the calling module.

    Returns:
        A structlog :class:`~structlog.stdlib.BoundLogger` instance.
    """
    return structlog.get_logger(name)  # type: ignore[no-any-return]
