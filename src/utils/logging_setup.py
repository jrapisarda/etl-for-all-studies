"""Utility helpers for configuring structlog JSON logging."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any, Dict

import structlog


def configure_logging(settings: Dict[str, Any]) -> None:
    """Configure structlog to emit JSON logs based on the provided settings.

    Parameters
    ----------
    settings:
        Mapping loaded from configuration. Expected keys: ``level`` (str), ``json`` (bool), and
        ``log_file`` (optional str).
    """

    level_name = settings.get("level", "info").upper()
    level = getattr(logging, level_name, logging.INFO)
    handlers: list[logging.Handler] = []

    if settings.get("json", True):
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.processors.KeyValueRenderer(key_order=["event", "level", "run_id"])

    shared_processors = [
        structlog.processors.TimeStamper(key="timestamp", fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    log_file = settings.get("log_file")
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        handlers.append(file_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    handlers.append(stream_handler)

    logging.basicConfig(level=level, handlers=handlers, format="%(message)s")

    structlog.configure(
        processors=shared_processors + [renderer],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def get_logger(name: str = "etl") -> structlog.stdlib.BoundLogger:
    """Return a logger bound to the ETL namespace."""

    return structlog.get_logger(name)
