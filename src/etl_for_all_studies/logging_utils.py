"""Logging utilities for the ETL pipeline."""
from __future__ import annotations

import logging
import logging.handlers
import pathlib
from typing import Optional

from .config import AppConfig


def configure_logging(config: AppConfig) -> None:
    """Configure logging based on configuration values."""

    level = getattr(logging, config.logging.log_level.upper(), logging.INFO)
    log_dir = pathlib.Path(config.logging.log_directory)
    log_dir.mkdir(parents=True, exist_ok=True)

    handlers: list[logging.Handler] = []

    log_file = log_dir / "etl.log"
    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=10_000_000, backupCount=5
    )
    handlers.append(file_handler)

    console_handler = logging.StreamHandler()
    handlers.append(console_handler)

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=handlers,
    )


__all__ = ["configure_logging"]
