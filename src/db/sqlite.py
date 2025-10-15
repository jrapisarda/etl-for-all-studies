"""SQLite specific helpers."""

from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def create_sqlite_engine(url: str) -> Engine:
    """Create a SQLite engine with WAL mode enabled when possible."""

    engine = create_engine(url, future=True)
    with engine.connect() as conn:
        conn.execute(text("PRAGMA journal_mode=WAL"))
    return engine

from sqlalchemy import text  # imported late to keep public API minimal
