"""Database utilities and session management."""
from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker

from .config import AppConfig

LOGGER = logging.getLogger(__name__)


def _enable_sqlite_foreign_keys(engine: Engine) -> None:
    if engine.dialect.name == "sqlite":
        @event.listens_for(engine, "connect")
        def _set_sqlite_pragma(dbapi_connection, connection_record):  # type: ignore[no-redef]
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()


def create_engine_with_retries(config: AppConfig) -> Engine:
    """Create a SQLAlchemy engine with retry logic."""

    delay = config.database.retry_backoff_seconds
    attempts = 0
    last_error: OperationalError | None = None

    while attempts <= config.database.max_retries:
        try:
            engine = create_engine(
                config.database.connection_string,
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=10,
                pool_timeout=config.database.connection_timeout,
                future=True,
            )
            _enable_sqlite_foreign_keys(engine)
            return engine
        except OperationalError as error:  # pragma: no cover - requires db failure
            last_error = error
            attempts += 1
            LOGGER.warning(
                "Database connection failed (%s/%s). Retrying in %s seconds...",
                attempts,
                config.database.max_retries,
                delay,
            )
            time.sleep(delay)
            delay *= 2

    assert last_error is not None
    raise last_error


def create_session_factory(engine: Engine) -> sessionmaker[Session]:
    """Return a session factory for the given engine."""

    return sessionmaker(bind=engine, expire_on_commit=False, future=True)


@contextmanager
def session_scope(session_factory: sessionmaker[Session]) -> Iterator[Session]:
    """Provide a transactional scope around a series of operations."""

    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


__all__ = [
    "create_engine_with_retries",
    "create_session_factory",
    "session_scope",
]
