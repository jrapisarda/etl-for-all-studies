"""Database engine helpers."""

from __future__ import annotations

from typing import Any, Mapping

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from . import mssql, sqlite


def create_engine_from_config(settings: Mapping[str, Any]) -> Engine:
    """Create a SQLAlchemy engine based on the configuration mapping."""

    variant = settings.get("sql_variant", "sqlite").lower()
    if variant == "sqlite":
        url = settings.get("sqlite_url")
        if not url:
            raise ValueError("sqlite_url must be provided for sqlite variant")
        return sqlite.create_sqlite_engine(url)
    if variant in {"mssql", "sqlserver"}:
        dsn = settings.get("mssql_dsn")
        if not dsn:
            raise ValueError("mssql_dsn must be provided for SQL Server variant")
        return mssql.create_mssql_engine(dsn)
    raise ValueError(f"Unknown sql_variant '{variant}'")
