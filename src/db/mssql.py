"""SQL Server engine helpers."""

from __future__ import annotations

from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def create_mssql_engine(dsn: str) -> Engine:
    """Create a SQL Server engine configured for fast executemany."""

    connection_url = f"mssql+pyodbc:///?odbc_connect={quote_plus(dsn)}"
    engine = create_engine(connection_url, fast_executemany=True, future=True)
    return engine
