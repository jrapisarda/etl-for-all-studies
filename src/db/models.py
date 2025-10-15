"""SQLAlchemy table metadata shared across SQL Server and SQLite."""

from __future__ import annotations

from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    text,
)

metadata = MetaData()

runs = Table(
    "runs",
    metadata,
    Column("run_id", String(64), primary_key=True),
    Column("started_at", DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("status", String(32), nullable=False),
    Column("config_hash", String(128), nullable=False),
)

checkpoints = Table(
    "checkpoints",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String(64), nullable=False),
    Column("study_id", String(64), nullable=False),
    Column("table_name", String(64), nullable=False),
    Column("batch_index", Integer, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP")),
)

metrics = Table(
    "metrics",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String(64), nullable=False),
    Column("study_id", String(64), nullable=False),
    Column("metric_key", String(64), nullable=False),
    Column("metric_value", Float, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP")),
)

study_dimension = Table(
    "dim_study",
    metadata,
    Column("study_id", String(64), primary_key=True),
    Column("payload", JSON, nullable=False),
)

fact_expression = Table(
    "fact_expression",
    metadata,
    Column("run_id", String(64), nullable=False),
    Column("study_id", String(64), nullable=False),
    Column("ensembl_id", String(32), nullable=False),
    Column("expression_value", Float, nullable=False),
    Column("batch_id", Integer, nullable=False),
)

