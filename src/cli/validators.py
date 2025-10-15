"""Configuration loading and validation utilities."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml
from pydantic import BaseModel, Field, validator


class LoggingConfig(BaseModel):
    level: str = Field(default="info")
    json: bool = Field(default=True)
    log_file: str | None = None


class RetryConfig(BaseModel):
    attempts: int = Field(default=3, ge=1)
    backoff_seconds: int = Field(default=2, ge=1)
    backoff_max_seconds: int = Field(default=30, ge=1)


class MetricsConfig(BaseModel):
    enable: bool = True
    export_path: str | None = None


class RunConfig(BaseModel):
    default_batch_size: int = Field(default=1000, gt=0)
    max_concurrent_studies: int = Field(default=1, ge=1)
    gene_filter_path: str
    input_root: str
    artifacts_db: str
    sql_variant: str = Field(default="sqlite")
    sqlite_url: str | None = None
    mssql_dsn: str | None = None


class AppConfig(BaseModel):
    run: RunConfig
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)

    @validator("run")
    def _validate_sql_variant(cls, value: RunConfig) -> RunConfig:
        if value.sql_variant.lower() == "sqlite" and not value.sqlite_url:
            raise ValueError("sqlite_url must be provided when sql_variant is sqlite")
        if value.sql_variant.lower() in {"mssql", "sqlserver"} and not value.mssql_dsn:
            raise ValueError("mssql_dsn must be provided when sql_variant is SQL Server")
        return value


def load_config(path: Path) -> AppConfig:
    """Load and validate the YAML configuration file."""

    if not path.exists():
        raise FileNotFoundError(f"Configuration file '{path}' does not exist")

    with path.open("r", encoding="utf-8") as handle:
        raw: Dict[str, Any] = yaml.safe_load(handle) or {}

    return AppConfig.model_validate(raw)
