"""Configuration loading utilities for the genomic ETL pipeline."""
from __future__ import annotations

import dataclasses
import pathlib
from typing import Any, Dict, Iterable, List, Optional

import yaml


@dataclasses.dataclass(slots=True)
class DatabaseConfig:
    """Database related settings."""

    connection_string: str
    batch_size: int = 1000
    connection_timeout: int = 30
    max_retries: int = 5
    retry_backoff_seconds: int = 5


@dataclasses.dataclass(slots=True)
class ProcessingConfig:
    """File system and processing settings."""

    input_directory: pathlib.Path
    gene_filter_file: pathlib.Path
    max_concurrent_studies: int = 1
    state_directory: pathlib.Path | None = None


@dataclasses.dataclass(slots=True)
class LoggingConfig:
    """Logging related settings."""

    log_level: str = "INFO"
    log_directory: pathlib.Path = pathlib.Path("./logs")
    log_processing_time: bool = True
    log_record_counts: bool = True
    log_data_quality: bool = True


@dataclasses.dataclass(slots=True)
class FieldMappingConfig:
    """Dynamic metadata column mappings."""

    age_fields: tuple[str, ...] = (
        "refinebio_age",
        "characteristics_ch1_Age",
        "characteristics_ch1_age",
        "MetaSRA_age",
    )
    sex_fields: tuple[str, ...] = (
        "refinebio_sex",
        "characteristics_ch1_Sex",
        "characteristics_ch1_Gender",
        "sex",
    )
    illness_fields: tuple[str, ...] = (
        "characteristics_ch1_Illness",
        "refinebio_disease",
        "illness",
    )
    platform_fields: tuple[str, ...] = (
        "refinebio_platform",
        "platform_id",
    )


@dataclasses.dataclass(slots=True)
class AppConfig:
    """Root configuration object."""

    database: DatabaseConfig
    processing: ProcessingConfig
    logging: LoggingConfig
    field_mappings: FieldMappingConfig


class ConfigurationError(RuntimeError):
    """Raised when configuration cannot be loaded or is invalid."""


_DEFAULT_FIELD_MAPPINGS = FieldMappingConfig()


def _ensure_path(value: str | pathlib.Path, *, must_exist: bool = False) -> pathlib.Path:
    path = pathlib.Path(value).expanduser().resolve()
    if must_exist and not path.exists():
        raise ConfigurationError(f"Configured path does not exist: {path}")
    return path


def _load_section(data: Dict[str, Any], key: str, *, optional: bool = False) -> Dict[str, Any]:
    try:
        section = data[key]
    except KeyError:
        if optional:
            return {}
        raise ConfigurationError(f"Missing required configuration section '{key}'") from None
    if not isinstance(section, dict):
        raise ConfigurationError(f"Configuration section '{key}' must be a mapping")
    return section


def _coerce_sequence(values: Iterable[str] | None) -> tuple[str, ...]:
    if not values:
        return tuple()
    return tuple(str(v) for v in values if v)


def load_config(path: str | pathlib.Path, *, ensure_paths_exist: bool = True) -> AppConfig:
    """Load the ETL configuration from a YAML file."""

    config_path = pathlib.Path(path).expanduser().resolve()
    if not config_path.exists():
        raise ConfigurationError(f"Configuration file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ConfigurationError("Configuration root must be a mapping")

    db_section = _load_section(data, "database")
    processing_section = _load_section(data, "processing")
    logging_section = _load_section(data, "logging", optional=True)
    field_mapping_section = _load_section(data, "field_mappings", optional=True)

    database = DatabaseConfig(
        connection_string=str(db_section.get("connection_string", "")),
        batch_size=int(db_section.get("batch_size", 1000)),
        connection_timeout=int(db_section.get("connection_timeout", 30)),
        max_retries=int(db_section.get("max_retries", 5)),
        retry_backoff_seconds=int(db_section.get("retry_backoff_seconds", 5)),
    )
    if not database.connection_string:
        raise ConfigurationError("Database connection string is required")

    input_directory = _ensure_path(
        processing_section.get("input_directory", "./data"),
        must_exist=ensure_paths_exist,
    )
    gene_filter_file = _ensure_path(
        processing_section.get("gene_filter_file", "./genes.tsv"),
        must_exist=ensure_paths_exist,
    )
    state_directory = processing_section.get("state_directory")
    state_path = (
        _ensure_path(state_directory, must_exist=False)
        if state_directory
        else input_directory.joinpath(".etl_state")
    )

    processing = ProcessingConfig(
        input_directory=input_directory,
        gene_filter_file=gene_filter_file,
        max_concurrent_studies=int(processing_section.get("max_concurrent_studies", 1)),
        state_directory=state_path,
    )

    logging = LoggingConfig(
        log_level=str(logging_section.get("log_level", "INFO")),
        log_directory=_ensure_path(
            logging_section.get("log_directory", "./logs"),
            must_exist=False,
        ),
        log_processing_time=bool(logging_section.get("log_processing_time", True)),
        log_record_counts=bool(logging_section.get("log_record_counts", True)),
        log_data_quality=bool(logging_section.get("log_data_quality", True)),
    )

    mappings = FieldMappingConfig(
        age_fields=_coerce_sequence(field_mapping_section.get("age_fields"))
        or _DEFAULT_FIELD_MAPPINGS.age_fields,
        sex_fields=_coerce_sequence(field_mapping_section.get("sex_fields"))
        or _DEFAULT_FIELD_MAPPINGS.sex_fields,
        illness_fields=_coerce_sequence(field_mapping_section.get("illness_fields"))
        or _DEFAULT_FIELD_MAPPINGS.illness_fields,
        platform_fields=_coerce_sequence(field_mapping_section.get("platform_fields"))
        or _DEFAULT_FIELD_MAPPINGS.platform_fields,
    )

    logging.log_directory.mkdir(parents=True, exist_ok=True)
    if processing.state_directory:
        processing.state_directory.mkdir(parents=True, exist_ok=True)

    return AppConfig(
        database=database,
        processing=processing,
        logging=logging,
        field_mappings=mappings,
    )


__all__ = [
    "AppConfig",
    "DatabaseConfig",
    "FieldMappingConfig",
    "LoggingConfig",
    "ProcessingConfig",
    "ConfigurationError",
    "load_config",
]
