"""Typer CLI commands for orchestrating ETL operations."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import List
from uuid import uuid4

import typer
import yaml
from rich import print as rprint

from src.cli.validators import AppConfig, load_config
from src.db import models
from src.db.engine import create_engine_from_config
from src.etl.checkpoint import Checkpoint, CheckpointStore
from src.etl.discovery import StudyDiscovery
from src.etl.loader import BatchLoader, LoaderConfig
from src.etl.rollback import RollbackManager
from src.etl.transform import ExpressionTransformer, MetadataTransformer, TransformConfig
from src.utils.logging_setup import configure_logging, get_logger

app = typer.Typer(name="etl", help="Genomics ETL orchestration commands")
logger = get_logger(__name__)


def _load_gene_filter(path: Path) -> List[str]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as handle:
        return [line.strip() for line in handle if line.strip()]


def _load_field_mappings(config_path: Path) -> dict[str, str]:
    candidates = [config_path.parent / "field_mappings.yaml", config_path.parent / "field_mappings.example.yaml"]
    for candidate in candidates:
        if candidate.exists():
            with candidate.open("r", encoding="utf-8") as handle:
                data = yaml.safe_load(handle) or {}
            metadata = data.get("metadata", {})
            return {str(k): str(v) for k, v in metadata.items()}
    return {}


def _initialise(config_path: Path) -> AppConfig:
    config = load_config(config_path)
    configure_logging(config.logging.model_dump())
    return config


@app.command()
def run(config: Path = typer.Option(..., exists=True, help="Path to YAML configuration")) -> None:
    """Execute a new ETL run using the provided configuration."""

    settings = _initialise(config)
    run_id = str(uuid4())
    logger.info("cli.run.start", run_id=run_id)

    gene_filter = _load_gene_filter(Path(settings.run.gene_filter_path))
    transform_config = TransformConfig(
        metadata_mappings=_load_field_mappings(config) or {},
        gene_filter=gene_filter,
    )

    discovery = StudyDiscovery(Path(settings.run.input_root))
    plan = discovery.build_plan()

    engine = create_engine_from_config(settings.run.model_dump())
    models.metadata.create_all(engine)
    loader = BatchLoader(
        engine,
        LoaderConfig(
            batch_size=settings.run.default_batch_size,
            retry_attempts=settings.retry.attempts,
        ),
    )

    checkpoint_store = CheckpointStore(Path(settings.run.artifacts_db))

    metadata_transformer = MetadataTransformer(transform_config)
    expression_transformer = ExpressionTransformer(gene_filter)

    for study_dir in plan:
        study_id = study_dir.name
        metadata_path = study_dir / "metadata.tsv"
        expression_path = study_dir / "expression.tsv"

        try:
            payload = metadata_transformer.transform(metadata_path)
        except Exception as exc:  # pragma: no cover - depends on polars availability
            logger.error("metadata_transform_failed", study_id=study_id, error=str(exc))
            continue

        loader.upsert_study_metadata(study_id, payload)

        batch: list[dict[str, float | str]] = []
        batch_index = 0
        for row in expression_transformer.stream_filtered(expression_path):
            batch.append(row)
            if len(batch) >= settings.run.default_batch_size:
                loader.insert_expression_batch(run_id, study_id, batch_index, batch)
                checkpoint_store.record(
                    Checkpoint(run_id=run_id, study_id=study_id, table_name="fact_expression", batch_index=batch_index)
                )
                batch_index += 1
                batch = []

        if batch:
            loader.insert_expression_batch(run_id, study_id, batch_index, batch)
            checkpoint_store.record(
                Checkpoint(run_id=run_id, study_id=study_id, table_name="fact_expression", batch_index=batch_index)
            )

    logger.info("cli.run.complete", run_id=run_id)


@app.command()
def resume(
    run_id: str = typer.Option(..., help="Run identifier to resume"),
    config: Path = typer.Option(..., exists=True, help="Path to YAML configuration"),
) -> None:
    """Resume a run by inspecting checkpoints and reporting restart details."""

    settings = _initialise(config)
    store = CheckpointStore(Path(settings.run.artifacts_db))
    latest = store.latest_batch(run_id, "*", "fact_expression")
    if latest is None:
        rprint(f"[yellow]No checkpoints found for run {run_id}[/yellow]")
    else:
        rprint(f"[green]Last completed batch: {latest}[/green]")


@app.command()
def rollback(
    run_id: str = typer.Option(..., help="Run identifier to rollback"),
    config: Path = typer.Option(..., exists=True, help="Path to YAML configuration"),
) -> None:
    """Rollback a run across fact tables and metadata."""

    settings = _initialise(config)
    engine = create_engine_from_config(settings.run.model_dump())
    manager = RollbackManager(engine)
    manager.rollback_run(run_id)
    rprint(f"[green]Rollback completed for run {run_id}[/green]")


@app.command()
def validate(input_dir: Path = typer.Option(..., exists=True, help="Directory containing studies")) -> None:
    """Validate discovery preflight without running the full pipeline."""

    discovery = StudyDiscovery(input_dir)
    plan = discovery.build_plan()
    rprint(f"[green]Discovered {len(plan.studies)} studies ready for ingestion[/green]")


@app.command()
def report(
    run_id: str = typer.Option(..., help="Run identifier to summarize"),
    config: Path = typer.Option(..., exists=True, help="Path to YAML configuration"),
) -> None:
    """Report metrics for a run stored in the artifacts SQLite database."""

    settings = _initialise(config)
    db_path = Path(settings.run.artifacts_db)
    if not db_path.exists():
        rprint(f"[red]Artifacts database {db_path} not found[/red]")
        raise typer.Exit(code=1)

    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            "SELECT study_id, metric_key, metric_value FROM metrics WHERE run_id = ?", (run_id,)
        )
        rows = cursor.fetchall()

    if not rows:
        rprint(f"[yellow]No metrics recorded for run {run_id}[/yellow]")
        return

    summary: dict[str, dict[str, float]] = {}
    for study_id, key, value in rows:
        summary.setdefault(study_id, {})[key] = value

    rprint(json.dumps(summary, indent=2))
