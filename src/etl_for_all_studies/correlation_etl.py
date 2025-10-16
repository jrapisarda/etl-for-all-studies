"""Standalone ETL job for computing gene pair correlations from expression data."""
from __future__ import annotations

import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from .config import AppConfig
from .correlation import compute_gene_pair_correlations
from .database import (
    create_engine_with_retries,
    create_session_factory,
    session_scope,
)
from .logging_utils import configure_logging
from .models import (
    Base,
    DimSample,
    DimStudy,
    FactExpression,
)
from .repositories import (
    bulk_insert_gene_pair_correlations,
    delete_gene_pair_correlations_for_study,
)

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class StudyCorrelationMetrics:
    """Performance metrics captured for each processed study."""

    study_key: int
    load_seconds: float
    compute_seconds: float
    insert_seconds: float
    total_seconds: float
    expressions_loaded: int
    genes_considered: int
    samples_considered: int
    illnesses_considered: int
    correlations_written: int


@dataclass(slots=True)
class CorrelationEtlSummary:
    """Aggregate metrics describing an ETL run."""

    studies_discovered: int = 0
    studies_processed: int = 0
    total_correlations: int = 0
    total_runtime_seconds: float = 0.0

    def update(self, metrics: StudyCorrelationMetrics) -> None:
        self.studies_processed += 1
        self.total_correlations += metrics.correlations_written
        self.total_runtime_seconds += metrics.total_seconds


def _load_study_keys(session: Session) -> list[int]:
    result = session.execute(select(DimStudy.study_key).order_by(DimStudy.study_key))
    return list(result.scalars())


def _load_expression_matrix(session: Session, study_key: int) -> tuple[
    dict[int, dict[str, float]],
    dict[str, int],
    int,
    float,
]:
    """Return expression matrix and sample-to-illness mapping for a study."""

    start = time.perf_counter()
    rows = session.execute(
        select(
            FactExpression.gene_key,
            DimSample.gsm_accession,
            DimSample.illness_key,
            FactExpression.expression_value,
        )
        .join(DimSample, FactExpression.sample_key == DimSample.sample_key)
        .where(FactExpression.study_key == study_key)
    ).all()
    load_seconds = time.perf_counter() - start

    gene_expression: dict[int, dict[str, float]] = defaultdict(dict)
    sample_illness: dict[str, int] = {}
    expressions_loaded = 0

    for gene_key, sample_accession, illness_key, expression_value in rows:
        if sample_accession is None or illness_key is None:
            continue
        sample_illness[str(sample_accession)] = int(illness_key)
        gene_expression[int(gene_key)][str(sample_accession)] = float(expression_value)
        expressions_loaded += 1

    return gene_expression, sample_illness, expressions_loaded, load_seconds


def _compute_and_store_correlations(
    session_factory: sessionmaker[Session],
    *,
    study_key: int,
    min_samples: int,
) -> StudyCorrelationMetrics | None:
    start_total = time.perf_counter()
    with session_scope(session_factory) as session:
        gene_expression, sample_illness, expressions_loaded, load_seconds = _load_expression_matrix(
            session, study_key
        )

        if not gene_expression:
            LOGGER.info("Study %s skipped: no expression data with illness assignments", study_key)
            return None

        compute_start = time.perf_counter()
        correlations = compute_gene_pair_correlations(
            gene_expression,
            sample_illness_map=sample_illness,
            study_key=study_key,
            min_samples=min_samples,
        )
        compute_seconds = time.perf_counter() - compute_start

        if not correlations:
            LOGGER.info(
                "Study %s skipped: insufficient data to compute correlations", study_key
            )
            return StudyCorrelationMetrics(
                study_key=study_key,
                load_seconds=load_seconds,
                compute_seconds=compute_seconds,
                insert_seconds=0.0,
                total_seconds=time.perf_counter() - start_total,
                expressions_loaded=expressions_loaded,
                genes_considered=len(gene_expression),
                samples_considered=len(sample_illness),
                illnesses_considered=len({ill for ill in sample_illness.values()}),
                correlations_written=0,
            )

        delete_gene_pair_correlations_for_study(session, study_key)

        insert_start = time.perf_counter()
        bulk_insert_gene_pair_correlations(session, correlations)
        insert_seconds = time.perf_counter() - insert_start

    metrics = StudyCorrelationMetrics(
        study_key=study_key,
        load_seconds=load_seconds,
        compute_seconds=compute_seconds,
        insert_seconds=insert_seconds,
        total_seconds=time.perf_counter() - start_total,
        expressions_loaded=expressions_loaded,
        genes_considered=len(gene_expression),
        samples_considered=len(sample_illness),
        illnesses_considered=len({ill for ill in sample_illness.values()}),
        correlations_written=len(correlations),
    )

    LOGGER.info(
        (
            "Study %s processed: %s correlations across %s illnesses "
            "(%s genes, %s samples) in %.2fs"
        ),
        metrics.study_key,
        metrics.correlations_written,
        metrics.illnesses_considered,
        metrics.genes_considered,
        metrics.samples_considered,
        metrics.total_seconds,
    )
    LOGGER.debug(
        "Study %s timings - load: %.2fs, compute: %.2fs, insert: %.2fs",
        metrics.study_key,
        metrics.load_seconds,
        metrics.compute_seconds,
        metrics.insert_seconds,
    )

    return metrics


def run_correlation_etl(config: AppConfig, *, min_samples: int = 3) -> CorrelationEtlSummary:
    """Execute the gene pair correlation ETL job."""

    configure_logging(config)
    engine = create_engine_with_retries(config)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)

    summary = CorrelationEtlSummary()

    with session_scope(session_factory) as session:
        study_keys = _load_study_keys(session)
    summary.studies_discovered = len(study_keys)

    LOGGER.info("Discovered %s studies for correlation processing", summary.studies_discovered)

    for study_key in study_keys:
        try:
            metrics = _compute_and_store_correlations(
                session_factory, study_key=study_key, min_samples=min_samples
            )
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.exception("Failed to compute correlations for study %s", study_key)
            continue

        if metrics is None:
            continue
        summary.update(metrics)

    LOGGER.info(
        "Correlation ETL completed: %s studies processed, %s correlations written in %.2fs",
        summary.studies_processed,
        summary.total_correlations,
        summary.total_runtime_seconds,
    )
    return summary


__all__ = [
    "CorrelationEtlSummary",
    "StudyCorrelationMetrics",
    "run_correlation_etl",
]
