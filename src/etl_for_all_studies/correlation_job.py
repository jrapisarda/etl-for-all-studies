"""Standalone job for refreshing gene pair correlations."""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Iterable

from sqlalchemy.orm import Session, sessionmaker

from .config import AppConfig
from .correlation import compute_gene_pair_correlations
from .database import create_engine_with_retries, create_session_factory
from .logging_utils import configure_logging
from .repositories import (
    StudyDescriptor,
    bulk_insert_gene_pair_correlations,
    delete_gene_pair_correlations_for_study,
    iter_studies_with_expression,
    load_gene_expression_matrix,
)

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class CorrelationMetrics:
    """Performance and volume metrics collected for a study."""

    study_key: int
    study_accession: str
    gene_count: int
    sample_count: int
    correlation_count: int
    deleted_count: int
    load_seconds: float
    compute_seconds: float
    write_seconds: float
    total_seconds: float


def _count_samples(expression_matrix: dict[int, dict[str, float]]) -> int:
    samples: set[str] = set()
    for sample_map in expression_matrix.values():
        samples.update(sample_map.keys())
    return len(samples)


def _process_single_study(
    session: Session,
    descriptor: StudyDescriptor,
) -> CorrelationMetrics:
    total_start = time.perf_counter()

    load_start = time.perf_counter()
    expression_matrix = load_gene_expression_matrix(session, descriptor.study_key)
    load_seconds = time.perf_counter() - load_start

    gene_count = len(expression_matrix)
    sample_count = _count_samples(expression_matrix)

    if not expression_matrix:
        deleted = delete_gene_pair_correlations_for_study(session, descriptor.study_key)
        session.commit()
        total_seconds = time.perf_counter() - total_start
        LOGGER.warning(
            "Study %s has no expression data; cleared %s existing correlations",
            descriptor.accession,
            deleted,
        )
        return CorrelationMetrics(
            study_key=descriptor.study_key,
            study_accession=descriptor.accession,
            gene_count=0,
            sample_count=0,
            correlation_count=0,
            deleted_count=deleted,
            load_seconds=load_seconds,
            compute_seconds=0.0,
            write_seconds=0.0,
            total_seconds=total_seconds,
        )

    compute_start = time.perf_counter()
    correlations = compute_gene_pair_correlations(
        expression_matrix,
        study_key=descriptor.study_key,
    )
    compute_seconds = time.perf_counter() - compute_start

    write_start = time.perf_counter()
    deleted = delete_gene_pair_correlations_for_study(session, descriptor.study_key)
    if correlations:
        bulk_insert_gene_pair_correlations(session, correlations)
    session.commit()
    write_seconds = time.perf_counter() - write_start

    total_seconds = time.perf_counter() - total_start
    return CorrelationMetrics(
        study_key=descriptor.study_key,
        study_accession=descriptor.accession,
        gene_count=gene_count,
        sample_count=sample_count,
        correlation_count=len(correlations),
        deleted_count=deleted,
        load_seconds=load_seconds,
        compute_seconds=compute_seconds,
        write_seconds=write_seconds,
        total_seconds=total_seconds,
    )


def _log_metrics(metrics: CorrelationMetrics, logging_config) -> None:
    LOGGER.info(
        "Correlation refresh for study %s (%s) loaded %s genes across %s samples in %.2fs",
        metrics.study_accession,
        metrics.study_key,
        metrics.gene_count,
        metrics.sample_count,
        metrics.load_seconds,
    )
    LOGGER.info(
        "Computed %s correlation pairs for study %s in %.2fs",
        metrics.correlation_count,
        metrics.study_accession,
        metrics.compute_seconds,
    )
    LOGGER.info(
        "Persisted correlation results for study %s in %.2fs (replaced %s rows)",
        metrics.study_accession,
        metrics.write_seconds,
        metrics.deleted_count,
    )

    if getattr(logging_config, "log_record_counts", False):
        LOGGER.info(
            "Correlation totals for %s: genes=%s samples=%s pairs=%s",
            metrics.study_accession,
            metrics.gene_count,
            metrics.sample_count,
            metrics.correlation_count,
        )

    if getattr(logging_config, "log_processing_time", False):
        LOGGER.info(
            "Correlation refresh for study %s completed in %.2fs",
            metrics.study_accession,
            metrics.total_seconds,
        )


def _resolve_target_studies(
    session_factory: sessionmaker[Session],
    study_accessions: Iterable[str] | None,
) -> tuple[list[StudyDescriptor], set[str]]:
    with session_factory() as session:
        descriptors = iter_studies_with_expression(session, study_accessions)

    requested = {accession for accession in (study_accessions or []) if accession}
    found = {descriptor.accession for descriptor in descriptors}
    missing = requested - found
    return descriptors, missing


def run_correlation_job(
    config: AppConfig,
    *,
    study_accessions: Iterable[str] | None = None,
) -> None:
    """Execute the correlation refresh job for the given studies."""

    configure_logging(config)
    engine = create_engine_with_retries(config)
    session_factory = create_session_factory(engine)

    descriptors, missing = _resolve_target_studies(session_factory, study_accessions)
    if missing:
        LOGGER.warning(
            "Requested studies missing expression data: %s",
            ", ".join(sorted(missing)),
        )

    if not descriptors:
        LOGGER.warning("No studies with expression data available for correlation refresh")
        return

    total_start = time.perf_counter()
    processed: list[CorrelationMetrics] = []
    failures = 0

    LOGGER.info(
        "Starting correlation job for %s study(ies)",
        len(descriptors),
    )

    for descriptor in descriptors:
        session = session_factory()
        try:
            LOGGER.info(
                "Processing correlations for study %s (%s)",
                descriptor.accession,
                descriptor.study_key,
            )
            metrics = _process_single_study(session, descriptor)
        except Exception:
            failures += 1
            session.rollback()
            LOGGER.exception(
                "Correlation refresh failed for study %s", descriptor.accession
            )
        else:
            processed.append(metrics)
            _log_metrics(metrics, config.logging)
        finally:
            session.close()

    total_seconds = time.perf_counter() - total_start

    if processed:
        total_pairs = sum(item.correlation_count for item in processed)
        total_genes = sum(item.gene_count for item in processed)
        LOGGER.info(
            "Correlation job processed %s study(ies) in %.2fs (failures=%s)",
            len(processed),
            total_seconds,
            failures,
        )
        if getattr(config.logging, "log_record_counts", False):
            LOGGER.info(
                "Aggregate correlation totals: genes=%s pairs=%s",
                total_genes,
                total_pairs,
            )
    else:
        LOGGER.warning(
            "Correlation job finished without processing any studies (failures=%s)",
            failures,
        )


__all__ = ["run_correlation_job"]
