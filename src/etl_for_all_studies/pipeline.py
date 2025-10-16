"""Main orchestration logic for the genomic ETL pipeline."""
from __future__ import annotations

import concurrent.futures
import logging
import pathlib
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable, Mapping

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from .config import AppConfig
from .correlation import compute_gene_pair_correlations
from .database import create_engine_with_retries, create_session_factory
from .expression_processing import ExpressionFormatError, iter_filtered_expression
from .gene_filter import load_gene_filter
from .logging_utils import configure_logging
from .metadata_processing import (
    MetadataFormatError,
    MetadataQuality,
    SampleMetadata,
    UNKNOWN_VALUE,
    load_metadata,
)
from .models import Base, EtlStudyState, FactExpression
from .repositories import (
    DimensionCache,
    bulk_insert_expression_records,
    bulk_insert_gene_pair_correlations,
    bootstrap_cache,
    clear_state,
    delete_gene_pair_correlations_for_study,
    get_or_create_gene,
    get_or_create_sample,
    get_or_create_study,
    get_or_create_illness,
    upsert_state,
)

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class StudyFiles:
    study_accession: str
    metadata_file: pathlib.Path
    expression_file: pathlib.Path


class StudyProcessingError(RuntimeError):
    """Raised when processing a study fails."""


def discover_study_files(study_dir: pathlib.Path) -> StudyFiles:
    metadata_candidates = sorted(study_dir.glob("metadata_*.tsv"))
    if not metadata_candidates:
        raise StudyProcessingError(
            f"Study directory {study_dir} missing metadata or expression TSV files"
        )

    metadata_file = metadata_candidates[0]
    study_accession = metadata_file.stem.replace("metadata_", "")
    if not study_accession:
        raise StudyProcessingError(
            f"Unable to derive study accession from metadata file {metadata_file}"
        )

    expression_candidates: list[pathlib.Path] = []

    # Prefer conventional naming that includes the "expression_" prefix.
    preferred_patterns = [f"expression_{study_accession}.tsv", "expression_*.tsv"]
    for pattern in preferred_patterns:
        matches = sorted(study_dir.glob(pattern))
        # Filter out the metadata file in case the glob pattern is too broad.
        matches = [match for match in matches if match != metadata_file]
        if matches:
            expression_candidates.extend(matches)
            break

    if not expression_candidates:
        # Fall back to any TSV file whose stem includes the study accession.
        fallback_matches = sorted(
            match
            for match in study_dir.glob("*.tsv")
            if match != metadata_file and study_accession in match.stem
        )
        expression_candidates.extend(fallback_matches)

    if not expression_candidates:
        raise StudyProcessingError(
            f"Study directory {study_dir} missing metadata or expression TSV files"
        )

    expression_file = expression_candidates[0]

    return StudyFiles(
        study_accession=study_accession,
        metadata_file=metadata_file,
        expression_file=expression_file,
    )


def _load_resume_state(session: Session, accession: str) -> tuple[bool, str | None, int]:
    state = session.get(EtlStudyState, accession)
    if not state:
        return False, None, 0
    return bool(state.metadata_loaded), state.last_processed_gene, state.last_sample_index


def _load_existing_expression_keys(session: Session, study_key: int) -> set[tuple[int, int]]:
    rows = session.execute(
        select(FactExpression.sample_key, FactExpression.gene_key).where(
            FactExpression.study_key == study_key
        )
    ).all()
    return {(sample_key, gene_key) for sample_key, gene_key in rows}


def _load_existing_expression_matrix(
    session: Session,
    study_key: int,
    sample_key_to_accession: Mapping[int, str],
) -> defaultdict[int, dict[str, float]]:
    rows = session.execute(
        select(
            FactExpression.gene_key,
            FactExpression.sample_key,
            FactExpression.expression_value,
        ).where(FactExpression.study_key == study_key)
    ).all()
    matrix: defaultdict[int, dict[str, float]] = defaultdict(dict)
    for gene_key, sample_key, expression_value in rows:
        sample_accession = sample_key_to_accession.get(sample_key)
        if sample_accession is None:
            continue
        matrix[gene_key][sample_accession] = expression_value
    return matrix


def _process_metadata(
    session: Session,
    cache: DimensionCache,
    study_files: StudyFiles,
    *,
    config: AppConfig,
) -> tuple[int, list[SampleMetadata], MetadataQuality]:
    samples, quality = load_metadata(study_files.metadata_file, config.field_mappings)
    if not samples:
        raise StudyProcessingError(f"No valid samples found in metadata {study_files.metadata_file}")

    study_accession = samples[0].study_accession
    study_key = get_or_create_study(session, cache, study_accession)

    for sample in samples:
        if sample.study_accession != study_accession:
            LOGGER.warning(
                "Sample %s references differing study accession %s (expected %s)",
                sample.gsm_accession,
                sample.study_accession,
                study_accession,
            )
        get_or_create_sample(session, cache, sample, study_key=study_key)

    session.commit()
    if config.logging.log_record_counts:
        LOGGER.info(
            "Metadata processed for study %s: %s samples",
            study_accession,
            len(samples),
        )
    if config.logging.log_data_quality:
        LOGGER.info(
            "Data quality for study %s: age %.2f%%, sex %.2f%%",
            study_accession,
            quality.age_completion * 100,
            quality.sex_completion * 100,
        )

    return study_key, samples, quality


def _process_expression(
    session: Session,
    cache: DimensionCache,
    study_key: int,
    samples: list[SampleMetadata],
    study_files: StudyFiles,
    *,
    config: AppConfig,
    gene_filter: set[str],
    batch_size: int,
    resume_gene: str | None,
    resume_sample_index: int,
) -> tuple[int, int]:
    sample_key_map: dict[str, int] = {}
    for sample in samples:
        key = (sample.gsm_accession, study_key)
        sample_key = cache.samples.get(key)
        if sample_key is None:
            raise StudyProcessingError(
                f"Sample {sample.gsm_accession} missing from dimension cache for study {study_key}"
            )
        sample_key_map[sample.gsm_accession] = sample_key

    expected_samples = set(sample_key_map.keys())
    sample_key_to_accession = {value: key for key, value in sample_key_map.items()}

    existing_facts = _load_existing_expression_keys(session, study_key)

    batch: list[FactExpression] = []
    total_records = 0
    total_genes = set()

    if existing_facts:
        gene_expression_by_sample: defaultdict[int, dict[str, float]] = (
            _load_existing_expression_matrix(session, study_key, sample_key_to_accession)
        )
    else:
        gene_expression_by_sample = defaultdict(dict)
    sample_illness_map: dict[str, int | None] = {}
    for sample in samples:
        illness_key = None
        if sample.illness_label and sample.illness_label != UNKNOWN_VALUE:
            illness_key = cache.illnesses.get(sample.illness_label)
            if illness_key is None:
                illness_key = get_or_create_illness(session, cache, sample.illness_label)
        sample_illness_map[sample.gsm_accession] = illness_key

    last_gene = resume_gene
    last_sample = resume_sample_index

    for row in iter_filtered_expression(
        str(study_files.expression_file),
        allowed_genes=gene_filter,
        sample_columns=expected_samples,
        resume_gene=resume_gene,
        resume_sample_index=resume_sample_index,
    ):
        last_gene = row.gene_id
        last_sample = row.sample_index
        gene_key = get_or_create_gene(session, cache, row.gene_id)
        sample_key = sample_key_map[row.sample_accession]
        cache.samples[(row.sample_accession, study_key)] = sample_key
        gene_expression_by_sample[gene_key][row.sample_accession] = row.expression_value

        if (sample_key, gene_key) in existing_facts:
            continue

        fact = FactExpression(
            sample_key=sample_key,
            gene_key=gene_key,
            study_key=study_key,
            expression_value=row.expression_value,
        )
        batch.append(fact)
        total_records += 1
        total_genes.add(row.gene_id)

        if len(batch) >= batch_size:
            bulk_insert_expression_records(session, batch)
            upsert_state(
                session,
                study_files.study_accession,
                last_gene=last_gene,
                last_sample_index=last_sample,
                metadata_loaded=True,
            )
            session.commit()
            batch.clear()

    if batch:
        bulk_insert_expression_records(session, batch)
        upsert_state(
            session,
            study_files.study_accession,
            last_gene=last_gene,
            last_sample_index=last_sample,
            metadata_loaded=True,
        )
        session.commit()

    delete_gene_pair_correlations_for_study(session, study_key)
    correlations = compute_gene_pair_correlations(
        gene_expression_by_sample,
        sample_illness_map=sample_illness_map,
        study_key=study_key,
    )
    if correlations:
        bulk_insert_gene_pair_correlations(session, correlations)
    session.commit()

    if config.logging.log_record_counts:
        LOGGER.info(
            "Expression processed for study %s: %s records, %s genes",
            study_files.study_accession,
            total_records,
            len(total_genes),
        )

    return total_records, len(total_genes)


def _process_single_study(
    config: AppConfig,
    session_factory: sessionmaker,
    study_dir: pathlib.Path,
    gene_filter: set[str],
) -> None:
    study_files = discover_study_files(study_dir)
    LOGGER.info("Starting study %s", study_files.study_accession)

    start_time = time.perf_counter()
    with session_factory() as session:
        cache = bootstrap_cache(session)
        _metadata_loaded, resume_gene, resume_index = _load_resume_state(
            session, study_files.study_accession
        )

        try:
            study_key, samples, quality = _process_metadata(
                session, cache, study_files, config=config
            )
            upsert_state(
                session,
                study_files.study_accession,
                last_gene=resume_gene,
                last_sample_index=resume_index,
                metadata_loaded=True,
            )
            session.commit()
            record_count, gene_count = _process_expression(
                session,
                cache,
                study_key,
                samples,
                study_files,
                config=config,
                gene_filter=gene_filter,
                batch_size=config.database.batch_size,
                resume_gene=resume_gene,
                resume_sample_index=resume_index,
            )
        except (MetadataFormatError, ExpressionFormatError, StudyProcessingError) as exc:
            session.rollback()
            LOGGER.exception("Processing failed for study %s", study_files.study_accession)
            raise StudyProcessingError(str(exc)) from exc
        else:
            clear_state(session, study_files.study_accession)
            session.commit()

    elapsed = time.perf_counter() - start_time
    if config.logging.log_processing_time:
        LOGGER.info(
            "Completed study %s in %.2fs", study_files.study_accession, elapsed
        )


def run_pipeline(config: AppConfig) -> None:
    configure_logging(config)
    gene_filter = load_gene_filter(str(config.processing.gene_filter_file))
    LOGGER.info("Loaded %s gene identifiers from filter", len(gene_filter))

    engine = create_engine_with_retries(config)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)

    input_dir = pathlib.Path(config.processing.input_directory)
    study_dirs = sorted([p for p in input_dir.iterdir() if p.is_dir()])
    if not study_dirs:
        LOGGER.warning("No studies found in %s", input_dir)
        return

    max_workers = max(1, config.processing.max_concurrent_studies)
    LOGGER.info(
        "Processing %s studies with up to %s concurrent workers",
        len(study_dirs),
        max_workers,
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _process_single_study,
                config,
                session_factory,
                study_dir,
                gene_filter,
            ): study_dir
            for study_dir in study_dirs
        }
        for future in concurrent.futures.as_completed(futures):
            study_dir = futures[future]
            try:
                future.result()
            except Exception as exc:
                LOGGER.error("Study %s failed: %s", study_dir.name, exc)


__all__ = ["run_pipeline"]
