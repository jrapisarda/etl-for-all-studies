"""Repository helpers for interacting with the dimensional schema."""
from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass

from sqlalchemy import delete, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from .metadata_processing import SampleMetadata
from .models import (
    DimGene,
    DimIllness,
    DimPlatform,
    DimSample,
    DimStudy,
    EtlStudyState,
    FactExpression,
    FactGenePairCorrelation,
)

LOGGER = logging.getLogger(__name__)
UNKNOWN_VALUE = "UNKNOWN"


@dataclass(slots=True)
class DimensionCache:
    genes: dict[str, int]
    studies: dict[str, int]
    platforms: dict[str, int]
    illnesses: dict[str, int]
    samples: dict[tuple[str, int], int]


def bootstrap_cache(session: Session) -> DimensionCache:
    """Load existing dimension keys into memory for duplicate detection."""

    genes = {row.ensembl_id: row.gene_key for row in session.execute(select(DimGene)).scalars()}
    studies = {row.gse_accession: row.study_key for row in session.execute(select(DimStudy)).scalars()}
    platforms = {
        row.platform_accession: row.platform_key
        for row in session.execute(select(DimPlatform)).scalars()
    }
    illnesses = {
        row.illness_label: row.illness_key
        for row in session.execute(select(DimIllness)).scalars()
    }
    samples = {
        (row.gsm_accession, row.study_key): row.sample_key
        for row in session.execute(select(DimSample)).scalars()
    }
    return DimensionCache(genes, studies, platforms, illnesses, samples)


def get_or_create_study(session: Session, cache: DimensionCache, gse_accession: str) -> int:
    if gse_accession in cache.studies:
        return cache.studies[gse_accession]
    study = DimStudy(gse_accession=gse_accession)
    session.add(study)
    session.flush()
    cache.studies[gse_accession] = study.study_key
    LOGGER.debug("Inserted study %s -> %s", gse_accession, study.study_key)
    return study.study_key


def _get_or_create_unique_dimension(
    session: Session,
    cache: dict[str, int],
    value: str,
    *,
    model: type[DimGene] | type[DimPlatform] | type[DimIllness],
    unique_column,
    key_attribute: str,
    log_label: str,
) -> int:
    if value in cache:
        return cache[value]

    existing = session.execute(select(model).where(unique_column == value)).scalar_one_or_none()
    if existing is not None:
        key = getattr(existing, key_attribute)
        cache[value] = key
        LOGGER.debug("Loaded existing %s %s -> %s", log_label, value, key)
        return key

    bind = session.get_bind()
    inserted = False
    if bind is not None and bind.dialect.name == "sqlite":
        stmt = (
            sqlite_insert(model)
            .values({unique_column.key: value})
            .on_conflict_do_nothing(index_elements=[unique_column])
        )
        result = session.execute(stmt)
        inserted = bool(result.rowcount)
        session.flush()
    else:
        instance = model(**{unique_column.key: value})
        session.add(instance)
        session.flush()
        key = getattr(instance, key_attribute)
        cache[value] = key
        LOGGER.debug("Inserted %s %s -> %s", log_label, value, key)
        return key

    existing = session.execute(select(model).where(unique_column == value)).scalar_one()
    key = getattr(existing, key_attribute)
    cache[value] = key
    if inserted:
        LOGGER.debug("Inserted %s %s -> %s", log_label, value, key)
    else:
        LOGGER.debug("Re-used existing %s %s -> %s", log_label, value, key)
    return key


def get_or_create_platform(session: Session, cache: DimensionCache, accession: str) -> int | None:
    if not accession or accession == UNKNOWN_VALUE:
        return None
    return _get_or_create_unique_dimension(
        session,
        cache.platforms,
        accession,
        model=DimPlatform,
        unique_column=DimPlatform.platform_accession,
        key_attribute="platform_key",
        log_label="platform",
    )


def get_or_create_illness(session: Session, cache: DimensionCache, label: str) -> int | None:
    if not label or label == UNKNOWN_VALUE:
        return None
    return _get_or_create_unique_dimension(
        session,
        cache.illnesses,
        label,
        model=DimIllness,
        unique_column=DimIllness.illness_label,
        key_attribute="illness_key",
        log_label="illness",
    )


def get_or_create_gene(session: Session, cache: DimensionCache, ensembl_id: str) -> int:
    return _get_or_create_unique_dimension(
        session,
        cache.genes,
        ensembl_id,
        model=DimGene,
        unique_column=DimGene.ensembl_id,
        key_attribute="gene_key",
        log_label="gene",
    )


def get_or_create_sample(
    session: Session,
    cache: DimensionCache,
    sample: SampleMetadata,
    *,
    study_key: int,
) -> int:
    key = (sample.gsm_accession, study_key)
    if key in cache.samples:
        sample_key = cache.samples[key]
        dim_sample = session.get(DimSample, sample_key)
        if dim_sample is None:
            cache.samples.pop(key, None)
        else:
            platform_key = get_or_create_platform(session, cache, sample.platform_accession)
            illness_key = get_or_create_illness(session, cache, sample.illness_label)
            updated = False
            if platform_key and dim_sample.platform_key != platform_key:
                dim_sample.platform_key = platform_key
                updated = True
            if illness_key and dim_sample.illness_key != illness_key:
                dim_sample.illness_key = illness_key
                updated = True
            if sample.age and sample.age != UNKNOWN_VALUE and (
                not dim_sample.age or dim_sample.age == UNKNOWN_VALUE
            ):
                dim_sample.age = sample.age
                updated = True
            if sample.sex and sample.sex != UNKNOWN_VALUE and (
                not dim_sample.sex or dim_sample.sex == UNKNOWN_VALUE
            ):
                dim_sample.sex = sample.sex
                updated = True
            if updated:
                LOGGER.debug(
                    "Updated sample %s/%s metadata (platform=%s, illness=%s)",
                    sample.gsm_accession,
                    study_key,
                    platform_key,
                    illness_key,
                )
            return sample_key

    platform_key = get_or_create_platform(session, cache, sample.platform_accession)
    illness_key = get_or_create_illness(session, cache, sample.illness_label)

    dim_sample = DimSample(
        gsm_accession=sample.gsm_accession,
        study_key=study_key,
        platform_key=platform_key,
        illness_key=illness_key,
        age=sample.age or UNKNOWN_VALUE,
        sex=sample.sex or UNKNOWN_VALUE,
    )
    session.add(dim_sample)
    session.flush()
    cache.samples[key] = dim_sample.sample_key
    LOGGER.debug(
        "Inserted sample %s/%s -> %s",
        sample.gsm_accession,
        study_key,
        dim_sample.sample_key,
    )
    return dim_sample.sample_key


def upsert_state(
    session: Session,
    study_accession: str,
    *,
    last_gene: str | None,
    last_sample_index: int,
    metadata_loaded: bool,
) -> None:
    state = session.get(EtlStudyState, study_accession)
    if state is None:
        state = EtlStudyState(
            study_accession=study_accession,
            last_processed_gene=last_gene,
            last_sample_index=last_sample_index,
            metadata_loaded=1 if metadata_loaded else 0,
        )
        session.add(state)
    else:
        state.last_processed_gene = last_gene
        state.last_sample_index = last_sample_index
        state.metadata_loaded = 1 if metadata_loaded else 0
    LOGGER.debug(
        "Updated state for %s (last_gene=%s, last_sample_index=%s, metadata_loaded=%s)",
        study_accession,
        last_gene,
        last_sample_index,
        metadata_loaded,
    )


def clear_state(session: Session, study_accession: str) -> None:
    state = session.get(EtlStudyState, study_accession)
    if state:
        session.delete(state)


def bulk_insert_expression_records(
    session: Session,
    records: Iterable[FactExpression],
) -> None:
    session.add_all(records)


def bulk_insert_gene_pair_correlations(
    session: Session, records: Iterable[FactGenePairCorrelation]
) -> None:
    session.add_all(records)


def delete_gene_pair_correlations_for_study(session: Session, study_key: int) -> None:
    session.execute(
        delete(FactGenePairCorrelation).where(
            FactGenePairCorrelation.study_key == study_key
        )
    )


__all__ = [
    "DimensionCache",
    "bootstrap_cache",
    "get_or_create_gene",
    "get_or_create_sample",
    "get_or_create_study",
    "get_or_create_platform",
    "get_or_create_illness",
    "bulk_insert_expression_records",
    "bulk_insert_gene_pair_correlations",
    "delete_gene_pair_correlations_for_study",
    "upsert_state",
    "clear_state",
]
