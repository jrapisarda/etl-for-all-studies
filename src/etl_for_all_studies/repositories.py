"""Repository helpers for interacting with the dimensional schema."""
from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass

from sqlalchemy import select
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


def get_or_create_platform(session: Session, cache: DimensionCache, accession: str) -> int | None:
    if not accession or accession == UNKNOWN_VALUE:
        return None
    if accession in cache.platforms:
        return cache.platforms[accession]
    platform = DimPlatform(platform_accession=accession)
    session.add(platform)
    session.flush()
    cache.platforms[accession] = platform.platform_key
    LOGGER.debug("Inserted platform %s -> %s", accession, platform.platform_key)
    return platform.platform_key


def get_or_create_illness(session: Session, cache: DimensionCache, label: str) -> int | None:
    if not label or label == UNKNOWN_VALUE:
        return None
    if label in cache.illnesses:
        return cache.illnesses[label]
    illness = DimIllness(illness_label=label)
    session.add(illness)
    session.flush()
    cache.illnesses[label] = illness.illness_key
    LOGGER.debug("Inserted illness %s -> %s", label, illness.illness_key)
    return illness.illness_key


def get_or_create_gene(session: Session, cache: DimensionCache, ensembl_id: str) -> int:
    if ensembl_id in cache.genes:
        return cache.genes[ensembl_id]
    gene = DimGene(ensembl_id=ensembl_id)
    session.add(gene)
    session.flush()
    cache.genes[ensembl_id] = gene.gene_key
    LOGGER.debug("Inserted gene %s -> %s", ensembl_id, gene.gene_key)
    return gene.gene_key


def get_or_create_sample(
    session: Session,
    cache: DimensionCache,
    sample: SampleMetadata,
    *,
    study_key: int,
) -> int:
    key = (sample.gsm_accession, study_key)
    if key in cache.samples:
        return cache.samples[key]

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


__all__ = [
    "DimensionCache",
    "bootstrap_cache",
    "get_or_create_gene",
    "get_or_create_sample",
    "get_or_create_study",
    "get_or_create_platform",
    "get_or_create_illness",
    "bulk_insert_expression_records",
    "upsert_state",
    "clear_state",
]
