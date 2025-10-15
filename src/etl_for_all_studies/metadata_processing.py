"""Metadata extraction and transformation utilities."""
from __future__ import annotations

import csv
import logging
import re
from dataclasses import dataclass
from typing import Iterable, Sequence

from .config import FieldMappingConfig

LOGGER = logging.getLogger(__name__)
UNKNOWN_VALUE = "UNKNOWN"


@dataclass(slots=True)
class SampleMetadata:
    gsm_accession: str
    study_accession: str
    platform_accession: str
    illness_label: str
    age: str
    sex: str


@dataclass(slots=True)
class MetadataQuality:
    total_samples: int
    complete_age: int
    complete_sex: int

    @property
    def age_completion(self) -> float:
        return (self.complete_age / self.total_samples) if self.total_samples else 0.0

    @property
    def sex_completion(self) -> float:
        return (self.complete_sex / self.total_samples) if self.total_samples else 0.0


class MetadataFormatError(RuntimeError):
    """Raised when metadata files are missing required columns."""


def _normalize_header(name: str | None) -> str:
    """Return a case-insensitive representation with sequential digits stripped.

    Many refine.bio metadata files expose repeated characteristic columns whose
    names only differ by the numeric component (for example
    ``characteristics_ch1_Illness`` vs ``characteristics_ch2_illness``).  The
    ETL configuration typically lists a single canonical header.  By removing
    the numeric fragments we can treat these variants as the same logical
    column, while still preferring exact matches when they exist.
    """

    if not name:
        return ""
    return re.sub(r"\d+", "", name).strip().casefold()


def _first_non_empty(row: dict[str, str], candidates: Sequence[str]) -> str:
    if not row:
        return UNKNOWN_VALUE

    # Pre-compute lookups so we can resolve dynamic header variations quickly.
    casefold_lookup: dict[str, str] = {}
    normalized_lookup: dict[str, list[str]] = {}
    for header, raw_value in row.items():
        if header is None:
            continue
        if raw_value is not None and raw_value.strip():
            casefold_lookup.setdefault(header.casefold(), raw_value)
            normalized_key = _normalize_header(header)
            normalized_lookup.setdefault(normalized_key, []).append(raw_value)

    for candidate in candidates:
        if not candidate:
            continue

        # 1. Exact header match.
        value = row.get(candidate)
        if value is None:
            value = row.get(candidate.strip())

        # 2. Case-insensitive match.
        if value is None:
            value = casefold_lookup.get(candidate.casefold())

        # 3. Header variants that only differ by numeric suffix/prefix.
        if value is None:
            matches = normalized_lookup.get(_normalize_header(candidate), [])
            if matches:
                value = matches[0]

        if value is None:
            continue

        value = value.strip()
        if value:
            return value

    return UNKNOWN_VALUE


def load_metadata(
    file_path: str,
    mappings: FieldMappingConfig,
    *,
    enforce_required: bool = True,
) -> tuple[list[SampleMetadata], MetadataQuality]:
    """Load and transform sample metadata from a TSV file."""

    samples: list[SampleMetadata] = []
    total_samples = complete_age = complete_sex = 0

    with open(file_path, "r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        headers = reader.fieldnames or []
        required = {"refinebio_accession_code", "experiment_accession"}
        missing_required = required - set(headers)
        if enforce_required and missing_required:
            raise MetadataFormatError(
                f"Metadata file {file_path} missing required columns: {sorted(missing_required)}"
            )

        for row in reader:
            total_samples += 1
            gsm = row.get("refinebio_accession_code", "").strip()
            if not gsm:
                LOGGER.warning("Skipping metadata row without GSM accession in %s", file_path)
                continue

            study_accession = row.get("experiment_accession", "").strip() or UNKNOWN_VALUE
            platform_accession = _first_non_empty(row, mappings.platform_fields)
            illness_label = _first_non_empty(row, mappings.illness_fields)
            age = _first_non_empty(row, mappings.age_fields)
            sex = _first_non_empty(row, mappings.sex_fields)

            if age != UNKNOWN_VALUE:
                complete_age += 1
            if sex != UNKNOWN_VALUE:
                complete_sex += 1

            sample = SampleMetadata(
                gsm_accession=gsm,
                study_accession=study_accession or UNKNOWN_VALUE,
                platform_accession=platform_accession or UNKNOWN_VALUE,
                illness_label=illness_label or UNKNOWN_VALUE,
                age=age or UNKNOWN_VALUE,
                sex=sex or UNKNOWN_VALUE,
            )
            samples.append(sample)

    quality = MetadataQuality(
        total_samples=len(samples),
        complete_age=complete_age,
        complete_sex=complete_sex,
    )

    LOGGER.info(
        "Loaded %s samples from %s (age completion %.2f%%, sex completion %.2f%%)",
        quality.total_samples,
        file_path,
        quality.age_completion * 100,
        quality.sex_completion * 100,
    )

    return samples, quality


__all__ = [
    "SampleMetadata",
    "MetadataQuality",
    "MetadataFormatError",
    "load_metadata",
]
