"""Metadata extraction and transformation utilities."""
from __future__ import annotations

import csv
import logging
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


def _first_non_empty(row: dict[str, str], candidates: Sequence[str]) -> str:
    for candidate in candidates:
        value = row.get(candidate)
        if value is not None:
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
