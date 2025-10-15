"""Transformations for metadata and expression data."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, Mapping, Sequence

try:  # pragma: no cover - import guard
    import polars as pl
except Exception as exc:  # pragma: no cover
    pl = None  # type: ignore[assignment]
    _polars_error = exc
else:  # pragma: no cover - polars import success path is exercised in tests when available
    _polars_error = None

UNKNOWN_VALUE = "UNKNOWN"


@dataclass(slots=True)
class TransformConfig:
    metadata_mappings: Mapping[str, str]
    gene_filter: Sequence[str]


class MetadataTransformer:
    """Normalize metadata TSVs into dictionaries suitable for dimension tables."""

    def __init__(self, config: TransformConfig) -> None:
        self.config = config
        self._mapping = dict(config.metadata_mappings)

    def transform(self, path: Path) -> Dict[str, str]:
        if pl is None:
            raise RuntimeError(
                "Polars is required for metadata transforms but could not be imported"
            ) from _polars_error

        df = pl.read_csv(path, separator="\t", ignore_errors=True)
        if df.height == 0:
            raise ValueError(f"Metadata file '{path}' is empty")

        first_row = df.row(0, named=True)
        normalized: dict[str, str] = {}
        for target_field, source_field in self._mapping.items():
            value = first_row.get(source_field)
            if value is None or value == "":
                normalized[target_field] = UNKNOWN_VALUE
            else:
                normalized[target_field] = str(value)
        return normalized


class ExpressionTransformer:
    """Stream expression rows filtered to a known set of Ensembl identifiers."""

    def __init__(self, gene_filter: Iterable[str]):
        self.allowed = {gene.strip() for gene in gene_filter if gene.strip()}

    def stream_filtered(self, path: Path) -> Iterator[Dict[str, str]]:
        if pl is None:
            raise RuntimeError(
                "Polars is required for expression transforms but could not be imported"
            ) from _polars_error

        scan = pl.scan_csv(path, separator="\t")
        for row in scan.iter_rows(named=True):
            ensembl_id = str(row.get("ensembl_id", "")).strip()
            if ensembl_id and (not self.allowed or ensembl_id in self.allowed):
                yield {
                    "ensembl_id": ensembl_id,
                    "expression_value": float(row.get("expression_value", 0.0)),
                }
