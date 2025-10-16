"""Expression data ingestion and filtering logic."""
from __future__ import annotations

import csv
import logging
from collections.abc import Iterable, Iterator
from dataclasses import dataclass

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class ExpressionRow:
    gene_id: str
    sample_accession: str
    expression_value: float
    sample_index: int


class ExpressionFormatError(RuntimeError):
    """Raised when the expression file does not meet structural expectations."""


def iter_filtered_expression(
    path: str,
    *,
    allowed_genes: set[str],
    sample_columns: Iterable[str],
    resume_gene: str | None = None,
    resume_sample_index: int = 0,
) -> Iterator[ExpressionRow]:
    """Yield expression rows filtered by the allowed genes."""

    sample_columns = list(sample_columns)
    if not sample_columns:
        raise ExpressionFormatError("No sample columns provided for expression processing")

    with open(path, "r", encoding="utf-8") as handle:
        reader = csv.reader(handle, delimiter="\t")
        try:
            header = next(reader)
        except StopIteration as exc:  # pragma: no cover - empty file
            raise ExpressionFormatError(f"Expression file {path} is empty") from exc

        if len(header) < 2:
            raise ExpressionFormatError(
                f"Expression file {path} must contain gene column and at least one sample column"
            )
        if header[0].strip().lower() not in {"gene", "ensembl_id"}:
            raise ExpressionFormatError(
                f"Expression file {path} must begin with a gene identifier column"
            )

        sample_headers = header[1:]
        header_set = set(sample_headers)
        missing_samples = [sample for sample in sample_columns if sample not in header_set]
        if missing_samples:
            LOGGER.warning(
                "Expression file %s missing expected sample columns: %s",
                path,
                sorted(missing_samples),
            )
            sample_columns = [sample for sample in sample_columns if sample in header_set]
            if not sample_columns:
                raise ExpressionFormatError(
                    f"Expression file {path} missing all expected sample columns from metadata"
                )

        resume_reached = resume_gene is None
        for row in reader:
            if not row:
                continue
            gene_id = row[0].strip()
            if not gene_id:
                continue

            if not resume_reached:
                if gene_id == resume_gene:
                    resume_reached = True
                else:
                    continue

            if gene_id not in allowed_genes:
                continue

            for idx, (sample_name, value) in enumerate(zip(sample_headers, row[1:])):
                if sample_name not in sample_columns:
                    continue

                if gene_id == resume_gene and idx < resume_sample_index:
                    continue

                try:
                    expression_value = float(value)
                except ValueError:
                    LOGGER.warning(
                        "Skipping invalid expression value '%s' for gene %s sample %s",
                        value,
                        gene_id,
                        sample_name,
                    )
                    continue

                yield ExpressionRow(
                    gene_id=gene_id,
                    sample_accession=sample_name,
                    expression_value=expression_value,
                    sample_index=idx,
                )

            if gene_id == resume_gene:
                resume_gene = None
                resume_sample_index = 0


__all__ = ["ExpressionRow", "ExpressionFormatError", "iter_filtered_expression"]
