"""Utilities for loading the gene filter list."""
from __future__ import annotations

import csv
from typing import Set


class GeneFilterError(RuntimeError):
    """Raised when the gene filter file cannot be processed."""


def load_gene_filter(path: str) -> Set[str]:
    """Return a set of Ensembl gene identifiers to keep."""

    genes: set[str] = set()
    with open(path, "r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        if "ensembl_id" not in (reader.fieldnames or []):
            raise GeneFilterError(
                f"Gene filter file {path} must include an 'ensembl_id' column"
            )
        for row in reader:
            gene_id = (row.get("ensembl_id") or "").strip()
            if gene_id:
                genes.add(gene_id)
    if not genes:
        raise GeneFilterError(f"Gene filter file {path} did not contain any gene identifiers")
    return genes


__all__ = ["load_gene_filter", "GeneFilterError"]
