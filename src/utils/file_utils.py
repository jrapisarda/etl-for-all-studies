"""File system helpers shared across the ETL pipeline."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Iterable, List

REQUIRED_TSVS = {"metadata.tsv", "expression.tsv"}


def discover_study_dirs(root: Path) -> List[Path]:
    """Return directories under ``root`` containing required TSV artifacts."""

    studies: list[Path] = []
    for candidate in root.iterdir():
        if candidate.is_dir() and REQUIRED_TSVS.issubset({p.name for p in candidate.iterdir()}):
            studies.append(candidate)
    return sorted(studies)


def hash_file(path: Path, chunk_size: int = 8192) -> str:
    """Return a SHA256 hash of a file."""

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def ensure_required_files(study_dir: Path, required: Iterable[str] | None = None) -> None:
    """Validate that the ``study_dir`` contains the expected TSV files."""

    required_files = {name.lower() for name in (required or REQUIRED_TSVS)}
    present = {p.name.lower() for p in study_dir.iterdir() if p.is_file()}
    missing = required_files - present
    if missing:
        missing_list = ", ".join(sorted(required or REQUIRED_TSVS))
    required_files = set(required or REQUIRED_TSVS)
    present = {p.name for p in study_dir.iterdir() if p.is_file()}
    missing = required_files - present
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise FileNotFoundError(f"Study '{study_dir.name}' is missing required files: {missing_list}")
