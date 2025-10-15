"""File system helpers shared across the ETL pipeline."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Iterable, List

REQUIRED_TSVS = {"metadata.tsv", "expression.tsv"}


def _has_required_files(directory: Path, required_files: set[str]) -> bool:
    try:
        present = {entry.name.lower() for entry in directory.iterdir() if entry.is_file()}
    except FileNotFoundError:
        return False
    return required_files.issubset(present)


def discover_study_dirs(root: Path, required_files: Iterable[str] | None = None) -> List[Path]:
    """Return directories under ``root`` containing required TSV artifacts.

    Supports passing a root that is itself a study directory and performs case-insensitive
    checks for the required files.
    """

    required = {name.lower() for name in (required_files or REQUIRED_TSVS)}
    studies: list[Path] = []

    if root.is_dir() and _has_required_files(root, required):
        studies.append(root)

    if root.is_dir():
        for candidate in sorted(child for child in root.iterdir() if child.is_dir()):
            if candidate in studies:
                continue
            if _has_required_files(candidate, required):
                studies.append(candidate)

    return studies


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
        raise FileNotFoundError(f"Study '{study_dir.name}' is missing required files: {missing_list}")
