"""Study discovery utilities for locating genomics studies on disk."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List

from src.utils.file_utils import discover_study_dirs, ensure_required_files


@dataclass(slots=True)
class DiscoveryPlan:
    """Encapsulates discovery results for downstream pipeline stages."""

    input_root: Path
    studies: List[Path]

    def __iter__(self) -> Iterable[Path]:
        return iter(self.studies)


class StudyDiscovery:
    """Discover study directories that contain the required TSV files."""

    def __init__(self, input_root: Path, required_files: Iterable[str] | None = None) -> None:
        self.input_root = input_root
        self.required_files = list(required_files) if required_files else None

    def build_plan(self) -> DiscoveryPlan:
        if not self.input_root.exists():
            raise FileNotFoundError(f"Input root '{self.input_root}' does not exist")

        studies = discover_study_dirs(self.input_root)
        if self.required_files:
            filtered: list[Path] = []
            for study_dir in studies:
                ensure_required_files(study_dir, self.required_files)
                filtered.append(study_dir)
            studies = filtered
        else:
            for study_dir in studies:
                ensure_required_files(study_dir)

        return DiscoveryPlan(input_root=self.input_root, studies=studies)
