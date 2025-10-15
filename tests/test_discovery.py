from pathlib import Path

from src.utils.file_utils import discover_study_dirs, ensure_required_files


def create_study(tmp_path: Path, name: str) -> Path:
    study = tmp_path / name
    study.mkdir()
    (study / "metadata.tsv").write_text("header\n", encoding="utf-8")
    (study / "expression.tsv").write_text("header\n", encoding="utf-8")
    return study


def test_discovery_plan_lists_valid_studies(tmp_path):
    from src.etl.discovery import StudyDiscovery

    create_study(tmp_path, "study_a")
    create_study(tmp_path, "study_b")
    (tmp_path / "random").mkdir()

    plan = StudyDiscovery(tmp_path).build_plan()

    assert len(plan.studies) == 2
    assert all(study.name.startswith("study_") for study in plan.studies)


def test_discovery_includes_root_when_it_is_study(tmp_path: Path) -> None:
    study_dir = create_study(tmp_path, "study_a")

    discovered = discover_study_dirs(study_dir)

    assert discovered == [study_dir]


def test_required_file_validation_is_case_insensitive(tmp_path: Path) -> None:
    study_dir = tmp_path / "study"
    study_dir.mkdir()
    (study_dir / "METADATA.TSV").write_text("header\n", encoding="utf-8")
    (study_dir / "Expression.TSV").write_text("header\n", encoding="utf-8")

    ensure_required_files(study_dir)
