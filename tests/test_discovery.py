from pathlib import Path

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
