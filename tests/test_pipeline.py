import importlib.util
import pathlib
import sys
import types


MODULE_PATH = pathlib.Path(__file__).resolve().parents[1] / "src" / "etl_for_all_studies" / "pipeline.py"
# Provide lightweight stubs for optional heavy dependencies when they are not installed.
try:  # pragma: no cover - exercised only when dependencies are available
    import sqlalchemy  # type: ignore
    import sqlalchemy.orm  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - ensures test runs without heavy deps
    sqlalchemy_stub = types.ModuleType("sqlalchemy")
    sqlalchemy_stub.select = lambda *args, **kwargs: None
    orm_stub = types.ModuleType("sqlalchemy.orm")
    orm_stub.Session = object
    orm_stub.sessionmaker = lambda *args, **kwargs: None
    sys.modules.setdefault("sqlalchemy", sqlalchemy_stub)
    sys.modules.setdefault("sqlalchemy.orm", orm_stub)

try:  # pragma: no cover - exercised only when dependencies are available
    import yaml  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - ensures test runs without heavy deps
    yaml_stub = types.ModuleType("yaml")
    yaml_stub.safe_load = lambda *args, **kwargs: {}
    sys.modules.setdefault("yaml", yaml_stub)

# Create a lightweight package container so relative imports succeed.
package_name = "etl_for_all_studies"
package_module = types.ModuleType(package_name)
package_module.__path__ = [str(MODULE_PATH.parent)]
sys.modules.setdefault(package_name, package_module)

SPEC = importlib.util.spec_from_file_location("etl_for_all_studies.pipeline", MODULE_PATH)
pipeline = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = pipeline
SPEC.loader.exec_module(pipeline)

from etl_for_all_studies.config import FieldMappingConfig
from etl_for_all_studies.models import DimSample, DimStudy

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session


def test_discover_study_files_accepts_accession_named_expression(tmp_path: pathlib.Path) -> None:
    study_dir = tmp_path / "GSE11907"
    study_dir.mkdir()

    metadata_file = study_dir / "metadata_GSE11907.tsv"
    metadata_file.write_text("sample_id\n", encoding="utf-8")

    expression_file = study_dir / "GSE11907.tsv"
    expression_file.write_text("gene\tsample\n", encoding="utf-8")

    study_files = pipeline.discover_study_files(study_dir)

    assert study_files.metadata_file == metadata_file
    assert study_files.expression_file == expression_file
    assert study_files.study_accession == "GSE11907"


def test_process_metadata_uses_directory_study_accession(tmp_path: pathlib.Path) -> None:
    metadata_file = tmp_path / "metadata_GSE123.tsv"
    metadata_file.write_text(
        "refinebio_accession_code\texperiment_accession\trefinebio_age\trefinebio_sex\n"
        "GSM_A\tGSE999\t\t\n"
        "GSM_B\tGSE123\t45\tfemale\n",
        encoding="utf-8",
    )
    expression_file = tmp_path / "expression_GSE123.tsv"
    expression_file.write_text("gene\tGSM_A\tGSM_B\n", encoding="utf-8")

    class StubLogging:
        log_record_counts = False
        log_data_quality = False

    stub_config = types.SimpleNamespace(
        field_mappings=FieldMappingConfig(),
        logging=StubLogging(),
    )

    engine = create_engine("sqlite:///:memory:")
    pipeline.Base.metadata.create_all(engine)
    session = Session(engine)
    cache = pipeline.DimensionCache({}, {}, {}, {}, {})

    study_files = pipeline.StudyFiles(
        study_accession="GSE123",
        metadata_file=metadata_file,
        expression_file=expression_file,
    )

    study_key, samples, _quality = pipeline._process_metadata(
        session, cache, study_files, config=stub_config
    )

    study = session.execute(select(DimStudy).where(DimStudy.gse_accession == "GSE123")).scalar_one()
    assert study.study_key == study_key

    dim_samples = session.execute(select(DimSample)).scalars().all()
    assert {sample.gsm_accession for sample in dim_samples} == {"GSM_A", "GSM_B"}
    assert all(sample.study_key == study_key for sample in dim_samples)
    assert set(cache.samples.keys()) == {("GSM_A", study_key), ("GSM_B", study_key)}
    assert all(sample.study_accession == "GSE123" for sample in samples)
