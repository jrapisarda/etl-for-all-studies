import pathlib
import sys
import types

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

SRC_ROOT = pathlib.Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from etl_for_all_studies.config import (
    AppConfig,
    DatabaseConfig,
    FieldMappingConfig,
    LoggingConfig,
    ProcessingConfig,
)
from etl_for_all_studies.correlation_job import run_correlation_job
from etl_for_all_studies.models import (
    Base,
    DimGene,
    DimSample,
    DimStudy,
    FactExpression,
    FactGenePairCorrelation,
)
from etl_for_all_studies.repositories import (
    DimensionCache,
    get_or_create_gene,
    get_or_create_sample,
    get_or_create_study,
    iter_studies_with_expression,
    load_gene_expression_matrix,
)


def _build_config(tmp_path: pathlib.Path, db_path: pathlib.Path) -> AppConfig:
    gene_filter = tmp_path / "genes.tsv"
    gene_filter.write_text("ENSG000001\n", encoding="utf-8")
    logging_dir = tmp_path / "logs"
    processing_dir = tmp_path / "input"
    processing_dir.mkdir()

    database = DatabaseConfig(
        connection_string=f"sqlite:///{db_path}",
        batch_size=50,
        connection_timeout=30,
        max_retries=0,
        retry_backoff_seconds=1,
    )
    processing = ProcessingConfig(
        input_directory=processing_dir,
        gene_filter_file=gene_filter,
        max_concurrent_studies=1,
        state_directory=tmp_path / "state",
    )
    logging = LoggingConfig(
        log_level="INFO",
        log_directory=logging_dir,
        log_processing_time=True,
        log_record_counts=True,
        log_data_quality=False,
    )
    return AppConfig(
        database=database,
        processing=processing,
        logging=logging,
        field_mappings=FieldMappingConfig(),
    )


def _prime_expression_data(session: Session) -> DimStudy:
    cache = DimensionCache({}, {}, {}, {}, {})
    study_key = get_or_create_study(session, cache, "GSE500")
    study = session.get(DimStudy, study_key)
    assert study is not None

    samples = []
    for index in range(3):
        metadata = types.SimpleNamespace(
            gsm_accession=f"GSM{index}",
            study_accession="GSE500",
            platform_accession="UNKNOWN",
            illness_label="UNKNOWN",
            age="UNKNOWN",
            sex="UNKNOWN",
        )
        sample_key = get_or_create_sample(session, cache, metadata, study_key=study_key)
        samples.append(session.get(DimSample, sample_key))

    genes = []
    for ensembl_id in ("ENSG1", "ENSG2"):
        gene_key = get_or_create_gene(session, cache, ensembl_id)
        genes.append(session.get(DimGene, gene_key))

    session.flush()

    values = {
        ("ENSG1", "GSM0"): 1.0,
        ("ENSG1", "GSM1"): 2.0,
        ("ENSG1", "GSM2"): 3.0,
        ("ENSG2", "GSM0"): 1.0,
        ("ENSG2", "GSM1"): 1.5,
        ("ENSG2", "GSM2"): 2.0,
    }

    for gene in genes:
        assert gene is not None
    for sample in samples:
        assert sample is not None

    for (gene_id, sample_id), value in values.items():
        gene = next(g for g in genes if g.ensembl_id == gene_id)
        sample = next(s for s in samples if s.gsm_accession == sample_id)
        session.add(
            FactExpression(
                gene_key=gene.gene_key,
                sample_key=sample.sample_key,
                study_key=study.study_key,
                expression_value=value,
            )
        )

    session.commit()
    return study


def test_load_gene_expression_matrix_returns_samples(tmp_path):
    db_path = tmp_path / "job.db"
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        study = _prime_expression_data(session)
        matrix = load_gene_expression_matrix(session, study.study_key)
        gene_keys = {gene.gene_key for gene in session.execute(select(DimGene)).scalars()}

    assert set(matrix.keys()) == gene_keys
    for sample_values in matrix.values():
        assert set(sample_values.keys()) == {"GSM0", "GSM1", "GSM2"}


def test_run_correlation_job_generates_pairs(tmp_path):
    db_path = tmp_path / "correlation.db"
    config = _build_config(tmp_path, db_path)

    engine = create_engine(config.database.connection_string)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        study = _prime_expression_data(session)
        # Seed an obsolete correlation row to verify replacement
        session.add(
            FactGenePairCorrelation(
                gene_a_key=1,
                gene_b_key=2,
                illness_key=None,
                rho_spearman=0.0,
                p_value=1.0,
                q_value=1.0,
                n_samples=0,
                computed_at="1970-01-01T00:00:00Z",
                study_key=study.study_key,
            )
        )
        session.commit()
        study_key = study.study_key

    run_correlation_job(config, study_accessions=["GSE500", "GSE404"])

    with Session(engine) as session:
        descriptors = iter_studies_with_expression(session, ["GSE500"])
        assert len(descriptors) == 1
        correlations = session.execute(select(FactGenePairCorrelation)).scalars().all()

    assert len(correlations) == 1
    record = correlations[0]
    assert record.study_key == study_key
    assert record.n_samples == 3
    assert record.gene_a_key != record.gene_b_key
    assert record.rho_spearman != 0.0
