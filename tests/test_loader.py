import pytest

sqlalchemy = pytest.importorskip("sqlalchemy")
from sqlalchemy import create_engine, select

from src.db import models
from src.etl.loader import BatchLoader, LoaderConfig


def setup_engine():
    engine = create_engine("sqlite:///:memory:", future=True)
    models.metadata.create_all(engine)
    return engine


def test_upsert_study_metadata_overwrites_existing():
    engine = setup_engine()
    loader = BatchLoader(engine, LoaderConfig(batch_size=10, retry_attempts=1))

    loader.upsert_study_metadata("S1", {"study_id": "S1"})
    loader.upsert_study_metadata("S1", {"study_id": "S1", "organism": "human"})

    with engine.connect() as conn:
        payload = conn.execute(select(models.study_dimension.c.payload)).scalar_one()
    assert payload["organism"] == "human"


def test_insert_expression_batch_inserts_rows():
    engine = setup_engine()
    loader = BatchLoader(engine)

    rows = [
        {"ensembl_id": "ENSG1", "expression_value": 1.2},
        {"ensembl_id": "ENSG2", "expression_value": 3.4},
    ]

    inserted = loader.insert_expression_batch("run", "S1", 0, rows)
    assert inserted == 2

    with engine.connect() as conn:
        count = conn.execute(select(models.fact_expression)).fetchall()
    assert len(count) == 2
