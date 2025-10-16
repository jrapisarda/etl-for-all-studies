import pathlib
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

SRC_ROOT = pathlib.Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from etl_for_all_studies.metadata_processing import SampleMetadata
from etl_for_all_studies.models import Base, DimSample
from etl_for_all_studies.repositories import (
    DimensionCache,
    get_or_create_gene,
    get_or_create_platform,
    get_or_create_sample,
    get_or_create_study,
)


def create_session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return Session(engine)


def test_get_or_create_sample_updates_existing_metadata() -> None:
    session = create_session()
    cache = DimensionCache({}, {}, {}, {}, {})
    study_key = get_or_create_study(session, cache, "GSE100")

    initial_sample = SampleMetadata(
        gsm_accession="GSM1",
        study_accession="GSE100",
        platform_accession="",
        illness_label="UNKNOWN",
        age="UNKNOWN",
        sex="UNKNOWN",
    )
    sample_key = get_or_create_sample(session, cache, initial_sample, study_key=study_key)
    session.commit()

    updated_sample = SampleMetadata(
        gsm_accession="GSM1",
        study_accession="GSE100",
        platform_accession="GPL200",
        illness_label="Influenza",
        age="32",
        sex="female",
    )
    new_key = get_or_create_sample(session, cache, updated_sample, study_key=study_key)
    session.commit()

    assert sample_key == new_key

    refreshed = session.get(DimSample, sample_key)
    assert refreshed is not None
    assert refreshed.platform is not None
    assert refreshed.platform.platform_accession == "GPL200"
    assert refreshed.illness is not None
    assert refreshed.illness.illness_label == "Influenza"
    assert refreshed.age == "32"
    assert refreshed.sex == "female"


def test_get_or_create_gene_handles_existing_without_cache() -> None:
    session = create_session()
    cache = DimensionCache({}, {}, {}, {}, {})

    first_key = get_or_create_gene(session, cache, "ENSG000001")
    session.commit()

    cache.genes.clear()
    second_key = get_or_create_gene(session, cache, "ENSG000001")

    assert first_key == second_key


def test_get_or_create_platform_handles_existing_without_cache() -> None:
    session = create_session()
    cache = DimensionCache({}, {}, {}, {}, {})

    first_key = get_or_create_platform(session, cache, "GPL570")
    assert first_key is not None
    session.commit()

    cache.platforms.clear()
    second_key = get_or_create_platform(session, cache, "GPL570")

    assert first_key == second_key
