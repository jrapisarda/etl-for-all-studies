import pathlib
import sys

SRC_ROOT = pathlib.Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from etl_for_all_studies.config import FieldMappingConfig
from etl_for_all_studies.metadata_processing import UNKNOWN_VALUE, load_metadata


def test_load_metadata_handles_variant_characteristic_headers(tmp_path):
    metadata_path = tmp_path / "metadata.tsv"
    metadata_path.write_text(
        "refinebio_accession_code\texperiment_accession\tcharacteristics_ch2_illness\n"
        "GSM1\tGSE1\tFlu\n",
        encoding="utf-8",
    )

    samples, _ = load_metadata(
        str(metadata_path),
        FieldMappingConfig(illness_fields=("characteristics_ch1_Illness",)),
    )

    assert samples[0].illness_label == "Flu"


def test_load_metadata_returns_unknown_when_no_match(tmp_path):
    metadata_path = tmp_path / "metadata.tsv"
    metadata_path.write_text(
        "refinebio_accession_code\texperiment_accession\tsome_other_column\n"
        "GSM1\tGSE1\tvalue\n",
        encoding="utf-8",
    )

    samples, _ = load_metadata(
        str(metadata_path),
        FieldMappingConfig(illness_fields=("characteristics_ch1_Illness",)),
    )

    assert samples[0].illness_label == UNKNOWN_VALUE
