import logging
import pathlib

import pytest

from etl_for_all_studies.expression_processing import iter_filtered_expression


def test_iter_filtered_expression_filters_missing_samples(tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture) -> None:
    expression_file = tmp_path / "expression.tsv"
    expression_file.write_text(
        "gene\tS1\tS2\n" "ENSG000001\t1.0\t2.0\n",
        encoding="utf-8",
    )

    caplog.set_level(logging.WARNING)

    rows = list(
        iter_filtered_expression(
            str(expression_file),
            allowed_genes={"ENSG000001"},
            sample_columns=["S1", "S3"],
        )
    )

    assert len(rows) == 1
    assert rows[0].sample_accession == "S1"
    assert "missing expected sample columns" in caplog.text
