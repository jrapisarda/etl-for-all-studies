import pytest

from src.etl import transform
from src.etl.transform import ExpressionTransformer, MetadataTransformer, TransformConfig, UNKNOWN_VALUE


@pytest.mark.skipif(transform.pl is None, reason="Polars not available in test environment")
def test_metadata_transform_coalesces_missing(tmp_path):
    data = "STUDY_ID\tORGANISM\nS1\t\n"
    path = tmp_path / "metadata.tsv"
    path.write_text(data, encoding="utf-8")

    config = TransformConfig(metadata_mappings={"study_id": "STUDY_ID", "organism": "ORGANISM"}, gene_filter=[])
    transformer = MetadataTransformer(config)

    payload = transformer.transform(path)
    assert payload["study_id"] == "S1"
    assert payload["organism"] == UNKNOWN_VALUE


@pytest.mark.skipif(transform.pl is None, reason="Polars not available in test environment")
def test_expression_transform_filters_genes(tmp_path):
    data = "ensembl_id\texpression_value\nENSG1\t1.0\nENSG2\t2.0\n"
    path = tmp_path / "expression.tsv"
    path.write_text(data, encoding="utf-8")

    transformer = ExpressionTransformer(["ENSG2"])
    rows = list(transformer.stream_filtered(path))

    assert rows == [{"ensembl_id": "ENSG2", "expression_value": 2.0}]


def test_metadata_transform_requires_polars_when_missing(tmp_path, monkeypatch):
    if transform.pl is not None:
        pytest.skip("Polars installed")

    config = TransformConfig(metadata_mappings={"study_id": "STUDY_ID"}, gene_filter=[])
    transformer = MetadataTransformer(config)

    with pytest.raises(RuntimeError):
        transformer.transform(tmp_path / "metadata.tsv")
