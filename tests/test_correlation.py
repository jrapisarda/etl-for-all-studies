import pathlib
import sys

SRC_ROOT = pathlib.Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from etl_for_all_studies.correlation import compute_gene_pair_correlations


def test_compute_gene_pair_correlations_returns_expected_pairs() -> None:
    gene_expression = {
        1: {"S1": 1.0, "S2": 2.0, "S3": 3.0},
        2: {"S1": 1.0, "S2": 1.5, "S3": 2.0},
        3: {"S1": 3.0, "S2": 2.0, "S3": 1.0},
    }
    sample_illness = {"S1": 10, "S2": 10, "S3": 10}

    results = compute_gene_pair_correlations(
        gene_expression,
        sample_illness_map=sample_illness,
        study_key=5,
    )

    assert len(results) == 3
    for record in results:
        assert record.study_key == 5
        assert record.illness_key is None
        assert record.n_samples == 3
        assert -1.0 <= record.rho_spearman <= 1.0
        assert 0.0 <= record.p_value <= 1.0
        if record.q_value is not None:
            assert 0.0 <= record.q_value <= 1.0
        assert len(record.computed_at) > 0

    # Ensure pairs use ordered gene keys to avoid duplicates
    ordered_pairs = {(min(r.gene_a_key, r.gene_b_key), max(r.gene_a_key, r.gene_b_key)) for r in results}
    assert len(ordered_pairs) == len(results)


def test_compute_gene_pair_correlations_skips_insufficient_samples() -> None:
    gene_expression = {1: {"S1": 5.0}, 2: {"S1": 2.0}}

    results = compute_gene_pair_correlations(
        gene_expression,
        sample_illness_map={},
        study_key=7,
    )

    assert results == []


def test_compute_gene_pair_correlations_without_illness_metadata() -> None:
    gene_expression = {
        1: {"S1": 1.0, "S2": 2.0, "S3": 3.0},
        2: {"S1": 2.0, "S2": 3.0, "S3": 4.0},
    }

    results = compute_gene_pair_correlations(
        gene_expression,
        sample_illness_map={},
        study_key=12,
    )

    assert len(results) == 1
    assert results[0].illness_key is None


def test_compute_gene_pair_correlations_handles_two_samples() -> None:
    gene_expression = {
        1: {"S1": 1.0, "S2": 2.0},
        2: {"S1": 3.0, "S2": 4.0},
    }

    results = compute_gene_pair_correlations(
        gene_expression,
        sample_illness_map={},
        study_key=42,
    )

    assert len(results) == 1
    record = results[0]
    assert record.n_samples == 2
    assert 0.0 <= record.p_value <= 1.0
    assert record.q_value is None
