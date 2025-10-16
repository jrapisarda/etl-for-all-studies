"""Utilities for computing gene pair correlation statistics."""
from __future__ import annotations

import datetime as dt
import itertools
import math
from collections import defaultdict, namedtuple
from statistics import NormalDist
from typing import Mapping

try:  # pragma: no cover - exercised when SciPy is available
    from scipy.stats import spearmanr  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - lightweight fallback used in tests
    _SpearmanResult = namedtuple("SpearmanResult", ["statistic", "pvalue"])

    def _rankdata(values: list[float]) -> list[float]:
        indexed = sorted(enumerate(values), key=lambda item: item[1])
        ranks = [0.0] * len(values)
        i = 0
        while i < len(indexed):
            j = i
            total = 0.0
            while j < len(indexed) and indexed[j][1] == indexed[i][1]:
                total += j + 1
                j += 1
            avg_rank = total / (j - i)
            for k in range(i, j):
                ranks[indexed[k][0]] = avg_rank
            i = j
        return ranks

    def _pearson(x: list[float], y: list[float]) -> float:
        n = len(x)
        if n == 0:
            return math.nan
        mean_x = sum(x) / n
        mean_y = sum(y) / n
        num = sum((a - mean_x) * (b - mean_y) for a, b in zip(x, y))
        denom_x = math.sqrt(sum((a - mean_x) ** 2 for a in x))
        denom_y = math.sqrt(sum((b - mean_y) ** 2 for b in y))
        denom = denom_x * denom_y
        if denom == 0:
            return math.nan
        return num / denom

    def spearmanr(values_a: list[float], values_b: list[float]) -> tuple[float, float]:  # type: ignore
        ranks_a = _rankdata(list(values_a))
        ranks_b = _rankdata(list(values_b))
        rho = _pearson(ranks_a, ranks_b)
        if math.isnan(rho):
            return _SpearmanResult(math.nan, math.nan)
        n = len(values_a)
        if n < 3:
            return _SpearmanResult(rho, math.nan)
        if abs(rho) >= 1.0:
            return _SpearmanResult(max(min(rho, 1.0), -1.0), 0.0)
        t_stat = rho * math.sqrt((n - 2) / (1 - rho**2))
        dist = NormalDist()
        p_value = 2 * (1 - dist.cdf(abs(t_stat)))
        p_value = min(max(p_value, 0.0), 1.0)
        return _SpearmanResult(rho, p_value)

from .models import FactGenePairCorrelation

MIN_SAMPLES_FOR_CORRELATION = 3


def _benjamini_hochberg(p_values: list[float]) -> list[float | None]:
    if not p_values:
        return []

    m = len(p_values)
    sorted_indices = sorted(range(m), key=lambda idx: p_values[idx])
    adjusted = [None] * m
    prev = 1.0

    for rank, index in enumerate(reversed(sorted_indices), start=1):
        p_val = p_values[index]
        if math.isnan(p_val):
            adjusted[index] = None
            continue
        raw = (p_val * m) / (m - rank + 1)
        value = min(prev, raw)
        prev = value
        adjusted[index] = min(value, 1.0)

    # Replace any None (from NaN inputs) with None explicitly
    return adjusted


def compute_gene_pair_correlations(
    gene_expression_by_sample: Mapping[int, Mapping[str, float]],
    *,
    sample_illness_map: Mapping[str, int | None],
    study_key: int,
    min_samples: int = MIN_SAMPLES_FOR_CORRELATION,
) -> list[FactGenePairCorrelation]:
    """Compute Spearman correlations for all gene pairs grouped by illness."""

    illness_samples: dict[int, list[str]] = defaultdict(list)
    for sample_accession, illness_key in sample_illness_map.items():
        if illness_key is None:
            continue
        illness_samples[int(illness_key)].append(sample_accession)

    computed_at = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    correlations: list[FactGenePairCorrelation] = []

    for illness_key, samples in illness_samples.items():
        if len(samples) < min_samples:
            continue

        gene_keys = sorted(gene_expression_by_sample.keys())
        pair_stats: list[tuple[int, int, int, float, float]] = []

        for gene_a_key, gene_b_key in itertools.combinations(gene_keys, 2):
            expr_a = gene_expression_by_sample[gene_a_key]
            expr_b = gene_expression_by_sample[gene_b_key]
            shared_samples = [s for s in samples if s in expr_a and s in expr_b]
            if len(shared_samples) < min_samples:
                continue

            values_a = [expr_a[sample] for sample in shared_samples]
            values_b = [expr_b[sample] for sample in shared_samples]

            if len(set(values_a)) < 2 or len(set(values_b)) < 2:
                continue

            result = spearmanr(values_a, values_b)
            rho = float(result.statistic)
            p_value = float(result.pvalue)
            if math.isnan(rho) or math.isnan(p_value):
                continue

            pair_stats.append((gene_a_key, gene_b_key, len(shared_samples), rho, p_value))

        if not pair_stats:
            continue

        p_values = [entry[4] for entry in pair_stats]
        q_values = _benjamini_hochberg(p_values)

        for (gene_a_key, gene_b_key, n_samples, rho, p_value), q_value in zip(
            pair_stats, q_values
        ):
            correlations.append(
                FactGenePairCorrelation(
                    gene_a_key=gene_a_key,
                    gene_b_key=gene_b_key,
                    illness_key=illness_key,
                    rho_spearman=rho,
                    p_value=p_value,
                    q_value=q_value,
                    n_samples=n_samples,
                    computed_at=computed_at,
                    study_key=study_key,
                )
            )

    return correlations


__all__ = ["compute_gene_pair_correlations", "MIN_SAMPLES_FOR_CORRELATION"]
