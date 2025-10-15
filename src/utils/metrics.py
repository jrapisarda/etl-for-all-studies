"""Lightweight metrics helpers for the ETL pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Iterable


@dataclass
class Metric:
    """Simple container that stores a numeric metric."""

    key: str
    value: float
    observed_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class StudyMetrics:
    """Collection of metrics emitted for a given study/run combination."""

    run_id: str
    study_id: str
    metrics: Dict[str, Metric] = field(default_factory=dict)

    def record(self, key: str, value: float) -> None:
        self.metrics[key] = Metric(key=key, value=value)

    def export(self) -> Dict[str, float]:
        return {key: metric.value for key, metric in self.metrics.items()}

    def extend(self, items: Iterable[Metric]) -> None:
        for metric in items:
            self.metrics[metric.key] = metric
