"""Database loader responsible for batching inserts and maintaining idempotency."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from sqlalchemy import delete, insert
from sqlalchemy.engine import Engine
from tenacity import RetryError, retry, stop_after_attempt, wait_exponential

from src.db import models
from src.utils.logging_setup import get_logger

logger = get_logger(__name__)


@dataclass(slots=True)
class LoaderConfig:
    batch_size: int = 1000
    retry_attempts: int = 3


class BatchLoader:
    """Coordinate batch inserts into the dimensional schema."""

    def __init__(self, engine: Engine, config: LoaderConfig | None = None) -> None:
        self.engine = engine
        self.config = config or LoaderConfig()

    def upsert_study_metadata(self, study_id: str, payload: dict[str, str]) -> None:
        logger.info("loader.metadata_upsert", study_id=study_id)

        @retry(
            stop=stop_after_attempt(self.config.retry_attempts),
            wait=wait_exponential(multiplier=1, min=1, max=8),
        )
        def _execute() -> None:
            with self.engine.begin() as conn:
                conn.execute(delete(models.study_dimension).where(models.study_dimension.c.study_id == study_id))
                conn.execute(
                    insert(models.study_dimension).values(study_id=study_id, payload=payload)
                )

        try:
            _execute()
        except RetryError as exc:  # pragma: no cover - guarded by tests
            raise RuntimeError("Failed to upsert study metadata") from exc

    def insert_expression_batch(
        self,
        run_id: str,
        study_id: str,
        batch_id: int,
        rows: Sequence[dict[str, float | str]],
    ) -> int:
        if not rows:
            return 0

        logger.info(
            "loader.expression_batch", run_id=run_id, study_id=study_id, batch_id=batch_id, size=len(rows)
        )

        @retry(
            stop=stop_after_attempt(self.config.retry_attempts),
            wait=wait_exponential(multiplier=1, min=1, max=8),
        )
        def _execute() -> int:
            with self.engine.begin() as conn:
                result = conn.execute(
                    insert(models.fact_expression),
                    [
                        {
                            "run_id": run_id,
                            "study_id": study_id,
                            "ensembl_id": str(row["ensembl_id"]),
                            "expression_value": float(row["expression_value"]),
                            "batch_id": batch_id,
                        }
                        for row in rows
                    ],
                )
            return result.rowcount or len(rows)

        try:
            return _execute()
        except RetryError as exc:  # pragma: no cover
            raise RuntimeError("Failed to insert expression batch") from exc
