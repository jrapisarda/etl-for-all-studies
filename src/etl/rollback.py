"""Rollback helper that removes data associated with a run."""

from __future__ import annotations

from sqlalchemy import delete
from sqlalchemy.engine import Engine

from src.db import models
from src.utils.logging_setup import get_logger

logger = get_logger(__name__)


class RollbackManager:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def rollback_run(self, run_id: str) -> None:
        logger.info("rollback.start", run_id=run_id)
        with self.engine.begin() as conn:
            conn.execute(delete(models.fact_expression).where(models.fact_expression.c.run_id == run_id))
            conn.execute(delete(models.metrics).where(models.metrics.c.run_id == run_id))
            conn.execute(delete(models.checkpoints).where(models.checkpoints.c.run_id == run_id))
            conn.execute(delete(models.runs).where(models.runs.c.run_id == run_id))
        logger.info("rollback.complete", run_id=run_id)
