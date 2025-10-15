"""Checkpoint persistence for resumable ETL runs."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(slots=True)
class Checkpoint:
    run_id: str
    study_id: str
    table_name: str
    batch_index: int


class CheckpointStore:
    """Persist checkpoints inside the local artifacts SQLite database."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS checkpoints (
                    run_id TEXT NOT NULL,
                    study_id TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    batch_index INTEGER NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(run_id, study_id, table_name, batch_index)
                )
                """
            )

    def record(self, checkpoint: Checkpoint) -> None:
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                """INSERT OR IGNORE INTO checkpoints(run_id, study_id, table_name, batch_index)
                VALUES (?, ?, ?, ?)""",
                (checkpoint.run_id, checkpoint.study_id, checkpoint.table_name, checkpoint.batch_index),
            )

    def latest_batch(self, run_id: str, study_id: str, table_name: str) -> Optional[int]:
        query = ("SELECT MAX(batch_index) FROM checkpoints WHERE run_id = ? AND table_name = ?")
        params: tuple[object, ...]
        if study_id == "*":
            params = (run_id, table_name)
        else:
            query += " AND study_id = ?"
            params = (run_id, table_name, study_id)

        with sqlite3.connect(self.path) as conn:
            cursor = conn.execute(query, params)
            row = cursor.fetchone()
        return row[0] if row and row[0] is not None else None
