from pathlib import Path

from src.etl.checkpoint import Checkpoint, CheckpointStore


def test_checkpoint_store_records_and_retrieves(tmp_path):
    db_path = tmp_path / "artifacts.db"
    store = CheckpointStore(db_path)

    store.record(Checkpoint(run_id="run", study_id="study", table_name="fact_expression", batch_index=1))
    store.record(Checkpoint(run_id="run", study_id="study", table_name="fact_expression", batch_index=3))

    latest = store.latest_batch("run", "study", "fact_expression")
    assert latest == 3


def test_checkpoint_store_supports_wildcard(tmp_path):
    db_path = tmp_path / "artifacts.db"
    store = CheckpointStore(db_path)

    store.record(Checkpoint(run_id="run", study_id="s1", table_name="fact_expression", batch_index=2))
    store.record(Checkpoint(run_id="run", study_id="s2", table_name="fact_expression", batch_index=5))

    latest = store.latest_batch("run", "*", "fact_expression")
    assert latest == 5
