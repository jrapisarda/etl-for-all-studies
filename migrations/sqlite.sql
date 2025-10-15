-- SQLite schema for artifacts tracking and dimensional model
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    status TEXT NOT NULL,
    config_hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS checkpoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    study_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    batch_index INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(run_id, study_id, table_name, batch_index)
);

CREATE TABLE IF NOT EXISTS metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    study_id TEXT NOT NULL,
    metric_key TEXT NOT NULL,
    metric_value REAL NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_expression (
    run_id TEXT NOT NULL,
    study_id TEXT NOT NULL,
    ensembl_id TEXT NOT NULL,
    expression_value REAL NOT NULL,
    batch_id INTEGER NOT NULL,
    PRIMARY KEY (run_id, study_id, ensembl_id, batch_id)
);
