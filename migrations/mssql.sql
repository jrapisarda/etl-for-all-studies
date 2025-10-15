-- SQL Server schema aligned with SQLite artifacts
CREATE TABLE dbo.runs (
    run_id NVARCHAR(64) NOT NULL PRIMARY KEY,
    started_at DATETIME2 NOT NULL,
    status NVARCHAR(32) NOT NULL,
    config_hash NVARCHAR(128) NOT NULL
);

CREATE TABLE dbo.checkpoints (
    id INT IDENTITY(1,1) PRIMARY KEY,
    run_id NVARCHAR(64) NOT NULL,
    study_id NVARCHAR(64) NOT NULL,
    table_name NVARCHAR(64) NOT NULL,
    batch_index INT NOT NULL,
    created_at DATETIME2 NOT NULL,
    CONSTRAINT uq_checkpoint UNIQUE (run_id, study_id, table_name, batch_index)
);

CREATE TABLE dbo.metrics (
    id INT IDENTITY(1,1) PRIMARY KEY,
    run_id NVARCHAR(64) NOT NULL,
    study_id NVARCHAR(64) NOT NULL,
    metric_key NVARCHAR(64) NOT NULL,
    metric_value FLOAT NOT NULL,
    created_at DATETIME2 NOT NULL
);

CREATE TABLE dbo.fact_expression (
    run_id NVARCHAR(64) NOT NULL,
    study_id NVARCHAR(64) NOT NULL,
    ensembl_id NVARCHAR(32) NOT NULL,
    expression_value FLOAT NOT NULL,
    batch_id INT NOT NULL,
    CONSTRAINT pk_fact_expression PRIMARY KEY (run_id, study_id, ensembl_id, batch_id)
);
