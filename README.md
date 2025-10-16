# Genomic Expression ETL

This repository implements a configurable ETL pipeline that ingests genomic expression
studies, transforms inconsistent metadata, filters expression data to a curated set of
Ensembl genes, and loads the results into a dimensional warehouse that supports
analytics-friendly querying.

## Features

- Concurrent processing of multiple study directories discovered automatically
- Streaming TSV ingestion with early gene filtering using an external whitelist
- Metadata normalization with configurable field mappings and UNKNOWN substitution
- Star schema population with duplicate detection and resume support after failures
- Configurable batch sizes, connection retries, and logging destinations

## Getting Started

1. Create and populate a configuration file (see `config/example_config.yaml`).
2. Ensure your gene filter TSV contains an `ensembl_id` column (see `config/filter_genes.tsv`).
3. Arrange study directories to include `metadata_*.tsv` and `expression_*.tsv` files.
4. Install dependencies and execute the pipeline:

```bash
pip install -r requirements.txt
./scripts/run_etl.py --config config/example_config.yaml
```

The example configuration uses SQLite for local development. Replace the connection
string with your SQL Server details for production environments.

### Computing Gene Pair Correlations

When the dimensional tables have already been populated you can run the dedicated
gene pair correlation ETL to populate `fact_gene_pair_corr` based on the existing
expression data. The job includes detailed logging and performance metrics for each
study:

```bash
./scripts/run_gene_corr_etl.py --config config/example_config.yaml
```

Use `--min-samples` to require a higher minimum number of overlapping samples for
Spearman correlation calculations.

## Logging & Resume State

The pipeline writes log files to the configured directory and maintains per-study state
records that allow processing to resume after transient database failures. State records
are cleared automatically when a study finishes successfully.

## Schema

The ETL populates the following schema:

- `dim_gene(ensembl_id)`
- `dim_sample(gsm_accession, study_key, platform_key, illness_key, age, sex)`
- `dim_study(gse_accession)`
- `dim_illness(illness_label)`
- `dim_platform(platform_accession)`
- `fact_expression(sample_key, gene_key, study_key, expression_value)`

Refer to `docs/genomic-etl-requirements.md` for the full set of functional requirements.
