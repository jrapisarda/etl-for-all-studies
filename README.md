# Bioinformatics ETL CLI

A local-first ETL toolkit that ingests genomics TSV studies into a dimensional model targeting SQL
Server or SQLite. The project focuses on debuggable CLI operations, resilient batch processing, and
checkpointed recovery workflows.

## Getting Started

1. **Install dependencies** (recommended: [`uv`](https://docs.astral.sh/uv/)):

   ```bash
   uv venv
   source .venv/bin/activate
   uv pip install -e .[dev]
   ```

2. **Copy the example configuration** and adjust to your environment:

   ```bash
   cp config/config.example.yaml config/config.yaml
   cp config/field_mappings.example.yaml config/field_mappings.yaml
   ```

3. **Run the CLI** using the Typer entry point:

   ```bash
   python -m src.cli.main etl run --config ./config/config.yaml
   ```

## CLI Commands

- `etl run --config ./config.yaml` – Execute a new ETL run with discovery, transform, and load
  phases.
- `etl resume --run-id <RUN_ID>` – Resume a halted run by replaying checkpoints.
- `etl rollback --run-id <RUN_ID>` – Revert fact and dimension data associated with a run.
- `etl validate --input-dir <PATH>` – Verify study folders and TSV structure.
- `etl report --run-id <RUN_ID>` – Summarize metrics for a previous execution.

## Project Layout

The repository follows an ETL-centric package layout:

```
src/
  cli/            # Typer CLI definition and validators
  db/             # SQLAlchemy engines and metadata
  etl/            # Discovery, transform, load, and resilience layers
  utils/          # Logging, file utilities, metrics utilities
migrations/       # SQL Server and SQLite DDL
config/           # Configuration templates
logs/, artifacts/ # Runtime outputs and checkpoints
```

## Testing

Run the pytest suite:

```bash
pytest
```

Tests rely on lightweight fixtures and do not require live database connections. Integration tests
can be added under `tests/` as the pipeline is expanded.

## License

Distributed under the terms of the MIT License. See [LICENSE](LICENSE).
