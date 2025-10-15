from pathlib import Path
import sys

import pytest

pytest.importorskip("pydantic")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from kimi_coding_agent_v5 import normalize_requirements_data


def test_normalize_requirements_data_handles_full_payload():
    payload = {
        "project": {
            "name": "bioinformatics-etl-cli",
            "version": "1.0.0",
            "description": "Local-first, CLI ETL for genomic TSV studies to SQL Server star schema with checkpointing and rollback",
            "type": "etl-cli",
            "complexity": "advanced",
        },
        "specifications": {
            "architecture": {
                "pattern": "etl-pipeline",
                "components": [
                    "study-discovery",
                    "metadata-transform",
                    "gene-filtering",
                    "batch-loader",
                    "checkpointing",
                    "rollback-manager",
                    "json-logging",
                    "config-manager",
                ],
                "deployment": "local-workstation",
                "communication": "in-process",
            },
            "technical_requirements": {
                "core_platform": [
                    "Python 3.13.x",
                    "Polars 1.34.0",
                    "SQLAlchemy 2.0.44",
                    "pyodbc 5.2.0",
                    "Pydantic 2.12",
                    "structlog 25.4.0",
                    "tenacity 9.1.2",
                    "SQLite 3.x",
                ],
                "development_tools": [
                    "uv",
                    "pytest 8.4.2+",
                    "python-dotenv 1.1.x",
                    "Typer 0.19.x",
                ],
                "execution_environment": [
                    "Local filesystem access",
                    "SQL Server via ODBC Driver 17+",
                    "SQLite artifacts.db",
                    "Configurable concurrency",
                    "Batch transactions",
                    "Network reconnects",
                ],
            },
            "functional_requirements": {
                "core_features": [
                    "Scan root directory for studies with required TSVs",
                    "Concurrent multi-study processing with limits",
                    "Filter expression by Ensembl IDs during ingestion",
                    "Standardize metadata to dims with UNKNOWN for missing",
                    "Idempotent upserts and conflict skipping",
                    "Batching per table with transactional commits",
                    "Resume from last successful batch with checkpoints",
                    "Externalized configuration (YAML)",
                    "Structured JSON logging and metrics",
                    "Local-first mode with SQLite artifacts",
                    "Operational snapshots and rollback by run_id",
                    "Aligned migrations for SQL Server and SQLite",
                ],
                "cli_operations": [
                    "etl run --config ./config.yaml",
                    "etl resume --run-id <RUN_ID>",
                    "etl rollback --run-id <RUN_ID>",
                    "etl validate --input-dir <PATH>",
                    "etl report --run-id <RUN_ID>",
                ],
                "agent_capabilities": {
                    "requirements_analysis": {
                        "input": ["yaml_config", "directory_tree", "tsv_headers"],
                        "output": "discovery_plan",
                        "tools": [
                            "config_loader",
                            "study_discoverer",
                            "preflight_validator",
                        ],
                        "validation": [
                            "required_columns_check",
                            "encoding_and_delimiter_check",
                        ],
                    },
                    "coding_agent": {
                        "input": "discovery_plan",
                        "output": [
                            "etl_pipeline",
                            "ddl_migrations",
                            "cli_commands",
                        ],
                        "tools": [
                            "polars_streamer",
                            "transform_mapper",
                            "db_bulk_loader",
                            "checkpoint_store",
                        ],
                        "validation": [
                            "schema_alignment",
                            "idempotency_checks",
                            "smoke_run_single_study",
                        ],
                    },
                },
            },
        },
        "development_plan": {
            "phases": [
                {
                    "name": "Core Scaffolding",
                    "duration": "2 weeks",
                    "components": [
                        "Project structure",
                        "Config models (Pydantic)",
                        "Logging setup (structlog)",
                        "Artifacts SQLite",
                    ],
                },
                {
                    "name": "Validation & Packaging",
                    "duration": "2 weeks",
                    "components": [
                        "Preflight validators",
                        "Test fixtures & pytest",
                        "Docs and CLI help",
                        "Migrations for SQL Server/SQLite",
                    ],
                },
            ],
            "milestones": ["Week 2: CLI runs with config and logging"],
        },
        "file_structure": {
            "directories": ["src/etl/", "tests/"],
            "files": {"src/etl/": ["discovery.py"]},
        },
        "dependencies": {
            "core": ["polars>=1.34.0"],
            "dev": ["pytest>=8.4.2"],
        },
        "configuration": {
            "api_settings": {
                "openai_api_key": "env:OPENAI_API_KEY",
                "model": "gpt-5",
                "max_tokens": 4000,
                "temperature": 0.1,
            }
        },
        "execution_workflow": {
            "setup": ["Initialize artifacts.db"],
            "main_execution": ["Discover studies and enqueue"],
            "error_handling": ["Retry DB ops with tenacity"],
        },
        "quality_assurance": {
            "testing_strategy": {
                "unit_tests": ["Discovery logic"],
                "integration_tests": ["End-to-end with sample TSVs"],
                "acceptance_tests": ["Process 5 studies concurrently"],
            },
            "code_quality": {
                "type_checking": "mypy with strict",
                "formatting": "black line-length 100",
                "linting": "ruff all rules",
                "coverage_target": "80% minimum",
            },
            "monitoring": {
                "structured_logging": "JSON logs with run_id and study tags",
                "performance_metrics": "Records/sec, memory, batch latencies",
                "error_tracking": "Categorized errors with context",
                "audit_trail": "Complete run history in SQLite",
            },
        },
        "agent_specifications": {
            "requirements_analysis_agent": {
                "input": ["yaml_config"],
                "output": "discovery_plan",
                "tools": ["config_loader"],
                "validation": ["required_columns_check"],
            }
        },
        "deliverables": {
            "final_package": {
                "required_files": ["src/", "README.md"],
                "metadata_includes": ["python_version"],
                "packaging_format": "ZIP",
            }
        },
    }

    normalized = normalize_requirements_data(payload)

    assert normalized["project"]["name"] == "bioinformatics-etl-cli"
    assert "etl run --config ./config.yaml" in normalized["specifications"]["functional_requirements"]["cli_operations"]
    assert len(normalized["development_plan"]["phases"]) == 2
    assert normalized["dependencies"]["core"] == ["polars>=1.34.0"]


def test_normalize_requirements_data_accepts_scalar_lists():
    payload = {
        "project": {"name": "demo"},
        "specifications": {
            "functional_requirements": {
                "cli_operations": "etl run",
                "agent_capabilities": {
                    "coding_agent": {
                        "input": "plan",
                        "output": "code",
                        "tools": "editor",
                        "validation": "tests",
                    }
                },
            }
        },
    }

    normalized = normalize_requirements_data(payload)

    assert normalized["specifications"]["functional_requirements"]["cli_operations"] == ["etl run"]
    capabilities = normalized["specifications"]["functional_requirements"]["agent_capabilities"]["coding_agent"]
    assert capabilities["input"] == ["plan"]
    assert capabilities["output"] == ["code"]
    assert capabilities["tools"] == ["editor"]
    assert capabilities["validation"] == ["tests"]


def test_normalize_requirements_data_best_effort_agent_capabilities_lists():
    payload = {
        "specifications": {
            "functional_requirements": {
                "agent_capabilities": {
                    "requirements_analysis": [
                        "Parse YAML config",
                        "Discover studies",
                        "Load gene filter",
                    ]
                }
            }
        }
    }

    normalized = normalize_requirements_data(payload)

    capabilities = normalized["specifications"]["functional_requirements"]["agent_capabilities"][
        "requirements_analysis"
    ]
    assert capabilities["summary"] == [
        "Parse YAML config",
        "Discover studies",
        "Load gene filter",
    ]


def test_normalize_requirements_data_returns_raw_payload_for_text():
    payload = "Plain english description of requirements"

    normalized = normalize_requirements_data(payload)

    assert normalized["raw_payload"] == "Plain english description of requirements"
