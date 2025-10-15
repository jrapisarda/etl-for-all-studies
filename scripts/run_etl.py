#!/usr/bin/env python3
"""Command-line entry point for the genomic ETL pipeline."""
from __future__ import annotations

import argparse
import logging
import sys

from etl_for_all_studies.config import ConfigurationError, load_config
from etl_for_all_studies.pipeline import run_pipeline

LOGGER = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the genomic ETL pipeline")
    parser.add_argument(
        "--config",
        dest="config_path",
        required=True,
        help="Path to the ETL configuration YAML file",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        config = load_config(args.config_path)
    except ConfigurationError as exc:
        LOGGER.error("Configuration error: %s", exc)
        return 2

    run_pipeline(config)
    return 0


if __name__ == "__main__":
    sys.exit(main())
