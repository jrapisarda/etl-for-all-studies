#!/usr/bin/env python3
"""Entry point for the standalone gene pair correlation ETL job."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from etl_for_all_studies.config import ConfigurationError, load_config
from etl_for_all_studies.correlation_etl import run_correlation_etl


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the gene pair correlation ETL against an existing database",
    )
    parser.add_argument(
        "--config",
        dest="config_path",
        required=True,
        help="Path to the ETL configuration YAML file",
    )
    parser.add_argument(
        "--min-samples",
        dest="min_samples",
        type=int,
        default=3,
        help="Minimum number of shared samples required to compute a correlation",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        config = load_config(args.config_path)
    except ConfigurationError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    summary = run_correlation_etl(config, min_samples=args.min_samples)
    if summary.total_correlations == 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
