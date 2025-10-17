from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from etl_for_all_studies.config import ConfigurationError, load_config
from etl_for_all_studies.correlation_job import run_correlation_job

LOGGER = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the standalone correlation refresh job"
    )
    parser.add_argument(
        "--config",
        dest="config_path",
        required=True,
        help="Path to the ETL configuration YAML file",
    )
    parser.add_argument(
        "--study",
        dest="studies",
        action="append",
        default=None,
        help="Optional GSE accession to limit processing (can be repeated)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        config = load_config(args.config_path)
    except ConfigurationError as exc:
        LOGGER.error("Configuration error: %s", exc)
        return 2

    run_correlation_job(config, study_accessions=args.studies)
    return 0


if __name__ == "__main__":
    sys.exit(main())
