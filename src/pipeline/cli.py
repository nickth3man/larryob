"""
CLI entry point for the ingest pipeline.

Defines the argument parser, validates argument combinations, and wires
everything together into the `main()` function.

Usage
-----
    # Recommended (registered script):
    uv run ingest

    # Module invocation:
    uv run python -m src.pipeline

    # Direct file invocation:
    uv run python src/pipeline/cli.py
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from src.db.schema import init_db
from src.etl.utils import setup_logging
from src.pipeline.analytics import run_analytics_view
from src.pipeline.constants import DEFAULT_SEASONS
from src.pipeline.exceptions import AnalyticsError, ValidationError
from src.pipeline.executor import (
    _log_config_summary,
    finalize_metrics,
    run_ingest_pipeline,
    set_metrics_env,
)
from src.pipeline.models import IngestConfig
from src.pipeline.validation import (
    _normalize_seasons,
    _validate_analytics_output_path,
    _validate_log_level,
    _validate_seasons,
    validate_view_name,
)

logger = logging.getLogger(__name__)


def create_argument_parser() -> argparse.ArgumentParser:
    """Create and configure the argument parser."""
    parser = argparse.ArgumentParser(
        description="NBA database ingest pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Season configuration
    parser.add_argument(
        "--seasons",
        nargs="+",
        default=DEFAULT_SEASONS,
        help="Season strings to ingest, e.g. 2023-24 2024-25",
    )

    # Dimension options
    parser.add_argument(
        "--dims-only",
        action="store_true",
        help="Only seed dimension tables (fast, no box-score calls)",
    )
    parser.add_argument(
        "--enrich-bio",
        action="store_true",
        help="Enrich dim_player with bio data via CommonPlayerInfo (many API calls)",
    )

    # Feature flags
    parser.add_argument(
        "--awards",
        action="store_true",
        help="Load fact_player_award from PlayerAwards endpoint",
    )
    parser.add_argument(
        "--salaries",
        action="store_true",
        help="Load dim_salary_cap and scrape fact_salary from Basketball-Reference",
    )
    parser.add_argument(
        "--rosters",
        action="store_true",
        help="Load fact_roster from CommonTeamRoster",
    )

    # Game log options
    parser.add_argument(
        "--include-playoffs",
        action="store_true",
        help="Also ingest Playoffs game logs",
    )
    parser.add_argument(
        "--pbp-limit",
        type=int,
        default=0,
        help="Number of games to load PBP for (0 = skip PBP)",
    )

    # Reconciliation options
    parser.add_argument(
        "--skip-reconciliation",
        action="store_true",
        help="Skip post-boxscore reconciliation checks",
    )
    parser.add_argument(
        "--reconciliation-warn-only",
        action="store_true",
        help="Do not fail ingest when reconciliation checks find discrepancies",
    )

    # Raw backfill options
    parser.add_argument(
        "--raw-backfill",
        action="store_true",
        help="Seed all tables from the raw/ CSV directory",
    )
    parser.add_argument(
        "--raw-dir",
        type=str,
        default=None,
        help="Path to the raw/ directory (default: <repo_root>/raw)",
    )
    parser.add_argument(
        "--raw-backfill-fail-fast",
        action="store_true",
        help="Stop raw backfill immediately when a loader fails",
    )

    # Analytics options
    parser.add_argument(
        "--analytics-view",
        type=str,
        default=None,
        help="DuckDB analytics view name to query (e.g. vw_player_season_totals)",
    )
    parser.add_argument(
        "--analytics-limit",
        type=int,
        default=20,
        help="Row limit for --analytics-view queries",
    )
    parser.add_argument(
        "--analytics-output",
        type=str,
        default=None,
        help="Optional analytics export path (.csv, .parquet, .json)",
    )
    parser.add_argument(
        "--analytics-only",
        action="store_true",
        help="Skip ingest and only run the --analytics-view query",
    )

    # Metrics options
    parser.add_argument(
        "--metrics",
        action="store_true",
        help="Enable in-memory metrics collection for this run",
    )
    parser.add_argument(
        "--metrics-summary",
        action="store_true",
        help="Log metrics summary at end of run",
    )
    parser.add_argument(
        "--metrics-export-endpoint",
        type=str,
        default=None,
        help="Optional HTTP endpoint URL for metrics JSON export",
    )

    # Logging options
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR)",
    )
    parser.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="Optional path to log file",
    )
    parser.add_argument(
        "--runlog-tail",
        type=int,
        default=12,
        help="How many latest etl_run_log rows to show at each checkpoint",
    )

    return parser


def validate_arguments(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    """Validate argument combinations.

    Args:
        parser: Argument parser for error reporting.
        args: Parsed arguments.

    Raises:
        SystemExit: If validation fails.
    """
    if args.analytics_only and not args.analytics_view:
        parser.error("--analytics-only requires --analytics-view")
    if args.analytics_view and args.analytics_limit <= 0:
        parser.error("--analytics-limit must be greater than 0")
    if args.pbp_limit < 0:
        parser.error("--pbp-limit must be greater than or equal to 0")
    if args.runlog_tail <= 0:
        parser.error("--runlog-tail must be greater than 0")
    if args.raw_backfill and args.raw_dir:
        raw_dir = Path(args.raw_dir)
        if not raw_dir.exists():
            parser.error(f"--raw-dir does not exist: {raw_dir}")
        if not raw_dir.is_dir():
            parser.error(f"--raw-dir must be a directory: {raw_dir}")

    try:
        _validate_log_level(args.log_level)
        seasons = _normalize_seasons(args.seasons)
        _validate_seasons(seasons)
        if args.analytics_view:
            validate_view_name(args.analytics_view)
        if args.analytics_output:
            _validate_analytics_output_path(Path(args.analytics_output))
    except ValidationError as exc:
        parser.error(str(exc))
    except AnalyticsError as exc:
        parser.error(str(exc))


def main() -> None:
    """Main entry point for the ingest pipeline."""
    from dotenv import load_dotenv

    load_dotenv()

    parser = create_argument_parser()
    args = parser.parse_args()

    validate_arguments(parser, args)

    set_metrics_env(args.metrics)

    log_file = Path(args.log_file) if args.log_file else None
    setup_logging(level=_validate_log_level(args.log_level), log_file=log_file)

    config = IngestConfig.from_args(args)
    _log_config_summary(config, log_file)

    try:
        if config.analytics_only:
            if config.analytics_view is None:
                parser.error("--analytics-only requires --analytics-view")
            run_analytics_view(
                view_name=config.analytics_view,
                limit=config.analytics_limit,
                output_path=config.analytics_output,
            )
            return

        logger.info("Initialising database schema...")
        con = init_db()

        try:
            run_ingest_pipeline(con, config)
        finally:
            con.close()

        logger.info("Ingest complete.")

        if config.analytics_view:
            run_analytics_view(
                view_name=config.analytics_view,
                limit=config.analytics_limit,
                output_path=config.analytics_output,
            )
    finally:
        finalize_metrics(
            config.metrics_enabled,
            config.metrics_summary,
            config.metrics_export_endpoint,
        )


if __name__ == "__main__":
    main()
