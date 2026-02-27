"""CLI validation and execution."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from src.db.schema import init_db
from src.etl.logging import setup_logging
from src.pipeline.analytics import run_analytics_view
from src.pipeline.exceptions import AnalyticsError, IngestError, ValidationError
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

from .commands import (
    EXIT_INGEST_ERROR,
    EXIT_SUCCESS,
    EXIT_UNEXPECTED_ERROR,
    EXIT_VALIDATION_ERROR,
)

logger = logging.getLogger(__name__)


def validate_arguments(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    """Validate parsed CLI arguments."""
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

    if args.pbp_source == "bulk" and args.pbp_bulk_dir:
        pbp_bulk_dir = Path(args.pbp_bulk_dir)
        if not pbp_bulk_dir.exists():
            parser.error(f"--pbp-bulk-dir does not exist: {pbp_bulk_dir}")
    if args.salary_source == "open" and args.salary_open_file:
        salary_open_file = Path(args.salary_open_file)
        if not salary_open_file.exists():
            parser.error(f"--salary-open-file does not exist: {salary_open_file}")
        if not salary_open_file.is_file():
            parser.error(f"--salary-open-file must be a file, not a directory: {salary_open_file}")

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


def run_from_parsed_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> int:
    """Execute CLI command from a parsed namespace."""
    try:
        validate_arguments(parser, args)
    except SystemExit:
        return EXIT_VALIDATION_ERROR

    set_metrics_env(args.metrics)

    log_file = Path(args.log_file) if args.log_file else None
    setup_logging(level=_validate_log_level(args.log_level), log_file=log_file)

    try:
        config = IngestConfig.from_args(args)
    except ValueError as exc:
        logger.error("Configuration error: %s", exc)
        return EXIT_VALIDATION_ERROR

    _log_config_summary(config, log_file)

    try:
        if config.analytics_only:
            if config.analytics_view is None:
                parser.error("--analytics-only requires --analytics-view")
                return EXIT_VALIDATION_ERROR
            run_analytics_view(
                view_name=config.analytics_view,
                limit=config.analytics_limit,
                output_path=config.analytics_output,
            )
            return EXIT_SUCCESS

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

        return EXIT_SUCCESS

    except IngestError as exc:
        logger.error("Ingest failed: %s", exc)
        if exc.context:
            logger.debug("Error context: %s", exc.context)
        return EXIT_INGEST_ERROR
    except Exception as exc:  # noqa: BLE001
        logger.exception("Unexpected error during ingest: %s", exc)
        return EXIT_UNEXPECTED_ERROR
    finally:
        finalize_metrics(
            config.metrics_enabled,
            config.metrics_summary,
            config.metrics_export_endpoint,
        )
