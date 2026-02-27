"""Top-level ingest pipeline orchestration."""

from __future__ import annotations

import logging
import os
import sqlite3
import time
from pathlib import Path

from src.etl.metrics import export_metrics, log_metrics_summary
from src.pipeline.models import CheckpointState, IngestConfig, Stage

from .steps import (
    _build_stage_plan,
    _execute_optional_post_gamelogs_steps,
    _execute_raw_backfill_stage,
    _execute_stage,
)

logger = logging.getLogger(__name__)


def _log_config_summary(config: IngestConfig, log_file: Path | None) -> None:
    """Log configuration summary at start of ingest."""
    logger.info(
        "Ingest arguments: seasons=%s dims_only=%s enrich_bio=%s awards=%s salaries=%s "
        "rosters=%s include_playoffs=%s pbp_limit=%s skip_reconciliation=%s "
        "reconciliation_warn_only=%s raw_backfill=%s raw_dir=%s raw_backfill_fail_fast=%s "
        "analytics_view=%s analytics_limit=%s analytics_output=%s analytics_only=%s "
        "metrics=%s metrics_summary=%s metrics_export_endpoint=%s log_file=%s runlog_tail=%s",
        config.seasons,
        config.dims_only,
        config.enrich_bio,
        config.awards,
        config.salaries,
        config.rosters,
        config.include_playoffs,
        config.pbp_limit,
        config.skip_reconciliation,
        config.reconciliation_warn_only,
        config.raw_backfill,
        str(config.raw_dir) if config.raw_dir else None,
        config.raw_backfill_fail_fast,
        config.analytics_view,
        config.analytics_limit,
        config.analytics_output,
        config.analytics_only,
        config.metrics_enabled,
        config.metrics_summary,
        config.metrics_export_endpoint,
        str(log_file) if log_file else None,
        config.runlog_tail,
    )


def set_metrics_env(enabled: bool) -> None:
    """Set process-level metrics flag only when explicitly requested."""
    if enabled:
        os.environ["LARRYOB_METRICS_ENABLED"] = "true"


def finalize_metrics(
    metrics_enabled: bool,
    show_summary: bool,
    export_endpoint: str | None,
) -> None:
    """Finalize and optionally export metrics."""
    if not metrics_enabled:
        return
    if show_summary:
        try:
            log_metrics_summary()
        except Exception:
            logger.exception("Metrics summary logging failed")
            raise
    if export_endpoint:
        try:
            export_metrics(export_endpoint)
        except Exception:
            logger.exception("Metrics export failed endpoint=%s", export_endpoint)
            raise


def run_ingest_pipeline(con: sqlite3.Connection, config: IngestConfig) -> None:
    """Execute the main ingest pipeline."""
    state = CheckpointState()
    ingest_start = time.perf_counter()

    logger.info("Loading dimension tables...")
    stage_plan = _build_stage_plan(config)
    for stage, tables, stage_fn, args, kwargs in stage_plan:
        if stage is Stage.RAW_BACKFILL:
            _execute_raw_backfill_stage(con, state, config)
            continue
        _execute_stage(con, stage, tables, state, config, stage_fn, *args, **kwargs)

    if config.dims_only:
        logger.info("--dims-only set; skipping box scores, reconciliation, and PBP.")
    else:
        _execute_optional_post_gamelogs_steps(con, state, config)

    logger.info("Ingest total elapsed=%.2fs", time.perf_counter() - ingest_start)
