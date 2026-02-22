"""
Pipeline orchestration: stage plan construction, stage execution, and the top-level
`run_ingest_pipeline` entry point.

This module wires together the stage runners from `stages.py`, the checkpoint
logger from `checkpoint.py`, and the optional ETL metrics from `src.etl.metrics`.

Design Decisions
----------------
- Stage plan is built dynamically based on IngestConfig flags
- Each stage is a tuple of (Stage, tables, function, args, kwargs) for uniformity
- Raw backfill has special handling for its summary return value
- Metrics finalization runs in a finally block to ensure cleanup

Usage
-----
    config = IngestConfig.from_args(args)
    con = init_db()
    run_ingest_pipeline(con, config)
"""

from __future__ import annotations

import logging
import os
import sqlite3
import time
from collections.abc import Sequence
from pathlib import Path

from src.etl.awards import load_all_awards
from src.etl.metrics import export_metrics, log_metrics_summary
from src.etl.roster import load_rosters_for_seasons
from src.etl.salaries import load_salaries_for_seasons
from src.pipeline.checkpoint import log_checkpoint
from src.pipeline.constants import (
    AWARDS_TABLES,
    DIMENSIONS_TABLES,
    GAME_LOGS_TABLES,
    PBP_TABLES,
    RAW_BACKFILL_TABLES,
    ROSTERS_TABLES,
    SALARIES_TABLES,
    StageFn,
)
from src.pipeline.exceptions import IngestError
from src.pipeline.models import CheckpointState, IngestConfig, Stage
from src.pipeline.stages import (
    run_dimensions_stage,
    run_game_logs_stage,
    run_pbp_stage,
    run_raw_backfill_stage,
    run_reconciliation,
)

logger = logging.getLogger(__name__)

#: Type alias for a stage plan tuple: (stage, tables, function, args, kwargs)
StagePlan = tuple[Stage, Sequence[str], StageFn, tuple, dict]


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
    """Set process-level metrics env flag only when explicitly requested."""
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


def _build_stage_plan(
    config: IngestConfig,
) -> list[StagePlan]:
    """Build the linear stage plan for the ingest run.

    Returns a list of stage tuples: (stage, tables, function, args, kwargs).

    The stage plan is built dynamically based on config flags:
    - DIMENSIONS: Always runs first
    - RAW_BACKFILL: Only if --raw-backfill and not --dims-only
    - AWARDS/SALARIES/ROSTERS: Optional feature flags
    - GAME_LOGS: Always unless --dims-only
    """
    plan: list[StagePlan] = [
        (Stage.DIMENSIONS, DIMENSIONS_TABLES, run_dimensions_stage, (config,), {}),
    ]

    if config.raw_backfill and not config.dims_only:
        plan.append(
            (Stage.RAW_BACKFILL, RAW_BACKFILL_TABLES, run_raw_backfill_stage, (config,), {})
        )
    if config.awards:
        plan.append((Stage.AWARDS, AWARDS_TABLES, load_all_awards, (), {"active_only": True}))
    if config.salaries:
        plan.append(
            (
                Stage.SALARIES,
                SALARIES_TABLES,
                load_salaries_for_seasons,
                (list(config.seasons),),
                {},
            )
        )
    if config.rosters:
        plan.append(
            (Stage.ROSTERS, ROSTERS_TABLES, load_rosters_for_seasons, (list(config.seasons),), {})
        )
    if not config.dims_only:
        plan.append((Stage.GAME_LOGS, GAME_LOGS_TABLES, run_game_logs_stage, (config,), {}))

    return plan


def _execute_raw_backfill_stage(
    con: sqlite3.Connection,
    state: CheckpointState,
    config: IngestConfig,
) -> None:
    """Execute raw backfill with summary-aware logging and fail-fast semantics."""
    logger.info("Starting stage: %s", Stage.RAW_BACKFILL.value)
    stage_start = time.perf_counter()
    summary = run_raw_backfill_stage(con, config)
    elapsed = time.perf_counter() - stage_start

    ok_count = len(summary.get("ok", []))
    skipped_count = len(summary.get("skipped", []))
    failed = summary.get("failed", [])
    failed_count = len(failed)

    logger.info(
        "Stage %s elapsed=%.2fs ok=%d skipped=%d failed=%d",
        Stage.RAW_BACKFILL.value.replace("post-", ""),
        elapsed,
        ok_count,
        skipped_count,
        failed_count,
    )

    if failed_count > 0:
        logger.warning("Raw backfill reported failed loaders: %s", failed)
        if config.raw_backfill_fail_fast:
            raise IngestError("Raw backfill failed in fail-fast mode.")

    log_checkpoint(con, Stage.RAW_BACKFILL, RAW_BACKFILL_TABLES, state, config.runlog_tail)


def _execute_optional_post_gamelogs_steps(
    con: sqlite3.Connection,
    state: CheckpointState,
    config: IngestConfig,
) -> None:
    """Run reconciliation and optional PBP after game logs."""
    if config.skip_reconciliation:
        logger.info("--skip-reconciliation set; skipping consistency checks.")
    else:
        run_reconciliation(con, config)

    if config.pbp_limit <= 0:
        return

    logger.info("Starting stage: %s", Stage.PBP.value)
    stage_start = time.perf_counter()
    run_pbp_stage(con, config)
    logger.info(
        "Stage %s elapsed=%.2fs",
        Stage.PBP.value.replace("post-", ""),
        time.perf_counter() - stage_start,
    )
    log_checkpoint(con, Stage.PBP, PBP_TABLES, state, config.runlog_tail)


def _execute_stage(
    con: sqlite3.Connection,
    stage: Stage,
    tables: Sequence[str],
    state: CheckpointState,
    config: IngestConfig,
    stage_fn: StageFn,
    *args,
    **kwargs,
) -> None:
    """Execute a pipeline stage with timing and checkpoint logging.

    Args:
        con: SQLite connection.
        stage: Stage identifier for logging.
        tables: Tables affected by this stage (for checkpoint row counts).
        state: Mutable checkpoint state.
        config: Ingest configuration.
        stage_fn: The stage execution function.
        *args: Positional arguments passed to stage_fn.
        **kwargs: Keyword arguments passed to stage_fn.

    Raises:
        Exception: Re-raises any exception from the stage function after logging.
    """
    logger.info("Starting stage: %s", stage.value)
    stage_start = time.perf_counter()
    try:
        stage_fn(con, *args, **kwargs)
    except Exception:
        logger.exception("Stage %s failed", stage.value.replace("post-", ""))
        raise
    elapsed = time.perf_counter() - stage_start
    logger.info("Stage %s elapsed=%.2fs", stage.value.replace("post-", ""), elapsed)
    log_checkpoint(con, stage, tables, state, config.runlog_tail)


def run_ingest_pipeline(con: sqlite3.Connection, config: IngestConfig) -> None:
    """Execute the main ingest pipeline.

    Args:
        con: SQLite connection.
        config: Ingest configuration.

    Raises:
        IngestError: If the pipeline fails.
    """
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
