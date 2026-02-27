"""Pipeline execution step definitions and helpers."""

from __future__ import annotations

import logging
import sqlite3
import time
from collections.abc import Sequence

from src.etl.awards import load_all_awards
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

StagePlan = tuple[Stage, Sequence[str], StageFn, tuple, dict]


def _build_stage_plan(config: IngestConfig) -> list[StagePlan]:
    """Build the linear stage plan for the ingest run."""
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
    """Execute a single pipeline stage with timing/checkpoint logging."""
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
