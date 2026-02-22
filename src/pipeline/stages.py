"""
Individual pipeline stage runners.

Each function in this module wraps one ETL loader call with the exact
arguments derived from IngestConfig. They are thin adapters — all real
ETL logic lives in src/etl/.
"""

from __future__ import annotations

import logging
import sqlite3
import time

from src.etl.dimensions import run_all as run_dimensions
from src.etl.game_logs import load_multiple_seasons
from src.etl.play_by_play import load_season_pbp
from src.etl.raw_backfill import RAW_DIR, run_raw_backfill
from src.etl.validate import run_consistency_checks
from src.pipeline.exceptions import ReconciliationError
from src.pipeline.models import IngestConfig

logger = logging.getLogger(__name__)


def run_dimensions_stage(con: sqlite3.Connection, config: IngestConfig) -> None:
    """Execute the dimensions loading stage."""
    run_dimensions(con, full_players=not config.dims_only, enrich_bio=config.enrich_bio)


def run_raw_backfill_stage(con: sqlite3.Connection, config: IngestConfig) -> dict:
    """Execute the raw backfill stage.

    Returns:
        Summary dict with 'ok', 'skipped', and 'failed' lists.
    """
    raw_dir = config.raw_dir or RAW_DIR
    logger.info("Running raw/ backfill from %s...", raw_dir)
    return run_raw_backfill(con, raw_dir, fail_fast=config.raw_backfill_fail_fast)


def run_game_logs_stage(con: sqlite3.Connection, config: IngestConfig) -> None:
    """Execute the game logs loading stage."""
    season_types = ["Regular Season", "Playoffs"] if config.include_playoffs else ["Regular Season"]
    logger.info("Loading box scores for seasons: %s", config.seasons)
    load_multiple_seasons(con, config.seasons, season_types=season_types)


def run_reconciliation(con: sqlite3.Connection, config: IngestConfig) -> None:
    """Run reconciliation checks for all seasons.

    Raises:
        ReconciliationError: If discrepancies found and not in warn-only mode.
    """
    total_warnings = 0
    for idx, season in enumerate(config.seasons, start=1):
        logger.info("Running reconciliation checks for season %s...", season)
        stage_start = time.perf_counter()
        season_warnings = run_consistency_checks(con, season)
        total_warnings += season_warnings
        logger.info(
            "Reconciliation season=%s (%d/%d) warnings=%d elapsed=%.2fs running_total=%d",
            season,
            idx,
            len(config.seasons),
            season_warnings,
            time.perf_counter() - stage_start,
            total_warnings,
        )

    if total_warnings > 0:
        if config.reconciliation_warn_only:
            logger.warning(
                "Reconciliation checks found %d discrepancy warning(s).",
                total_warnings,
            )
        else:
            raise ReconciliationError(total_warnings)


def run_pbp_stage(con: sqlite3.Connection, config: IngestConfig) -> None:
    """Execute the play-by-play loading stage."""
    for idx, season in enumerate(config.seasons, start=1):
        logger.info(
            "Loading PBP for up to %d games in %s (%d/%d)...",
            config.pbp_limit,
            season,
            idx,
            len(config.seasons),
        )
        load_season_pbp(con, season, limit=config.pbp_limit)
