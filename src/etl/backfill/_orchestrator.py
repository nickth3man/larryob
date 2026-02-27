"""
Orchestrator for raw data backfill operations.

This module coordinates the execution of all backfill loaders in
dependency order, providing logging, error handling, and run tracking.
"""

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.db.tracking import already_loaded, record_run
from src.etl.backfill._advanced_stats import (
    load_player_advanced,
    load_player_pbp_season,
    load_player_shooting,
)
from src.etl.backfill._all_nba import load_all_nba_teams, load_all_nba_votes
from src.etl.backfill._all_star import load_all_star_selections
from src.etl.backfill._awards import load_awards
from src.etl.backfill._dims import enrich_dim_player, enrich_dim_team, load_team_history
from src.etl.backfill._draft import load_draft
from src.etl.backfill._game_logs import load_player_game_logs, load_team_game_logs
from src.etl.backfill._games import load_games, load_schedule
from src.etl.backfill._pbp_bulk import load_bulk_pbp
from src.etl.backfill._player_career import enrich_player_career
from src.etl.backfill._registry import LOADERS, LoaderConfig
from src.etl.backfill._season_stats import (
    load_league_season,
    load_player_season_stats,
    load_team_season,
)

logger = logging.getLogger(__name__)

RAW_DIR = Path("raw")

# Re-export loader functions for test patching
# These allow tests to patch orchestrator_mod.load_team_history etc.
__all__ = [
    "run_raw_backfill",
    "load_team_history",
    "enrich_dim_team",
    "enrich_dim_player",
    "load_games",
    "load_schedule",
    "load_player_game_logs",
    "load_team_game_logs",
    "load_team_season",
    "load_league_season",
    "load_draft",
    "load_player_season_stats",
    "load_player_advanced",
    "load_player_shooting",
    "load_player_pbp_season",
    "enrich_player_career",
    "load_all_star_selections",
    "load_all_nba_teams",
    "load_all_nba_votes",
    "load_awards",
    "load_bulk_pbp",
    "_load_salary_history_adapter",
]


def _load_salary_history_adapter(con: sqlite3.Connection, raw_dir: Path) -> int:
    from src.etl.backfill._salary_history import load_salary_history

    return load_salary_history(con, raw_dir=raw_dir)


@dataclass
class LoaderResult:
    """Result of a single loader execution."""

    loader: str
    status: str  # "ok", "skipped", "error"
    table: str
    before_row_count: int | None
    after_row_count: int | None
    delta_row_count: int | None
    elapsed_sec: float
    error: str | None = None


@dataclass
class BackfillSummary:
    """Summary of a complete backfill run."""

    ok: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    details: list[LoaderResult] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "ok": self.ok,
            "skipped": self.skipped,
            "failed": self.failed,
            "details": [
                {
                    "loader": r.loader,
                    "status": r.status,
                    "table": r.table,
                    "before_row_count": r.before_row_count,
                    "after_row_count": r.after_row_count,
                    "delta_row_count": r.delta_row_count,
                    "elapsed_sec": round(r.elapsed_sec, 3),
                    "error": r.error,
                }
                for r in self.details
            ],
        }


# Backward-compatible alias used by existing tests and patch points.
_LOADERS = LOADERS


def _get_table_count(con: sqlite3.Connection, table_name: str) -> int | None:
    """
    Safely get row count from a table.

    Args:
        con: SQLite connection
        table_name: Table to count

    Returns:
        Row count or None if table doesn't exist
    """
    try:
        return con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    except sqlite3.OperationalError:
        return None


def _run_single_loader(
    con: sqlite3.Connection,
    config: LoaderConfig,
    raw_dir: Path,
    loader_idx: int,
    total_loaders: int,
) -> LoaderResult:
    """
    Execute a single loader with timing and error handling.

    Args:
        con: SQLite connection
        config: Loader configuration
        raw_dir: Directory containing raw CSV files
        loader_idx: Current loader index (1-based)
        total_loaders: Total number of loaders

    Returns:
        LoaderResult with execution details
    """
    loader_id = f"backfill.{config.name}"
    started_at = datetime.now(UTC).isoformat()
    started_perf = time.perf_counter()
    before_count = _get_table_count(con, config.table_name)

    logger.info(
        "Raw backfill [%d/%d] starting loader=%s target_table=%s before_row_count=%s",
        loader_idx,
        total_loaders,
        config.name,
        config.table_name,
        before_count if before_count is not None else "n/a",
    )

    # Check if already loaded (idempotency)
    if already_loaded(con, config.table_name, None, loader_id):
        elapsed = time.perf_counter() - started_perf
        logger.info(
            "Raw backfill [%d/%d] skipped loader=%s (already loaded) elapsed=%.2fs",
            loader_idx,
            total_loaders,
            config.name,
            elapsed,
        )
        return LoaderResult(
            loader=config.name,
            status="skipped",
            table=config.table_name,
            before_row_count=before_count,
            after_row_count=before_count,
            delta_row_count=0,
            elapsed_sec=elapsed,
        )

    try:
        # Execute the loader (look up by name to support test patching)
        loader_func = globals()[config.loader_name]
        loader_func(con, raw_dir)

        # Record success
        row_count = _get_table_count(con, config.table_name)
        elapsed = time.perf_counter() - started_perf
        delta = (
            row_count - before_count if row_count is not None and before_count is not None else None
        )

        record_run(con, config.table_name, None, loader_id, row_count, "ok", started_at)

        logger.info(
            "Raw backfill [%d/%d] completed loader=%s row_count=%s before=%s delta=%s elapsed=%.2fs",
            loader_idx,
            total_loaders,
            config.name,
            row_count if row_count is not None else "n/a",
            before_count if before_count is not None else "n/a",
            delta if delta is not None else "n/a",
            elapsed,
        )

        return LoaderResult(
            loader=config.name,
            status="ok",
            table=config.table_name,
            before_row_count=before_count,
            after_row_count=row_count,
            delta_row_count=delta,
            elapsed_sec=elapsed,
        )

    except Exception as exc:
        # Record failure
        elapsed = time.perf_counter() - started_perf
        record_run(con, config.table_name, None, loader_id, None, "error", started_at)

        logger.exception("Loader %s failed during raw backfill:", config.name)
        logger.error(
            "Raw backfill [%d/%d] failed loader=%s before=%s elapsed=%.2fs",
            loader_idx,
            total_loaders,
            config.name,
            before_count if before_count is not None else "n/a",
            elapsed,
        )

        return LoaderResult(
            loader=config.name,
            status="error",
            table=config.table_name,
            before_row_count=before_count,
            after_row_count=None,
            delta_row_count=None,
            elapsed_sec=elapsed,
            error=str(exc),
        )


def run_raw_backfill(
    con: sqlite3.Connection,
    raw_dir: Path = RAW_DIR,
    *,
    fail_fast: bool = False,
) -> dict[str, Any]:
    """
    Execute all raw-data loaders in dependency order.

    Args:
        con: SQLite database connection
        raw_dir: Directory containing raw CSV files
        fail_fast: If True, stop on first error

    Returns:
        Machine-readable run summary:
        {
            "ok": [...loader names...],
            "skipped": [...loader names...],
            "failed": [...loader names...],
            "details": [{"loader": ..., "status": ..., ...}]
        }
    """
    logger.info("=== Raw backfill starting (raw_dir=%s) ===", raw_dir)

    summary = BackfillSummary()
    total = len(_LOADERS)

    for idx, config in enumerate(_LOADERS, start=1):
        result = _run_single_loader(con, config, raw_dir, idx, total)

        # Update summary based on status
        if result.status == "ok":
            summary.ok.append(result.loader)
        elif result.status == "skipped":
            summary.skipped.append(result.loader)
        else:  # error
            summary.failed.append(result.loader)

        summary.details.append(result)

        # Stop on error if fail_fast
        if result.status == "error" and fail_fast:
            break

    # Log final summary
    logger.info(
        "=== Raw backfill complete: ok=%d skipped=%d failed=%d ===",
        len(summary.ok),
        len(summary.skipped),
        len(summary.failed),
    )

    for detail in summary.details:
        logger.info(
            "Raw backfill detail: loader=%s status=%s table=%s before=%s after=%s delta=%s elapsed_sec=%s error=%s",
            detail.loader,
            detail.status,
            detail.table,
            detail.before_row_count,
            detail.after_row_count,
            detail.delta_row_count,
            round(detail.elapsed_sec, 3),
            detail.error,
        )

    return summary.to_dict()
