"""
NBA database ingest entrypoint.

Usage
-----
    # Seed dimensions + box scores for recent seasons:
    uv run python ingest.py

    # Seed dimensions only (fast, no network-heavy calls):
    uv run python ingest.py --dims-only

    # Backfill all tables from the raw/ CSV directory:
    uv run python ingest.py --raw-backfill

    # Run analytics view query after ingest:
    uv run python ingest.py --analytics-view vw_player_season_totals --analytics-limit 25

    # Analytics-only mode with export:
    uv run python ingest.py --analytics-only --analytics-view vw_team_standings --analytics-output out.csv
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sqlite3
import time
from collections.abc import Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any

from src.db.analytics import get_duck_con
from src.db.schema import init_db
from src.etl.awards import load_all_awards
from src.etl.config import MetricsConfig
from src.etl.dimensions import run_all as run_dimensions
from src.etl.game_logs import load_multiple_seasons
from src.etl.metrics import export_metrics, log_metrics_summary
from src.etl.play_by_play import load_season_pbp
from src.etl.raw_backfill import RAW_DIR, run_raw_backfill
from src.etl.roster import load_rosters_for_seasons
from src.etl.salaries import load_salaries_for_seasons
from src.etl.utils import _validate_identifier as _validate_sql_identifier
from src.etl.utils import setup_logging
from src.etl.validate import run_consistency_checks

if TYPE_CHECKING:
    import duckdb

logger = logging.getLogger("ingest")

DEFAULT_SEASONS = ["2023-24", "2024-25"]
_SEASON_ID_PATTERN = re.compile(r"^\d{4}-\d{2}$")

# Pre-compiled regex for identifier validation
_VALID_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

DIMENSIONS_TABLES = ["dim_season", "dim_team", "dim_player"]
RAW_BACKFILL_TABLES = [
    "dim_team_history",
    "fact_game",
    "player_game_log",
    "team_game_log",
    "fact_team_season",
    "fact_player_season_stats",
    "fact_player_advanced_season",
    "fact_player_shooting_season",
    "fact_player_pbp_season",
]
AWARDS_TABLES = ["fact_player_award"]
SALARIES_TABLES = ["dim_salary_cap", "fact_salary"]
ROSTERS_TABLES = ["fact_roster"]
GAME_LOGS_TABLES = ["fact_game", "player_game_log", "team_game_log"]
PBP_TABLES = ["fact_play_by_play"]


StageFn = Callable[..., Any]


def _normalize_seasons(raw_seasons: Sequence[str]) -> list[str]:
    """Normalize seasons by trimming and de-duplicating while preserving order."""
    normalized: list[str] = []
    seen: set[str] = set()
    for season in raw_seasons:
        cleaned = season.strip()
        if not cleaned or cleaned in seen:
            continue
        normalized.append(cleaned)
        seen.add(cleaned)
    return normalized


class Stage(StrEnum):
    """ETL pipeline stages for checkpoint tracking."""

    DIMENSIONS = "post-dimensions"
    RAW_BACKFILL = "post-raw-backfill"
    AWARDS = "post-awards"
    SALARIES = "post-salaries"
    ROSTERS = "post-rosters"
    GAME_LOGS = "post-game-logs"
    PBP = "post-pbp"


@dataclass
class IngestConfig:
    """Configuration for the ingest pipeline."""

    seasons: list[str]
    dims_only: bool = False
    enrich_bio: bool = False
    awards: bool = False
    salaries: bool = False
    rosters: bool = False
    include_playoffs: bool = False
    pbp_limit: int = 0
    skip_reconciliation: bool = False
    reconciliation_warn_only: bool = False
    raw_backfill: bool = False
    raw_dir: Path | None = None
    raw_backfill_fail_fast: bool = False
    analytics_view: str | None = None
    analytics_limit: int = 20
    analytics_output: Path | None = None
    analytics_only: bool = False
    metrics_enabled: bool = False
    metrics_summary: bool = False
    metrics_export_endpoint: str | None = None
    runlog_tail: int = 12

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> IngestConfig:
        """Create config from parsed arguments."""
        metrics_enabled = args.metrics or MetricsConfig.enabled()
        metrics_export = args.metrics_export_endpoint or MetricsConfig.export_endpoint()
        seasons = _normalize_seasons(args.seasons)

        return cls(
            seasons=seasons,
            dims_only=args.dims_only,
            enrich_bio=args.enrich_bio,
            awards=args.awards,
            salaries=args.salaries,
            rosters=args.rosters,
            include_playoffs=args.include_playoffs,
            pbp_limit=args.pbp_limit,
            skip_reconciliation=args.skip_reconciliation,
            reconciliation_warn_only=args.reconciliation_warn_only,
            raw_backfill=args.raw_backfill,
            raw_dir=Path(args.raw_dir) if args.raw_dir else None,
            raw_backfill_fail_fast=args.raw_backfill_fail_fast,
            analytics_view=args.analytics_view,
            analytics_limit=args.analytics_limit,
            analytics_output=Path(args.analytics_output) if args.analytics_output else None,
            analytics_only=args.analytics_only,
            metrics_enabled=metrics_enabled,
            metrics_summary=args.metrics_summary,
            metrics_export_endpoint=metrics_export,
            runlog_tail=args.runlog_tail,
        )


@dataclass
class CheckpointState:
    """Mutable state for checkpoint tracking."""

    status_map: dict[str, int] = field(default_factory=dict)
    table_counts: dict[str, int | None] = field(default_factory=dict)
    last_timestamp: float | None = None

    def update(
        self,
        status_map: dict[str, int],
        table_counts: dict[str, int | None],
        timestamp: float,
    ) -> None:
        """Update checkpoint state atomically."""
        self.status_map = status_map
        self.table_counts = table_counts
        self.last_timestamp = timestamp


class IngestError(RuntimeError):
    """Base exception for ingest pipeline errors."""

    pass


class ReconciliationError(IngestError):
    """Raised when reconciliation checks find discrepancies."""

    def __init__(self, warning_count: int) -> None:
        self.warning_count = warning_count
        super().__init__(
            f"Reconciliation checks found {warning_count} discrepancy warning(s). "
            "Re-run with --reconciliation-warn-only to continue despite mismatches."
        )


class AnalyticsError(IngestError):
    """Raised when analytics view operations fail."""

    pass


class ValidationError(IngestError):
    """Raised when ingest CLI arguments are invalid."""

    pass


def validate_view_name(name: str) -> str:
    """Validate and return a safe analytics view name.

    Args:
        name: The view name to validate.

    Returns:
        The validated view name.

    Raises:
        AnalyticsError: If the view name is invalid.
    """
    if not _VALID_IDENTIFIER.fullmatch(name):
        raise AnalyticsError(f"Invalid analytics view name: {name!r}")
    _validate_sql_identifier(name)
    return name


def _validate_log_level(level: str) -> str:
    """Validate and normalize log level string."""
    candidate = level.upper()
    if candidate not in logging.getLevelNamesMapping():
        raise ValidationError(
            f"Invalid --log-level {level!r}. "
            "Expected one of DEBUG, INFO, WARNING, ERROR, CRITICAL."
        )
    return candidate


def _validate_analytics_output_path(path: Path) -> None:
    """Validate analytics output extension early for friendlier CLI errors."""
    suffix = path.suffix.lower()
    if suffix not in {".csv", ".parquet", ".json"}:
        raise ValidationError(
            f"Unsupported analytics output format: {path} "
            "(expected .csv, .parquet, or .json)"
        )


def _validate_seasons(seasons: Sequence[str]) -> list[str]:
    """Validate normalized season IDs and return a cleaned copy."""
    if not seasons:
        raise ValidationError("At least one season must be provided via --seasons")

    invalid = [s for s in seasons if not _SEASON_ID_PATTERN.fullmatch(s)]
    if invalid:
        raise ValidationError(
            "Invalid --seasons values "
            f"{invalid}. Expected format YYYY-YY (e.g. 2023-24)."
        )
    return list(seasons)


def _safe_table_count(con: sqlite3.Connection, table_name: str) -> int | None:
    """Safely get row count for a table.

    Args:
        con: SQLite connection.
        table_name: Table name to count.

    Returns:
        Row count or None if table doesn't exist or name is invalid.
    """
    if not _VALID_IDENTIFIER.fullmatch(table_name):
        return None
    try:
        _validate_sql_identifier(table_name)
        result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        return int(result[0]) if result else 0
    except sqlite3.OperationalError:
        return None
    except ValueError:
        return None


def _get_runlog_status_map(con: sqlite3.Connection) -> dict[str, int]:
    """Get status counts from etl_run_log.

    Args:
        con: SQLite connection.

    Returns:
        Dictionary mapping status to count.
    """
    try:
        rows = con.execute("SELECT status, COUNT(*) FROM etl_run_log GROUP BY status").fetchall()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc).lower():
            return {}
        raise
    return {status: int(count) for status, count in rows}


def _compute_delta(previous: dict[str, int], current: dict[str, int]) -> dict[str, int]:
    """Compute the delta between two status maps.

    Args:
        previous: Previous status map.
        current: Current status map.

    Returns:
        Dictionary of changed keys with their delta values.
    """
    if previous is current:
        return {}

    all_keys = set(previous) | set(current)
    return {
        key: current.get(key, 0) - previous.get(key, 0)
        for key in sorted(all_keys)
        if current.get(key, 0) != previous.get(key, 0)
    }


def _log_runlog_tail(con: sqlite3.Connection, checkpoint: str, limit: int) -> None:
    """Log the most recent entries from etl_run_log.

    Args:
        con: SQLite connection.
        checkpoint: Checkpoint name for logging context.
        limit: Maximum number of rows to log.
    """
    try:
        rows = con.execute(
            """
            SELECT
                id, table_name, COALESCE(season_id, '-'), loader, status,
                COALESCE(row_count, -1), started_at, COALESCE(finished_at, '-')
            FROM etl_run_log
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc).lower():
            logger.debug("Skipping etl_run_log tail at checkpoint=%s: etl_run_log missing", checkpoint)
            return
        raise

    logger.info("Checkpoint %s: etl_run_log tail (latest %d rows)", checkpoint, limit)
    for row in rows:
        row_count_display = row[5] if row[5] >= 0 else None
        logger.info(
            "Checkpoint %s: runlog id=%s table=%s season=%s loader=%s status=%s "
            "row_count=%s started=%s finished=%s",
            checkpoint,
            row[0],
            row[1],
            row[2],
            row[3],
            row[4],
            row_count_display,
            row[6],
            row[7],
        )


def _log_checkpoint(
    con: sqlite3.Connection,
    stage: Stage,
    tables: list[str],
    state: CheckpointState,
    runlog_tail: int,
) -> None:
    """Log checkpoint status including runlog counts and table row counts.

    Args:
        con: SQLite connection.
        stage: Pipeline stage identifier.
        tables: Tables to include in row count.
        state: Mutable checkpoint state to update.
        runlog_tail: Number of runlog rows to display.
    """
    status_map = _get_runlog_status_map(con)
    status_delta = _compute_delta(state.status_map, status_map)
    now = time.perf_counter()
    elapsed = (now - state.last_timestamp) if state.last_timestamp is not None else None

    logger.info(
        "Checkpoint %s: etl_run_log status counts=%s delta=%s elapsed_since_previous=%s",
        stage.value,
        status_map,
        status_delta or {},
        f"{elapsed:.2f}s" if elapsed is not None else "n/a",
    )

    new_table_counts: dict[str, int | None] = dict(state.table_counts)
    for table in tables:
        row_count = _safe_table_count(con, table)
        previous_count = state.table_counts.get(table)
        delta = (
            row_count - previous_count
            if row_count is not None and previous_count is not None
            else None
        )
        logger.info(
            "Checkpoint %s: table=%s row_count=%s delta=%s previous=%s",
            stage.value,
            table,
            row_count if row_count is not None else "n/a",
            delta if delta is not None else "n/a",
            previous_count if previous_count is not None else "n/a",
        )
        new_table_counts[table] = row_count

    _log_runlog_tail(con, stage.value, limit=runlog_tail)
    state.update(status_map, new_table_counts, now)


def _run_analytics_view(
    view_name: str,
    limit: int,
    output_path: Path | None,
) -> None:
    """Execute an analytics view and output results.

    Args:
        view_name: Name of the DuckDB view to query.
        limit: Maximum rows to return.
        output_path: Optional path to export results.

    Raises:
        AnalyticsError: If the view name is invalid or export format unsupported.
    """
    if limit <= 0:
        raise AnalyticsError(f"analytics limit must be > 0, got {limit}")

    safe_view = validate_view_name(view_name)
    duck = get_duck_con(force_refresh=True)

    try:
        df = duck.execute(f"SELECT * FROM {safe_view} LIMIT {limit}").df()
    except Exception as exc:
        raise AnalyticsError(
            f"Failed analytics query for view={safe_view!r} limit={limit}: {exc}"
        ) from exc
    finally:
        _cleanup_duck_connection(duck)

    if output_path:
        _export_dataframe(df, output_path, safe_view, limit)
        return

    logger.info("Analytics view %s returned %d rows (limit=%d)", safe_view, len(df), limit)
    if not df.empty:
        print(df.to_string(index=False))


def _cleanup_duck_connection(duck: duckdb.DuckDBPyConnection) -> None:
    """Clean up DuckDB connection and cached state.

    Args:
        duck: The DuckDB connection to close.
    """
    with suppress(Exception):
        duck.close()

    # Clear module-level cache
    from src.db import analytics as analytics_mod

    if hasattr(analytics_mod, "_local"):
        analytics_mod._local.cached_con = None
        analytics_mod._local.cached_sqlite_path = None
        analytics_mod._local.cached_duck_db_path = None


def _export_dataframe(
    df,
    output_path: Path,
    view_name: str,
    limit: int,
) -> None:
    """Export DataFrame to file based on extension.

    Args:
        df: DataFrame to export.
        output_path: Target file path.
        view_name: View name for logging.
        limit: Query limit for logging.

    Raises:
        AnalyticsError: If the export format is unsupported.
    """
    output_path = output_path.expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    suffix = output_path.suffix.lower()

    exporters = {
        ".csv": lambda: df.to_csv(output_path, index=False),
        ".parquet": lambda: df.to_parquet(output_path, index=False),
        ".json": lambda: df.to_json(output_path, orient="records"),
    }

    if suffix not in exporters:
        raise AnalyticsError(
            f"Unsupported analytics output format: {output_path} "
            "(expected .csv, .parquet, or .json)"
        )

    try:
        exporters[suffix]()
    except Exception as exc:
        raise AnalyticsError(
            f"Failed exporting analytics view {view_name} to {output_path}: {exc}"
        ) from exc

    logger.info("Analytics view %s exported (%d rows) to %s", view_name, len(df), output_path)


def _finalize_metrics(
    metrics_enabled: bool,
    show_summary: bool,
    export_endpoint: str | None,
) -> None:
    """Finalize and optionally export metrics.

    Args:
        metrics_enabled: Whether metrics collection is enabled.
        show_summary: Whether to log a metrics summary.
        export_endpoint: Optional HTTP endpoint for metrics export.
    """
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


def _log_config_summary(config: IngestConfig, log_file: Path | None) -> None:
    """Log configuration summary at start of ingest.

    Args:
        config: Ingest configuration.
        log_file: Optional log file path.
    """
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


def _set_metrics_env(enabled: bool) -> None:
    """Set process-level metrics env flag only when explicitly requested."""
    if enabled:
        os.environ["LARRYOB_METRICS_ENABLED"] = "true"


def _build_stage_plan(config: IngestConfig) -> list[tuple[Stage, list[str], StageFn, tuple[Any, ...], dict[str, Any]]]:
    """Build the linear stage plan for the ingest run.

    Returns a list of stage tuples: (stage, tables, function, args, kwargs).
    """
    plan: list[tuple[Stage, list[str], StageFn, tuple[Any, ...], dict[str, Any]]] = [
        (Stage.DIMENSIONS, DIMENSIONS_TABLES, _run_dimensions_stage, (config,), {}),
    ]

    if config.raw_backfill and not config.dims_only:
        plan.append((Stage.RAW_BACKFILL, RAW_BACKFILL_TABLES, _run_raw_backfill_stage, (config,), {}))
    if config.awards:
        plan.append((Stage.AWARDS, AWARDS_TABLES, load_all_awards, (), {"active_only": True}))
    if config.salaries:
        plan.append((Stage.SALARIES, SALARIES_TABLES, load_salaries_for_seasons, (config.seasons,), {}))
    if config.rosters:
        plan.append((Stage.ROSTERS, ROSTERS_TABLES, load_rosters_for_seasons, (config.seasons,), {}))
    if not config.dims_only:
        plan.append((Stage.GAME_LOGS, GAME_LOGS_TABLES, _run_game_logs_stage, (config,), {}))

    return plan


def _execute_raw_backfill_stage(
    con: sqlite3.Connection,
    state: CheckpointState,
    config: IngestConfig,
) -> None:
    """Execute raw backfill with summary-aware logging and fail-fast semantics."""
    logger.info("Starting stage: %s", Stage.RAW_BACKFILL.value)
    stage_start = time.perf_counter()
    summary = _run_raw_backfill_stage(con, config)
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

    _log_checkpoint(con, Stage.RAW_BACKFILL, RAW_BACKFILL_TABLES, state, config.runlog_tail)


def _execute_optional_post_gamelogs_steps(
    con: sqlite3.Connection,
    state: CheckpointState,
    config: IngestConfig,
) -> None:
    """Run reconciliation and optional PBP after game logs."""
    if config.skip_reconciliation:
        logger.info("--skip-reconciliation set; skipping consistency checks.")
    else:
        _run_reconciliation(con, config)

    if config.pbp_limit <= 0:
        return

    logger.info("Starting stage: %s", Stage.PBP.value)
    stage_start = time.perf_counter()
    _run_pbp_stage(con, config)
    logger.info(
        "Stage %s elapsed=%.2fs",
        Stage.PBP.value.replace("post-", ""),
        time.perf_counter() - stage_start,
    )
    _log_checkpoint(con, Stage.PBP, PBP_TABLES, state, config.runlog_tail)


def _execute_stage(
    con: sqlite3.Connection,
    stage: Stage,
    tables: list[str],
    state: CheckpointState,
    config: IngestConfig,
    stage_fn: StageFn,
    *args,
    **kwargs,
) -> None:
    """Execute a pipeline stage with timing and checkpoint logging.

    Args:
        con: SQLite connection.
        stage: Stage identifier.
        tables: Tables to track for this stage.
        state: Checkpoint state to update.
        config: Ingest configuration.
        stage_fn: Function to execute for this stage.
        *args: Positional arguments for stage_fn.
        **kwargs: Keyword arguments for stage_fn.
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
    _log_checkpoint(con, stage, tables, state, config.runlog_tail)


def _run_dimensions_stage(con: sqlite3.Connection, config: IngestConfig) -> None:
    """Execute the dimensions loading stage."""
    run_dimensions(con, full_players=not config.dims_only, enrich_bio=config.enrich_bio)


def _run_raw_backfill_stage(con: sqlite3.Connection, config: IngestConfig) -> dict:
    """Execute the raw backfill stage.

    Returns:
        Summary dict with 'ok', 'skipped', and 'failed' lists.
    """
    raw_dir = config.raw_dir or RAW_DIR
    logger.info("Running raw/ backfill from %s...", raw_dir)
    return run_raw_backfill(con, raw_dir, fail_fast=config.raw_backfill_fail_fast)


def _run_game_logs_stage(con: sqlite3.Connection, config: IngestConfig) -> None:
    """Execute the game logs loading stage."""
    season_types = ["Regular Season", "Playoffs"] if config.include_playoffs else ["Regular Season"]
    logger.info("Loading box scores for seasons: %s", config.seasons)
    load_multiple_seasons(con, config.seasons, season_types=season_types)


def _run_reconciliation(con: sqlite3.Connection, config: IngestConfig) -> None:
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


def _run_pbp_stage(con: sqlite3.Connection, config: IngestConfig) -> None:
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


def create_argument_parser() -> argparse.ArgumentParser:
    """Create and configure the argument parser.

    Returns:
        Configured ArgumentParser instance.
    """
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


def main() -> None:
    """Main entry point for the ingest pipeline."""
    from dotenv import load_dotenv

    load_dotenv()

    parser = create_argument_parser()
    args = parser.parse_args()

    validate_arguments(parser, args)

    # Enable metrics env flag only after argument validation succeeds
    _set_metrics_env(args.metrics)

    # Setup logging
    log_file = Path(args.log_file) if args.log_file else None
    setup_logging(level=_validate_log_level(args.log_level), log_file=log_file)

    config = IngestConfig.from_args(args)
    _log_config_summary(config, log_file)

    try:
        # Handle analytics-only mode
        if config.analytics_only:
            if config.analytics_view is None:
                parser.error("--analytics-only requires --analytics-view")
            _run_analytics_view(
                view_name=config.analytics_view,
                limit=config.analytics_limit,
                output_path=config.analytics_output,
            )
            return

        # Initialize and run pipeline
        logger.info("Initialising database schema...")
        con = init_db()

        try:
            run_ingest_pipeline(con, config)
        finally:
            con.close()

        logger.info("Ingest complete.")

        # Run analytics view if requested
        if config.analytics_view:
            _run_analytics_view(
                view_name=config.analytics_view,
                limit=config.analytics_limit,
                output_path=config.analytics_output,
            )
    finally:
        _finalize_metrics(
            config.metrics_enabled,
            config.metrics_summary,
            config.metrics_export_endpoint,
        )


if __name__ == "__main__":
    main()
