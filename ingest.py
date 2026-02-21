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

import argparse
import logging
import os
import re
import sqlite3
import time
from pathlib import Path

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
from src.etl.utils import setup_logging
from src.etl.validate import run_consistency_checks

logger = logging.getLogger("ingest")

DEFAULT_SEASONS = ["2023-24", "2024-25"]

_VALID_IDENTIFIER = re.compile(r"[a-zA-Z_][a-zA-Z0-9_]*")


def _validate_view_name(name: str) -> str:
    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", name):
        raise ValueError(f"Invalid analytics view name: {name!r}")
    return name


def _run_analytics_view(view_name: str, limit: int, output_path: Path | None) -> None:
    safe_view = _validate_view_name(view_name)
    duck = get_duck_con(force_refresh=True)
    try:
        df = duck.execute(f"SELECT * FROM {safe_view} LIMIT {int(limit)}").df()
    finally:
        # Close and clear cached connection to avoid lingering handles in CLI runs.
        try:
            duck.close()
        except Exception:
            pass
        from src.db import analytics as analytics_mod
        analytics_mod._local.cached_con = None
        analytics_mod._local.cached_sqlite_path = None
        analytics_mod._local.cached_duck_db_path = None

    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        suffix = output_path.suffix.lower()
        if suffix == ".csv":
            df.to_csv(output_path, index=False)
        elif suffix == ".parquet":
            df.to_parquet(output_path, index=False)
        elif suffix == ".json":
            df.to_json(output_path, orient="records")
        else:
            raise ValueError(
                f"Unsupported analytics output format: {output_path} "
                "(expected .csv, .parquet, or .json)"
            )
        logger.info("Analytics view %s exported (%d rows) to %s", safe_view, len(df), output_path)
        return

    logger.info("Analytics view %s returned %d rows (limit=%d)", safe_view, len(df), limit)
    if not df.empty:
        print(df.to_string(index=False))


def _safe_table_count(con: sqlite3.Connection, table_name: str) -> int | None:
    if not _VALID_IDENTIFIER.fullmatch(table_name):
        return None
    try:
        return con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    except sqlite3.OperationalError:
        return None


def _runlog_status_map(con: sqlite3.Connection) -> dict[str, int]:
    status_rows = con.execute("SELECT status, COUNT(*) FROM etl_run_log GROUP BY status").fetchall()
    return {status: count for status, count in status_rows}


def _map_delta(previous: dict[str, int], current: dict[str, int]) -> dict[str, int]:
    keys = set(previous) | set(current)
    delta: dict[str, int] = {}
    for key in sorted(keys):
        old = previous.get(key, 0)
        new = current.get(key, 0)
        if new != old:
            delta[key] = new - old
    return delta


def _log_runlog_tail(con: sqlite3.Connection, checkpoint: str, limit: int = 10) -> None:
    rows = con.execute(
        """
        SELECT id, table_name, COALESCE(season_id, '-'), loader, status, COALESCE(row_count, -1),
               started_at, COALESCE(finished_at, '-')
        FROM etl_run_log
        ORDER BY id DESC
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall()
    logger.info("Checkpoint %s: etl_run_log tail (latest %d rows)", checkpoint, limit)
    for row in rows:
        logger.info(
            "Checkpoint %s: runlog id=%s table=%s season=%s loader=%s status=%s row_count=%s started=%s finished=%s",
            checkpoint,
            row[0],
            row[1],
            row[2],
            row[3],
            row[4],
            row[5] if row[5] >= 0 else None,
            row[6],
            row[7],
        )


def _log_ingest_checkpoint(
    con: sqlite3.Connection,
    checkpoint: str,
    tables: list[str],
    *,
    previous_status_map: dict[str, int],
    previous_table_counts: dict[str, int | None],
    previous_ts: float | None,
    runlog_tail: int,
) -> tuple[dict[str, int], dict[str, int | None], float]:
    status_map = _runlog_status_map(con)
    status_delta = _map_delta(previous_status_map, status_map)
    now_ts = time.perf_counter()
    elapsed = (now_ts - previous_ts) if previous_ts is not None else None

    logger.info(
        "Checkpoint %s: etl_run_log status counts=%s delta=%s elapsed_since_previous=%s",
        checkpoint,
        status_map,
        status_delta if status_delta else {},
        f"{elapsed:.2f}s" if elapsed is not None else "n/a",
    )

    next_table_counts = dict(previous_table_counts)
    for table in tables:
        row_count = _safe_table_count(con, table)
        previous_count = previous_table_counts.get(table)
        delta = (
            row_count - previous_count
            if row_count is not None and previous_count is not None
            else None
        )
        logger.info(
            "Checkpoint %s: table=%s row_count=%s delta=%s previous=%s",
            checkpoint,
            table,
            row_count if row_count is not None else "n/a",
            delta if delta is not None else "n/a",
            previous_count if previous_count is not None else "n/a",
        )
        next_table_counts[table] = row_count

    _log_runlog_tail(con, checkpoint, limit=runlog_tail)
    return status_map, next_table_counts, now_ts


def _finalize_metrics(metrics_enabled: bool, show_summary: bool, export_endpoint: str | None) -> None:
    if not metrics_enabled:
        return
    if show_summary:
        log_metrics_summary()
    if export_endpoint:
        export_metrics(export_endpoint)


def main() -> None:
    from dotenv import load_dotenv

    load_dotenv()

    parser = argparse.ArgumentParser(description="NBA database ingest pipeline")
    parser.add_argument(
        "--seasons", nargs="+", default=DEFAULT_SEASONS,
        help="Season strings to ingest, e.g. 2023-24 2024-25",
    )
    parser.add_argument(
        "--dims-only", action="store_true",
        help="Only seed dimension tables (fast, no box-score calls)",
    )
    parser.add_argument(
        "--enrich-bio", action="store_true",
        help="Enrich dim_player with bio data via CommonPlayerInfo (many API calls)",
    )
    parser.add_argument(
        "--awards", action="store_true",
        help="Load fact_player_award from PlayerAwards endpoint",
    )
    parser.add_argument(
        "--salaries", action="store_true",
        help="Load dim_salary_cap and scrape fact_salary from Basketball-Reference",
    )
    parser.add_argument(
        "--rosters", action="store_true",
        help="Load fact_roster from CommonTeamRoster",
    )
    parser.add_argument(
        "--include-playoffs", action="store_true",
        help="Also ingest Playoffs game logs",
    )
    parser.add_argument(
        "--pbp-limit", type=int, default=0,
        help="Number of games to load PBP for (0 = skip PBP)",
    )
    parser.add_argument(
        "--skip-reconciliation", action="store_true",
        help="Skip post-boxscore reconciliation checks (player totals vs team totals)",
    )
    parser.add_argument(
        "--reconciliation-warn-only", action="store_true",
        help="Do not fail ingest when reconciliation checks find discrepancies",
    )
    parser.add_argument(
        "--raw-backfill", action="store_true",
        help="Seed all tables from the raw/ CSV directory (handles load order automatically)",
    )
    parser.add_argument(
        "--raw-dir", type=str, default=None,
        help="Path to the raw/ directory (default: <repo_root>/raw)",
    )
    parser.add_argument(
        "--raw-backfill-fail-fast", action="store_true",
        help="Stop raw backfill immediately when a loader fails",
    )
    parser.add_argument(
        "--analytics-view", type=str, default=None,
        help="DuckDB analytics view name to query (e.g. vw_player_season_totals)",
    )
    parser.add_argument(
        "--analytics-limit", type=int, default=20,
        help="Row limit for --analytics-view queries",
    )
    parser.add_argument(
        "--analytics-output", type=str, default=None,
        help="Optional analytics export path (.csv, .parquet, .json)",
    )
    parser.add_argument(
        "--analytics-only", action="store_true",
        help="Skip ingest and only run the --analytics-view query",
    )
    parser.add_argument(
        "--metrics", action="store_true",
        help="Enable in-memory metrics collection for this run",
    )
    parser.add_argument(
        "--metrics-summary", action="store_true",
        help="Log metrics summary at end of run",
    )
    parser.add_argument(
        "--metrics-export-endpoint", type=str, default=None,
        help="Optional HTTP endpoint URL for metrics JSON export",
    )
    parser.add_argument(
        "--log-level", default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR)",
    )
    parser.add_argument(
        "--log-file", type=str, default=None,
        help="Optional path to log file",
    )
    parser.add_argument(
        "--runlog-tail", type=int, default=12,
        help="How many latest etl_run_log rows to show at each checkpoint",
    )
    args = parser.parse_args()

    if args.metrics:
        os.environ["LARRYOB_METRICS_ENABLED"] = "true"
    metrics_enabled = args.metrics or MetricsConfig.enabled()
    metrics_export_endpoint = args.metrics_export_endpoint or MetricsConfig.export_endpoint()

    if args.analytics_only and not args.analytics_view:
        parser.error("--analytics-only requires --analytics-view")
    if args.analytics_view and args.analytics_limit <= 0:
        parser.error("--analytics-limit must be greater than 0")
    if args.runlog_tail <= 0:
        parser.error("--runlog-tail must be greater than 0")

    log_file = Path(args.log_file) if args.log_file else None
    setup_logging(level=args.log_level, log_file=log_file)
    logger.info(
        "Ingest arguments: seasons=%s dims_only=%s enrich_bio=%s awards=%s salaries=%s rosters=%s "
        "include_playoffs=%s pbp_limit=%s skip_reconciliation=%s reconciliation_warn_only=%s "
        "raw_backfill=%s raw_dir=%s raw_backfill_fail_fast=%s analytics_view=%s analytics_limit=%s "
        "analytics_output=%s analytics_only=%s metrics=%s metrics_summary=%s metrics_export_endpoint=%s "
        "log_level=%s log_file=%s runlog_tail=%s",
        args.seasons,
        args.dims_only,
        args.enrich_bio,
        args.awards,
        args.salaries,
        args.rosters,
        args.include_playoffs,
        args.pbp_limit,
        args.skip_reconciliation,
        args.reconciliation_warn_only,
        args.raw_backfill,
        args.raw_dir,
        args.raw_backfill_fail_fast,
        args.analytics_view,
        args.analytics_limit,
        args.analytics_output,
        args.analytics_only,
        args.metrics,
        args.metrics_summary,
        args.metrics_export_endpoint,
        args.log_level,
        str(log_file) if log_file else None,
        args.runlog_tail,
    )

    analytics_output = Path(args.analytics_output) if args.analytics_output else None
    if args.analytics_only:
        _run_analytics_view(args.analytics_view, args.analytics_limit, analytics_output)
        _finalize_metrics(metrics_enabled, args.metrics_summary, metrics_export_endpoint)
        return

    logger.info("Initialising database schema...")
    con = init_db()
    status_map: dict[str, int] = {}
    table_counts: dict[str, int | None] = {}
    last_checkpoint_ts: float | None = None
    runlog_tail = args.runlog_tail
    try:
        ingest_start = time.perf_counter()
        logger.info("Loading dimension tables...")
        stage_start = time.perf_counter()
        run_dimensions(
            con,
            full_players=not args.dims_only,
            enrich_bio=args.enrich_bio,
        )
        logger.info("Stage dimensions elapsed=%.2fs", time.perf_counter() - stage_start)
        status_map, table_counts, last_checkpoint_ts = _log_ingest_checkpoint(
            con,
            "post-dimensions",
            ["dim_season", "dim_team", "dim_player"],
            previous_status_map=status_map,
            previous_table_counts=table_counts,
            previous_ts=last_checkpoint_ts,
            runlog_tail=runlog_tail,
        )

        if args.raw_backfill and not args.dims_only:
            raw_dir = Path(args.raw_dir) if args.raw_dir else RAW_DIR
            logger.info("Running raw/ backfill from %s...", raw_dir)
            stage_start = time.perf_counter()
            summary = run_raw_backfill(con, raw_dir, fail_fast=args.raw_backfill_fail_fast)
            logger.info(
                "Stage raw_backfill elapsed=%.2fs summary_ok=%d summary_skipped=%d summary_failed=%d",
                time.perf_counter() - stage_start,
                len(summary["ok"]),
                len(summary["skipped"]),
                len(summary["failed"]),
            )
            if summary["failed"] and args.raw_backfill_fail_fast:
                raise RuntimeError("Raw backfill failed in fail-fast mode.")
            status_map, table_counts, last_checkpoint_ts = _log_ingest_checkpoint(
                con,
                "post-raw-backfill",
                [
                    "dim_team_history",
                    "fact_game",
                    "player_game_log",
                    "team_game_log",
                    "fact_team_season",
                    "fact_player_season_stats",
                    "fact_player_advanced_season",
                    "fact_player_shooting_season",
                    "fact_player_pbp_season",
                ],
                previous_status_map=status_map,
                previous_table_counts=table_counts,
                previous_ts=last_checkpoint_ts,
                runlog_tail=runlog_tail,
            )

        if args.awards:
            logger.info("Loading player awards...")
            stage_start = time.perf_counter()
            load_all_awards(con, active_only=True)
            logger.info("Stage awards elapsed=%.2fs", time.perf_counter() - stage_start)
            status_map, table_counts, last_checkpoint_ts = _log_ingest_checkpoint(
                con,
                "post-awards",
                ["fact_player_award"],
                previous_status_map=status_map,
                previous_table_counts=table_counts,
                previous_ts=last_checkpoint_ts,
                runlog_tail=runlog_tail,
            )

        if args.salaries:
            logger.info("Loading salary cap and player salaries...")
            stage_start = time.perf_counter()
            load_salaries_for_seasons(con, args.seasons)
            logger.info("Stage salaries elapsed=%.2fs", time.perf_counter() - stage_start)
            status_map, table_counts, last_checkpoint_ts = _log_ingest_checkpoint(
                con,
                "post-salaries",
                ["dim_salary_cap", "fact_salary"],
                previous_status_map=status_map,
                previous_table_counts=table_counts,
                previous_ts=last_checkpoint_ts,
                runlog_tail=runlog_tail,
            )

        if args.rosters:
            logger.info("Loading rosters...")
            stage_start = time.perf_counter()
            load_rosters_for_seasons(con, args.seasons)
            logger.info("Stage rosters elapsed=%.2fs", time.perf_counter() - stage_start)
            status_map, table_counts, last_checkpoint_ts = _log_ingest_checkpoint(
                con,
                "post-rosters",
                ["fact_roster"],
                previous_status_map=status_map,
                previous_table_counts=table_counts,
                previous_ts=last_checkpoint_ts,
                runlog_tail=runlog_tail,
            )

        if args.dims_only:
            logger.info("--dims-only set; skipping box scores.")
        else:
            season_types = ["Regular Season"]
            if args.include_playoffs:
                season_types.append("Playoffs")

            logger.info("Loading box scores for seasons: %s", args.seasons)
            stage_start = time.perf_counter()
            load_multiple_seasons(con, args.seasons, season_types=season_types)
            logger.info("Stage game_logs elapsed=%.2fs", time.perf_counter() - stage_start)
            status_map, table_counts, last_checkpoint_ts = _log_ingest_checkpoint(
                con,
                "post-game-logs",
                ["fact_game", "player_game_log", "team_game_log"],
                previous_status_map=status_map,
                previous_table_counts=table_counts,
                previous_ts=last_checkpoint_ts,
                runlog_tail=runlog_tail,
            )

            if not args.skip_reconciliation:
                total_warnings = 0
                for season in args.seasons:
                    logger.info("Running reconciliation checks for season %s...", season)
                    stage_start = time.perf_counter()
                    season_warnings = run_consistency_checks(con, season)
                    total_warnings += season_warnings
                    logger.info(
                        "Reconciliation season=%s warnings=%d elapsed=%.2fs running_total_warnings=%d",
                        season,
                        season_warnings,
                        time.perf_counter() - stage_start,
                        total_warnings,
                    )
                if total_warnings > 0:
                    msg = (
                        f"Reconciliation checks found {total_warnings} discrepancy warning(s). "
                        "Re-run with --reconciliation-warn-only to continue despite mismatches."
                    )
                    if args.reconciliation_warn_only:
                        logger.warning(msg)
                    else:
                        raise RuntimeError(msg)

            if args.pbp_limit > 0:
                stage_start = time.perf_counter()
                for season in args.seasons:
                    logger.info("Loading PBP for up to %d games in %s...", args.pbp_limit, season)
                    load_season_pbp(con, season, limit=args.pbp_limit)
                logger.info("Stage pbp elapsed=%.2fs", time.perf_counter() - stage_start)
                status_map, table_counts, last_checkpoint_ts = _log_ingest_checkpoint(
                    con,
                    "post-pbp",
                    ["fact_play_by_play"],
                    previous_status_map=status_map,
                    previous_table_counts=table_counts,
                    previous_ts=last_checkpoint_ts,
                    runlog_tail=runlog_tail,
                )

        logger.info("Ingest total elapsed=%.2fs", time.perf_counter() - ingest_start)
    finally:
        con.close()

    logger.info("Ingest complete.")

    if args.analytics_view:
        _run_analytics_view(args.analytics_view, args.analytics_limit, analytics_output)

    _finalize_metrics(metrics_enabled, args.metrics_summary, metrics_export_endpoint)


if __name__ == "__main__":
    main()
