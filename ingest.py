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
    args = parser.parse_args()

    if args.metrics:
        os.environ["LARRYOB_METRICS_ENABLED"] = "true"
    metrics_enabled = args.metrics or MetricsConfig.enabled()
    metrics_export_endpoint = args.metrics_export_endpoint or MetricsConfig.export_endpoint()

    if args.analytics_only and not args.analytics_view:
        parser.error("--analytics-only requires --analytics-view")
    if args.analytics_view and args.analytics_limit <= 0:
        parser.error("--analytics-limit must be greater than 0")

    log_file = Path(args.log_file) if args.log_file else None
    setup_logging(level=args.log_level, log_file=log_file)

    analytics_output = Path(args.analytics_output) if args.analytics_output else None
    if args.analytics_only:
        _run_analytics_view(args.analytics_view, args.analytics_limit, analytics_output)
        _finalize_metrics(metrics_enabled, args.metrics_summary, metrics_export_endpoint)
        return

    logger.info("Initialising database schema...")
    con = init_db()
    try:
        logger.info("Loading dimension tables...")
        run_dimensions(
            con,
            full_players=not args.dims_only,
            enrich_bio=args.enrich_bio,
        )

        if args.raw_backfill and not args.dims_only:
            raw_dir = Path(args.raw_dir) if args.raw_dir else RAW_DIR
            logger.info("Running raw/ backfill from %s...", raw_dir)
            summary = run_raw_backfill(con, raw_dir, fail_fast=args.raw_backfill_fail_fast)
            if summary["failed"] and args.raw_backfill_fail_fast:
                raise RuntimeError("Raw backfill failed in fail-fast mode.")

        if args.awards:
            logger.info("Loading player awards...")
            load_all_awards(con, active_only=True)

        if args.salaries:
            logger.info("Loading salary cap and player salaries...")
            load_salaries_for_seasons(con, args.seasons)

        if args.rosters:
            logger.info("Loading rosters...")
            load_rosters_for_seasons(con, args.seasons)

        if args.dims_only:
            logger.info("--dims-only set; skipping box scores.")
        else:
            season_types = ["Regular Season"]
            if args.include_playoffs:
                season_types.append("Playoffs")

            logger.info("Loading box scores for seasons: %s", args.seasons)
            load_multiple_seasons(con, args.seasons, season_types=season_types)

            if not args.skip_reconciliation:
                total_warnings = 0
                for season in args.seasons:
                    logger.info("Running reconciliation checks for season %s...", season)
                    total_warnings += run_consistency_checks(con, season)
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
                for season in args.seasons:
                    logger.info("Loading PBP for up to %d games in %s...", args.pbp_limit, season)
                    load_season_pbp(con, season, limit=args.pbp_limit)
    finally:
        con.close()

    logger.info("Ingest complete.")

    if args.analytics_view:
        _run_analytics_view(args.analytics_view, args.analytics_limit, analytics_output)

    _finalize_metrics(metrics_enabled, args.metrics_summary, metrics_export_endpoint)


if __name__ == "__main__":
    main()
