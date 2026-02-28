"""CLI argument parser definition."""

from __future__ import annotations

import argparse

from src.pipeline.constants import DEFAULT_SEASONS


def create_argument_parser() -> argparse.ArgumentParser:
    """Create and configure the argument parser."""
    parser = argparse.ArgumentParser(
        description="NBA database ingest pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--seasons",
        nargs="+",
        default=list(DEFAULT_SEASONS),
        help=f"Season strings to ingest (default: {' '.join(DEFAULT_SEASONS)})",
    )
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
    parser.add_argument(
        "--awards",
        action="store_true",
        default=True,
        help="Load fact_player_award from PlayerAwards endpoint (default: on)",
    )
    parser.add_argument(
        "--no-awards",
        dest="awards",
        action="store_false",
        help="Skip loading fact_player_award",
    )
    parser.add_argument(
        "--salaries",
        action="store_true",
        default=True,
        help="Load dim_salary_cap and scrape fact_salary from Basketball-Reference (default: on)",
    )
    parser.add_argument(
        "--no-salaries",
        dest="salaries",
        action="store_false",
        help="Skip loading dim_salary_cap and fact_salary",
    )
    parser.add_argument(
        "--rosters",
        action="store_true",
        default=True,
        help="Load fact_roster from CommonTeamRoster (default: on)",
    )
    parser.add_argument(
        "--no-rosters",
        dest="rosters",
        action="store_false",
        help="Skip loading fact_roster",
    )
    parser.add_argument(
        "--include-playoffs",
        action="store_true",
        default=True,
        help="Also ingest Playoffs game logs (default: on)",
    )
    parser.add_argument(
        "--no-playoffs",
        dest="include_playoffs",
        action="store_false",
        help="Skip Playoffs game logs",
    )
    parser.add_argument(
        "--pbp-limit",
        type=int,
        default=0,
        help="Number of games to load PBP for (0 = skip PBP)",
    )
    parser.add_argument(
        "--pbp-source",
        choices=["api", "bulk", "auto"],
        default="auto",
        help="PBP data source strategy",
    )
    parser.add_argument(
        "--pbp-bulk-dir",
        type=str,
        default=None,
        help="Path to raw/pbp/ directory for bulk PBP loading",
    )
    parser.add_argument(
        "--salary-source",
        choices=["bref", "open", "auto"],
        default="auto",
        help="Salary data source strategy",
    )
    parser.add_argument(
        "--salary-open-file",
        type=str,
        default=None,
        help="Path to open-source salary CSV",
    )
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
