"""
NBA database ingest entrypoint.

Usage
-----
    # Seed dimensions + box scores for recent seasons:
    uv run python ingest.py

    # Seed dimensions only (fast, no network-heavy calls):
    uv run python ingest.py --dims-only

    # Custom seasons:
    uv run python ingest.py --seasons 2022-23 2023-24 2024-25

    # Include playoff logs:
    uv run python ingest.py --include-playoffs

    # Seed PBP for up to N games already in fact_game:
    uv run python ingest.py --pbp-limit 10

    # Backfill all tables from the raw/ CSV directory:
    uv run python ingest.py --raw-backfill

    # Custom raw/ directory location:
    uv run python ingest.py --raw-backfill --raw-dir /path/to/raw
"""

import argparse
import logging
from pathlib import Path

from dotenv import load_dotenv

from src.db.schema import init_db

load_dotenv()
from src.etl.awards import load_all_awards
from src.etl.raw_backfill import run_raw_backfill
from src.etl.utils import setup_logging
from src.etl.dimensions import run_all as run_dimensions
from src.etl.game_logs import load_multiple_seasons
from src.etl.play_by_play import load_season_pbp
from src.etl.roster import load_rosters_for_seasons
from src.etl.salaries import load_salaries_for_seasons

logger = logging.getLogger("ingest")

DEFAULT_SEASONS = ["2023-24", "2024-25"]


def main() -> None:
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
        help="Load dim_salary_cap (hardcoded) and scrape fact_salary from Basketball-Reference",
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
        "--raw-backfill", action="store_true",
        help="Seed all tables from the raw/ CSV directory (handles load order automatically)",
    )
    parser.add_argument(
        "--raw-dir", type=str, default=None,
        help="Path to the raw/ directory (default: <repo_root>/raw)",
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

    log_file = args.log_file
    if log_file:
        log_file = Path(log_file)
    setup_logging(
        level=args.log_level,
        log_file=log_file,
    )

    logger.info("Initialising database schema…")
    con = init_db()

    logger.info("Loading dimension tables…")
    run_dimensions(
        con,
        full_players=not args.dims_only,
        enrich_bio=args.enrich_bio,
    )

    if args.raw_backfill:
        from src.etl.raw_backfill import RAW_DIR
        raw_dir = Path(args.raw_dir) if args.raw_dir else RAW_DIR
        logger.info("Running raw/ backfill from %s…", raw_dir)
        run_raw_backfill(con, raw_dir)

    if args.awards:
        logger.info("Loading player awards…")
        load_all_awards(con, active_only=True)

    if args.salaries:
        logger.info("Loading salary cap and player salaries…")
        load_salaries_for_seasons(con, args.seasons)

    if args.rosters:
        logger.info("Loading rosters…")
        load_rosters_for_seasons(con, args.seasons)

    if args.dims_only:
        logger.info("--dims-only set; skipping box scores.")
        con.close()
        return

    season_types = ["Regular Season"]
    if args.include_playoffs:
        season_types.append("Playoffs")

    logger.info("Loading box scores for seasons: %s", args.seasons)
    load_multiple_seasons(con, args.seasons, season_types=season_types)

    if args.pbp_limit > 0:
        for season in args.seasons:
            logger.info("Loading PBP for up to %d games in %s…", args.pbp_limit, season)
            load_season_pbp(con, season, limit=args.pbp_limit)

    con.close()
    logger.info("Ingest complete.")


if __name__ == "__main__":
    main()
