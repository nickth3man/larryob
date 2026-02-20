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
"""

import argparse
import logging

from src.db.schema import init_db
from src.etl.dimensions import run_all as run_dimensions
from src.etl.game_logs import load_multiple_seasons
from src.etl.play_by_play import load_season_pbp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
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
        "--include-playoffs", action="store_true",
        help="Also ingest Playoffs game logs",
    )
    parser.add_argument(
        "--pbp-limit", type=int, default=0,
        help="Number of games to load PBP for (0 = skip PBP)",
    )
    args = parser.parse_args()

    logger.info("Initialising database schema…")
    con = init_db()

    logger.info("Loading dimension tables…")
    run_dimensions(con, full_players=not args.dims_only)

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
