import logging
import sqlite3
from pathlib import Path

from src.etl.backfill._advanced_stats import (
    load_player_advanced,
    load_player_pbp_season,
    load_player_shooting,
)
from src.etl.backfill._awards import load_awards
from src.etl.backfill._dims import enrich_dim_player, enrich_dim_team, load_team_history
from src.etl.backfill._draft import load_draft
from src.etl.backfill._game_logs import load_player_game_logs, load_team_game_logs
from src.etl.backfill._games import load_games, load_schedule
from src.etl.backfill._season_stats import (
    load_league_season,
    load_player_season_stats,
    load_team_season,
)

logger = logging.getLogger(__name__)
RAW_DIR = Path("raw")

def run_raw_backfill(
    con: sqlite3.Connection, raw_dir: Path = RAW_DIR
) -> None:
    """
    Execute all raw-data loaders in dependency order.
    Safe to re-run (all inserts use INSERT OR IGNORE or REPLACE).
    """
    logger.info("=== Raw backfill starting (raw_dir=%s) ===", raw_dir)

    loaders = [
        ("team_history", load_team_history),
        ("dim_team_enrich", enrich_dim_team),
        ("dim_player_enrich", enrich_dim_player),
        ("games", load_games),
        ("schedule", load_schedule),
        ("player_game_logs", load_player_game_logs),
        ("team_game_logs", load_team_game_logs),
        ("team_season", load_team_season),
        ("league_season", load_league_season),
        ("draft", load_draft),
        ("player_season_stats", load_player_season_stats),
        ("player_advanced", load_player_advanced),
        ("player_shooting", load_player_shooting),
        ("player_pbp_season", load_player_pbp_season),
        ("awards", load_awards),
    ]

    for name, loader in loaders:
        try:
            loader(con, raw_dir)
        except Exception:
            logger.exception("Loader %s failed during raw backfill:", name)

    logger.info("=== Raw backfill complete ===")
