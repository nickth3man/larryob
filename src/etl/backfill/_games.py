"""
Backfill loaders for game schedule and results.

This module handles loading of game data from NBA API CSV exports
into the fact_game table, including both completed games and future
scheduled games.
"""

import logging
import sqlite3
from pathlib import Path
from typing import Any

from src.db.operations import upsert_rows
from src.db.tracking import log_load_summary
from src.etl.backfill._base import (
    RAW_DIR,
    csv_path,
    get_valid_set,
    read_csv_safe,
    safe_int,
    safe_str,
)
from src.etl.helpers import pad_game_id, season_id_from_date, season_type_from_game_id
from src.etl.validate import validate_rows

logger = logging.getLogger(__name__)


# Schedule files for upcoming seasons
_SCHEDULE_FILES = [
    "LeagueSchedule24_25.csv",
    "LeagueSchedule25_26.csv",
]


def _determine_season_type_from_label(game_id: str, label: str) -> str:
    """
    Determine season type from game label and game ID.

    Args:
        game_id: NBA game ID
        label: Game label string (e.g., "Preseason", "Playoffs")

    Returns:
        Season type string
    """
    label_lc = str(label).strip().lower()

    if "preseason" in label_lc:
        return "Preseason"
    elif "play-in" in label_lc or "playin" in label_lc:
        return "Play-In"
    elif "playoff" in label_lc:
        return "Playoffs"
    else:
        return season_type_from_game_id(game_id)


def _transform_game_row(
    row: dict[str, Any],
    valid_seasons: set[str],
    valid_teams: set[str],
) -> dict[str, Any] | None:
    """
    Transform a row from Games.csv to fact_game schema.

    Args:
        row: Raw CSV row
        valid_seasons: Set of valid season IDs
        valid_teams: Set of valid team IDs

    Returns:
        Transformed row dict, or None to skip
    """
    game_id = pad_game_id(row["gameId"])
    season_type = season_type_from_game_id(game_id)
    home_id = str(int(row["hometeamId"]))
    away_id = str(int(row["awayteamId"]))

    # Derive season from game date — more reliable than game-ID encoding
    raw_date = str(row["gameDateTimeEst"])
    season_id = season_id_from_date(raw_date)

    if season_id not in valid_seasons:
        return None
    if home_id not in valid_teams or away_id not in valid_teams:
        return None

    game_date = raw_date[:10]
    home_score = safe_int(row.get("homeScore"))
    away_score = safe_int(row.get("awayScore"))
    attendance = safe_int(row.get("attendance"))

    return {
        "game_id": game_id,
        "season_id": season_id,
        "game_date": game_date,
        "home_team_id": home_id,
        "away_team_id": away_id,
        "home_score": home_score,
        "away_score": away_score,
        "season_type": season_type,
        "status": "Final",
        "arena": None,
        "attendance": attendance,
    }


def _transform_schedule_row(
    row: dict[str, Any],
    valid_seasons: set[str],
    valid_teams: set[str],
) -> dict[str, Any] | None:
    """
    Transform a row from LeagueSchedule*.csv to fact_game schema.

    Args:
        row: Raw CSV row (with lowercase column names)
        valid_seasons: Set of valid season IDs
        valid_teams: Set of valid team IDs

    Returns:
        Transformed row dict, or None to skip
    """
    game_id = pad_game_id(row["gameid"])
    raw_date = str(row["gamedatetimeest"])
    season_id = season_id_from_date(raw_date)
    home_id = str(int(row["hometeamid"]))
    away_id = str(int(row["awayteamid"]))

    if season_id not in valid_seasons:
        return None
    if home_id not in valid_teams or away_id not in valid_teams:
        return None

    label = str(row.get("gamelabel", "")).strip().lower()
    season_type = _determine_season_type_from_label(game_id, label)
    game_date = raw_date[:10]
    arena = safe_str(row.get("arenaname"))

    return {
        "game_id": game_id,
        "season_id": season_id,
        "game_date": game_date,
        "home_team_id": home_id,
        "away_team_id": away_id,
        "home_score": None,
        "away_score": None,
        "season_type": season_type,
        "status": "Scheduled",
        "arena": arena,
        "attendance": None,
    }


def load_games(
    con: sqlite3.Connection,
    raw_dir: Path = RAW_DIR,
) -> None:
    """
    Load completed games from Games.csv.

    Args:
        con: SQLite database connection
        raw_dir: Directory containing raw CSV files
    """
    path = csv_path(raw_dir, "Games.csv")
    if path is None:
        return

    df = read_csv_safe(path, low_memory=False)
    valid_seasons = get_valid_set(con, "dim_season", "season_id")
    valid_teams = get_valid_set(con, "dim_team", "team_id")

    rows: list[dict] = []
    skipped = 0

    for row in df.to_dict("records"):
        transformed = _transform_game_row(row, valid_seasons, valid_teams)
        if transformed is None:
            skipped += 1
        else:
            rows.append(transformed)

    inserted = upsert_rows(con, "fact_game", validate_rows("fact_game", rows))
    logger.info(
        "fact_game (Games.csv): %d inserted/ignored, %d skipped",
        inserted,
        skipped,
    )
    log_load_summary(con, "fact_game")


def load_schedule(
    con: sqlite3.Connection,
    raw_dir: Path = RAW_DIR,
) -> None:
    """
    Load scheduled games from LeagueSchedule*.csv files.

    Handles column name normalization for files with different casing.

    Args:
        con: SQLite database connection
        raw_dir: Directory containing raw CSV files
    """
    valid_seasons = get_valid_set(con, "dim_season", "season_id")
    valid_teams = get_valid_set(con, "dim_team", "team_id")

    total_inserted = 0

    for filename in _SCHEDULE_FILES:
        path = csv_path(raw_dir, filename)
        if path is None:
            continue

        df = read_csv_safe(path)

        # Normalize column names — files may have different casing
        df.columns = [c.lower() for c in df.columns]

        rows: list[dict] = []
        for row in df.to_dict("records"):
            transformed = _transform_schedule_row(row, valid_seasons, valid_teams)
            if transformed is not None:
                rows.append(transformed)

        inserted = upsert_rows(con, "fact_game", validate_rows("fact_game", rows))
        total_inserted += inserted
        logger.info("%s: %d rows inserted/ignored", filename, inserted)

    logger.info("fact_game (schedule): %d total inserted/ignored", total_inserted)
