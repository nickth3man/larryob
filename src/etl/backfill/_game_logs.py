"""
Backfill loaders for player and team game logs.

This module handles loading of game-level statistics from NBA API
CSV exports into player_game_log and team_game_log tables.
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
)
from src.etl.helpers import _flt, _int, pad_game_id
from src.etl.validation import validate_rows

logger = logging.getLogger(__name__)

# Chunk size for processing large CSV files
_CHUNK_SIZE = 50_000

# Chunk size for processing large CSV files
_CHUNK_SIZE = 50_000


def _normalize_early_era_rebounds(
    oreb: int | None, dreb: int | None, reb: int | None
) -> tuple[int | None, int | None]:
    """
    Normalize early-era rebound data where oreb/dreb were not tracked.

    In pre-1973-74 seasons, offensive and defensive rebounds were not officially
    tracked. When both are 0 but total rebounds > 0, this indicates the split
    was not recorded (not that the player had 0 offensive/defensive rebounds).

    Args:
        oreb: Offensive rebounds from source
        dreb: Defensive rebounds from source
        reb: Total rebounds from source

    Returns:
        Tuple of (oreb, dreb) with None for untracked values
    """
    # If oreb and dreb are both 0 but reb > 0, the split wasn't tracked
    if oreb == 0 and dreb == 0 and reb is not None and reb > 0:
        return (None, None)
    return (oreb, dreb)


def _build_team_lookup(
    raw_dir: Path,
) -> dict[tuple[int, int], str]:
    """
    Build (game_id_int, home_flag) → team_id lookup from TeamStatistics.csv.

    Args:
        raw_dir: Directory containing raw CSV files

    Returns:
        Dictionary mapping (gameId, home) tuples to team_id strings
    """
    team_lookup: dict[tuple[int, int], str] = {}
    ts_path = raw_dir / "TeamStatistics.csv"

    if ts_path.exists():
        ts_df = read_csv_safe(ts_path, usecols=["gameId", "teamId", "home"])
        for r in ts_df.to_dict("records"):
            game_id = int(r["gameId"])
            home_flag = int(r["home"])
            team_id = str(int(r["teamId"]))
            team_lookup[(game_id, home_flag)] = team_id
    else:
        logger.warning("TeamStatistics.csv not found; team_id may be missing")

    return team_lookup


def _transform_player_game_log_row(
    row: dict[str, Any],
    valid_games: set[str],
    valid_players: set[str],
    team_lookup: dict[tuple[int, int], str],
) -> dict[str, Any] | None:
    """
    Transform a row from PlayerStatistics.csv to player_game_log schema.

    Args:
        row: Raw CSV row
        valid_games: Set of valid game IDs
        valid_players: Set of valid player IDs
        team_lookup: (gameId, home) → team_id lookup

    Returns:
        Transformed row dict, or None to skip
    """
    game_id = pad_game_id(row["gameId"])
    player_id = str(int(row["personId"]))

    if game_id not in valid_games or player_id not in valid_players:
        return None

    home_flag = _int(row.get("home"))
    if home_flag is None:
        return None

    team_id = team_lookup.get((int(row["gameId"]), home_flag))
    if team_id is None:
        return None

    # Extract rebound values
    oreb_raw = _int(row.get("reboundsOffensive"))
    dreb_raw = _int(row.get("reboundsDefensive"))
    reb_raw = _int(row.get("reboundsTotal"))

    # Normalize early-era data where oreb/dreb weren't tracked
    oreb, dreb = _normalize_early_era_rebounds(oreb_raw, dreb_raw, reb_raw)

    return {
        "game_id": game_id,
        "player_id": player_id,
        "team_id": team_id,
        "minutes_played": _flt(row.get("numMinutes")),
        "fgm": _int(row.get("fieldGoalsMade")),
        "fga": _int(row.get("fieldGoalsAttempted")),
        "fg3m": _int(row.get("threePointersMade")),
        "fg3a": _int(row.get("threePointersAttempted")),
        "ftm": _int(row.get("freeThrowsMade")),
        "fta": _int(row.get("freeThrowsAttempted")),
        "oreb": oreb,
        "dreb": dreb,
        "reb": reb_raw,
        "ast": _int(row.get("assists")),
        "stl": _int(row.get("steals")),
        "blk": _int(row.get("blocks")),
        "tov": _int(row.get("turnovers")),
        "pf": _int(row.get("foulsPersonal")),
        "pts": _int(row.get("points")),
        "plus_minus": _int(row.get("plusMinusPoints")),
        "starter": None,
    }


def _transform_team_game_log_row(
    row: dict[str, Any],
    valid_games: set[str],
    valid_teams: set[str],
) -> dict[str, Any] | None:
    """
    Transform a row from TeamStatistics.csv to team_game_log schema.

    Args:
        row: Raw CSV row
        valid_games: Set of valid game IDs
        valid_teams: Set of valid team IDs

    Returns:
        Transformed row dict, or None to skip
    """
    game_id = pad_game_id(row["gameId"])
    team_id = str(int(row["teamId"]))

    if game_id not in valid_games or team_id not in valid_teams:
        return None

    # Extract rebound values
    oreb_raw = _int(row.get("reboundsOffensive"))
    dreb_raw = _int(row.get("reboundsDefensive"))
    reb_raw = _int(row.get("reboundsTotal"))

    # Normalize early-era data where oreb/dreb weren't tracked
    oreb, dreb = _normalize_early_era_rebounds(oreb_raw, dreb_raw, reb_raw)

    return {
        "game_id": game_id,
        "team_id": team_id,
        "fgm": _int(row.get("fieldGoalsMade")),
        "fga": _int(row.get("fieldGoalsAttempted")),
        "fg3m": _int(row.get("threePointersMade")),
        "fg3a": _int(row.get("threePointersAttempted")),
        "ftm": _int(row.get("freeThrowsMade")),
        "fta": _int(row.get("freeThrowsAttempted")),
        "oreb": oreb,
        "dreb": dreb,
        "reb": reb_raw,
        "ast": _int(row.get("assists")),
        "stl": _int(row.get("steals")),
        "blk": _int(row.get("blocks")),
        "tov": _int(row.get("turnovers")),
        "pf": _int(row.get("foulsPersonal")),
        "pts": _int(row.get("teamScore")),
        "plus_minus": _int(row.get("plusMinusPoints")),
    }


def load_player_game_logs(
    con: sqlite3.Connection,
    raw_dir: Path = RAW_DIR,
) -> None:
    """
    Load player game logs from PlayerStatistics.csv.

    Uses chunked processing for memory efficiency on large files.

    Args:
        con: SQLite database connection
        raw_dir: Directory containing raw CSV files
    """
    path = csv_path(raw_dir, "PlayerStatistics.csv")
    if path is None:
        return

    # Build team lookup from TeamStatistics.csv
    team_lookup = _build_team_lookup(raw_dir)

    # Valid game and player IDs already in DB
    valid_games = get_valid_set(con, "fact_game", "game_id")
    valid_players = get_valid_set(con, "dim_player", "player_id")

    total_inserted = 0
    skipped = 0

    reader = read_csv_safe(path, low_memory=False, chunksize=_CHUNK_SIZE)

    for chunk in reader:
        rows: list[dict] = []
        for row in chunk.to_dict("records"):
            transformed = _transform_player_game_log_row(
                row, valid_games, valid_players, team_lookup
            )
            if transformed is None:
                skipped += 1
            else:
                rows.append(transformed)

        inserted = upsert_rows(
            con,
            "player_game_log",
            validate_rows("player_game_log", rows),
            autocommit=False,
        )
        total_inserted += inserted
        logger.debug("player_game_log chunk: +%d rows", inserted)

    con.commit()
    logger.info(
        "player_game_log (PlayerStatistics.csv): %d inserted/ignored, %d skipped",
        total_inserted,
        skipped,
    )
    log_load_summary(con, "player_game_log")


def load_team_game_logs(
    con: sqlite3.Connection,
    raw_dir: Path = RAW_DIR,
) -> None:
    """
    Load team game logs from TeamStatistics.csv.

    Args:
        con: SQLite database connection
        raw_dir: Directory containing raw CSV files
    """
    path = csv_path(raw_dir, "TeamStatistics.csv")
    if path is None:
        return

    df = read_csv_safe(path, low_memory=False)
    valid_games = get_valid_set(con, "fact_game", "game_id")
    valid_teams = get_valid_set(con, "dim_team", "team_id")

    rows: list[dict] = []
    skipped = 0

    for row in df.to_dict("records"):
        transformed = _transform_team_game_log_row(row, valid_games, valid_teams)
        if transformed is None:
            skipped += 1
        else:
            rows.append(transformed)

    inserted = upsert_rows(
        con,
        "team_game_log",
        validate_rows("team_game_log", rows),
    )
    logger.info(
        "team_game_log (TeamStatistics.csv): %d inserted/ignored, %d skipped",
        inserted,
        skipped,
    )
    log_load_summary(con, "team_game_log")
