import logging
import sqlite3
from pathlib import Path

import pandas as pd

from src.etl.helpers import _flt, _int, _isna, pad_game_id
from src.etl.utils import log_load_summary, upsert_rows
from src.etl.validate import validate_rows

logger = logging.getLogger(__name__)
RAW_DIR = Path("raw")

def load_player_game_logs(
    con: sqlite3.Connection, raw_dir: Path = RAW_DIR
) -> None:
    path = raw_dir / "PlayerStatistics.csv"
    ts_path = raw_dir / "TeamStatistics.csv"
    if not path.exists():
        logger.warning("PlayerStatistics.csv not found, skipping")
        return

    # Build (game_id_int, home_flag) → team_id lookup from TeamStatistics.csv.
    team_lookup: dict[tuple[int, int], str] = {}
    if ts_path.exists():
        ts_df = pd.read_csv(ts_path, usecols=["gameId", "teamId", "home"])
        for r in ts_df.to_dict("records"):
            team_lookup[(int(r["gameId"]), int(r["home"]))] = str(int(r["teamId"]))
    else:
        logger.warning("TeamStatistics.csv not found; team_id may be missing")

    # Valid game and player IDs already in DB.
    valid_games   = {r[0] for r in con.execute("SELECT game_id   FROM fact_game")}
    valid_players = {r[0] for r in con.execute("SELECT player_id FROM dim_player")}

    total, skipped = 0, 0
    chunk_size = 50_000

    for chunk in pd.read_csv(path, chunksize=chunk_size, low_memory=False):
        rows: list[dict] = []
        for row in chunk.to_dict("records"):
            game_id   = pad_game_id(row["gameId"])
            player_id = str(int(row["personId"]))

            if game_id not in valid_games or player_id not in valid_players:
                skipped += 1
                continue

            home_flag = int(row["home"]) if not _isna(row.get("home")) else None
            team_id   = team_lookup.get(
                (int(row["gameId"]), home_flag if home_flag is not None else -1)
            )
            if team_id is None:
                skipped += 1
                continue

            rows.append({
                "game_id":        game_id,
                "player_id":      player_id,
                "team_id":        team_id,
                "minutes_played": _flt(row.get("numMinutes")),
                "fgm":  _int(row.get("fieldGoalsMade")),
                "fga":  _int(row.get("fieldGoalsAttempted")),
                "fg3m": _int(row.get("threePointersMade")),
                "fg3a": _int(row.get("threePointersAttempted")),
                "ftm":  _int(row.get("freeThrowsMade")),
                "fta":  _int(row.get("freeThrowsAttempted")),
                "oreb": _int(row.get("reboundsOffensive")),
                "dreb": _int(row.get("reboundsDefensive")),
                "reb":  _int(row.get("reboundsTotal")),
                "ast":  _int(row.get("assists")),
                "stl":  _int(row.get("steals")),
                "blk":  _int(row.get("blocks")),
                "tov":  _int(row.get("turnovers")),
                "pf":   _int(row.get("foulsPersonal")),
                "pts":  _int(row.get("points")),
                "plus_minus": _int(row.get("plusMinusPoints")),
                "starter": None,
            })

        inserted = upsert_rows(con, "player_game_log", validate_rows("player_game_log", rows), autocommit=False)
        total += inserted
        logger.debug("player_game_log chunk: +%d rows", inserted)

    con.commit()
    logger.info(
        "player_game_log (PlayerStatistics.csv): %d inserted/ignored, %d skipped",
        total, skipped,
    )
    log_load_summary(con, "player_game_log")

def load_team_game_logs(
    con: sqlite3.Connection, raw_dir: Path = RAW_DIR
) -> None:
    path = raw_dir / "TeamStatistics.csv"
    if not path.exists():
        logger.warning("TeamStatistics.csv not found, skipping")
        return

    df = pd.read_csv(path, low_memory=False)
    valid_games = {r[0] for r in con.execute("SELECT game_id FROM fact_game")}
    valid_teams = {r[0] for r in con.execute("SELECT team_id FROM dim_team")}

    rows: list[dict] = []
    skipped = 0

    for row in df.to_dict("records"):
        game_id = pad_game_id(row["gameId"])
        team_id = str(int(row["teamId"]))

        if game_id not in valid_games or team_id not in valid_teams:
            skipped += 1
            continue

        rows.append({
            "game_id":     game_id,
            "team_id":     team_id,
            "fgm":  _int(row.get("fieldGoalsMade")),
            "fga":  _int(row.get("fieldGoalsAttempted")),
            "fg3m": _int(row.get("threePointersMade")),
            "fg3a": _int(row.get("threePointersAttempted")),
            "ftm":  _int(row.get("freeThrowsMade")),
            "fta":  _int(row.get("freeThrowsAttempted")),
            "oreb": _int(row.get("reboundsOffensive")),
            "dreb": _int(row.get("reboundsDefensive")),
            "reb":  _int(row.get("reboundsTotal")),
            "ast":  _int(row.get("assists")),
            "stl":  _int(row.get("steals")),
            "blk":  _int(row.get("blocks")),
            "tov":  _int(row.get("turnovers")),
            "pf":   _int(row.get("foulsPersonal")),
            "pts":  _int(row.get("teamScore")),
            "plus_minus": _int(row.get("plusMinusPoints")),
        })

    inserted = upsert_rows(con, "team_game_log", validate_rows("team_game_log", rows))
    logger.info(
        "team_game_log (TeamStatistics.csv): %d inserted/ignored, %d skipped",
        inserted, skipped,
    )
    log_load_summary(con, "team_game_log")
