import logging
import sqlite3
from pathlib import Path

import pandas as pd

from src.etl.helpers import _isna, pad_game_id, season_id_from_date, season_type_from_game_id
from src.etl.utils import log_load_summary, upsert_rows
from src.etl.validate import validate_rows

logger = logging.getLogger(__name__)
RAW_DIR = Path("raw")

def load_games(con: sqlite3.Connection, raw_dir: Path = RAW_DIR) -> None:
    path = raw_dir / "Games.csv"
    if not path.exists():
        logger.warning("Games.csv not found, skipping")
        return

    df = pd.read_csv(path, low_memory=False)

    # Build set of valid season_ids and team_ids to skip orphan rows.
    valid_seasons = {r[0] for r in con.execute("SELECT season_id FROM dim_season")}
    valid_teams   = {r[0] for r in con.execute("SELECT team_id   FROM dim_team")}

    rows: list[dict] = []
    skipped = 0
    for row in df.to_dict("records"):
        game_id     = pad_game_id(row["gameId"])
        season_type = season_type_from_game_id(game_id)
        home_id     = str(int(row["hometeamId"]))
        away_id     = str(int(row["awayteamId"]))
        # Derive season from game date — more reliable than game-ID encoding for
        # historical records where the ID format may have differed.
        raw_date    = str(row["gameDateTimeEst"])
        season_id   = season_id_from_date(raw_date)

        if season_id not in valid_seasons or home_id not in valid_teams or away_id not in valid_teams:
            skipped += 1
            continue

        game_date = raw_date[:10]

        home_score = int(row["homeScore"]) if not _isna(row.get("homeScore")) else None
        away_score = int(row["awayScore"]) if not _isna(row.get("awayScore")) else None
        attendance = int(row["attendance"]) if not _isna(row.get("attendance")) else None

        rows.append({
            "game_id":      game_id,
            "season_id":    season_id,
            "game_date":    game_date,
            "home_team_id": home_id,
            "away_team_id": away_id,
            "home_score":   home_score,
            "away_score":   away_score,
            "season_type":  season_type,
            "status":       "Final",
            "arena":        None,
            "attendance":   attendance,
        })

    inserted = upsert_rows(con, "fact_game", validate_rows("fact_game", rows))
    logger.info(
        "fact_game (Games.csv): %d inserted/ignored, %d skipped", inserted, skipped
    )
    log_load_summary(con, "fact_game")

def load_schedule(con: sqlite3.Connection, raw_dir: Path = RAW_DIR) -> None:
    files = [
        raw_dir / "LeagueSchedule24_25.csv",
        raw_dir / "LeagueSchedule25_26.csv",
    ]
    valid_seasons = {r[0] for r in con.execute("SELECT season_id FROM dim_season")}
    valid_teams   = {r[0] for r in con.execute("SELECT team_id   FROM dim_team")}

    total_inserted = 0
    for path in files:
        if not path.exists():
            logger.warning("%s not found, skipping", path.name)
            continue

        df = pd.read_csv(path)
        # Normalise column names — the two files have slightly different casing.
        df.columns = [c.lower() for c in df.columns]
        home_col = "hometeamid"
        away_col = "awayteamid"

        rows: list[dict] = []
        for row in df.to_dict("records"):
            game_id     = pad_game_id(row["gameid"])
            raw_date_s  = str(row["gamedatetimeest"])
            season_id   = season_id_from_date(raw_date_s)
            home_id     = str(int(row[home_col]))
            away_id     = str(int(row[away_col]))

            if season_id not in valid_seasons or home_id not in valid_teams or away_id not in valid_teams:
                continue

            label = str(row.get("gamelabel", "")).strip().lower()
            if "preseason" in label:
                season_type = "Preseason"
            elif "play-in" in label or "playin" in label:
                season_type = "Play-In"
            elif "playoff" in label:
                season_type = "Playoffs"
            else:
                season_type = season_type_from_game_id(game_id)

            game_date = raw_date_s[:10]

            arena = str(row.get("arenaname", "")).strip() or None

            rows.append({
                "game_id":      game_id,
                "season_id":    season_id,
                "game_date":    game_date,
                "home_team_id": home_id,
                "away_team_id": away_id,
                "home_score":   None,
                "away_score":   None,
                "season_type":  season_type,
                "status":       "Scheduled",
                "arena":        arena,
                "attendance":   None,
            })

        inserted = upsert_rows(con, "fact_game", validate_rows("fact_game", rows))
        total_inserted += inserted
        logger.info("%s: %d rows inserted/ignored", path.name, inserted)

    logger.info("fact_game (schedule): %d total inserted/ignored", total_inserted)
