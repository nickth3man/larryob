import logging
import sqlite3
from pathlib import Path

import pandas as pd

from src.etl.helpers import _isna, int_season_to_id
from src.etl.utils import upsert_rows

logger = logging.getLogger(__name__)
RAW_DIR = Path("raw")

def load_draft(con: sqlite3.Connection, raw_dir: Path = RAW_DIR) -> None:
    path = raw_dir / "Draft Pick History.csv"
    if not path.exists():
        logger.warning("Draft Pick History.csv not found, skipping")
        return

    df = pd.read_csv(path, low_memory=False)
    valid_seasons = {r[0] for r in con.execute("SELECT season_id FROM dim_season")}

    rows: list[dict] = []
    skipped = 0
    for row in df.to_dict("records"):
        season_id = int_season_to_id(row["season"])
        if season_id not in valid_seasons:
            skipped += 1
            continue

        rows.append({
            "season_id":        season_id,
            "draft_round":      int(row["round"])         if not _isna(row.get("round"))        else None,
            "overall_pick":     int(row["overall_pick"])  if not _isna(row.get("overall_pick")) else None,
            "bref_team_abbrev": str(row["tm"]).strip()    if not _isna(row.get("tm"))           else None,
            "bref_player_id":   str(row["player_id"]).strip() if not _isna(row.get("player_id")) else None,
            "player_name":      str(row["player"]).strip()    if not _isna(row.get("player"))    else None,
            "college":          str(row["college"]).strip()   if not _isna(row.get("college"))   else None,
            "lg":               str(row["lg"]).strip()        if not _isna(row.get("lg"))        else None,
        })

    inserted = upsert_rows(con, "fact_draft", rows)
    logger.info(
        "fact_draft: %d inserted/ignored, %d skipped", inserted, skipped
    )
