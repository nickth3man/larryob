"""
Backfill loader for NBA draft history.

This module handles loading of draft pick data from Basketball-Reference
CSV exports into the fact_draft table.
"""

import logging
import sqlite3
from pathlib import Path
from typing import Any

from src.etl.backfill._base import (
    RAW_DIR,
    csv_path,
    get_valid_set,
    read_csv_safe,
    safe_int,
    safe_str,
)
from src.etl.helpers import _isna, int_season_to_id
from src.etl.utils import upsert_rows

logger = logging.getLogger(__name__)


# Column mapping for Draft Pick History.csv -> fact_draft
_DRAFT_COLUMN_MAP: dict[str, str] = {
    "season": "season_id",
    "round": "draft_round",
    "overall_pick": "overall_pick",
    "tm": "bref_team_abbrev",
    "player_id": "bref_player_id",
    "player": "player_name",
    "college": "college",
    "lg": "lg",
}


def _transform_draft_row(
    row: dict[str, Any],
    valid_seasons: set[str],
) -> dict[str, Any] | None:
    """
    Transform a row from Draft Pick History.csv to fact_draft schema.
    
    Args:
        row: Raw CSV row
        valid_seasons: Set of valid season IDs
        
    Returns:
        Transformed row dict, or None to skip
    """
    season_id = int_season_to_id(row["season"])
    if season_id not in valid_seasons:
        return None
    
    return {
        "season_id": season_id,
        "draft_round": safe_int(row.get("round")),
        "overall_pick": safe_int(row.get("overall_pick")),
        "bref_team_abbrev": safe_str(row.get("tm")),
        "bref_player_id": safe_str(row.get("player_id")),
        "player_name": safe_str(row.get("player")),
        "college": safe_str(row.get("college")),
        "lg": safe_str(row.get("lg")),
    }


def load_draft(
    con: sqlite3.Connection,
    raw_dir: Path = RAW_DIR,
) -> None:
    """
    Load draft pick history from Draft Pick History.csv.
    
    Args:
        con: SQLite database connection
        raw_dir: Directory containing raw CSV files
    """
    path = csv_path(raw_dir, "Draft Pick History.csv")
    if path is None:
        return

    df = read_csv_safe(path, low_memory=False)
    valid_seasons = get_valid_set(con, "dim_season", "season_id")

    rows: list[dict] = []
    skipped = 0
    
    for row in df.to_dict("records"):
        transformed = _transform_draft_row(row, valid_seasons)
        if transformed is None:
            skipped += 1
        else:
            rows.append(transformed)

    inserted = upsert_rows(con, "fact_draft", rows)
    logger.info(
        "fact_draft: %d inserted/ignored, %d skipped",
        inserted,
        skipped,
    )
