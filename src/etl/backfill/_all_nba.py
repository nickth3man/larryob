"""
Backfill loaders for All-NBA / End-of-Season teams data.
"""

import logging
import sqlite3
from pathlib import Path
from typing import Any

from src.db.operations import upsert_rows
from src.etl.backfill._base import (
    RAW_DIR,
    csv_path,
    get_valid_set,
    read_csv_safe,
    safe_float,
    safe_int,
    safe_str,
)
from src.etl.helpers import _isna, int_season_to_id
from src.etl.identity.resolver import resolve_or_create_player

logger = logging.getLogger(__name__)

_TEAM_TYPE_MAP = {
    "all_nba": "All-NBA",
    "all-defense": "All-Defense",
    "all_defense": "All-Defense",
    "all-rookie": "All-Rookie",
    "all_rookie": "All-Rookie",
    "all-aba": "All-ABA",
    "all_aba": "All-ABA",
    "all-baa": "All-BAA",
    "all_baa": "All-BAA",
}


def _normalize_team_type(raw: Any) -> str:
    text = safe_str(raw)
    if not text:
        return "Unknown"
    normalized = text.strip().lower()
    return _TEAM_TYPE_MAP.get(normalized, text.strip())


def _parse_team_number(raw: Any) -> int | None:
    text = safe_str(raw)
    if not text:
        return None

    upper = text.strip().upper()
    if upper.startswith("1"):
        return 1
    if upper.startswith("2"):
        return 2
    if upper.startswith("3"):
        return 3
    return None


def _normalize_position(raw: Any) -> str | None:
    if _isna(raw):
        return None
    text = str(raw).strip()
    if not text or text.upper() == "NA":
        return None
    return text.upper()


def _build_context(con: sqlite3.Connection) -> tuple[set[str], dict[str, str]]:
    valid_seasons = get_valid_set(con, "dim_season", "season_id")
    bref_to_player_id = {
        bref_id: player_id
        for bref_id, player_id in con.execute(
            "SELECT bref_id, player_id FROM dim_player WHERE bref_id IS NOT NULL"
        ).fetchall()
    }
    return valid_seasons, bref_to_player_id


def load_all_nba_teams(
    con: sqlite3.Connection,
    raw_dir: Path = RAW_DIR,
) -> int:
    """
    Load end-of-season team selections from End of Season Teams.csv.
    """
    path = csv_path(raw_dir, "End of Season Teams.csv")
    if path is None:
        return 0

    df = read_csv_safe(path, low_memory=False)
    valid_seasons, bref_to_player_id = _build_context(con)

    rows: list[dict[str, Any]] = []
    skipped = 0

    for row in df.to_dict("records"):
        season_id = int_season_to_id(row["season"])
        bref_id = safe_str(row.get("player_id"))
        player_id = bref_to_player_id.get(bref_id or "")

        if season_id not in valid_seasons:
            skipped += 1
            continue

        if not player_id and bref_id:
            full_name = safe_str(row.get("player")) or bref_id
            player_id = resolve_or_create_player(con, "bref", bref_id, full_name)

        if not player_id:
            skipped += 1
            continue

        rows.append(
            {
                "player_id": player_id,
                "season_id": season_id,
                "team_type": _normalize_team_type(row.get("type")),
                "team_number": _parse_team_number(row.get("number_tm")),
                "position": _normalize_position(row.get("position")),
            }
        )

    inserted = upsert_rows(con, "fact_all_nba", rows)
    logger.info("fact_all_nba: %d inserted/ignored, %d skipped", inserted, skipped)
    return inserted


def load_all_nba_votes(
    con: sqlite3.Connection,
    raw_dir: Path = RAW_DIR,
) -> int:
    """
    Load end-of-season voting detail from End of Season Teams (Voting).csv.
    """
    path = csv_path(raw_dir, "End of Season Teams (Voting).csv")
    if path is None:
        return 0

    df = read_csv_safe(path, low_memory=False)
    valid_seasons, bref_to_player_id = _build_context(con)

    rows: list[dict[str, Any]] = []
    skipped = 0

    for row in df.to_dict("records"):
        season_id = int_season_to_id(row["season"])
        bref_id = safe_str(row.get("player_id"))
        player_id = bref_to_player_id.get(bref_id or "")

        if season_id not in valid_seasons:
            skipped += 1
            continue

        if not player_id and bref_id:
            full_name = safe_str(row.get("player")) or bref_id
            player_id = resolve_or_create_player(con, "bref", bref_id, full_name)

        if not player_id:
            skipped += 1
            continue

        rows.append(
            {
                "player_id": player_id,
                "season_id": season_id,
                "team_type": _normalize_team_type(row.get("type")),
                "team_number": _parse_team_number(row.get("number_tm")),
                "position": _normalize_position(row.get("position")),
                "pts_won": safe_int(row.get("pts_won")),
                "pts_max": safe_int(row.get("pts_max")),
                "share": safe_float(row.get("share")),
                "first_team_votes": safe_int(row.get("x1st_tm")),
                "second_team_votes": safe_int(row.get("x2nd_tm")),
                "third_team_votes": safe_int(row.get("x3rd_tm")),
            }
        )

    inserted = upsert_rows(con, "fact_all_nba_vote", rows)
    logger.info("fact_all_nba_vote: %d inserted/ignored, %d skipped", inserted, skipped)
    return inserted
