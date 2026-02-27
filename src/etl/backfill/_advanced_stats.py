"""
Backfill loaders for player advanced statistics.

This module handles loading of advanced player stats, shooting stats,
and play-by-play season stats from Basketball-Reference CSV exports.
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
    safe_str,
)
from src.etl.helpers import _flt, _int, int_season_to_id
from src.etl.validation import validate_rows

logger = logging.getLogger(__name__)


# Column mappings for Advanced.csv -> fact_player_advanced_season
_ADVANCED_COLUMN_MAP: dict[str, str] = {
    "player_id": "bref_player_id",
    "season": "season_id",
    "team": "team_abbrev",
    "pos": "pos",
    "age": "age",
    "g": "g",
    "gs": "gs",
    "mp": "mp",
    "per": "per",
    "ts_percent": "ts_pct",
    "x3p_ar": "x3p_ar",
    "f_tr": "f_tr",
    "orb_percent": "orb_pct",
    "drb_percent": "drb_pct",
    "trb_percent": "trb_pct",
    "ast_percent": "ast_pct",
    "stl_percent": "stl_pct",
    "blk_percent": "blk_pct",
    "tov_percent": "tov_pct",
    "usg_percent": "usg_pct",
    "ows": "ows",
    "dws": "dws",
    "ws": "ws",
    "ws_48": "ws_48",
    "obpm": "obpm",
    "dbpm": "dbpm",
    "bpm": "bpm",
    "vorp": "vorp",
}

# Column mappings for Player Shooting.csv -> fact_player_shooting_season
_SHOOTING_COLUMN_MAP: dict[str, str] = {
    "player_id": "bref_player_id",
    "season": "season_id",
    "team": "team_abbrev",
    "g": "g",
    "mp": "mp",
    "avg_dist_fga": "avg_dist_fga",
    "percent_fga_from_x2p_range": "pct_fga_2p",
    "percent_fga_from_x0_3_range": "pct_fga_0_3",
    "percent_fga_from_x3_10_range": "pct_fga_3_10",
    "percent_fga_from_x10_16_range": "pct_fga_10_16",
    "percent_fga_from_x16_3p_range": "pct_fga_16_3p",
    "percent_fga_from_x3p_range": "pct_fga_3p",
    "fg_percent_from_x2p_range": "fg_pct_2p",
    "fg_percent_from_x0_3_range": "fg_pct_0_3",
    "fg_percent_from_x3_10_range": "fg_pct_3_10",
    "fg_percent_from_x10_16_range": "fg_pct_10_16",
    "fg_percent_from_x16_3p_range": "fg_pct_16_3p",
    "fg_percent_from_x3p_range": "fg_pct_3p",
    "percent_assisted_x2p_fg": "pct_ast_2p",
    "percent_assisted_x3p_fg": "pct_ast_3p",
    "percent_dunks_of_fga": "pct_dunks_fga",
    "num_of_dunks": "num_dunks",
    "percent_corner_3s_of_3pa": "pct_corner3_3pa",
    "corner_3_point_percent": "corner3_pct",
}

# Column mappings for Player Play By Play.csv -> fact_player_pbp_season
_PBP_COLUMN_MAP: dict[str, str] = {
    "player_id": "bref_player_id",
    "season": "season_id",
    "team": "team_abbrev",
    "g": "g",
    "mp": "mp",
    "pg_percent": "pg_pct",
    "sg_percent": "sg_pct",
    "sf_percent": "sf_pct",
    "pf_percent": "pf_pct",
    "c_percent": "c_pct",
    "on_court_plus_minus_per_100_poss": "on_court_pm_per100",
    "net_plus_minus_per_100_poss": "net_pm_per100",
    "bad_pass_turnover": "bad_pass_tov",
    "lost_ball_turnover": "lost_ball_tov",
    "shooting_foul_committed": "shoot_foul_committed",
    "offensive_foul_committed": "off_foul_committed",
    "shooting_foul_drawn": "shoot_foul_drawn",
    "offensive_foul_drawn": "off_foul_drawn",
    "points_generated_by_assists": "pts_gen_by_ast",
    "and1": "and1",
    "fga_blocked": "fga_blocked",
}


def _transform_advanced_row(
    row: dict[str, Any],
    valid_seasons: set[str],
) -> dict[str, Any] | None:
    """
    Transform a row from Advanced.csv to fact_player_advanced_season schema.

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
        "bref_player_id": safe_str(row.get("player_id")),
        "season_id": season_id,
        "team_abbrev": safe_str(row.get("team")),
        "pos": safe_str(row.get("pos")),
        "age": _int(row.get("age")),
        "g": _int(row.get("g")),
        "gs": _int(row.get("gs")),
        "mp": _int(row.get("mp")),
        "per": _flt(row.get("per")),
        "ts_pct": _flt(row.get("ts_percent")),
        "x3p_ar": _flt(row.get("x3p_ar")),
        "f_tr": _flt(row.get("f_tr")),
        "orb_pct": _flt(row.get("orb_percent")),
        "drb_pct": _flt(row.get("drb_percent")),
        "trb_pct": _flt(row.get("trb_percent")),
        "ast_pct": _flt(row.get("ast_percent")),
        "stl_pct": _flt(row.get("stl_percent")),
        "blk_pct": _flt(row.get("blk_percent")),
        "tov_pct": _flt(row.get("tov_percent")),
        "usg_pct": _flt(row.get("usg_percent")),
        "ows": _flt(row.get("ows")),
        "dws": _flt(row.get("dws")),
        "ws": _flt(row.get("ws")),
        "ws_48": _flt(row.get("ws_48")),
        "obpm": _flt(row.get("obpm")),
        "dbpm": _flt(row.get("dbpm")),
        "bpm": _flt(row.get("bpm")),
        "vorp": _flt(row.get("vorp")),
    }


def _transform_shooting_row(
    row: dict[str, Any],
    valid_seasons: set[str],
) -> dict[str, Any] | None:
    """
    Transform a row from Player Shooting.csv to fact_player_shooting_season schema.

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
        "bref_player_id": safe_str(row.get("player_id")),
        "season_id": season_id,
        "team_abbrev": safe_str(row.get("team")),
        "g": _int(row.get("g")),
        "mp": _int(row.get("mp")),
        "avg_dist_fga": _flt(row.get("avg_dist_fga")),
        "pct_fga_2p": _flt(row.get("percent_fga_from_x2p_range")),
        "pct_fga_0_3": _flt(row.get("percent_fga_from_x0_3_range")),
        "pct_fga_3_10": _flt(row.get("percent_fga_from_x3_10_range")),
        "pct_fga_10_16": _flt(row.get("percent_fga_from_x10_16_range")),
        "pct_fga_16_3p": _flt(row.get("percent_fga_from_x16_3p_range")),
        "pct_fga_3p": _flt(row.get("percent_fga_from_x3p_range")),
        "fg_pct_2p": _flt(row.get("fg_percent_from_x2p_range")),
        "fg_pct_0_3": _flt(row.get("fg_percent_from_x0_3_range")),
        "fg_pct_3_10": _flt(row.get("fg_percent_from_x3_10_range")),
        "fg_pct_10_16": _flt(row.get("fg_percent_from_x10_16_range")),
        "fg_pct_16_3p": _flt(row.get("fg_percent_from_x16_3p_range")),
        "fg_pct_3p": _flt(row.get("fg_percent_from_x3p_range")),
        "pct_ast_2p": _flt(row.get("percent_assisted_x2p_fg")),
        "pct_ast_3p": _flt(row.get("percent_assisted_x3p_fg")),
        "pct_dunks_fga": _flt(row.get("percent_dunks_of_fga")),
        "num_dunks": _int(row.get("num_of_dunks")),
        "pct_corner3_3pa": _flt(row.get("percent_corner_3s_of_3pa")),
        "corner3_pct": _flt(row.get("corner_3_point_percent")),
    }


def _transform_pbp_row(
    row: dict[str, Any],
    valid_seasons: set[str],
) -> dict[str, Any] | None:
    """
    Transform a row from Player Play By Play.csv to fact_player_pbp_season schema.

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
        "bref_player_id": safe_str(row.get("player_id")),
        "season_id": season_id,
        "team_abbrev": safe_str(row.get("team")),
        "g": _int(row.get("g")),
        "mp": _int(row.get("mp")),
        "pg_pct": _flt(row.get("pg_percent")),
        "sg_pct": _flt(row.get("sg_percent")),
        "sf_pct": _flt(row.get("sf_percent")),
        "pf_pct": _flt(row.get("pf_percent")),
        "c_pct": _flt(row.get("c_percent")),
        "on_court_pm_per100": _flt(row.get("on_court_plus_minus_per_100_poss")),
        "net_pm_per100": _flt(row.get("net_plus_minus_per_100_poss")),
        "bad_pass_tov": _int(row.get("bad_pass_turnover")),
        "lost_ball_tov": _int(row.get("lost_ball_turnover")),
        "shoot_foul_committed": _int(row.get("shooting_foul_committed")),
        "off_foul_committed": _int(row.get("offensive_foul_committed")),
        "shoot_foul_drawn": _int(row.get("shooting_foul_drawn")),
        "off_foul_drawn": _int(row.get("offensive_foul_drawn")),
        "pts_gen_by_ast": _int(row.get("points_generated_by_assists")),
        "and1": _int(row.get("and1")),
        "fga_blocked": _int(row.get("fga_blocked")),
    }


def load_player_advanced(
    con: sqlite3.Connection,
    raw_dir: Path = RAW_DIR,
) -> None:
    """
    Load player advanced season stats from Advanced.csv.

    Args:
        con: SQLite database connection
        raw_dir: Directory containing raw CSV files
    """
    path = csv_path(raw_dir, "Advanced.csv")
    if path is None:
        return

    df = read_csv_safe(path, low_memory=False)
    valid_seasons = get_valid_set(con, "dim_season", "season_id")

    rows: list[dict] = []
    skipped = 0

    for row in df.to_dict("records"):
        transformed = _transform_advanced_row(row, valid_seasons)
        if transformed is None:
            skipped += 1
        else:
            rows.append(transformed)

    inserted = upsert_rows(
        con,
        "fact_player_advanced_season",
        validate_rows("fact_player_advanced_season", rows),
    )
    logger.info(
        "fact_player_advanced_season: %d inserted/ignored, %d skipped",
        inserted,
        skipped,
    )
    log_load_summary(con, "fact_player_advanced_season")


def load_player_shooting(
    con: sqlite3.Connection,
    raw_dir: Path = RAW_DIR,
) -> None:
    """
    Load player shooting season stats from Player Shooting.csv.

    Args:
        con: SQLite database connection
        raw_dir: Directory containing raw CSV files
    """
    path = csv_path(raw_dir, "Player Shooting.csv")
    if path is None:
        return

    df = read_csv_safe(path, low_memory=False)
    valid_seasons = get_valid_set(con, "dim_season", "season_id")

    rows: list[dict] = []
    skipped = 0

    for row in df.to_dict("records"):
        transformed = _transform_shooting_row(row, valid_seasons)
        if transformed is None:
            skipped += 1
        else:
            rows.append(transformed)

    inserted = upsert_rows(
        con,
        "fact_player_shooting_season",
        validate_rows("fact_player_shooting_season", rows),
    )
    logger.info(
        "fact_player_shooting_season: %d inserted/ignored, %d skipped",
        inserted,
        skipped,
    )
    log_load_summary(con, "fact_player_shooting_season")


def load_player_pbp_season(
    con: sqlite3.Connection,
    raw_dir: Path = RAW_DIR,
) -> None:
    """
    Load player play-by-play season stats from Player Play By Play.csv.

    Args:
        con: SQLite database connection
        raw_dir: Directory containing raw CSV files
    """
    path = csv_path(raw_dir, "Player Play By Play.csv")
    if path is None:
        return

    df = read_csv_safe(path, low_memory=False)
    valid_seasons = get_valid_set(con, "dim_season", "season_id")

    rows: list[dict] = []
    skipped = 0

    for row in df.to_dict("records"):
        transformed = _transform_pbp_row(row, valid_seasons)
        if transformed is None:
            skipped += 1
        else:
            rows.append(transformed)

    inserted = upsert_rows(
        con,
        "fact_player_pbp_season",
        validate_rows("fact_player_pbp_season", rows),
    )
    logger.info(
        "fact_player_pbp_season: %d inserted/ignored, %d skipped",
        inserted,
        skipped,
    )
    log_load_summary(con, "fact_player_pbp_season")
