"""
Backfill loaders for season-level statistics.

This module handles loading of aggregated season statistics from
Basketball-Reference CSV exports into fact tables for team and player
season stats, as well as league-wide averages.
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
    safe_str,
)
from src.etl.helpers import _flt, _int, _isna, int_season_to_id
from src.etl.utils import log_load_summary, upsert_rows
from src.etl.validate import validate_rows

logger = logging.getLogger(__name__)


def _parse_playoffs_flag(value: Any) -> int:
    """
    Parse a playoffs flag value to integer (0 or 1).

    Args:
        value: Raw playoffs value from CSV

    Returns:
        1 if playoffs, 0 otherwise
    """
    if _isna(value):
        return 0
    return 1 if str(value).lower() == "true" else 0


def _transform_team_season_row(
    row: dict[str, Any],
    valid_seasons: set[str],
) -> dict[str, Any] | None:
    """
    Transform a row from Team Summaries.csv to fact_team_season schema.

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
        "bref_abbrev": safe_str(row.get("abbreviation")),
        "lg": safe_str(row.get("lg")) or "NBA",
        "playoffs": _parse_playoffs_flag(row.get("playoffs")),
        "w": _int(row.get("w")),
        "l": _int(row.get("l")),
        "pw": _flt(row.get("pw")),
        "pl": _flt(row.get("pl")),
        "mov": _flt(row.get("mov")),
        "sos": _flt(row.get("sos")),
        "srs": _flt(row.get("srs")),
        "o_rtg": _flt(row.get("o_rtg")),
        "d_rtg": _flt(row.get("d_rtg")),
        "n_rtg": _flt(row.get("n_rtg")),
        "pace": _flt(row.get("pace")),
        "ts_pct": _flt(row.get("ts_percent")),
        "e_fg_pct": _flt(row.get("e_fg_percent")),
        "tov_pct": _flt(row.get("tov_percent")),
        "orb_pct": _flt(row.get("orb_percent")),
        "ft_fga": _flt(row.get("ft_fga")),
        "opp_e_fg_pct": _flt(row.get("opp_e_fg_percent")),
        "opp_tov_pct": _flt(row.get("opp_tov_percent")),
        "drb_pct": _flt(row.get("drb_percent")),
        "opp_ft_fga": _flt(row.get("opp_ft_fga")),
        "arena": safe_str(row.get("arena")),
        "attend": _int(row.get("attend")),
        "attend_g": _int(row.get("attend_g")),
    }


def _transform_player_season_stats_row(
    row: dict[str, Any],
    valid_seasons: set[str],
) -> dict[str, Any] | None:
    """
    Transform a row from Player Totals.csv to fact_player_season_stats schema.

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
        "lg": safe_str(row.get("lg")) or "NBA",
        "team_abbrev": safe_str(row.get("team")),
        "pos": safe_str(row.get("pos")),
        "age": _int(row.get("age")),
        "g": _int(row.get("g")),
        "gs": _int(row.get("gs")),
        "mp": _int(row.get("mp")),
        "fg": _int(row.get("fg")),
        "fga": _int(row.get("fga")),
        "x3p": _int(row.get("x3p")),
        "x3pa": _int(row.get("x3pa")),
        "ft": _int(row.get("ft")),
        "fta": _int(row.get("fta")),
        "orb": _int(row.get("orb")),
        "drb": _int(row.get("drb")),
        "reb": _int(row.get("trb")),
        "ast": _int(row.get("ast")),
        "stl": _int(row.get("stl")),
        "blk": _int(row.get("blk")),
        "tov": _int(row.get("tov")),
        "pf": _int(row.get("pf")),
        "pts": _int(row.get("pts")),
    }


def load_team_season(
    con: sqlite3.Connection,
    raw_dir: Path = RAW_DIR,
) -> None:
    """
    Load team season stats from Team Summaries.csv.

    Args:
        con: SQLite database connection
        raw_dir: Directory containing raw CSV files
    """
    path = csv_path(raw_dir, "Team Summaries.csv")
    if path is None:
        return

    df = read_csv_safe(path)
    valid_seasons = get_valid_set(con, "dim_season", "season_id")

    rows: list[dict] = []
    skipped = 0

    for row in df.to_dict("records"):
        transformed = _transform_team_season_row(row, valid_seasons)
        if transformed is None:
            skipped += 1
        else:
            rows.append(transformed)

    inserted = upsert_rows(con, "fact_team_season", rows)
    logger.info(
        "fact_team_season: %d inserted/ignored, %d skipped",
        inserted,
        skipped,
    )


def load_league_season(
    con: sqlite3.Connection,
    raw_dir: Path = RAW_DIR,
) -> None:
    """
    Load league-wide season averages from Team Summaries.csv and Team Stats Per Game.csv.

    Aggregates team stats to compute league averages for pace, rating,
    and per-game statistics.

    Args:
        con: SQLite database connection
        raw_dir: Directory containing raw CSV files
    """
    summaries_path = csv_path(raw_dir, "Team Summaries.csv")
    if summaries_path is None:
        return

    summaries = read_csv_safe(summaries_path)
    valid_seasons = get_valid_set(con, "dim_season", "season_id")

    # Aggregate pace and offensive rating from Team Summaries
    pace_ortg = (
        summaries.groupby("season")
        .agg(
            num_teams=("team", "count"),
            avg_pace=("pace", "mean"),
            avg_ortg=("o_rtg", "mean"),
        )
        .reset_index()
    )

    # Column mapping for per-game stats
    per_game_cols: dict[str, str] = {
        "avg_pts": "pts_per_game",
        "avg_fga": "fga_per_game",
        "avg_fta": "fta_per_game",
        "avg_trb": "trb_per_game",
        "avg_ast": "ast_per_game",
        "avg_stl": "stl_per_game",
        "avg_blk": "blk_per_game",
        "avg_tov": "tov_per_game",
    }

    # Merge with per-game stats if available
    per_game_path = csv_path(raw_dir, "Team Stats Per Game.csv")
    if per_game_path is not None:
        pg_df = read_csv_safe(per_game_path)
        per_game_agg = (
            pg_df.groupby("season")
            .agg(**{out: (src, "mean") for out, src in per_game_cols.items()})
            .reset_index()
        )
        merged = pace_ortg.merge(per_game_agg, on="season", how="left")
    else:
        merged = pace_ortg
        for col in per_game_cols:
            merged[col] = None

    rows: list[dict] = []
    for row in merged.to_dict("records"):
        season_id = int_season_to_id(row["season"])
        if season_id not in valid_seasons:
            continue

        rows.append(
            {
                "season_id": season_id,
                "num_teams": int(row["num_teams"]),
                "avg_pace": _flt(row.get("avg_pace")),
                "avg_ortg": _flt(row.get("avg_ortg")),
                "avg_pts": _flt(row.get("avg_pts")),
                "avg_fga": _flt(row.get("avg_fga")),
                "avg_fta": _flt(row.get("avg_fta")),
                "avg_trb": _flt(row.get("avg_trb")),
                "avg_ast": _flt(row.get("avg_ast")),
                "avg_stl": _flt(row.get("avg_stl")),
                "avg_blk": _flt(row.get("avg_blk")),
                "avg_tov": _flt(row.get("avg_tov")),
            }
        )

    inserted = upsert_rows(con, "dim_league_season", rows, conflict="REPLACE")
    logger.info("dim_league_season: %d rows upserted", inserted)


def load_player_season_stats(
    con: sqlite3.Connection,
    raw_dir: Path = RAW_DIR,
) -> None:
    """
    Load player season totals from Player Totals.csv.

    Args:
        con: SQLite database connection
        raw_dir: Directory containing raw CSV files
    """
    path = csv_path(raw_dir, "Player Totals.csv")
    if path is None:
        return

    df = read_csv_safe(path, low_memory=False)
    valid_seasons = get_valid_set(con, "dim_season", "season_id")

    rows: list[dict] = []
    skipped = 0

    for row in df.to_dict("records"):
        transformed = _transform_player_season_stats_row(row, valid_seasons)
        if transformed is None:
            skipped += 1
        else:
            rows.append(transformed)

    inserted = upsert_rows(
        con,
        "fact_player_season_stats",
        validate_rows("fact_player_season_stats", rows),
    )
    logger.info(
        "fact_player_season_stats: %d inserted/ignored, %d skipped",
        inserted,
        skipped,
    )
    log_load_summary(con, "fact_player_season_stats")
