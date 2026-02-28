"""Player advanced-stats backfill loaders."""

import sqlite3
from pathlib import Path
from typing import Any

from src.etl.backfill._base import RAW_DIR, safe_str
from src.etl.helpers import _flt, _int, int_season_to_id

from .base import BaseAdvancedStatsBackfill, logger


def _pct_01(value: Any) -> float | None:
    """
    Normalize percentage values to 0-1 scale.

    Basketball-Reference sometimes provides percentages in 0-100 scale
    (e.g., usg_percent=28.3) instead of 0-1 scale (usg_pct=0.283).
    This function normalizes any value > 1.0 to the 0-1 scale.

    Args:
        value: Raw percentage value (may be 0-100 or 0-1 scale)

    Returns:
        Normalized value in 0-1 scale, or None if input is None/invalid
    """
    v = _flt(value)
    if v is None:
        return None
    return v / 100.0 if v > 1.0 else v


def _transform_advanced_row(
    row: dict[str, Any],
    valid_seasons: set[str],
) -> dict[str, Any] | None:
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
        "ts_pct": _pct_01(row.get("ts_percent")),
        "x3p_ar": _pct_01(row.get("x3p_ar")),
        "f_tr": _pct_01(row.get("f_tr")),
        "orb_pct": _pct_01(row.get("orb_percent")),
        "drb_pct": _pct_01(row.get("drb_percent")),
        "trb_pct": _pct_01(row.get("trb_percent")),
        "ast_pct": _pct_01(row.get("ast_percent")),
        "stl_pct": _pct_01(row.get("stl_percent")),
        "blk_pct": _pct_01(row.get("blk_percent")),
        "tov_pct": _pct_01(row.get("tov_percent")),
        "usg_pct": _pct_01(row.get("usg_percent")),
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
        "pct_fga_2p": _pct_01(row.get("percent_fga_from_x2p_range")),
        "pct_fga_0_3": _pct_01(row.get("percent_fga_from_x0_3_range")),
        "pct_fga_3_10": _pct_01(row.get("percent_fga_from_x3_10_range")),
        "pct_fga_10_16": _pct_01(row.get("percent_fga_from_x10_16_range")),
        "pct_fga_16_3p": _pct_01(row.get("percent_fga_from_x16_3p_range")),
        "pct_fga_3p": _pct_01(row.get("percent_fga_from_x3p_range")),
        "fg_pct_2p": _pct_01(row.get("fg_percent_from_x2p_range")),
        "fg_pct_0_3": _pct_01(row.get("fg_percent_from_x0_3_range")),
        "fg_pct_3_10": _pct_01(row.get("fg_percent_from_x3_10_range")),
        "fg_pct_10_16": _pct_01(row.get("fg_percent_from_x10_16_range")),
        "fg_pct_16_3p": _pct_01(row.get("fg_percent_from_x16_3p_range")),
        "fg_pct_3p": _pct_01(row.get("fg_percent_from_x3p_range")),
        "pct_ast_2p": _pct_01(row.get("percent_assisted_x2p_fg")),
        "pct_ast_3p": _pct_01(row.get("percent_assisted_x3p_fg")),
        "pct_dunks_fga": _pct_01(row.get("percent_dunks_of_fga")),
        "num_dunks": _int(row.get("num_of_dunks")),
        "pct_corner3_3pa": _pct_01(row.get("percent_corner_3s_of_3pa")),
        "corner3_pct": _pct_01(row.get("corner_3_point_percent")),
    }


def _transform_pbp_row(
    row: dict[str, Any],
    valid_seasons: set[str],
) -> dict[str, Any] | None:
    season_id = int_season_to_id(row["season"])
    if season_id not in valid_seasons:
        return None

    return {
        "bref_player_id": safe_str(row.get("player_id")),
        "season_id": season_id,
        "team_abbrev": safe_str(row.get("team")),
        "g": _int(row.get("g")),
        "mp": _int(row.get("mp")),
        "pg_pct": _pct_01(row.get("pg_percent")),
        "sg_pct": _pct_01(row.get("sg_percent")),
        "sf_pct": _pct_01(row.get("sf_percent")),
        "pf_pct": _pct_01(row.get("pf_percent")),
        "c_pct": _pct_01(row.get("c_percent")),
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


class PlayerAdvancedStatsBackfill(BaseAdvancedStatsBackfill):
    """Backfill loader for player advanced stat CSVs."""

    def load_player_advanced(self) -> None:
        df = self._load_csv("Advanced.csv")
        if df is None:
            return
        valid_seasons = self._valid_seasons()

        rows: list[dict] = []
        skipped = 0
        for row in df.to_dict("records"):
            transformed = _transform_advanced_row(row, valid_seasons)
            if transformed is None:
                skipped += 1
            else:
                rows.append(transformed)

        inserted = self._upsert("fact_player_advanced_season", rows)
        logger.info(
            "fact_player_advanced_season: %d inserted/ignored, %d skipped", inserted, skipped
        )

    def load_player_shooting(self) -> None:
        df = self._load_csv("Player Shooting.csv")
        if df is None:
            return
        valid_seasons = self._valid_seasons()

        rows: list[dict] = []
        skipped = 0
        for row in df.to_dict("records"):
            transformed = _transform_shooting_row(row, valid_seasons)
            if transformed is None:
                skipped += 1
            else:
                rows.append(transformed)

        inserted = self._upsert("fact_player_shooting_season", rows)
        logger.info(
            "fact_player_shooting_season: %d inserted/ignored, %d skipped", inserted, skipped
        )

    def load_player_pbp_season(self) -> None:
        df = self._load_csv("Player Play By Play.csv")
        if df is None:
            return
        valid_seasons = self._valid_seasons()

        rows: list[dict] = []
        skipped = 0
        for row in df.to_dict("records"):
            transformed = _transform_pbp_row(row, valid_seasons)
            if transformed is None:
                skipped += 1
            else:
                rows.append(transformed)

        inserted = self._upsert("fact_player_pbp_season", rows)
        logger.info("fact_player_pbp_season: %d inserted/ignored, %d skipped", inserted, skipped)


def load_player_advanced(con: sqlite3.Connection, raw_dir: Path = RAW_DIR) -> None:
    """Compatibility function wrapper."""
    PlayerAdvancedStatsBackfill(con, raw_dir).load_player_advanced()


def load_player_shooting(con: sqlite3.Connection, raw_dir: Path = RAW_DIR) -> None:
    """Compatibility function wrapper."""
    PlayerAdvancedStatsBackfill(con, raw_dir).load_player_shooting()


def load_player_pbp_season(con: sqlite3.Connection, raw_dir: Path = RAW_DIR) -> None:
    """Compatibility function wrapper."""
    PlayerAdvancedStatsBackfill(con, raw_dir).load_player_pbp_season()
