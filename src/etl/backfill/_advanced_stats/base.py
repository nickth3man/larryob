"""Shared advanced-stats backfill infrastructure."""

import logging
import sqlite3
from pathlib import Path

from src.db.operations import upsert_rows
from src.db.tracking import log_load_summary
from src.etl.backfill._base import (
    RAW_DIR,
    csv_path,
    get_valid_set,
    read_csv_safe,
)
from src.etl.validation import validate_rows

logger = logging.getLogger(__name__)

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


class BaseAdvancedStatsBackfill:
    """Common helpers for advanced stats backfill loaders."""

    def __init__(self, con: sqlite3.Connection, raw_dir: Path = RAW_DIR):
        self.con = con
        self.raw_dir = raw_dir

    def _load_csv(self, filename: str):
        path = csv_path(self.raw_dir, filename)
        if path is None:
            return None
        return read_csv_safe(path, low_memory=False)

    def _valid_seasons(self) -> set[str]:
        return get_valid_set(self.con, "dim_season", "season_id")

    def _upsert(self, table: str, rows: list[dict]) -> int:
        inserted = upsert_rows(self.con, table, validate_rows(table, rows))
        log_load_summary(self.con, table)
        return inserted
