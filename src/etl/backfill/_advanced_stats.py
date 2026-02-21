import logging
import sqlite3
from pathlib import Path

import pandas as pd

from src.etl.helpers import _flt, _int, _isna, int_season_to_id
from src.etl.utils import log_load_summary, upsert_rows
from src.etl.validate import validate_rows

logger = logging.getLogger(__name__)
RAW_DIR = Path("raw")

def load_player_advanced(
    con: sqlite3.Connection, raw_dir: Path = RAW_DIR
) -> None:
    path = raw_dir / "Advanced.csv"
    if not path.exists():
        logger.warning("Advanced.csv not found, skipping")
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
            "bref_player_id": str(row["player_id"]).strip(),
            "season_id":   season_id,
            "team_abbrev": str(row["team"]).strip() if not _isna(row.get("team")) else None,
            "pos": str(row["pos"]).strip() if not _isna(row.get("pos")) else None,
            "age": _int(row.get("age")),
            "g":   _int(row.get("g")),
            "gs":  _int(row.get("gs")),
            "mp":  _int(row.get("mp")),
            "per":     _flt(row.get("per")),
            "ts_pct":  _flt(row.get("ts_percent")),
            "x3p_ar":  _flt(row.get("x3p_ar")),
            "f_tr":    _flt(row.get("f_tr")),
            "orb_pct": _flt(row.get("orb_percent")),
            "drb_pct": _flt(row.get("drb_percent")),
            "trb_pct": _flt(row.get("trb_percent")),
            "ast_pct": _flt(row.get("ast_percent")),
            "stl_pct": _flt(row.get("stl_percent")),
            "blk_pct": _flt(row.get("blk_percent")),
            "tov_pct": _flt(row.get("tov_percent")),
            "usg_pct": _flt(row.get("usg_percent")),
            "ows":    _flt(row.get("ows")),
            "dws":    _flt(row.get("dws")),
            "ws":     _flt(row.get("ws")),
            "ws_48":  _flt(row.get("ws_48")),
            "obpm":   _flt(row.get("obpm")),
            "dbpm":   _flt(row.get("dbpm")),
            "bpm":    _flt(row.get("bpm")),
            "vorp":   _flt(row.get("vorp")),
        })

    inserted = upsert_rows(con, "fact_player_advanced_season", validate_rows("fact_player_advanced_season", rows))
    logger.info(
        "fact_player_advanced_season: %d inserted/ignored, %d skipped", inserted, skipped
    )
    log_load_summary(con, "fact_player_advanced_season")

def load_player_shooting(
    con: sqlite3.Connection, raw_dir: Path = RAW_DIR
) -> None:
    path = raw_dir / "Player Shooting.csv"
    if not path.exists():
        logger.warning("Player Shooting.csv not found, skipping")
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
            "bref_player_id": str(row["player_id"]).strip(),
            "season_id":   season_id,
            "team_abbrev": str(row["team"]).strip() if not _isna(row.get("team")) else None,
            "g":  int(row["g"])  if not _isna(row.get("g"))  else None,
            "mp": int(row["mp"]) if not _isna(row.get("mp")) else None,
            "avg_dist_fga":   _flt(row.get("avg_dist_fga")),
            "pct_fga_2p":     _flt(row.get("percent_fga_from_x2p_range")),
            "pct_fga_0_3":    _flt(row.get("percent_fga_from_x0_3_range")),
            "pct_fga_3_10":   _flt(row.get("percent_fga_from_x3_10_range")),
            "pct_fga_10_16":  _flt(row.get("percent_fga_from_x10_16_range")),
            "pct_fga_16_3p":  _flt(row.get("percent_fga_from_x16_3p_range")),
            "pct_fga_3p":     _flt(row.get("percent_fga_from_x3p_range")),
            "fg_pct_2p":      _flt(row.get("fg_percent_from_x2p_range")),
            "fg_pct_0_3":     _flt(row.get("fg_percent_from_x0_3_range")),
            "fg_pct_3_10":    _flt(row.get("fg_percent_from_x3_10_range")),
            "fg_pct_10_16":   _flt(row.get("fg_percent_from_x10_16_range")),
            "fg_pct_16_3p":   _flt(row.get("fg_percent_from_x16_3p_range")),
            "fg_pct_3p":      _flt(row.get("fg_percent_from_x3p_range")),
            "pct_ast_2p":     _flt(row.get("percent_assisted_x2p_fg")),
            "pct_ast_3p":     _flt(row.get("percent_assisted_x3p_fg")),
            "pct_dunks_fga":  _flt(row.get("percent_dunks_of_fga")),
            "num_dunks":      int(row["num_of_dunks"]) if not _isna(row.get("num_of_dunks")) else None,
            "pct_corner3_3pa": _flt(row.get("percent_corner_3s_of_3pa")),
            "corner3_pct":     _flt(row.get("corner_3_point_percent")),
        })

    inserted = upsert_rows(con, "fact_player_shooting_season", validate_rows("fact_player_shooting_season", rows))
    logger.info(
        "fact_player_shooting_season: %d inserted/ignored, %d skipped", inserted, skipped
    )
    log_load_summary(con, "fact_player_shooting_season")

def load_player_pbp_season(
    con: sqlite3.Connection, raw_dir: Path = RAW_DIR
) -> None:
    path = raw_dir / "Player Play By Play.csv"
    if not path.exists():
        logger.warning("Player Play By Play.csv not found, skipping")
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
            "bref_player_id": str(row["player_id"]).strip(),
            "season_id":   season_id,
            "team_abbrev": str(row["team"]).strip() if not _isna(row.get("team")) else None,
            "g":  _int(row.get("g")),
            "mp": _int(row.get("mp")),
            "pg_pct": _flt(row.get("pg_percent")),
            "sg_pct": _flt(row.get("sg_percent")),
            "sf_pct": _flt(row.get("sf_percent")),
            "pf_pct": _flt(row.get("pf_percent")),
            "c_pct":  _flt(row.get("c_percent")),
            "on_court_pm_per100": _flt(row.get("on_court_plus_minus_per_100_poss")),
            "net_pm_per100":      _flt(row.get("net_plus_minus_per_100_poss")),
            "bad_pass_tov":          _int(row.get("bad_pass_turnover")),
            "lost_ball_tov":         _int(row.get("lost_ball_turnover")),
            "shoot_foul_committed":  _int(row.get("shooting_foul_committed")),
            "off_foul_committed":    _int(row.get("offensive_foul_committed")),
            "shoot_foul_drawn":      _int(row.get("shooting_foul_drawn")),
            "off_foul_drawn":        _int(row.get("offensive_foul_drawn")),
            "pts_gen_by_ast": _int(row.get("points_generated_by_assists")),
            "and1":           _int(row.get("and1")),
            "fga_blocked":    _int(row.get("fga_blocked")),
        })

    inserted = upsert_rows(con, "fact_player_pbp_season", validate_rows("fact_player_pbp_season", rows))
    logger.info(
        "fact_player_pbp_season: %d inserted/ignored, %d skipped", inserted, skipped
    )
    log_load_summary(con, "fact_player_pbp_season")
