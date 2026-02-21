import logging
import sqlite3
from pathlib import Path

import pandas as pd

from src.etl.helpers import _flt, _int, _isna, int_season_to_id
from src.etl.utils import log_load_summary, upsert_rows
from src.etl.validate import validate_rows

logger = logging.getLogger(__name__)
RAW_DIR = Path("raw")

def load_team_season(con: sqlite3.Connection, raw_dir: Path = RAW_DIR) -> None:
    path = raw_dir / "Team Summaries.csv"
    if not path.exists():
        logger.warning("Team Summaries.csv not found, skipping")
        return

    df = pd.read_csv(path)
    valid_seasons = {r[0] for r in con.execute("SELECT season_id FROM dim_season")}

    rows: list[dict] = []
    skipped = 0

    for row in df.to_dict("records"):
        season_id = int_season_to_id(row["season"])
        if season_id not in valid_seasons:
            skipped += 1
            continue

        rows.append({
            "season_id":   season_id,
            "bref_abbrev": str(row["abbreviation"]).strip(),
            "lg":          str(row.get("lg", "NBA")).strip(),
            "playoffs":    1 if str(row.get("playoffs", "False")).lower() == "true" else 0,
            "w":     _int(row.get("w")),
            "l":     _int(row.get("l")),
            "pw":    _flt(row.get("pw")),
            "pl":    _flt(row.get("pl")),
            "mov":   _flt(row.get("mov")),
            "sos":   _flt(row.get("sos")),
            "srs":   _flt(row.get("srs")),
            "o_rtg": _flt(row.get("o_rtg")),
            "d_rtg": _flt(row.get("d_rtg")),
            "n_rtg": _flt(row.get("n_rtg")),
            "pace":  _flt(row.get("pace")),
            "ts_pct":     _flt(row.get("ts_percent")),
            "e_fg_pct":   _flt(row.get("e_fg_percent")),
            "tov_pct":    _flt(row.get("tov_percent")),
            "orb_pct":    _flt(row.get("orb_percent")),
            "ft_fga":     _flt(row.get("ft_fga")),
            "opp_e_fg_pct": _flt(row.get("opp_e_fg_percent")),
            "opp_tov_pct":  _flt(row.get("opp_tov_percent")),
            "drb_pct":      _flt(row.get("drb_percent")),
            "opp_ft_fga":   _flt(row.get("opp_ft_fga")),
            "arena":     str(row["arena"]).strip() if not _isna(row.get("arena")) else None,
            "attend":    _int(row.get("attend")),
            "attend_g":  _int(row.get("attend_g")),
        })

    inserted = upsert_rows(con, "fact_team_season", rows)
    logger.info(
        "fact_team_season: %d inserted/ignored, %d skipped", inserted, skipped
    )

def load_league_season(con: sqlite3.Connection, raw_dir: Path = RAW_DIR) -> None:
    summaries_path = raw_dir / "Team Summaries.csv"
    per_game_path  = raw_dir / "Team Stats Per Game.csv"

    if not summaries_path.exists():
        logger.warning("Team Summaries.csv not found, skipping league_season")
        return

    summaries = pd.read_csv(summaries_path)
    valid_seasons = {r[0] for r in con.execute("SELECT season_id FROM dim_season")}

    # Pace and ortg from Team Summaries.
    pace_ortg = (
        summaries.groupby("season")
        .agg(num_teams=("team", "count"), avg_pace=("pace", "mean"), avg_ortg=("o_rtg", "mean"))
        .reset_index()
    )

    # Per-game scoring/volume averages from Team Stats Per Game.
    per_game_cols = {
        "avg_pts": "pts_per_game",
        "avg_fga": "fga_per_game",
        "avg_fta": "fta_per_game",
        "avg_trb": "trb_per_game",
        "avg_ast": "ast_per_game",
        "avg_stl": "stl_per_game",
        "avg_blk": "blk_per_game",
        "avg_tov": "tov_per_game",
    }
    if per_game_path.exists():
        pg_df = pd.read_csv(per_game_path)
        per_game_agg = pg_df.groupby("season").agg(
            **{out: (src, "mean") for out, src in per_game_cols.items()}
        ).reset_index()
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

        rows.append({
            "season_id": season_id,
            "num_teams": int(row["num_teams"]),
            "avg_pace":  _flt(row.get("avg_pace")),
            "avg_ortg":  _flt(row.get("avg_ortg")),
            "avg_pts":   _flt(row.get("avg_pts")),
            "avg_fga":   _flt(row.get("avg_fga")),
            "avg_fta":   _flt(row.get("avg_fta")),
            "avg_trb":   _flt(row.get("avg_trb")),
            "avg_ast":   _flt(row.get("avg_ast")),
            "avg_stl":   _flt(row.get("avg_stl")),
            "avg_blk":   _flt(row.get("avg_blk")),
            "avg_tov":   _flt(row.get("avg_tov")),
        })

    inserted = upsert_rows(con, "dim_league_season", rows, conflict="REPLACE")
    logger.info("dim_league_season: %d rows upserted", inserted)

def load_player_season_stats(
    con: sqlite3.Connection, raw_dir: Path = RAW_DIR
) -> None:
    path = raw_dir / "Player Totals.csv"
    if not path.exists():
        logger.warning("Player Totals.csv not found, skipping")
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
            "lg":          str(row.get("lg", "NBA")).strip(),
            "team_abbrev": str(row["team"]).strip() if not _isna(row.get("team")) else None,
            "pos":         str(row["pos"]).strip()  if not _isna(row.get("pos"))  else None,
            "age":   _int(row.get("age")),
            "g":     _int(row.get("g")),
            "gs":    _int(row.get("gs")),
            "mp":    _int(row.get("mp")),
            "fg":    _int(row.get("fg")),
            "fga":   _int(row.get("fga")),
            "x3p":   _int(row.get("x3p")),
            "x3pa":  _int(row.get("x3pa")),
            "ft":    _int(row.get("ft")),
            "fta":   _int(row.get("fta")),
            "orb":   _int(row.get("orb")),
            "drb":   _int(row.get("drb")),
            "reb":   _int(row.get("trb")),
            "ast":   _int(row.get("ast")),
            "stl":   _int(row.get("stl")),
            "blk":   _int(row.get("blk")),
            "tov":   _int(row.get("tov")),
            "pf":    _int(row.get("pf")),
            "pts":   _int(row.get("pts")),
        })

    inserted = upsert_rows(con, "fact_player_season_stats", validate_rows("fact_player_season_stats", rows))
    logger.info(
        "fact_player_season_stats: %d inserted/ignored, %d skipped", inserted, skipped
    )
    log_load_summary(con, "fact_player_season_stats")
