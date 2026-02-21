import logging
import sqlite3
from pathlib import Path

import pandas as pd

from src.etl.helpers import _isna, int_season_to_id
from src.etl.utils import upsert_rows

logger = logging.getLogger(__name__)
RAW_DIR = Path("raw")

_AWARD_MAP = {
    "Most Valuable Player": "MVP",
    "Rookie of the Year": "ROY",
    "Defensive Player of the Year": "DPOY",
    "Sixth Man of the Year": "SMOY",
    "Most Improved Player": "MIP",
    "Finals Most Valuable Player": "FMVP",
}

def _eos_award_name(raw: str) -> str | None:
    for k, v in _AWARD_MAP.items():
        if k in raw:
            return v
    return None

def _bref_to_player_id(
    con: sqlite3.Connection,
) -> dict[str, str]:
    """Build bref_id → player_id lookup from dim_player."""
    rows = con.execute(
        "SELECT bref_id, player_id FROM dim_player WHERE bref_id IS NOT NULL"
    ).fetchall()
    return dict(rows)

def load_awards(con: sqlite3.Connection, raw_dir: Path = RAW_DIR) -> None:
    bref_to_pid = _bref_to_player_id(con)
    valid_seasons = {r[0] for r in con.execute("SELECT season_id FROM dim_season")}

    total_inserted = 0

    # --- Player Award Shares ---
    aws_path = raw_dir / "Player Award Shares.csv"
    if aws_path.exists():
        df = pd.read_csv(aws_path)
        rows: list[dict] = []
        for row in df.to_dict("records"):
            season_id = int_season_to_id(row["season"])
            if season_id not in valid_seasons:
                continue
            bref_pid = str(row["player_id"]).strip()
            player_id = bref_to_pid.get(bref_pid)
            if not player_id:
                continue

            award_raw = str(row.get("award", "")).strip().lower()
            award_name, award_type = _AWARD_MAP.get(
                award_raw, (award_raw.upper(), "individual")
            )

            rows.append({
                "player_id":      player_id,
                "season_id":      season_id,
                "award_name":     award_name,
                "award_type":     award_type,
                "trophy_name":    None,
                "votes_received": int(row["pts_won"]) if not _isna(row.get("pts_won")) else None,
                "votes_possible": int(row["pts_max"]) if not _isna(row.get("pts_max")) else None,
            })
        inserted = upsert_rows(con, "fact_player_award", rows)
        total_inserted += inserted
        logger.info("fact_player_award (award shares): %d inserted/ignored", inserted)

    # --- All-Star Selections ---
    allstar_path = raw_dir / "All-Star Selections.csv"
    if allstar_path.exists():
        df = pd.read_csv(allstar_path)
        rows = []
        for row in df.to_dict("records"):
            season_id = int_season_to_id(row["season"])
            if season_id not in valid_seasons:
                continue
            bref_pid = str(row["player_id"]).strip()
            player_id = bref_to_pid.get(bref_pid)
            if not player_id:
                continue
            rows.append({
                "player_id":      player_id,
                "season_id":      season_id,
                "award_name":     "All-Star",
                "award_type":     "team_inclusion",
                "trophy_name":    None,
                "votes_received": None,
                "votes_possible": None,
            })
        inserted = upsert_rows(con, "fact_player_award", rows)
        total_inserted += inserted
        logger.info("fact_player_award (all-star): %d inserted/ignored", inserted)

    # --- End of Season Teams (Voting) — preferred source with vote data ---
    eos_voting_path = raw_dir / "End of Season Teams (Voting).csv"
    eos_path        = raw_dir / "End of Season Teams.csv"

    def _eos_award_name(type_: str, number_tm: str) -> str:
        type_clean = str(type_).strip().replace("_", "-").title()
        return f"{type_clean} {str(number_tm).strip()}"

    if eos_voting_path.exists():
        df = pd.read_csv(eos_voting_path)
        rows = []
        for row in df.to_dict("records"):
            season_id = int_season_to_id(row["season"])
            if season_id not in valid_seasons:
                continue
            bref_pid = str(row["player_id"]).strip()
            player_id = bref_to_pid.get(bref_pid)
            if not player_id:
                continue
            award_name = _eos_award_name(row["type"], row["number_tm"])
            rows.append({
                "player_id":      player_id,
                "season_id":      season_id,
                "award_name":     award_name,
                "award_type":     "team_inclusion",
                "trophy_name":    None,
                "votes_received": int(row["pts_won"]) if not _isna(row.get("pts_won")) else None,
                "votes_possible": int(row["pts_max"]) if not _isna(row.get("pts_max")) else None,
            })
        inserted = upsert_rows(con, "fact_player_award", rows)
        total_inserted += inserted
        logger.info("fact_player_award (EOS voting): %d inserted/ignored", inserted)

    elif eos_path.exists():
        df = pd.read_csv(eos_path)
        rows = []
        for row in df.to_dict("records"):
            season_id = int_season_to_id(row["season"])
            if season_id not in valid_seasons:
                continue
            bref_pid = str(row["player_id"]).strip()
            player_id = bref_to_pid.get(bref_pid)
            if not player_id:
                continue
            award_name = _eos_award_name(row["type"], row["number_tm"])
            rows.append({
                "player_id":      player_id,
                "season_id":      season_id,
                "award_name":     award_name,
                "award_type":     "team_inclusion",
                "trophy_name":    None,
                "votes_received": None,
                "votes_possible": None,
            })
        inserted = upsert_rows(con, "fact_player_award", rows)
        total_inserted += inserted
        logger.info("fact_player_award (EOS teams): %d inserted/ignored", inserted)

    logger.info("fact_player_award total: %d inserted/ignored", total_inserted)
