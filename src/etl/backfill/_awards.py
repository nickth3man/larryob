"""
Backfill loaders for player awards and honors.

This module handles loading of award voting data, All-Star selections,
and End of Season teams from Basketball-Reference CSV exports.
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
    safe_int,
    safe_str,
)
from src.etl.helpers import int_season_to_id
from src.etl.identity.resolver import resolve_or_create_player

logger = logging.getLogger(__name__)


# Award name normalization mapping
_AWARD_MAP: dict[str, tuple[str, str]] = {
    "most valuable player": ("MVP", "individual"),
    "rookie of the year": ("ROY", "individual"),
    "defensive player of the year": ("DPOY", "individual"),
    "sixth man of the year": ("SMOY", "individual"),
    "most improved player": ("MIP", "individual"),
    "finals most valuable player": ("FMVP", "individual"),
}


def _normalize_award_name(raw: str) -> tuple[str, str]:
    """
    Normalize an award name to standard format.

    Args:
        raw: Raw award name from CSV

    Returns:
        Tuple of (normalized_name, award_type)
    """
    raw_lc = str(raw).lower().strip()
    for pattern, (name, award_type) in _AWARD_MAP.items():
        if pattern in raw_lc:
            return (name, award_type)
    # Return uppercase original with default type
    return (raw.upper(), "individual")


def _format_eos_team_award_name(type_: str, number_tm: str) -> str:
    """
    Format End of Season team award name.

    Args:
        type_: Team type (e.g., "all_nba", "all_defense")
        number_tm: Team number (e.g., "1st", "2nd")

    Returns:
        Formatted award name (e.g., "All-Nba 1st")
    """
    type_clean = str(type_).strip().replace("_", "-").title()
    return f"{type_clean} {str(number_tm).strip()}"


def _build_bref_to_player_id_lookup(con: sqlite3.Connection) -> dict[str, str]:
    """
    Build bref_id → player_id lookup from dim_player.

    Args:
        con: SQLite database connection

    Returns:
        Dictionary mapping bref_id to player_id
    """
    rows = con.execute(
        "SELECT bref_id, player_id FROM dim_player WHERE bref_id IS NOT NULL"
    ).fetchall()
    return dict(rows)


class AwardsLoader:
    """
    Loader for player awards data from multiple CSV sources.

    Handles loading from:
    - Player Award Shares.csv (MVP, DPOY, etc. voting)
    - All-Star Selections.csv
    - End of Season Teams (Voting).csv (preferred) or End of Season Teams.csv
    """

    def __init__(self, raw_dir: Path = RAW_DIR):
        self.raw_dir = raw_dir
        self.bref_to_pid: dict[str, str] = {}
        self.valid_seasons: set[str] = set()
        self.total_inserted = 0

    def _init_lookups(self, con: sqlite3.Connection) -> None:
        """Initialize lookup dictionaries."""
        self.bref_to_pid = _build_bref_to_player_id_lookup(con)
        self.valid_seasons = get_valid_set(con, "dim_season", "season_id")

    def _is_valid_row(
        self,
        row: dict[str, Any],
        season_id: str,
        bref_pid: str,
    ) -> bool:
        """Check if a row should be processed."""
        if season_id not in self.valid_seasons:
            return False
        if not self.bref_to_pid.get(bref_pid):
            return False
        return True

    def _load_award_shares(self, con: sqlite3.Connection) -> int:
        """Load player award voting data from Player Award Shares.csv."""
        path = csv_path(self.raw_dir, "Player Award Shares.csv")
        if path is None:
            return 0

        df = read_csv_safe(path)
        rows: list[dict] = []

        for row in df.to_dict("records"):
            season_id = int_season_to_id(row["season"])
            bref_pid = safe_str(row.get("player_id"), strip=True) or ""
            player_id = self.bref_to_pid.get(bref_pid)

            if season_id not in self.valid_seasons:
                continue

            if not player_id and bref_pid:
                full_name = safe_str(row.get("player"), strip=True) or bref_pid
                player_id = resolve_or_create_player(con, "bref", bref_pid, full_name)

            if not player_id:
                continue

            award_raw = safe_str(row.get("award"), strip=True) or ""
            award_name, award_type = _normalize_award_name(award_raw)

            rows.append(
                {
                    "player_id": player_id,
                    "season_id": season_id,
                    "award_name": award_name,
                    "award_type": award_type,
                    "trophy_name": None,
                    "votes_received": safe_int(row.get("pts_won")),
                    "votes_possible": safe_int(row.get("pts_max")),
                }
            )

        inserted = upsert_rows(con, "fact_player_award", rows)
        logger.info("fact_player_award (award shares): %d inserted/ignored", inserted)
        return inserted

    def _load_all_star_selections(self, con: sqlite3.Connection) -> int:
        """Load All-Star selections from All-Star Selections.csv."""
        path = csv_path(self.raw_dir, "All-Star Selections.csv")
        if path is None:
            return 0

        df = read_csv_safe(path)
        rows: list[dict] = []

        for row in df.to_dict("records"):
            season_id = int_season_to_id(row["season"])
            bref_pid = safe_str(row.get("player_id"), strip=True) or ""
            player_id = self.bref_to_pid.get(bref_pid)

            if season_id not in self.valid_seasons:
                continue

            if not player_id and bref_pid:
                full_name = safe_str(row.get("player"), strip=True) or bref_pid
                player_id = resolve_or_create_player(con, "bref", bref_pid, full_name)

            if not player_id:
                continue

            rows.append(
                {
                    "player_id": player_id,
                    "season_id": season_id,
                    "award_name": "All-Star",
                    "award_type": "team_inclusion",
                    "trophy_name": None,
                    "votes_received": None,
                    "votes_possible": None,
                }
            )

        inserted = upsert_rows(con, "fact_player_award", rows)
        logger.info("fact_player_award (all-star): %d inserted/ignored", inserted)
        return inserted

    def _load_eos_teams_voting(self, con: sqlite3.Connection) -> int:
        """Load End of Season teams with vote data."""
        path = csv_path(self.raw_dir, "End of Season Teams (Voting).csv")
        if path is None:
            return 0

        df = read_csv_safe(path)
        rows: list[dict] = []

        for row in df.to_dict("records"):
            season_id = int_season_to_id(row["season"])
            bref_pid = safe_str(row.get("player_id"), strip=True) or ""
            player_id = self.bref_to_pid.get(bref_pid)

            if season_id not in self.valid_seasons:
                continue

            if not player_id and bref_pid:
                full_name = safe_str(row.get("player"), strip=True) or bref_pid
                player_id = resolve_or_create_player(con, "bref", bref_pid, full_name)

            if not player_id:
                continue

            award_name = _format_eos_team_award_name(row["type"], row["number_tm"])

            rows.append(
                {
                    "player_id": player_id,
                    "season_id": season_id,
                    "award_name": award_name,
                    "award_type": "team_inclusion",
                    "trophy_name": None,
                    "votes_received": safe_int(row.get("pts_won")),
                    "votes_possible": safe_int(row.get("pts_max")),
                }
            )

        inserted = upsert_rows(con, "fact_player_award", rows)
        logger.info("fact_player_award (EOS voting): %d inserted/ignored", inserted)
        return inserted

    def _load_eos_teams_fallback(self, con: sqlite3.Connection) -> int:
        """Load End of Season teams without vote data (fallback)."""
        path = csv_path(self.raw_dir, "End of Season Teams.csv")
        if path is None:
            return 0

        df = read_csv_safe(path)
        rows: list[dict] = []

        for row in df.to_dict("records"):
            season_id = int_season_to_id(row["season"])
            bref_pid = safe_str(row.get("player_id"), strip=True) or ""
            player_id = self.bref_to_pid.get(bref_pid)

            if season_id not in self.valid_seasons:
                continue

            if not player_id and bref_pid:
                full_name = safe_str(row.get("player"), strip=True) or bref_pid
                player_id = resolve_or_create_player(con, "bref", bref_pid, full_name)

            if not player_id:
                continue

            award_name = _format_eos_team_award_name(row["type"], row["number_tm"])

            rows.append(
                {
                    "player_id": player_id,
                    "season_id": season_id,
                    "award_name": award_name,
                    "award_type": "team_inclusion",
                    "trophy_name": None,
                    "votes_received": None,
                    "votes_possible": None,
                }
            )

        inserted = upsert_rows(con, "fact_player_award", rows)
        logger.info("fact_player_award (EOS teams): %d inserted/ignored", inserted)
        return inserted

    def load(self, con: sqlite3.Connection) -> int:
        """
        Execute the full awards load operation.

        Returns:
            Total number of rows inserted
        """
        self._init_lookups(con)

        # Load award shares
        self.total_inserted += self._load_award_shares(con)

        # Load All-Star selections
        self.total_inserted += self._load_all_star_selections(con)

        # Load End of Season teams (prefer voting data, fallback to teams only)
        eos_voting_inserted = self._load_eos_teams_voting(con)
        if eos_voting_inserted == 0:
            self.total_inserted += self._load_eos_teams_fallback(con)
        else:
            self.total_inserted += eos_voting_inserted

        logger.info("fact_player_award total: %d inserted/ignored", self.total_inserted)
        return self.total_inserted


def load_awards(con: sqlite3.Connection, raw_dir: Path = RAW_DIR) -> None:
    """
    Load all player awards data from CSV files.

    Args:
        con: SQLite database connection
        raw_dir: Directory containing raw CSV files
    """
    loader = AwardsLoader(raw_dir)
    loader.load(con)


# Backward-compatible function aliases for tests
def _bref_to_player_id(con: sqlite3.Connection) -> dict[str, str]:
    """
    Build bref_id → player_id lookup from dim_player.

    Deprecated: Use AwardsLoader._init_lookups() instead.
    """
    return _build_bref_to_player_id_lookup(con)


def _eos_award_name(raw: str) -> str | None:
    """
    Normalize an award name to standard format.

    Deprecated: Use _normalize_award_name() instead.

    Returns None for unmapped values (backward-compatible behavior).
    """
    raw_lc = str(raw).lower().strip()
    for pattern, (name, _) in _AWARD_MAP.items():
        if pattern in raw_lc:
            return name
    return None  # Return None for unmapped values (original behavior)
