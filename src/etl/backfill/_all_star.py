"""
Backfill loader for All-Star selections.
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
    safe_str,
)
from src.etl.helpers import _isna, int_season_to_id

logger = logging.getLogger(__name__)


def _parse_flag(value: Any) -> int:
    if _isna(value):
        return 0
    return 1 if str(value).strip().lower() in {"1", "true", "t", "yes", "y"} else 0


def _build_team_lookup(con: sqlite3.Connection) -> dict[str, str]:
    """
    Build a best-effort lookup from raw team labels to dim_team.team_id.

    All-Star source labels are often conference/captain labels rather than
    franchise abbreviations, so this mapping is intentionally permissive and
    allows unresolved labels (stored with team_id=NULL).
    """
    rows = con.execute(
        """
        SELECT team_id, abbreviation, bref_abbrev, full_name, city, nickname
        FROM dim_team
        """
    ).fetchall()

    candidates: dict[str, set[str]] = {}
    for team_id, abbreviation, bref_abbrev, full_name, city, nickname in rows:
        keys = {
            str(abbreviation or "").strip().upper(),
            str(bref_abbrev or "").strip().upper(),
            str(full_name or "").strip().lower(),
            str(city or "").strip().lower(),
            str(nickname or "").strip().lower(),
        }
        for key in keys:
            if not key:
                continue
            candidates.setdefault(key, set()).add(team_id)

    # Keep only unambiguous keys.
    return {key: next(iter(team_ids)) for key, team_ids in candidates.items() if len(team_ids) == 1}


def _resolve_team_id(team_label: str | None, team_lookup: dict[str, str]) -> str | None:
    if not team_label:
        return None

    label = team_label.strip()
    if not label:
        return None

    return team_lookup.get(label.upper()) or team_lookup.get(label.lower())


def load_all_star_selections(
    con: sqlite3.Connection,
    raw_dir: Path = RAW_DIR,
) -> int:
    """
    Load All-Star selections from All-Star Selections.csv.
    """
    path = csv_path(raw_dir, "All-Star Selections.csv")
    if path is None:
        return 0

    df = read_csv_safe(path, low_memory=False)
    valid_seasons = get_valid_set(con, "dim_season", "season_id")
    team_lookup = _build_team_lookup(con)
    bref_to_player_id = {
        bref_id: player_id
        for bref_id, player_id in con.execute(
            "SELECT bref_id, player_id FROM dim_player WHERE bref_id IS NOT NULL"
        ).fetchall()
    }

    rows: list[dict[str, Any]] = []
    skipped = 0

    for row in df.to_dict("records"):
        season_id = int_season_to_id(row["season"])
        bref_id = safe_str(row.get("player_id"))
        player_id = bref_to_player_id.get(bref_id or "")

        if season_id not in valid_seasons or not player_id:
            skipped += 1
            continue

        team_label = safe_str(row.get("team"))
        rows.append(
            {
                "player_id": player_id,
                "season_id": season_id,
                "team_id": _resolve_team_id(team_label, team_lookup),
                "selection_team": team_label,
                "is_starter": None,
                "is_replacement": _parse_flag(row.get("replaced")),
            }
        )

    inserted = upsert_rows(con, "fact_all_star", rows)
    logger.info("fact_all_star: %d inserted/ignored, %d skipped", inserted, skipped)
    return inserted
