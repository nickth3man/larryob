"""
Backfill loaders for dimension table enrichment.

This module handles loading and enrichment of dimension tables:
- dim_team_history: Team historical data
- dim_team: Team bref_abbrev enrichment
- dim_player: Player bio data enrichment
"""

import logging
import sqlite3
from pathlib import Path
from typing import Any

from src.db.operations import upsert_rows
from src.etl.backfill._base import (
    RAW_DIR,
    csv_path,
    read_csv_safe,
    safe_int,
    safe_str,
)
from src.etl.helpers import _isna, _norm_name

logger = logging.getLogger(__name__)


# Conversion constants
INCHES_TO_CM = 2.54
LBS_TO_KG = 0.453592


def _height_to_cm(ht: str | float | None) -> float | None:
    """
    Convert height string ('feet-inches') or numeric (inches) to centimeters.

    Args:
        ht: Height in 'feet-inches' format (e.g., '6-3') or inches

    Returns:
        Height in centimeters, or None if invalid
    """
    if _isna(ht):
        return None

    s = str(ht).strip()

    # Handle 'feet-inches' format
    if "-" in s:
        parts = s.split("-")
        try:
            total_inches = int(parts[0]) * 12 + int(parts[1])
            return total_inches * INCHES_TO_CM
        except (ValueError, IndexError):
            return None

    # Handle numeric inches
    try:
        return float(s) * INCHES_TO_CM
    except ValueError:
        return None


def _weight_to_kg(w: Any) -> float | None:
    """
    Convert weight in pounds to kilograms.

    Args:
        w: Weight in pounds

    Returns:
        Weight in kilograms, or None if invalid
    """
    if _isna(w):
        return None
    try:
        return float(w) * LBS_TO_KG
    except (TypeError, ValueError):
        return None


def _parse_hof_flag(value: Any) -> int:
    """
    Parse a Hall of Fame flag value to integer (0 or 1).

    Args:
        value: Raw HOF value from CSV

    Returns:
        1 if HOF, 0 otherwise
    """
    if _isna(value):
        return 0
    return 0 if str(value).lower() in ("false", "nan", "0", "") else 1


def load_team_history(
    con: sqlite3.Connection,
    raw_dir: Path = RAW_DIR,
) -> None:
    """
    Load team historical data from TeamHistories.csv.

    For historical franchises not in the current 30-team NBA set,
    placeholder entries are created in dim_team to satisfy FK constraints.

    Args:
        con: SQLite database connection
        raw_dir: Directory containing raw CSV files
    """
    path = csv_path(raw_dir, "TeamHistories.csv")
    if path is None:
        return

    df = read_csv_safe(path)

    rows: list[dict] = []

    for row in df.to_dict("records"):
        rows.append(
            {
                "team_id": str(int(row["teamId"])),
                "team_city": safe_str(row.get("teamCity")),
                "team_name": safe_str(row.get("teamName")),
                "team_abbrev": safe_str(row.get("teamAbbrev")),
                "season_founded": safe_int(row.get("seasonFounded")),
                "season_active_till": safe_int(row.get("seasonActiveTill")),
                "league": safe_str(row.get("league")),
            }
        )

    inserted = upsert_rows(con, "dim_team_history", rows)
    logger.info("dim_team_history: %d rows inserted/ignored", inserted)


def enrich_dim_team(
    con: sqlite3.Connection,
    raw_dir: Path = RAW_DIR,
) -> None:
    """
    Enrich dim_team with bref_abbrev from Team Abbrev.csv.

    Uses the most recent season's abbreviation for each team name.

    Args:
        con: SQLite database connection
        raw_dir: Directory containing raw CSV files
    """
    path = csv_path(raw_dir, "Team Abbrev.csv")
    if path is None:
        return

    df = read_csv_safe(path)

    # Keep only the most recent season's abbreviation for each team name
    latest = df.sort_values("season", ascending=False).drop_duplicates(
        subset=["team"], keep="first"
    )

    # Build a full_name → bref_abbrev lookup
    abbrev_map: dict[str, str] = {
        str(row["team"]).strip(): str(row["abbreviation"]).strip()
        for row in latest.to_dict("records")
    }

    updated = 0
    for full_name, bref_abbrev in abbrev_map.items():
        cur = con.execute(
            "UPDATE dim_team SET bref_abbrev = ? WHERE full_name = ?",
            (bref_abbrev, full_name),
        )
        updated += cur.rowcount

    con.commit()
    logger.info("dim_team bref_abbrev: %d teams updated", updated)


def _enrich_from_players_csv(
    con: sqlite3.Connection,
    raw_dir: Path,
) -> None:
    """
    Enrich dim_player with bio data from NBA API Players.csv.

    Args:
        con: SQLite database connection
        raw_dir: Directory containing raw CSV files
    """
    path = csv_path(raw_dir, "Players.csv")
    if path is None:
        return

    df = read_csv_safe(path, low_memory=False)
    updated = 0

    for row in df.to_dict("records"):
        pid = str(int(row["personId"])) if not _isna(row.get("personId")) else None
        if pid is None:
            continue

        height_cm = _height_to_cm(row.get("height"))
        weight_kg = _weight_to_kg(row.get("bodyWeight"))
        college = safe_str(row.get("lastAttended"))
        draft_year = safe_int(row.get("draftYear"))
        draft_round = safe_int(row.get("draftRound"))
        draft_number = safe_int(row.get("draftNumber"))

        cur = con.execute(
            """
            UPDATE dim_player SET
                height_cm    = COALESCE(height_cm,    ?),
                weight_kg    = COALESCE(weight_kg,    ?),
                college      = COALESCE(college,      ?),
                draft_year   = COALESCE(draft_year,   ?),
                draft_round  = COALESCE(draft_round,  ?),
                draft_number = COALESCE(draft_number, ?)
            WHERE player_id = ?
            """,
            (height_cm, weight_kg, college, draft_year, draft_round, draft_number, pid),
        )
        updated += cur.rowcount

    con.commit()
    logger.info("dim_player (Players.csv): %d rows enriched", updated)


def _enrich_from_career_info(
    con: sqlite3.Connection,
    raw_dir: Path,
) -> None:
    """
    Enrich dim_player with bref_id, college, hof from Player Career Info.csv.

    Matches Basketball-Reference players to dim_player by normalized name
    with birth_date as tiebreaker for duplicate names.

    Args:
        con: SQLite database connection
        raw_dir: Directory containing raw CSV files
    """
    path = csv_path(raw_dir, "Player Career Info.csv")
    if path is None:
        return

    bref_df = read_csv_safe(path)

    # Load existing dim_player names for matching
    rows = con.execute("SELECT player_id, full_name, birth_date FROM dim_player").fetchall()

    # Build normalized name → [(player_id, birth_date), ...] lookup
    name_to_ids: dict[str, list[tuple[str, str | None]]] = {}
    for pid, full_name, birth_date in rows:
        key = _norm_name(full_name)
        name_to_ids.setdefault(key, []).append((pid, birth_date))

    updated = 0
    skipped = 0

    for row in bref_df.to_dict("records"):
        bref_id = safe_str(row.get("player_id"), strip=True) or ""
        raw_name = safe_str(row.get("player"), strip=True) or ""
        key = _norm_name(raw_name)

        candidates = name_to_ids.get(key, [])
        if not candidates:
            skipped += 1
            continue

        # Resolve duplicates with birth_date tiebreaker
        if len(candidates) == 1:
            pid = candidates[0][0]
        else:
            bref_bd = str(row.get("birth_date", "")).strip()[:10]
            matched = [p for p, bd in candidates if bd and str(bd)[:10] == bref_bd]
            pid = matched[0] if matched else candidates[0][0]

        height_cm = _height_to_cm(row.get("ht_in_in"))
        weight_kg = _weight_to_kg(row.get("wt"))
        college = safe_str(row.get("colleges"))
        hof = _parse_hof_flag(row.get("hof"))

        cur = con.execute(
            """
            UPDATE dim_player SET
                bref_id   = COALESCE(bref_id,   ?),
                college   = COALESCE(college,   ?),
                hof       = ?,
                height_cm = COALESCE(height_cm, ?),
                weight_kg = COALESCE(weight_kg, ?)
            WHERE player_id = ?
            """,
            (bref_id, college, hof, height_cm, weight_kg, pid),
        )
        updated += cur.rowcount

    con.commit()
    logger.info(
        "dim_player (Career Info): %d enriched, %d unmatched",
        updated,
        skipped,
    )


def enrich_dim_player(
    con: sqlite3.Connection,
    raw_dir: Path = RAW_DIR,
) -> None:
    """
    Enrich dim_player with NBA API player bio data from Players.csv.

    Args:
        con: SQLite database connection
        raw_dir: Directory containing raw CSV files
    """
    _enrich_from_players_csv(con, raw_dir)
