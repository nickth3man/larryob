import logging
import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd

from src.etl.helpers import _isna, _norm_name
from src.etl.utils import upsert_rows

logger = logging.getLogger(__name__)
RAW_DIR = Path("raw")

def load_team_history(con: sqlite3.Connection, raw_dir: Path = RAW_DIR) -> None:
    path = raw_dir / "TeamHistories.csv"
    if not path.exists():
        logger.warning("TeamHistories.csv not found, skipping")
        return

    df = pd.read_csv(path)
    rows = []
    for row in df.to_dict("records"):
        rows.append({
            "team_id":            str(int(row["teamId"])),
            "team_city":          str(row["teamCity"]).strip(),
            "team_name":          str(row["teamName"]).strip(),
            "team_abbrev":        str(row["teamAbbrev"]).strip(),
            "season_founded":     int(row["seasonFounded"]),
            "season_active_till": int(row["seasonActiveTill"]),
            "league":             str(row["league"]).strip(),
        })

    inserted = upsert_rows(con, "dim_team_history", rows)
    logger.info("dim_team_history: %d rows inserted/ignored", inserted)

def enrich_dim_team(con: sqlite3.Connection, raw_dir: Path = RAW_DIR) -> None:
    path = raw_dir / "Team Abbrev.csv"
    if not path.exists():
        logger.warning("Team Abbrev.csv not found, skipping")
        return

    df = pd.read_csv(path)
    # Keep only the most recent season's abbreviation for each team name.
    latest = (
        df.sort_values("season", ascending=False)
        .drop_duplicates(subset=["team"], keep="first")
    )

    # Build a full_name → bref_abbrev lookup.
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
    con: sqlite3.Connection, raw_dir: Path
) -> None:
    """Enrich dim_player with bio data from NBA API Players.csv."""
    path = raw_dir / "Players.csv"
    if not path.exists():
        logger.warning("Players.csv not found, skipping")
        return

    df = pd.read_csv(path, low_memory=False)
    updated = 0

    def _ht_to_cm(ht: str | float | None) -> float | None:
        """Convert 'feet-inches' string or numeric (inches) to cm."""
        if _isna(ht):
            return None
        s = str(ht).strip()
        if "-" in s:
            parts = s.split("-")
            try:
                return (int(parts[0]) * 12 + int(parts[1])) * 2.54
            except (ValueError, IndexError):
                return None
        try:
            return float(s) * 2.54  # assume already inches
        except ValueError:
            return None

    def _lbs_to_kg(w: Any) -> float | None:
        if _isna(w):
            return None
        try:
            return float(w) * 0.453592
        except (TypeError, ValueError):
            return None

    for row in df.to_dict("records"):
        pid = str(int(row["personId"])) if not _isna(row["personId"]) else None
        if pid is None:
            continue

        height_cm = _ht_to_cm(row.get("height"))
        weight_kg = _lbs_to_kg(row.get("bodyWeight"))
        college = (
            str(row["lastAttended"]).strip()
            if not _isna(row.get("lastAttended"))
            else None
        )
        draft_year   = int(row["draftYear"])   if not _isna(row.get("draftYear"))   else None
        draft_round  = int(row["draftRound"])  if not _isna(row.get("draftRound"))  else None
        draft_number = int(row["draftNumber"]) if not _isna(row.get("draftNumber")) else None

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
    con: sqlite3.Connection, raw_dir: Path
) -> None:
    """
    Match Basketball-Reference players to dim_player by normalised name
    and populate bref_id, college, hof.
    """
    path = raw_dir / "Player Career Info.csv"
    if not path.exists():
        logger.warning("Player Career Info.csv not found, skipping")
        return

    bref_df = pd.read_csv(path)

    # Load existing dim_player names for matching.
    rows = con.execute(
        "SELECT player_id, full_name, birth_date FROM dim_player"
    ).fetchall()
    name_to_ids: dict[str, list[tuple[str, str | None]]] = {}
    for pid, full_name, birth_date in rows:
        key = _norm_name(full_name)
        name_to_ids.setdefault(key, []).append((pid, birth_date))

    updated = 0
    skipped = 0
    for row in bref_df.to_dict("records"):
        bref_id  = str(row["player_id"]).strip()
        raw_name = str(row["player"]).strip()
        key      = _norm_name(raw_name)

        candidates = name_to_ids.get(key, [])
        if not candidates:
            skipped += 1
            continue

        if len(candidates) == 1:
            pid = candidates[0][0]
        else:
            # Tiebreak on birth date.
            bref_bd = str(row.get("birth_date", "")).strip()[:10]
            matched = [
                p for p, bd in candidates if bd and str(bd)[:10] == bref_bd
            ]
            pid = matched[0] if matched else candidates[0][0]

        height_cm = (
            float(row["ht_in_in"]) * 2.54
            if not _isna(row.get("ht_in_in"))
            else None
        )
        weight_kg = (
            float(row["wt"]) * 0.453592
            if not _isna(row.get("wt"))
            else None
        )
        college = (
            str(row["colleges"]).strip()
            if not _isna(row.get("colleges"))
            else None
        )
        hof = 1 if str(row.get("hof", "False")).lower() not in ("false", "nan", "0", "") else 0

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
        "dim_player (Career Info): %d enriched, %d unmatched", updated, skipped
    )

def enrich_dim_player(con: sqlite3.Connection, raw_dir: Path = RAW_DIR) -> None:
    _enrich_from_players_csv(con, raw_dir)
    _enrich_from_career_info(con, raw_dir)
