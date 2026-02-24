"""
Backfill loader for player career metadata enrichment.

Loads Basketball-Reference player IDs and Hall of Fame flags from
Player Career Info.csv into dim_player.
"""

import logging
import sqlite3
from pathlib import Path
from typing import Any

from src.etl.backfill._base import RAW_DIR, csv_path, read_csv_safe, safe_str
from src.etl.helpers import _isna, _norm_name

logger = logging.getLogger(__name__)

INCHES_TO_CM = 2.54
LBS_TO_KG = 0.453592


def _height_to_cm(value: Any) -> float | None:
    if _isna(value):
        return None
    try:
        return float(value) * INCHES_TO_CM
    except (TypeError, ValueError):
        return None


def _weight_to_kg(value: Any) -> float | None:
    if _isna(value):
        return None
    try:
        return float(value) * LBS_TO_KG
    except (TypeError, ValueError):
        return None


def _parse_hof_flag(value: Any) -> int:
    if _isna(value):
        return 0
    return 0 if str(value).strip().lower() in {"false", "nan", "0", ""} else 1


def _resolve_player_id(
    raw_name: str,
    raw_birth_date: Any,
    name_lookup: dict[str, list[tuple[str, str | None]]],
) -> str | None:
    key = _norm_name(raw_name)
    candidates = name_lookup.get(key, [])
    if not candidates:
        return None

    if len(candidates) == 1:
        return candidates[0][0]

    birth_date = str(raw_birth_date or "").strip()[:10]
    matched = [
        player_id
        for player_id, candidate_bd in candidates
        if (candidate_bd or "")[:10] == birth_date
    ]
    if matched:
        return matched[0]

    return candidates[0][0]


def enrich_player_career(
    con: sqlite3.Connection,
    raw_dir: Path = RAW_DIR,
) -> int:
    """
    Enrich dim_player with bref_id, hof, and missing bio data from Player Career Info.csv.

    Returns:
        Number of rows updated.
    """
    path = csv_path(raw_dir, "Player Career Info.csv")
    if path is None:
        return 0

    df = read_csv_safe(path, low_memory=False)
    dim_rows = con.execute("SELECT player_id, full_name, birth_date FROM dim_player").fetchall()

    name_lookup: dict[str, list[tuple[str, str | None]]] = {}
    for player_id, full_name, birth_date in dim_rows:
        name_lookup.setdefault(_norm_name(full_name), []).append((player_id, birth_date))

    updated = 0
    unmatched = 0

    for row in df.to_dict("records"):
        bref_id = safe_str(row.get("player_id"))
        raw_name = safe_str(row.get("player"))
        if not bref_id or not raw_name:
            unmatched += 1
            continue

        player_id = _resolve_player_id(raw_name, row.get("birth_date"), name_lookup)
        if player_id is None:
            unmatched += 1
            continue

        birth_date = safe_str(row.get("birth_date"))
        birth_date = birth_date[:10] if birth_date else None
        college = safe_str(row.get("colleges"))
        hof = _parse_hof_flag(row.get("hof"))
        height_cm = _height_to_cm(row.get("ht_in_in"))
        weight_kg = _weight_to_kg(row.get("wt"))

        cur = con.execute(
            """
            UPDATE dim_player SET
                bref_id = COALESCE(bref_id, ?),
                college = COALESCE(college, ?),
                hof = CASE WHEN hof = 1 OR ? = 1 THEN 1 ELSE hof END,
                birth_date = COALESCE(birth_date, ?),
                height_cm = COALESCE(height_cm, ?),
                weight_kg = COALESCE(weight_kg, ?)
            WHERE player_id = ?
            """,
            (bref_id, college, hof, birth_date, height_cm, weight_kg, player_id),
        )
        updated += cur.rowcount

    con.commit()
    logger.info("dim_player (career info): %d rows enriched, %d unmatched", updated, unmatched)
    return updated
