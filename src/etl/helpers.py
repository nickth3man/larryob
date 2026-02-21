"""Pure transformation helpers for ETL modules."""

import unicodedata
from typing import Any

import numpy as np
import pandas as pd


def _isna(v: Any) -> bool:
    """Scalar-safe NA check that always returns a plain bool."""
    if v is None:
        return True
    try:
        return bool(pd.isna(v))
    except (TypeError, ValueError):
        return False


def _int(v: Any) -> int | None:
    try:
        return int(v) if not _isna(v) else None
    except (TypeError, ValueError):
        return None


def _flt(v: Any) -> float | None:
    try:
        return float(v) if not _isna(v) else None
    except (TypeError, ValueError):
        return None


def int_season_to_id(s: int | float) -> str:
    """
    Convert a Basketball-Reference ending-year integer to our season_id format.

    Examples
    --------
    2026  → '2025-26'
    2000  → '1999-00'
    1950  → '1949-50'
    1947  → '1946-47'
    """
    s = int(s)
    start = s - 1
    end_suffix = str(s)[2:]
    return f"{start}-{end_suffix}"


def pad_game_id(game_id: int | str) -> str:
    """Zero-pad a raw NBA game ID integer to the 10-char TEXT format."""
    return str(int(game_id)).zfill(10)


def season_type_from_game_id(padded: str) -> str:
    """
    Derive season_type from the 2-digit type code embedded in a padded game ID.

    NBA encoding: digits [2:4] of the zero-padded 10-char ID.
    """
    code = padded[2:4]
    return {
        "11": "Preseason",
        "22": "Regular Season",
        "52": "Play-In",
        "42": "Playoffs",
    }.get(code, "Regular Season")


def season_id_from_game_id(padded: str) -> str:
    """
    Derive season_id from a padded 10-char NBA game ID.

    The NBA embeds the season start year in digits [3:5] (0-indexed) of the
    10-character zero-padded game ID.  For example:
        '0022500686'  →  padded[3:5] = '25'  →  start_year = 2025  →  '2025-26'
        '0022301001'  →  padded[3:5] = '23'  →  start_year = 2023  →  '2023-24'
    """
    start_year = 2000 + int(padded[3:5])
    end_suffix = str(start_year + 1)[2:]
    return f"{start_year}-{end_suffix}"


def season_id_from_date(date_str: str) -> str:
    """
    Derive season_id from an ISO-8601 date string.

    NBA seasons run roughly October–June.
    July–September belong to the following season's start.
    """
    date_str = str(date_str)[:10]  # keep 'YYYY-MM-DD'
    year = int(date_str[:4])
    month = int(date_str[5:7])
    start_year = year if month >= 7 else year - 1
    end_suffix = str(start_year + 1)[2:]
    return f"{start_year}-{end_suffix}"


def _norm_name(name: str) -> str:
    """Lowercase, strip accents and extra whitespace for fuzzy name matching."""
    nfkd = unicodedata.normalize("NFKD", str(name))
    ascii_str = nfkd.encode("ascii", "ignore").decode("ascii")
    return " ".join(ascii_str.lower().split())