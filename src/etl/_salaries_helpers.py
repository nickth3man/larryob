"""Shared helper functions for salary ETL flows."""

import logging
import sqlite3

from ..db.operations import upsert_rows
from .config import get_all_salary_caps
from .helpers import _norm_name

logger = logging.getLogger(__name__)

# Use centralized config for salary cap data.
_SALARY_CAP_BY_SEASON = get_all_salary_caps()


def load_salary_cap(con: sqlite3.Connection) -> int:
    """
    Seed the dim_salary_cap table with historical cap amounts and ensure referenced seasons exist.

    This ensures dim_season contains seasons up to the latest start year present in the hardcoded cap data, then upserts season_id/cap_amount rows into dim_salary_cap.

    Returns:
        inserted (int): Number of rows inserted or replaced into dim_salary_cap.
    """
    # Ensure dim_season has the seasons we need (FK constraint).
    from .dimensions import load_seasons

    max_start_year = max(int(sid.split("-")[0]) for sid in _SALARY_CAP_BY_SEASON)
    load_seasons(con, up_to_start_year=max_start_year)
    rows = [{"season_id": sid, "cap_amount": cap} for sid, cap in _SALARY_CAP_BY_SEASON.items()]
    inserted = upsert_rows(con, "dim_salary_cap", rows, conflict="REPLACE")
    logger.info("dim_salary_cap: %d rows upserted.", inserted)
    return inserted


def _normalize_name(name: str) -> str:
    """
    Normalize a person name for consistent matching by removing accents, lowercasing, and stripping non-alphabetic characters.

    Parameters:
        name (str): Raw name to normalize.

    Returns:
        str: Normalized name suitable for lookup and comparison.
    """
    return _norm_name(name, strip_non_alpha=True)
