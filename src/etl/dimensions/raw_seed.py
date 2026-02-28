"""
Raw data seeding utilities for dimension tables.

This module provides functions to infer dimension data (seasons, teams, players)
directly from raw CSV files, enabling a raw-first ingestion model that doesn't
depend on external API availability.

Design Decisions
----------------
- infer_season_start_range: Derives season boundaries from Games.csv timestamps
- Uses pandas for efficient CSV parsing
- Returns sensible defaults when source files are unavailable

Usage
-----
    from src.etl.dimensions.raw_seed import infer_season_start_range

    min_year, max_year = infer_season_start_range("raw")
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from src.pipeline.completeness import NBA_LINEAGE_FIRST_START_YEAR

logger = logging.getLogger(__name__)


def infer_season_start_range(raw_dir: str | Path = "raw") -> tuple[int, int]:
    """
    Infer the season start year range from raw/Games.csv.

    The NBA season spans two calendar years (e.g., 2023-24 season runs from
    October 2023 to June 2024). This function determines the season range
    by examining game dates and converting to season start years.

    Logic:
    - Games in months Oct-Dec belong to season starting that year
    - Games in months Jan-Sep belong to season starting previous year

    Args:
        raw_dir: Path to the raw data directory containing Games.csv

    Returns:
        Tuple of (min_start_year, max_start_year)
        Returns (1946, current_year - 1) as fallback if file not found
    """
    path = Path(raw_dir) / "Games.csv"

    if not path.exists():
        logger.warning("Games.csv not found at %s, using default range", path)
        from datetime import datetime

        current_year = datetime.now().year
        return (NBA_LINEAGE_FIRST_START_YEAR, current_year - 1)

    try:
        df = pd.read_csv(path, usecols=["gameDateTimeEst"])
        dates = pd.to_datetime(df["gameDateTimeEst"], errors="coerce")

        # Drop any unparseable dates
        valid_dates = dates.dropna()
        if valid_dates.empty:
            logger.warning("No valid dates found in Games.csv, using default range")
            from datetime import datetime

            current_year = datetime.now().year
            return (NBA_LINEAGE_FIRST_START_YEAR, current_year - 1)

        min_date = valid_dates.min()
        max_date = valid_dates.max()

        # Convert to season start years
        # Games in Oct-Dec (month >= 10) belong to season starting that year
        # Games in Jan-Sep (month < 10) belong to season starting previous year
        min_year = min_date.year if min_date.month >= 10 else min_date.year - 1
        max_year = max_date.year if max_date.month >= 10 else max_date.year - 1

        return (min_year, max_year)

    except Exception as e:
        logger.error("Error reading Games.csv: %s", e)
        from datetime import datetime

        current_year = datetime.now().year
        return (NBA_LINEAGE_FIRST_START_YEAR, current_year - 1)
