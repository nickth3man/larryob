"""Utilities for seeding dimension data from raw CSV datasets."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


def infer_season_start_range(raw_dir: str | Path = "raw") -> tuple[int, int]:
    """Return the (min_season_start_year, max_season_start_year) from raw game dates.

    NBA seasons span two calendar years (e.g. the 1946-47 season).

    Season boundary rule (applied to both earliest and latest game):
      - A game in July or later belongs to the season that *starts* that calendar year
        (July marks the off-season / pre-season boundary for the new season).
      - A game in January through June belongs to the season that started the
        *previous* calendar year (e.g. a game on 1947-01-15 is in the 1946-47 season).

    This symmetry means October season-openers correctly map to that year, while
    mid-season January games correctly map to the prior year.
    """
    path = Path(raw_dir) / "Games.csv"
    df = pd.read_csv(path, usecols=["gameDateTimeEst"])
    dates = pd.to_datetime(df["gameDateTimeEst"], errors="coerce").dropna()

    earliest = dates.min()
    min_year = int(earliest.year) if int(earliest.month) >= 7 else int(earliest.year) - 1

    latest = dates.max()
    max_year = int(latest.year) if int(latest.month) >= 7 else int(latest.year) - 1

    return (min_year, max_year)
