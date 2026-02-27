"""Tests for raw_seed: infer season start range from raw game data."""

from __future__ import annotations

import shutil
from pathlib import Path

from src.etl.dimensions.raw_seed import infer_season_start_range

FIXTURE_CSV = Path(__file__).parent / "fixtures" / "Games_sample.csv"


def test_infer_season_start_range_from_raw(tmp_path):
    shutil.copy(FIXTURE_CSV, tmp_path / "Games.csv")
    min_y, max_y = infer_season_start_range(raw_dir=tmp_path)
    assert min_y == 1946
    # max_y >= 2025 to remain valid when run against a live raw/Games.csv
    assert max_y >= 2025
