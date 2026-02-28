"""Tests for src.etl.dimensions.raw_seed — raw data seeding utilities."""

from pathlib import Path

from src.etl.dimensions.raw_seed import infer_season_start_range


def test_infer_season_start_range_from_raw():
    """Verify season range inference from raw/Games.csv."""
    min_y, max_y = infer_season_start_range(raw_dir="raw")
    assert min_y == 1946
    assert max_y >= 2025


def test_infer_season_start_range_missing_file(tmp_path: Path):
    """Verify graceful handling when Games.csv is missing."""
    # Should return a reasonable default when file doesn't exist
    min_y, max_y = infer_season_start_range(raw_dir=tmp_path)
    # Should return some default range
    assert min_y >= 1946
    assert max_y >= min_y
