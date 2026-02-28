"""Tests for src.pipeline.completeness — full-history contract constants."""

from src.pipeline.completeness import (
    NBA_LINEAGE_FIRST_START_YEAR,
    REQUIRED_GAME_TYPES,
    full_history_seasons,
)


def test_full_history_contract_defaults():
    """Verify the full-history completeness contract defaults."""
    assert NBA_LINEAGE_FIRST_START_YEAR == 1946
    assert REQUIRED_GAME_TYPES == ("Preseason", "Regular Season", "Play-In", "Playoffs")
    seasons = full_history_seasons(up_to_start_year=2025)
    assert seasons[0] == "1946-47"
    assert seasons[-1] == "2025-26"


def test_full_history_seasons_count():
    """Verify the count of seasons from 1946 to present."""
    seasons = full_history_seasons(up_to_start_year=2025)
    # 1946-47 through 2025-26 = 80 seasons
    assert len(seasons) == 80


def test_full_history_seasons_format():
    """Verify season IDs follow the YYYY-YY format."""
    seasons = full_history_seasons(up_to_start_year=2025)
    for season in seasons:
        parts = season.split("-")
        assert len(parts) == 2
        assert len(parts[0]) == 4  # Year is 4 digits
        assert len(parts[1]) == 2  # Suffix is 2 digits
