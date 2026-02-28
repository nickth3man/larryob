from src.pipeline.completeness import (
    NBA_LINEAGE_FIRST_START_YEAR,
    REQUIRED_GAME_TYPES,
    full_history_seasons,
)


def test_full_history_contract_defaults():
    assert NBA_LINEAGE_FIRST_START_YEAR == 1946
    assert REQUIRED_GAME_TYPES == ("Preseason", "Regular Season", "Play-In", "Playoffs")
    seasons = full_history_seasons(up_to_start_year=2025)
    assert seasons[0] == "1946-47"
    assert seasons[-1] == "2025-26"
