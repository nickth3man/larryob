"""Tests: ETL awards — pure-logic helpers and DB insertion."""

import sqlite3
from unittest.mock import MagicMock, patch

import pandas as pd

from src.etl.awards import (
    _build_award_name,
    _map_award_type,
    _player_awards_to_rows,
    load_all_awards,
    load_player_awards,
)

# ------------------------------------------------------------------ #
# _build_award_name                                                   #
# ------------------------------------------------------------------ #


def test_build_award_name_returns_unknown_for_empty_description() -> None:
    assert _build_award_name("", None) == "Unknown"


def test_build_award_name_all_nba_first_team() -> None:
    assert _build_award_name("All-NBA", "1") == "All-NBA 1st"


def test_build_award_name_all_nba_second_team() -> None:
    assert _build_award_name("All-NBA", "2") == "All-NBA 2nd"


def test_build_award_name_all_nba_third_team() -> None:
    assert _build_award_name("All-NBA", "3") == "All-NBA 3rd"


def test_build_award_name_all_nba_without_number_returns_description() -> None:
    assert _build_award_name("All-NBA", None) == "All-NBA"


def test_build_award_name_all_defensive_first() -> None:
    assert _build_award_name("All-Defensive Team", "1") == "All-Defensive 1st"


def test_build_award_name_all_defensive_second() -> None:
    assert _build_award_name("All-Defensive Team", "2") == "All-Defensive 2nd"


def test_build_award_name_other_award_returns_description_unchanged() -> None:
    assert _build_award_name("MVP", None) == "MVP"
    assert _build_award_name("DPOY", "1") == "DPOY"


def test_build_award_name_strips_whitespace() -> None:
    assert _build_award_name("  MVP  ", None) == "MVP"


# ------------------------------------------------------------------ #
# _map_award_type                                                     #
# ------------------------------------------------------------------ #


def test_map_award_type_returns_individual_for_none() -> None:
    assert _map_award_type(None) == "individual"


def test_map_award_type_returns_individual_for_empty_string() -> None:
    assert _map_award_type("") == "individual"


def test_map_award_type_monthly() -> None:
    assert _map_award_type("Player of the Month") == "monthly"


def test_map_award_type_weekly() -> None:
    assert _map_award_type("Player of the Week") == "weekly"


def test_map_award_type_team_inclusion() -> None:
    assert _map_award_type("Team Inclusion") == "team_inclusion"


def test_map_award_type_individual_for_unrecognized() -> None:
    assert _map_award_type("Some Custom Award") == "individual"


# ------------------------------------------------------------------ #
# _player_awards_to_rows                                              #
# ------------------------------------------------------------------ #


def _make_api_records() -> list[dict]:
    return [
        {
            "PERSON_ID": "2544",
            "DESCRIPTION": "MVP",
            "SEASON": "2011-12",
            "TYPE": "Individual",
            "ALL_NBA_TEAM_NUMBER": None,
            "SUBTYPE1": "Maurice Podoloff Trophy",
        },
        {
            "PERSON_ID": "2544",
            "DESCRIPTION": "All-NBA",
            "SEASON": "2011-12",
            "TYPE": "Team Inclusion",
            "ALL_NBA_TEAM_NUMBER": "1",
            "SUBTYPE1": None,
        },
        {
            "PERSON_ID": "2544",
            "DESCRIPTION": "Something",
            "SEASON": "",  # blank season — should be skipped
            "TYPE": "Individual",
            "ALL_NBA_TEAM_NUMBER": None,
            "SUBTYPE1": None,
        },
    ]


def test_player_awards_to_rows_converts_records() -> None:
    rows = _player_awards_to_rows(_make_api_records())
    assert len(rows) == 2  # blank-season record skipped


def test_player_awards_to_rows_sets_award_name_correctly() -> None:
    rows = _player_awards_to_rows(_make_api_records())
    names = {r["award_name"] for r in rows}
    assert "MVP" in names
    assert "All-NBA 1st" in names


def test_player_awards_to_rows_sets_award_type_correctly() -> None:
    rows = _player_awards_to_rows(_make_api_records())
    by_award = {r["award_name"]: r["award_type"] for r in rows}
    assert by_award["MVP"] == "individual"
    assert by_award["All-NBA 1st"] == "team_inclusion"


def test_player_awards_to_rows_sets_trophy_name_to_none_when_missing() -> None:
    rows = _player_awards_to_rows(_make_api_records())
    all_nba_row = next(r for r in rows if r["award_name"] == "All-NBA 1st")
    assert all_nba_row["trophy_name"] is None


def test_player_awards_to_rows_returns_empty_for_empty_input() -> None:
    assert _player_awards_to_rows([]) == []


def test_player_awards_to_rows_skips_all_blank_season_records() -> None:
    records = [
        {
            "PERSON_ID": "1",
            "DESCRIPTION": "MVP",
            "SEASON": None,
            "TYPE": None,
            "ALL_NBA_TEAM_NUMBER": None,
            "SUBTYPE1": None,
        }
    ]
    assert _player_awards_to_rows(records) == []


# ------------------------------------------------------------------ #
# load_player_awards (mock API)                                       #
# ------------------------------------------------------------------ #


def test_load_player_awards_from_cache(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path,
) -> None:
    """When cache is warm, no API call is made and rows are inserted."""
    import src.etl.utils as utils_mod

    monkeypatch.setattr(utils_mod, "CACHE_DIR", tmp_path)

    from src.etl.utils import save_cache

    save_cache(
        "awards_2544",
        [
            {
                "PERSON_ID": "2544",
                "DESCRIPTION": "MVP",
                "SEASON": "2023-24",
                "TYPE": "Individual",
                "ALL_NBA_TEAM_NUMBER": None,
                "SUBTYPE1": None,
            }
        ],
    )
    inserted = load_player_awards(sqlite_con_with_data, ["2544"])
    assert inserted >= 1


def test_load_player_awards_filters_rows_missing_fk_targets(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path,
) -> None:
    import src.etl.utils as utils_mod

    monkeypatch.setattr(utils_mod, "CACHE_DIR", tmp_path)

    from src.etl.utils import save_cache

    save_cache(
        "awards_2544",
        [
            {
                "PERSON_ID": "2544",
                "DESCRIPTION": "MVP",
                "SEASON": "2023-24",
                "TYPE": "Individual",
                "ALL_NBA_TEAM_NUMBER": None,
                "SUBTYPE1": None,
            },
            {
                "PERSON_ID": "999999",
                "DESCRIPTION": "MVP",
                "SEASON": "2023-24",
                "TYPE": "Individual",
                "ALL_NBA_TEAM_NUMBER": None,
                "SUBTYPE1": None,
            },
            {
                "PERSON_ID": "2544",
                "DESCRIPTION": "MVP",
                "SEASON": "1999-00",
                "TYPE": "Individual",
                "ALL_NBA_TEAM_NUMBER": None,
                "SUBTYPE1": None,
            },
        ],
    )

    inserted = load_player_awards(sqlite_con_with_data, ["2544"])
    assert inserted == 1
    count = sqlite_con_with_data.execute("SELECT COUNT(*) FROM fact_player_award").fetchone()[0]
    assert count == 1


def test_load_player_awards_returns_zero_for_no_awards(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path,
) -> None:
    """No awards in cache or API → returns 0."""
    import src.etl.utils as utils_mod

    monkeypatch.setattr(utils_mod, "CACHE_DIR", tmp_path)

    mock_ep = MagicMock()
    mock_ep.get_data_frames.return_value = [pd.DataFrame()]
    with patch("src.etl.awards.playerawards.PlayerAwards", return_value=mock_ep):
        with patch("src.etl.utils.time.sleep"):
            inserted = load_player_awards(sqlite_con_with_data, ["2544"])
    assert inserted == 0


def test_load_player_awards_handles_api_exception(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path,
) -> None:
    """API failure is logged and skipped; function returns 0."""
    import src.etl.utils as utils_mod

    monkeypatch.setattr(utils_mod, "CACHE_DIR", tmp_path)

    with patch("src.etl.awards.playerawards.PlayerAwards", side_effect=RuntimeError("API down")):
        with patch("src.etl.utils.time.sleep"):
            inserted = load_player_awards(sqlite_con_with_data, ["2544"])
    assert inserted == 0


# ------------------------------------------------------------------ #
# load_all_awards                                                     #
# ------------------------------------------------------------------ #


def test_load_all_awards_returns_zero_when_no_players(
    sqlite_con: sqlite3.Connection,
    monkeypatch,
    tmp_path,
) -> None:
    import src.etl.utils as utils_mod

    monkeypatch.setattr(utils_mod, "CACHE_DIR", tmp_path)
    result = load_all_awards(sqlite_con, active_only=True)
    assert result == 0


def test_load_all_awards_active_only_queries_active_players(
    sqlite_con_with_data: sqlite3.Connection,
    monkeypatch,
    tmp_path,
) -> None:
    """active_only=True should not raise; no API calls because cache returns empty."""
    import src.etl.utils as utils_mod

    monkeypatch.setattr(utils_mod, "CACHE_DIR", tmp_path)
    with patch("src.etl.awards.playerawards.PlayerAwards", side_effect=RuntimeError("no API")):
        with patch("src.etl.utils.time.sleep"):
            result = load_all_awards(sqlite_con_with_data, active_only=True)
    assert result == 0
