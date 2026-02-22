"""Tests: ETL roster loader — pure helpers and mock-boundary load."""

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from src.etl.roster import _season_start_date, load_team_roster

# ------------------------------------------------------------------ #
# _season_start_date                                                  #
# ------------------------------------------------------------------ #


def test_season_start_date_returns_october_first() -> None:
    assert _season_start_date("2023-24") == "2023-10-01"


def test_season_start_date_uses_start_year() -> None:
    assert _season_start_date("1999-00") == "1999-10-01"


def test_season_start_date_recent_season() -> None:
    assert _season_start_date("2024-25") == "2024-10-01"


# ------------------------------------------------------------------ #
# load_team_roster — from cache                                       #
# ------------------------------------------------------------------ #


def test_load_team_roster_from_cache_inserts_rows(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """When cache is warm, roster rows are inserted without any API call."""
    import src.etl.utils as utils_mod

    monkeypatch.setattr(utils_mod, "CACHE_DIR", tmp_path)

    from src.etl.utils import save_cache

    cached_rows = [
        {
            "player_id": "2544",
            "team_id": "1610612747",
            "season_id": "2023-24",
            "start_date": "2023-10-01",
            "end_date": None,
        }
    ]
    save_cache("roster_1610612747_2023-24", cached_rows)

    inserted = load_team_roster(sqlite_con_with_data, "1610612747", "2023-24")
    assert inserted == 1

    count = sqlite_con_with_data.execute(
        "SELECT COUNT(*) FROM fact_roster WHERE player_id='2544'"
    ).fetchone()[0]
    assert count == 1


def test_load_team_roster_deduplication_from_cache(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """Calling twice with the same cached data must not create duplicates."""
    import src.etl.utils as utils_mod

    monkeypatch.setattr(utils_mod, "CACHE_DIR", tmp_path)

    from src.etl.utils import save_cache

    cached_rows = [
        {
            "player_id": "2544",
            "team_id": "1610612747",
            "season_id": "2023-24",
            "start_date": "2023-10-01",
            "end_date": None,
        }
    ]
    save_cache("roster_1610612747_2023-24", cached_rows)

    load_team_roster(sqlite_con_with_data, "1610612747", "2023-24")
    load_team_roster(sqlite_con_with_data, "1610612747", "2023-24")

    count = sqlite_con_with_data.execute(
        "SELECT COUNT(*) FROM fact_roster WHERE player_id='2544'"
    ).fetchone()[0]
    assert count == 1


def test_load_team_roster_returns_zero_for_empty_api_result(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """When API returns no valid players for a team, returns 0."""
    import src.etl.utils as utils_mod

    monkeypatch.setattr(utils_mod, "CACHE_DIR", tmp_path)

    mock_ep = MagicMock()
    mock_ep.get_data_frames.return_value = [
        pd.DataFrame([{"PLAYER_ID": "99999", "TeamID": "1610612747"}])
    ]
    with patch("src.etl.roster.commonteamroster.CommonTeamRoster", return_value=mock_ep):
        with patch("src.etl.utils.time.sleep"):
            inserted = load_team_roster(
                sqlite_con_with_data,
                "1610612747",
                "2023-24",
                valid_players=set(),
                valid_teams={"1610612747"},
            )
    assert inserted == 0


def test_load_team_roster_uses_cached_empty_list_without_api_call(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """A cached empty list is honoured and does not trigger an API call."""
    import src.etl.utils as utils_mod

    monkeypatch.setattr(utils_mod, "CACHE_DIR", tmp_path)

    from src.etl.utils import save_cache

    save_cache("roster_1610612747_2023-24", [])

    with patch("src.etl.roster.commonteamroster.CommonTeamRoster") as mock_api:
        inserted = load_team_roster(
            sqlite_con_with_data,
            "1610612747",
            "2023-24",
            valid_players={"2544"},
            valid_teams={"1610612747"},
        )
    mock_api.assert_not_called()
    assert inserted == 0


# ------------------------------------------------------------------ #
# load_team_roster — API path (mocked)                               #
# ------------------------------------------------------------------ #


def test_load_team_roster_from_api_success(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """When there is no cache, the API is called and rows inserted."""
    import src.etl.utils as utils_mod

    monkeypatch.setattr(utils_mod, "CACHE_DIR", tmp_path)

    mock_df = pd.DataFrame(
        [
            {"PLAYER_ID": "2544", "TeamID": "1610612747"},
        ]
    )
    mock_ep = MagicMock()
    mock_ep.get_data_frames.return_value = [mock_df]

    with patch("src.etl.roster.commonteamroster.CommonTeamRoster", return_value=mock_ep):
        with patch("src.etl.utils.time.sleep"):
            inserted = load_team_roster(
                sqlite_con_with_data,
                "1610612747",
                "2023-24",
                valid_players={"2544"},
                valid_teams={"1610612747"},
            )
    assert inserted == 1


def test_load_team_roster_filters_out_unknown_players(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """Players not in valid_players set must be filtered out."""
    import src.etl.utils as utils_mod

    monkeypatch.setattr(utils_mod, "CACHE_DIR", tmp_path)

    mock_df = pd.DataFrame(
        [
            {"PLAYER_ID": "99999", "TeamID": "1610612747"},
        ]
    )
    mock_ep = MagicMock()
    mock_ep.get_data_frames.return_value = [mock_df]

    with patch("src.etl.roster.commonteamroster.CommonTeamRoster", return_value=mock_ep):
        with patch("src.etl.utils.time.sleep"):
            inserted = load_team_roster(
                sqlite_con_with_data,
                "1610612747",
                "2023-24",
                valid_players={"2544"},
                valid_teams={"1610612747"},
            )
    assert inserted == 0


def test_load_team_roster_returns_zero_on_api_exception(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """API failure is caught; returns 0 without raising."""
    import src.etl.utils as utils_mod

    monkeypatch.setattr(utils_mod, "CACHE_DIR", tmp_path)

    with patch(
        "src.etl.roster.commonteamroster.CommonTeamRoster",
        side_effect=RuntimeError("API unavailable"),
    ):
        with patch("src.etl.utils.time.sleep"):
            inserted = load_team_roster(sqlite_con_with_data, "1610612747", "2023-24")
    assert inserted == 0


def test_load_team_roster_returns_zero_on_empty_api_response(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """Empty DataFrame from API → returns 0."""
    import src.etl.utils as utils_mod

    monkeypatch.setattr(utils_mod, "CACHE_DIR", tmp_path)

    mock_ep = MagicMock()
    mock_ep.get_data_frames.return_value = [pd.DataFrame()]

    with patch("src.etl.roster.commonteamroster.CommonTeamRoster", return_value=mock_ep):
        with patch("src.etl.utils.time.sleep"):
            inserted = load_team_roster(sqlite_con_with_data, "1610612747", "2023-24")
    assert inserted == 0
