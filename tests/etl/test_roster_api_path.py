"""Tests for roster loader API path and retry behavior."""

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from src.etl.roster import load_team_roster


def test_load_team_roster_from_api_success(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    mock_df = pd.DataFrame([{"PLAYER_ID": "2544", "TeamID": "1610612747"}])
    mock_ep = MagicMock()
    mock_ep.get_data_frames.return_value = [mock_df]

    with patch("src.etl.roster.commonteamroster.CommonTeamRoster", return_value=mock_ep):
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
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    mock_df = pd.DataFrame([{"PLAYER_ID": "99999", "TeamID": "1610612747"}])
    mock_ep = MagicMock()
    mock_ep.get_data_frames.return_value = [mock_df]

    with patch("src.etl.roster.commonteamroster.CommonTeamRoster", return_value=mock_ep):
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
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    with patch(
        "src.etl.roster.commonteamroster.CommonTeamRoster",
        side_effect=RuntimeError("API unavailable"),
    ):
        with patch("src.etl.api_client.time.sleep"):
            inserted = load_team_roster(sqlite_con_with_data, "1610612747", "2023-24")
    assert inserted == 0


def test_load_team_roster_returns_zero_on_empty_api_response(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    mock_ep = MagicMock()
    mock_ep.get_data_frames.return_value = [pd.DataFrame()]

    with patch("src.etl.roster.commonteamroster.CommonTeamRoster", return_value=mock_ep):
        inserted = load_team_roster(sqlite_con_with_data, "1610612747", "2023-24")
    assert inserted == 0


def test_load_team_roster_api_failure_retries_and_succeeds(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    mock_df = pd.DataFrame([{"PLAYER_ID": "2544", "TeamID": "1610612747"}])
    mock_ep = MagicMock()
    mock_ep.get_data_frames.return_value = [mock_df]

    call_count = 0

    def side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("API timeout")
        return mock_ep

    with patch("src.etl.roster.commonteamroster.CommonTeamRoster", side_effect=side_effect):
        with patch("src.etl.api_client.time.sleep"):
            inserted = load_team_roster(
                sqlite_con_with_data,
                "1610612747",
                "2023-24",
                valid_players={"2544"},
                valid_teams={"1610612747"},
            )
    assert inserted == 1
    assert call_count == 2


def test_load_team_roster_api_max_retries_exceeded(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    def side_effect(*args, **kwargs):
        raise RuntimeError("API unavailable")

    with patch("src.etl.roster.commonteamroster.CommonTeamRoster", side_effect=side_effect):
        with patch("src.etl.api_client.time.sleep"):
            inserted = load_team_roster(sqlite_con_with_data, "1610612747", "2023-24")
    assert inserted == 0


def test_load_team_roster_api_error_is_caught_and_logged(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
    caplog,
) -> None:
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    with patch(
        "src.etl.roster.commonteamroster.CommonTeamRoster",
        side_effect=ValueError("Bad request"),
    ):
        with patch("src.etl.api_client.time.sleep"):
            inserted = load_team_roster(sqlite_con_with_data, "1610612747", "2023-24")

    assert inserted == 0
    assert "CommonTeamRoster(1610612747,2023-24) failed" in caplog.text
