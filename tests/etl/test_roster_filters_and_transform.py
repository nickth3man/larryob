"""Tests for roster loader filters and row transformation behavior."""

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from src.etl.roster import load_team_roster


def test_load_team_roster_queries_players_from_db_when_not_provided(
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
            valid_teams={"1610612747"},
        )
    assert inserted == 1


def test_load_team_roster_queries_teams_from_db_when_not_provided(
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
        )
    assert inserted == 1


def test_load_team_roster_filters_by_valid_players_set(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    mock_df = pd.DataFrame(
        [
            {"PLAYER_ID": "2544", "TeamID": "1610612747"},
            {"PLAYER_ID": "203999", "TeamID": "1610612747"},
            {"PLAYER_ID": "99999", "TeamID": "1610612747"},
        ]
    )
    mock_ep = MagicMock()
    mock_ep.get_data_frames.return_value = [mock_df]

    with patch("src.etl.roster.commonteamroster.CommonTeamRoster", return_value=mock_ep):
        inserted = load_team_roster(
            sqlite_con_with_data,
            "1610612747",
            "2023-24",
            valid_players={"2544", "203999"},
            valid_teams={"1610612747"},
        )
    assert inserted == 2

    count = sqlite_con_with_data.execute(
        "SELECT COUNT(*) FROM fact_roster WHERE player_id='99999'"
    ).fetchone()[0]
    assert count == 0


def test_load_team_roster_filters_by_valid_teams_set(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    mock_df = pd.DataFrame([{"PLAYER_ID": "2544", "TeamID": "9999999999"}])
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


def test_load_team_roster_filters_by_both_players_and_teams(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    mock_df = pd.DataFrame(
        [
            {"PLAYER_ID": "2544", "TeamID": "1610612747"},
            {"PLAYER_ID": "99999", "TeamID": "1610612747"},
            {"PLAYER_ID": "2544", "TeamID": "9999999999"},
            {"PLAYER_ID": "99999", "TeamID": "9999999999"},
        ]
    )
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


def test_load_team_roster_uses_team_id_from_api_response(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    mock_df = pd.DataFrame([{"PLAYER_ID": "2544", "TeamID": "1610612744"}])
    mock_ep = MagicMock()
    mock_ep.get_data_frames.return_value = [mock_df]

    with patch("src.etl.roster.commonteamroster.CommonTeamRoster", return_value=mock_ep):
        inserted = load_team_roster(
            sqlite_con_with_data,
            "1610612747",
            "2023-24",
            valid_players={"2544"},
            valid_teams={"1610612744"},
        )

    assert inserted == 1
    row = sqlite_con_with_data.execute(
        "SELECT team_id FROM fact_roster WHERE player_id='2544'"
    ).fetchone()
    assert row[0] == "1610612744"


def test_load_team_roster_handles_missing_player_id(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    mock_df = pd.DataFrame(
        [
            {"PLAYER_ID": "2544", "TeamID": "1610612747"},
            {"TeamID": "1610612747"},
        ]
    )
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


def test_load_team_roster_sets_end_date_to_null(
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
    row = sqlite_con_with_data.execute(
        "SELECT end_date FROM fact_roster WHERE player_id='2544'"
    ).fetchone()
    assert row[0] is None
