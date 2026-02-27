"""Tests for roster loading cache-hit/cache-miss behavior."""

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from src.etl.roster import load_team_roster


def test_load_team_roster_from_cache_inserts_rows(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    from src.db.cache import save_cache

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
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)
    from src.db.cache import save_cache

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
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    mock_ep = MagicMock()
    mock_ep.get_data_frames.return_value = [
        pd.DataFrame([{"PLAYER_ID": "99999", "TeamID": "1610612747"}])
    ]
    with patch("src.etl.roster.commonteamroster.CommonTeamRoster", return_value=mock_ep):
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
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    from src.db.cache import save_cache

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


def test_load_team_roster_cache_hit_skips_api_call(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)
    from src.db.cache import save_cache

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

    with patch("src.etl.roster.commonteamroster.CommonTeamRoster") as mock_api:
        inserted = load_team_roster(
            sqlite_con_with_data,
            "1610612747",
            "2023-24",
            valid_players={"2544"},
            valid_teams={"1610612747"},
        )

    mock_api.assert_not_called()
    assert inserted == 1


def test_load_team_roster_cache_miss_calls_api_and_caches(
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

    from src.db.cache.file_cache import load_cache

    cached = load_cache("roster_1610612747_2023-24")
    assert cached is not None
    assert len(cached) == 1
    assert cached[0]["player_id"] == "2544"
