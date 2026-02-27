"""Tests: ETL roster loader — fetch operations."""

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


def test_season_start_date_with_current_season() -> None:
    """Test current season date calculation."""
    assert _season_start_date("2024-25") == "2024-10-01"


def test_season_start_date_with_historical_season() -> None:
    """Test historical season (e.g., 1980-81)."""
    assert _season_start_date("1980-81") == "1980-10-01"


def test_season_start_date_with_future_season() -> None:
    """Test future season."""
    assert _season_start_date("2027-28") == "2027-10-01"


def test_season_start_date_with_old_format() -> None:
    """Test old season format (1999-00)."""
    assert _season_start_date("1999-00") == "1999-10-01"


# ------------------------------------------------------------------ #
# load_team_roster — from cache                                       #
# ------------------------------------------------------------------ #


def test_load_team_roster_from_cache_inserts_rows(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """When cache is warm, roster rows are inserted without any API call."""
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
    """Calling twice with the same cached data must not create duplicates."""
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
    """When API returns no valid players for a team, returns 0."""
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
    """A cached empty list is honoured and does not trigger an API call."""
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


# ------------------------------------------------------------------ #
# load_team_roster — API path (mocked)                               #
# ------------------------------------------------------------------ #


def test_load_team_roster_from_api_success(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """When there is no cache, the API is called and rows inserted."""
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    mock_df = pd.DataFrame(
        [
            {"PLAYER_ID": "2544", "TeamID": "1610612747"},
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


def test_load_team_roster_filters_out_unknown_players(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """Players not in valid_players set must be filtered out."""
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    mock_df = pd.DataFrame(
        [
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
    """Empty DataFrame from API → returns 0."""
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    mock_ep = MagicMock()
    mock_ep.get_data_frames.return_value = [pd.DataFrame()]

    with patch("src.etl.roster.commonteamroster.CommonTeamRoster", return_value=mock_ep):
        inserted = load_team_roster(sqlite_con_with_data, "1610612747", "2023-24")
    assert inserted == 0


# ------------------------------------------------------------------ #
# load_team_roster — valid_players/valid_teams filter tests          #
# ------------------------------------------------------------------ #


def test_load_team_roster_queries_players_from_db_when_not_provided(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """When valid_players is None, queries dim_player table."""
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    mock_df = pd.DataFrame(
        [
            {"PLAYER_ID": "2544", "TeamID": "1610612747"},
        ]
    )
    mock_ep = MagicMock()
    mock_ep.get_data_frames.return_value = [mock_df]

    with patch("src.etl.roster.commonteamroster.CommonTeamRoster", return_value=mock_ep):
        inserted = load_team_roster(
            sqlite_con_with_data,
            "1610612747",
            "2023-24",
            # valid_players=None - should query DB
            valid_teams={"1610612747"},
        )
    assert inserted == 1


def test_load_team_roster_queries_teams_from_db_when_not_provided(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """When valid_teams is None, queries dim_team table."""
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    mock_df = pd.DataFrame(
        [
            {"PLAYER_ID": "2544", "TeamID": "1610612747"},
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
            # valid_teams=None - should query DB
        )
    assert inserted == 1


def test_load_team_roster_filters_by_valid_players_set(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """Only players in valid_players set are inserted."""
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    # API returns multiple players
    mock_df = pd.DataFrame(
        [
            {"PLAYER_ID": "2544", "TeamID": "1610612747"},  # Valid
            {"PLAYER_ID": "203999", "TeamID": "1610612747"},  # Valid
            {"PLAYER_ID": "99999", "TeamID": "1610612747"},  # Invalid
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

    # Verify invalid player was not inserted
    count = sqlite_con_with_data.execute(
        "SELECT COUNT(*) FROM fact_roster WHERE player_id='99999'"
    ).fetchone()[0]
    assert count == 0


def test_load_team_roster_filters_by_valid_teams_set(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """Only teams in valid_teams set are inserted."""
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    # API returns player for team not in valid_teams
    mock_df = pd.DataFrame(
        [
            {"PLAYER_ID": "2544", "TeamID": "9999999999"},  # Invalid team
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
            valid_teams={"1610612747"},  # 9999999999 not in valid set
        )
    assert inserted == 0


def test_load_team_roster_filters_by_both_players_and_teams(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """Both player and team must be valid to insert."""
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    mock_df = pd.DataFrame(
        [
            {"PLAYER_ID": "2544", "TeamID": "1610612747"},  # Both valid
            {"PLAYER_ID": "99999", "TeamID": "1610612747"},  # Invalid player
            {"PLAYER_ID": "2544", "TeamID": "9999999999"},  # Invalid team
            {"PLAYER_ID": "99999", "TeamID": "9999999999"},  # Both invalid
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


# ------------------------------------------------------------------ #
# load_team_roster — API retry logic tests                           #
# ------------------------------------------------------------------ #


def test_load_team_roster_api_failure_retries_and_succeeds(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """API fails on first call but succeeds on retry."""
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

    with patch(
        "src.etl.roster.commonteamroster.CommonTeamRoster",
        side_effect=side_effect,
    ):
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
    """API fails persistently; returns 0 after max retries."""
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    def side_effect(*args, **kwargs):
        """
        Placeholder callable used as a side effect that always raises a RuntimeError indicating the API is unavailable.

        Parameters:
            *args: Ignored positional arguments.
            **kwargs: Ignored keyword arguments.

        Raises:
            RuntimeError: Always raised with the message "API unavailable".
        """
        raise RuntimeError("API unavailable")

    with patch(
        "src.etl.roster.commonteamroster.CommonTeamRoster",
        side_effect=side_effect,
    ):
        with patch("src.etl.api_client.time.sleep"):
            inserted = load_team_roster(sqlite_con_with_data, "1610612747", "2023-24")
    assert inserted == 0


def test_load_team_roster_api_error_is_caught_and_logged(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
    caplog,
) -> None:
    """API errors are caught, logged, and return 0."""
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


# ------------------------------------------------------------------ #
# load_team_roster — cache tests                                     #
# ------------------------------------------------------------------ #


def test_load_team_roster_cache_hit_skips_api_call(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """When cache has data, API is not called."""
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
    """When cache miss, API is called and result is cached."""
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

    # Verify cache was written
    from src.db.cache.file_cache import load_cache

    cached = load_cache("roster_1610612747_2023-24")
    assert cached is not None
    assert len(cached) == 1
    assert cached[0]["player_id"] == "2544"


# ------------------------------------------------------------------ #
# load_team_roster — data transformation tests                       #
# ------------------------------------------------------------------ #


def test_load_team_roster_uses_team_id_from_api_response(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """TeamID from API response is used, not the parameter."""
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    # API returns different team_id than parameter
    mock_df = pd.DataFrame([{"PLAYER_ID": "2544", "TeamID": "1610612744"}])
    mock_ep = MagicMock()
    mock_ep.get_data_frames.return_value = [mock_df]

    with patch("src.etl.roster.commonteamroster.CommonTeamRoster", return_value=mock_ep):
        inserted = load_team_roster(
            sqlite_con_with_data,
            "1610612747",  # Parameter team_id
            "2023-24",
            valid_players={"2544"},
            valid_teams={"1610612744"},  # API response team_id
        )

    # Should insert with team_id from API response
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
    """Handles records with missing PLAYER_ID gracefully."""
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    # Mix of valid and missing PLAYER_ID
    mock_df = pd.DataFrame(
        [
            {"PLAYER_ID": "2544", "TeamID": "1610612747"},
            {"TeamID": "1610612747"},  # Missing PLAYER_ID
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

    # Only valid record inserted
    assert inserted == 1


def test_load_team_roster_sets_end_date_to_null(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """end_date is always set to NULL for current stints."""
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
