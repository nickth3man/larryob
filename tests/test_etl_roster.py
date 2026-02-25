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
# load_team_roster — API retry logic tests                            #
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
    from src.etl.utils import load_cache

    cached = load_cache("roster_1610612747_2023-24")
    assert cached is not None
    assert len(cached) == 1
    assert cached[0]["player_id"] == "2544"


# ------------------------------------------------------------------ #
# _season_start_date — edge cases                                    #
# ------------------------------------------------------------------ #


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


# ------------------------------------------------------------------ #
# load_season_rosters tests                                           #
# ------------------------------------------------------------------ #


def test_load_season_rosters_skips_when_already_loaded(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """Skips loading when etl_run_log shows already loaded."""
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    # Mark as already loaded
    from src.db.tracking import record_run

    record_run(
        sqlite_con_with_data,
        "fact_roster",
        "2023-24",
        "roster.load_season_rosters",
        10,
        "ok",
        None,
    )

    from src.etl.roster import load_season_rosters

    total = load_season_rosters(sqlite_con_with_data, "2023-24")
    assert total == 0


def test_load_season_rosters_loads_all_teams(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """Loads rosters for all teams in dim_team."""
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    # Add the test season to dim_season to avoid FK constraint errors
    sqlite_con_with_data.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES (?, ?, ?)",
        ("2099-00", 2099, 2100),
    )

    # Mock the api_caller.call_with_backoff to directly return our test data
    mock_caller = MagicMock()

    call_count = 0

    def mock_backoff(fn, *, label):
        nonlocal call_count
        call_count += 1
        # Call the function to get the mock endpoint, then return our data
        # Return appropriate player for each team
        if call_count == 1:
            return [{"PLAYER_ID": "2544", "TeamID": "1610612747"}]
        else:
            return [{"PLAYER_ID": "203999", "TeamID": "1610612744"}]

    mock_caller.call_with_backoff = mock_backoff
    mock_caller.sleep_between_calls = lambda: None

    with patch("src.etl.roster.APICaller", return_value=mock_caller):
        from src.etl.roster import load_season_rosters

        # Use a unique season_id to avoid idempotency conflicts with other tests
        total = load_season_rosters(sqlite_con_with_data, "2099-00", api_caller=mock_caller)

    # Should load for both teams (LAL and GSW) in fixture
    assert total == 2


def test_load_season_rosters_records_metrics(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """Records ETL metrics in etl_run_log."""
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    # Add the test season to dim_season to avoid FK constraint errors
    sqlite_con_with_data.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES (?, ?, ?)",
        ("2098-99", 2098, 2099),
    )

    mock_caller = MagicMock()

    call_count = 0

    def mock_backoff(fn, *, label):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return [{"PLAYER_ID": "2544", "TeamID": "1610612747"}]
        else:
            return [{"PLAYER_ID": "203999", "TeamID": "1610612744"}]

    mock_caller.call_with_backoff = mock_backoff
    mock_caller.sleep_between_calls = lambda: None

    with patch("src.etl.roster.APICaller", return_value=mock_caller):
        from src.etl.roster import load_season_rosters

        load_season_rosters(sqlite_con_with_data, "2098-99", api_caller=mock_caller)

    # Check etl_run_log
    row = sqlite_con_with_data.execute(
        """SELECT row_count, status
           FROM etl_run_log
           WHERE table_name='fact_roster'
           AND season_id='2098-99'
           AND loader='roster.load_season_rosters'"""
    ).fetchone()

    assert row is not None
    assert row[0] == 2  # 2 teams
    assert row[1] == "ok"


def test_load_season_rosters_records_error_on_failure(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """Records error status when loading fails."""
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    mock_caller = MagicMock()

    def mock_backoff(fn, *, label):
        # Always raise an error to simulate persistent failure
        raise RuntimeError("API failure")

    mock_caller.call_with_backoff = mock_backoff
    mock_caller.sleep_between_calls = lambda: None

    # Make load_team_roster raise an error (bypass the try/except)
    with patch("src.etl.roster.load_team_roster", side_effect=RuntimeError("Load failed")):
        from src.etl.roster import load_season_rosters

        try:
            load_season_rosters(sqlite_con_with_data, "2097-98", api_caller=mock_caller)
            assert False, "Should have raised"
        except RuntimeError:
            pass

    # Check etl_run_log has error entry
    row = sqlite_con_with_data.execute(
        """SELECT status
           FROM etl_run_log
           WHERE table_name='fact_roster'
           AND season_id='2097-98'
           AND loader='roster.load_season_rosters'"""
    ).fetchone()

    assert row is not None
    assert row[0] == "error"


def test_load_season_rosters_uses_shared_api_caller(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """All team loads use the same API caller instance."""
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    # Add the test season to dim_season to avoid FK constraint errors
    sqlite_con_with_data.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES (?, ?, ?)",
        ("2096-97", 2096, 2097),
    )

    mock_caller = MagicMock()
    call_count = 0
    sleep_count = 0

    def mock_backoff(fn, *, label):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return [{"PLAYER_ID": "2544", "TeamID": "1610612747"}]
        else:
            return [{"PLAYER_ID": "203999", "TeamID": "1610612744"}]

    def mock_sleep():
        nonlocal sleep_count
        sleep_count += 1

    mock_caller.call_with_backoff = mock_backoff
    mock_caller.sleep_between_calls = mock_sleep

    with patch("src.etl.roster.APICaller", return_value=mock_caller):
        from src.etl.roster import load_season_rosters

        total = load_season_rosters(sqlite_con_with_data, "2096-97", api_caller=mock_caller)

    # sleep_between_calls is called after each team load
    # 2 teams = 2 calls
    assert total == 2
    assert sleep_count == 2  # Called after each of the 2 team loads


# ------------------------------------------------------------------ #
# load_rosters_for_seasons tests                                      #
# ------------------------------------------------------------------ #


def test_load_rosters_for_seasons_multiple_seasons(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """Loads rosters for multiple seasons."""
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    mock_caller = MagicMock()
    call_count = 0

    def mock_backoff(fn, *, label):
        nonlocal call_count
        call_count += 1
        # Return appropriate player for each team/season
        if call_count == 1:
            # Season 1, Team 1 (LAL)
            return [{"PLAYER_ID": "2544", "TeamID": "1610612747"}]
        elif call_count == 2:
            # Season 1, Team 2 (GSW)
            return [{"PLAYER_ID": "203999", "TeamID": "1610612744"}]
        elif call_count == 3:
            # Season 2, Team 1 (LAL)
            return [{"PLAYER_ID": "2544", "TeamID": "1610612747"}]
        else:
            # Season 2, Team 2 (GSW)
            return [{"PLAYER_ID": "203999", "TeamID": "1610612744"}]

    mock_caller.call_with_backoff = mock_backoff
    mock_caller.sleep_between_calls = lambda: None

    with patch("src.etl.roster.APICaller", return_value=mock_caller):
        from src.etl.roster import load_rosters_for_seasons

        # Add two new seasons to dim_season to avoid conflicts
        sqlite_con_with_data.execute(
            "INSERT INTO dim_season (season_id, start_year, end_year) VALUES (?, ?, ?)",
            ("2095-96", 2095, 2096),
        )
        sqlite_con_with_data.execute(
            "INSERT INTO dim_season (season_id, start_year, end_year) VALUES (?, ?, ?)",
            ("2094-95", 2094, 2095),
        )

        total = load_rosters_for_seasons(sqlite_con_with_data, ["2095-96", "2094-95"])

    # 2 teams * 2 seasons = 4 rows
    assert total == 4


def test_load_rosters_for_seasons_returns_cumulative_total(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """Returns total across all seasons."""
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    mock_caller = MagicMock()
    call_count = 0

    def mock_backoff(fn, *, label):
        nonlocal call_count
        call_count += 1
        # Return appropriate player for each team in each season
        if call_count == 1:
            # Season 1, Team 1 (LAL)
            return [{"PLAYER_ID": "2544", "TeamID": "1610612747"}]
        elif call_count == 2:
            # Season 1, Team 2 (GSW)
            return [{"PLAYER_ID": "203999", "TeamID": "1610612744"}]
        elif call_count == 3:
            # Season 2, Team 1 (LAL)
            return [{"PLAYER_ID": "2544", "TeamID": "1610612747"}]
        else:
            # Season 2, Team 2 (GSW)
            return [{"PLAYER_ID": "203999", "TeamID": "1610612744"}]

    mock_caller.call_with_backoff = mock_backoff
    mock_caller.sleep_between_calls = lambda: None

    with patch("src.etl.roster.APICaller", return_value=mock_caller):
        from src.etl.roster import load_rosters_for_seasons

        # Add two new seasons to dim_season to avoid conflicts
        sqlite_con_with_data.execute(
            "INSERT INTO dim_season (season_id, start_year, end_year) VALUES (?, ?, ?)",
            ("2093-94", 2093, 2094),
        )
        sqlite_con_with_data.execute(
            "INSERT INTO dim_season (season_id, start_year, end_year) VALUES (?, ?, ?)",
            ("2092-93", 2092, 2093),
        )

        total = load_rosters_for_seasons(sqlite_con_with_data, ["2093-94", "2092-93"])

    # First season: 2 teams, Second season: 2 teams = 4 total
    assert total == 4


def test_load_season_rosters_logs_progress_every_5_teams(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
    caplog,
) -> None:
    """Logs progress message every 5 teams processed."""
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    # Add 3 more teams to trigger the progress log (total = 5 teams)
    for i in range(3):
        team_id = f"161061274{i + 1}"
        sqlite_con_with_data.execute(
            """INSERT INTO dim_team
               (team_id, abbreviation, full_name, city, nickname,
                conference, division, color_primary, color_secondary, arena_name, founded_year)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (
                team_id,
                f"T{i}",
                f"Team {i}",
                f"City {i}",
                f"Nickname{i}",
                "West",
                "Pacific",
                "#000000",
                "#FFFFFF",
                f"Arena {i}",
                2000,
            ),
        )

    mock_caller = MagicMock()

    def mock_backoff(fn, *, label):
        return [{"PLAYER_ID": "2544", "TeamID": "1610612747"}]

    mock_caller.call_with_backoff = mock_backoff
    mock_caller.sleep_between_calls = lambda: None

    with patch("src.etl.roster.APICaller", return_value=mock_caller):
        from src.etl.roster import load_season_rosters

        with caplog.at_level("INFO"):
            load_season_rosters(sqlite_con_with_data, "2023-24", api_caller=mock_caller)

    # Should have logged progress at team 5
    assert any(
        "Roster: 5/5 teams processed for 2023-24" in record.message for record in caplog.records
    )
