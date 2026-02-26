"""Tests: ETL roster loader — transformation and processing."""

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.etl.roster import load_rosters_for_seasons, load_season_rosters

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
        total = load_season_rosters(sqlite_con_with_data, "2096-97", api_caller=mock_caller)

    # sleep_between_calls is called after each team load
    # 2 teams = 2 calls
    assert total == 2
    assert sleep_count == 2  # Called after each of the 2 team loads


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
        with caplog.at_level("INFO"):
            load_season_rosters(sqlite_con_with_data, "2023-24", api_caller=mock_caller)

    # Should have logged progress at team 5
    assert any(
        "Roster: 5/5 teams processed for 2023-24" in record.message for record in caplog.records
    )


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
