"""Tests: Pipeline checkpoint logging and state tracking."""

import sqlite3
from unittest.mock import patch

import pytest

from src.pipeline.checkpoint import (
    _compute_delta,
    _get_runlog_status_map,
    _log_runlog_tail,
    _safe_table_count,
    log_checkpoint,
)
from src.pipeline.models import CheckpointState, Stage

# ------------------------------------------------------------------ #
# _safe_table_count() Tests                                           #
# ------------------------------------------------------------------ #


def test_safe_table_count_valid_table(sqlite_con):
    """Test _safe_table_count returns correct count for valid table."""
    count = _safe_table_count(sqlite_con, "dim_season")
    assert count == 0  # Empty table

    # Add a row
    sqlite_con.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES (?, ?, ?)",
        ("2023-24", 2023, 2024),
    )
    sqlite_con.commit()

    count = _safe_table_count(sqlite_con, "dim_season")
    assert count == 1


def test_safe_table_count_nonexistent_table(sqlite_con):
    """Test _safe_table_count returns None for missing table."""
    count = _safe_table_count(sqlite_con, "nonexistent_table")
    assert count is None


def test_safe_table_count_invalid_identifier(sqlite_con):
    """Test _safe_table_count returns None for invalid table name."""
    # Test SQL injection attempt
    count = _safe_table_count(sqlite_con, "dim_season; DROP TABLE dim_season;--")
    assert count is None

    # Test invalid characters
    count = _safe_table_count(sqlite_con, "dim-season")
    assert count is None

    count = _safe_table_count(sqlite_con, "dim season")
    assert count is None

    count = _safe_table_count(sqlite_con, "dim.season")
    assert count is None


def test_safe_table_count_validates_with_etl_utils(sqlite_con):
    """Test _safe_table_count uses _validate_identifier from etl.utils."""
    # Test that it calls the validation function
    # Even if the table name passes the regex, it should fail the deeper validation
    # if there's an issue (e.g., SQL keyword)
    count = _safe_table_count(sqlite_con, "select")  # SQL keyword
    # Should return None because validation should fail
    # The regex might allow it, but the deeper validation should not
    assert count is None


def test_safe_table_count_raises_value_error_from_validation(sqlite_con):
    """Test _safe_table_count handles ValueError from _validate_identifier.

    The ValueError case (lines 52-53) occurs when _validate_identifier raises ValueError.
    This test mocks that scenario to ensure the exception is caught and handled.
    """

    def mock_validate_raises_value(name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")

    # Patch at the source module where it's imported from
    with patch("src.etl.utils._validate_identifier", side_effect=mock_validate_raises_value):
        # Even though the name passes the regex, the validation function raises ValueError
        count = _safe_table_count(sqlite_con, "valid_table")
        assert count is None


# ------------------------------------------------------------------ #
# _get_runlog_status_map() Tests                                      #
# ------------------------------------------------------------------ #


def test_get_runlog_status_map_with_data(sqlite_con):
    """Test _get_runlog_status_map returns correct status counts."""
    # Insert some test data
    sqlite_con.execute(
        """
        INSERT INTO etl_run_log (table_name, season_id, loader, status, started_at)
        VALUES (?, ?, ?, ?, datetime('now'))
        """,
        ("dim_season", "2023-24", "test_loader", "DONE"),
    )
    sqlite_con.execute(
        """
        INSERT INTO etl_run_log (table_name, season_id, loader, status, started_at)
        VALUES (?, ?, ?, ?, datetime('now'))
        """,
        ("dim_team", "2023-24", "test_loader", "DONE"),
    )
    sqlite_con.execute(
        """
        INSERT INTO etl_run_log (table_name, season_id, loader, status, started_at)
        VALUES (?, ?, ?, ?, datetime('now'))
        """,
        ("dim_player", "2023-24", "test_loader", "FAILED"),
    )
    sqlite_con.commit()

    status_map = _get_runlog_status_map(sqlite_con)
    assert status_map == {"DONE": 2, "FAILED": 1}


def test_get_runlog_status_map_empty(sqlite_con):
    """Test _get_runlog_status_map returns empty dict for empty table."""
    status_map = _get_runlog_status_map(sqlite_con)
    assert status_map == {}


def test_get_runlog_status_map_missing_table(sqlite_con):
    """Test _get_runlog_status_map returns empty dict when table doesn't exist."""
    # Drop the etl_run_log table
    sqlite_con.execute("DROP TABLE etl_run_log")
    sqlite_con.commit()

    status_map = _get_runlog_status_map(sqlite_con)
    assert status_map == {}


def test_get_runlog_status_map_raises_on_other_operational_error(sqlite_con):
    """Test _get_runlog_status_map re-raises non-missing-table errors."""
    # We need to test that non-"no such table" OperationalErrors are re-raised
    # Since we can't easily mock sqlite_con.execute, let's test the actual behavior
    # by using a connection that's been closed

    # Create a separate connection and close it
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE etl_run_log (status TEXT)")
    con.close()

    # This should raise an error (but ProgrammingError, not OperationalError)
    # Actually, let's skip this test since the scenario is hard to reproduce
    # The code path is simple: if it's not "no such table", it re-raises
    # This is tested implicitly by the fact that missing table returns {}
    # Let's instead verify the code structure is correct by testing with a mock

    from unittest.mock import MagicMock

    mock_con = MagicMock()
    mock_con.execute.side_effect = sqlite3.OperationalError("database is locked")

    with pytest.raises(sqlite3.OperationalError, match="database is locked"):
        _get_runlog_status_map(mock_con)


# ------------------------------------------------------------------ #
# _compute_delta() Tests                                              #
# ------------------------------------------------------------------ #


def test_compute_delta_normal_case():
    """Test _compute_delta with different previous and current maps."""
    previous = {"DONE": 10, "FAILED": 2}
    current = {"DONE": 15, "FAILED": 1, "RUNNING": 3}

    delta = _compute_delta(previous, current)
    assert delta == {"DONE": 5, "FAILED": -1, "RUNNING": 3}


def test_compute_delta_no_changes():
    """Test _compute_delta returns empty dict when no changes."""
    previous = {"DONE": 10, "FAILED": 2}
    current = {"DONE": 10, "FAILED": 2}

    delta = _compute_delta(previous, current)
    assert delta == {}


def test_compute_delta_same_object_reference():
    """Test _compute_delta returns empty dict when same object passed."""
    status_map = {"DONE": 10, "FAILED": 2}

    # Pass same object as both arguments (tests identity check on line 85)
    delta = _compute_delta(status_map, status_map)
    assert delta == {}


def test_compute_delta_empty_previous():
    """Test _compute_delta with empty previous map."""
    previous = {}
    current = {"DONE": 5, "FAILED": 1}

    delta = _compute_delta(previous, current)
    assert delta == {"DONE": 5, "FAILED": 1}


def test_compute_delta_empty_current():
    """Test _compute_delta with empty current map."""
    previous = {"DONE": 5, "FAILED": 1}
    current = {}

    delta = _compute_delta(previous, current)
    assert delta == {"DONE": -5, "FAILED": -1}


def test_compute_delta_keys_only_in_current():
    """Test _compute_delta with new keys in current."""
    previous = {"DONE": 10}
    current = {"DONE": 10, "RUNNING": 3, "PENDING": 1}

    delta = _compute_delta(previous, current)
    assert delta == {"RUNNING": 3, "PENDING": 1}


def test_compute_delta_keys_only_in_previous():
    """Test _compute_delta with keys removed in current."""
    previous = {"DONE": 10, "RUNNING": 3}
    current = {"DONE": 10}

    delta = _compute_delta(previous, current)
    assert delta == {"RUNNING": -3}


def test_compute_delta_zero_deltas_excluded():
    """Test _compute_delta excludes keys with zero delta."""
    previous = {"DONE": 10, "FAILED": 2, "RUNNING": 5}
    current = {"DONE": 15, "FAILED": 2, "RUNNING": 3}

    # FAILED should not appear in delta (no change)
    delta = _compute_delta(previous, current)
    assert delta == {"DONE": 5, "RUNNING": -2}
    assert "FAILED" not in delta


# ------------------------------------------------------------------ #
# _log_runlog_tail() Tests                                            #
# ------------------------------------------------------------------ #


def test_log_runlog_tail_with_data(sqlite_con, caplog):
    """Test _log_runlog_tail logs recent entries."""
    import logging

    # Insert test data
    sqlite_con.execute(
        """
        INSERT INTO etl_run_log
        (table_name, season_id, loader, status, row_count, started_at, finished_at)
        VALUES (?, ?, ?, ?, ?, datetime('now', '-2 hours'), datetime('now', '-1 hour'))
        """,
        ("dim_season", "2023-24", "test_loader", "DONE", 10),
    )
    sqlite_con.execute(
        """
        INSERT INTO etl_run_log
        (table_name, season_id, loader, status, row_count, started_at, finished_at)
        VALUES (?, ?, ?, ?, ?, datetime('now', '-1 hour'), datetime('now'))
        """,
        ("dim_team", "2023-24", "test_loader", "DONE", 30),
    )
    sqlite_con.commit()

    with caplog.at_level(logging.INFO):
        _log_runlog_tail(sqlite_con, "test-checkpoint", limit=2)

    # Check that log entries were created
    assert any("test-checkpoint: etl_run_log tail" in record.message for record in caplog.records)
    assert any("dim_season" in record.message for record in caplog.records)
    assert any("dim_team" in record.message for record in caplog.records)


def test_log_runlog_tail_missing_table(sqlite_con, caplog):
    """Test _log_runlog_tail handles missing table gracefully."""
    import logging

    # Drop the etl_run_log table
    sqlite_con.execute("DROP TABLE etl_run_log")
    sqlite_con.commit()

    with caplog.at_level(logging.DEBUG):
        _log_runlog_tail(sqlite_con, "test-checkpoint", limit=5)

    # Should log debug message about missing table
    assert any(
        "etl_run_log missing" in record.message and record.levelno == logging.DEBUG
        for record in caplog.records
    )


def test_log_runlog_tail_with_null_row_count(sqlite_con, caplog):
    """Test _log_runlog_tail handles NULL row_count."""
    import logging

    # Insert data with NULL row_count (COALESCE will convert to -1)
    sqlite_con.execute(
        """
        INSERT INTO etl_run_log
        (table_name, season_id, loader, status, started_at)
        VALUES (?, ?, ?, ?, datetime('now'))
        """,
        ("dim_season", "2023-24", "test_loader", "RUNNING"),
    )
    sqlite_con.commit()

    with caplog.at_level(logging.INFO):
        _log_runlog_tail(sqlite_con, "test-checkpoint", limit=1)

    # Should log entry with row_count=None (displayed as None in log)
    assert any("dim_season" in record.message for record in caplog.records)


def test_log_runlog_tail_with_null_season_id(sqlite_con, caplog):
    """Test _log_runlog_tail handles NULL season_id."""
    import logging

    # Insert data with NULL season_id (COALESCE will convert to '-')
    sqlite_con.execute(
        """
        INSERT INTO etl_run_log
        (table_name, loader, status, row_count, started_at)
        VALUES (?, ?, ?, ?, datetime('now'))
        """,
        ("dim_season", "test_loader", "DONE", 10),
    )
    sqlite_con.commit()

    with caplog.at_level(logging.INFO):
        _log_runlog_tail(sqlite_con, "test-checkpoint", limit=1)

    # Should log entry with season_id='-'
    assert any("dim_season" in record.message for record in caplog.records)


def test_log_runlog_tail_limit_respected(sqlite_con, caplog):
    """Test _log_runlog_tail respects limit parameter."""
    import logging

    # Insert 5 rows
    for i in range(5):
        sqlite_con.execute(
            f"""
            INSERT INTO etl_run_log
            (table_name, season_id, loader, status, row_count, started_at)
            VALUES (?, ?, ?, ?, ?, datetime('now', '-{i} hours'))
            """,
            (f"table_{i}", "2023-24", "test_loader", "DONE", i * 10),
        )
    sqlite_con.commit()

    with caplog.at_level(logging.INFO):
        _log_runlog_tail(sqlite_con, "test-checkpoint", limit=3)

    # Should only log 3 rows (plus the header)
    checkpoint_logs = [r for r in caplog.records if "test-checkpoint: runlog id=" in r.message]
    assert len(checkpoint_logs) == 3


def test_log_runlog_tail_raises_on_other_operational_error():
    """Test _log_runlog_tail re-raises non-missing-table errors."""
    # Use a mock connection to test the error handling
    from unittest.mock import MagicMock

    mock_con = MagicMock()
    mock_con.execute.side_effect = sqlite3.OperationalError("database is locked")

    with pytest.raises(sqlite3.OperationalError, match="database is locked"):
        _log_runlog_tail(mock_con, "test-checkpoint", limit=5)


# ------------------------------------------------------------------ #
# log_checkpoint() Tests                                              #
# ------------------------------------------------------------------ #


def test_log_checkpoint_initial_state(sqlite_con, caplog):
    """Test log_checkpoint with initial empty state."""
    import logging

    state = CheckpointState()

    # Add some data to dim_season
    sqlite_con.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES (?, ?, ?)",
        ("2023-24", 2023, 2024),
    )
    sqlite_con.commit()

    with caplog.at_level(logging.INFO):
        log_checkpoint(
            sqlite_con,
            Stage.DIMENSIONS,
            ["dim_season", "dim_team"],
            state,
            runlog_tail=5,
        )

    # Verify state was updated
    assert state.status_map == {}
    assert state.table_counts == {"dim_season": 1, "dim_team": 0}
    assert state.last_timestamp is not None

    # Verify log messages
    assert any(
        "post-dimensions: etl_run_log status counts" in record.message for record in caplog.records
    )
    assert any("post-dimensions: table=dim_season" in record.message for record in caplog.records)
    assert any("post-dimensions: table=dim_team" in record.message for record in caplog.records)


def test_log_checkpoint_with_existing_state(sqlite_con, caplog):
    """Test log_checkpoint with existing state shows delta."""
    import logging

    # First, add one row to dim_season
    sqlite_con.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES (?, ?, ?)",
        ("2023-24", 2023, 2024),
    )
    sqlite_con.commit()

    # Setup initial state to reflect current DB state
    state = CheckpointState()
    state.status_map = {}  # Empty initially
    state.table_counts = {"dim_season": 1, "dim_team": 0}
    state.last_timestamp = 123.45

    # Add more data
    sqlite_con.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES (?, ?, ?)",
        ("2024-25", 2024, 2025),
    )
    # Add a run log entry
    sqlite_con.execute(
        "INSERT INTO etl_run_log (table_name, season_id, loader, status, started_at) VALUES (?, ?, ?, ?, datetime('now'))",
        ("dim_player", "2023-24", "test_loader", "DONE"),
    )
    sqlite_con.commit()

    with caplog.at_level(logging.INFO):
        log_checkpoint(
            sqlite_con,
            Stage.DIMENSIONS,
            ["dim_season", "dim_team"],
            state,
            runlog_tail=5,
        )

    # Verify delta was logged
    assert any("delta=" in record.message for record in caplog.records)

    # Verify state was updated with actual DB state
    assert state.status_map == {"DONE": 1}  # Only 1 entry in DB
    assert state.table_counts == {"dim_season": 2, "dim_team": 0}  # dim_season now has 2 rows
    assert state.last_timestamp != 123.45  # Should be updated

    # Check that dim_season delta is logged correctly
    assert any(
        "dim_season" in record.message and "delta=1" in record.message for record in caplog.records
    )


def test_log_checkpoint_with_missing_table(sqlite_con, caplog):
    """Test log_checkpoint handles missing table gracefully."""
    import logging

    state = CheckpointState()

    with caplog.at_level(logging.INFO):
        log_checkpoint(
            sqlite_con,
            Stage.DIMENSIONS,
            ["dim_season", "nonexistent_table"],
            state,
            runlog_tail=5,
        )

    # Should log n/a for missing table
    assert any(
        "nonexistent_table" in record.message and "n/a" in record.message
        for record in caplog.records
    )

    # State should have None for missing table
    assert state.table_counts["nonexistent_table"] is None


def test_log_checkpoint_elapsed_time(sqlite_con, caplog):
    """Test log_checkpoint includes elapsed time since previous checkpoint."""
    import logging

    state = CheckpointState()
    state.last_timestamp = 100.0

    with caplog.at_level(logging.INFO):
        log_checkpoint(
            sqlite_con,
            Stage.DIMENSIONS,
            ["dim_season"],
            state,
            runlog_tail=5,
        )

    # Should include elapsed time in log
    assert any("elapsed_since_previous=" in record.message for record in caplog.records)


def test_log_checkpoint_first_checkpoint_no_elapsed(sqlite_con, caplog):
    """Test log_checkpoint shows 'n/a' for elapsed on first checkpoint."""
    import logging

    state = CheckpointState()  # No last_timestamp set

    with caplog.at_level(logging.INFO):
        log_checkpoint(
            sqlite_con,
            Stage.DIMENSIONS,
            ["dim_season"],
            state,
            runlog_tail=5,
        )

    # Should show 'n/a' for elapsed time
    assert any("elapsed_since_previous=n/a" in record.message for record in caplog.records)


def test_log_checkpoint_tuple_of_tables(sqlite_con):
    """Test log_checkpoint accepts tuple of tables."""
    state = CheckpointState()

    # Should work with tuple
    log_checkpoint(
        sqlite_con,
        Stage.DIMENSIONS,
        ("dim_season", "dim_team", "dim_player"),
        state,
        runlog_tail=5,
    )

    # State should have all tables
    assert "dim_season" in state.table_counts
    assert "dim_team" in state.table_counts
    assert "dim_player" in state.table_counts


def test_log_checkpoint_updates_state_atomically(sqlite_con):
    """Test log_checkpoint updates all state fields together."""
    state = CheckpointState()

    # Set initial values
    state.status_map = {"DONE": 0}  # No entries yet
    state.table_counts = {"dim_season": 0}
    state.last_timestamp = 100.0

    # Add run log entries
    sqlite_con.execute(
        "INSERT INTO etl_run_log (table_name, season_id, loader, status, started_at) VALUES (?, ?, ?, ?, datetime('now'))",
        ("dim_team", "2023-24", "test_loader", "DONE"),
    )
    sqlite_con.commit()

    old_timestamp = state.last_timestamp
    old_status_map = state.status_map.copy()
    old_table_counts = state.table_counts.copy()

    log_checkpoint(
        sqlite_con,
        Stage.DIMENSIONS,
        ["dim_season", "dim_team"],
        state,
        runlog_tail=5,
    )

    # All fields should be updated
    assert state.last_timestamp != old_timestamp
    # status_map should change from {} to {"DONE": 1}
    assert state.status_map == {"DONE": 1}
    assert state.status_map != old_status_map
    # table_counts should have both tables
    assert "dim_season" in state.table_counts
    assert "dim_team" in state.table_counts
    assert state.table_counts != old_table_counts


# ------------------------------------------------------------------ #
# Integration Tests                                                   #
# ------------------------------------------------------------------ #


def test_checkpoint_integration_full_workflow(sqlite_con, caplog):
    """Test checkpoint tracking through multiple stages."""
    import logging

    state = CheckpointState()

    # Stage 1: Initial load
    sqlite_con.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES (?, ?, ?)",
        ("2023-24", 2023, 2024),
    )
    sqlite_con.execute(
        "INSERT INTO etl_run_log (table_name, season_id, loader, status, row_count, started_at) VALUES (?, ?, ?, ?, ?, datetime('now'))",
        ("dim_season", "2023-24", "loader1", "DONE", 1),
    )
    sqlite_con.commit()

    with caplog.at_level(logging.INFO):
        log_checkpoint(sqlite_con, Stage.DIMENSIONS, ["dim_season"], state, runlog_tail=10)

    assert state.status_map == {"DONE": 1}
    assert state.table_counts == {"dim_season": 1}

    # Stage 2: Add more data
    sqlite_con.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES (?, ?, ?)",
        ("2024-25", 2024, 2025),
    )
    sqlite_con.execute(
        "INSERT INTO etl_run_log (table_name, season_id, loader, status, row_count, started_at) VALUES (?, ?, ?, ?, ?, datetime('now'))",
        ("dim_season", "2024-25", "loader1", "DONE", 1),
    )
    sqlite_con.commit()

    caplog.clear()
    with caplog.at_level(logging.INFO):
        log_checkpoint(sqlite_con, Stage.DIMENSIONS, ["dim_season"], state, runlog_tail=10)

    # Should show delta
    assert state.status_map == {"DONE": 2}
    assert state.table_counts == {"dim_season": 2}

    # Check that delta was logged
    delta_logs = [r for r in caplog.records if "delta=" in r.message]
    assert len(delta_logs) > 0
    assert "DONE" in delta_logs[0].message
