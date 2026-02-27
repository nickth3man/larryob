"""Tests for pipeline checkpoint logging state updates."""

from src.pipeline.checkpoint import log_checkpoint
from src.pipeline.models import CheckpointState, Stage


def test_log_checkpoint_initial_state(sqlite_con, caplog):
    import logging

    state = CheckpointState()
    sqlite_con.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES (?, ?, ?)",
        ("2023-24", 2023, 2024),
    )
    sqlite_con.commit()

    with caplog.at_level(logging.INFO):
        log_checkpoint(
            sqlite_con, Stage.DIMENSIONS, ["dim_season", "dim_team"], state, runlog_tail=5
        )

    assert state.status_map == {}
    assert state.table_counts == {"dim_season": 1, "dim_team": 0}
    assert state.last_timestamp is not None
    assert any(
        "post-dimensions: etl_run_log status counts" in record.message for record in caplog.records
    )
    assert any("post-dimensions: table=dim_season" in record.message for record in caplog.records)
    assert any("post-dimensions: table=dim_team" in record.message for record in caplog.records)


def test_log_checkpoint_with_existing_state(sqlite_con, caplog):
    import logging

    sqlite_con.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES (?, ?, ?)",
        ("2023-24", 2023, 2024),
    )
    sqlite_con.commit()

    state = CheckpointState()
    state.status_map = {}
    state.table_counts = {"dim_season": 1, "dim_team": 0}
    state.last_timestamp = 123.45

    sqlite_con.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES (?, ?, ?)",
        ("2024-25", 2024, 2025),
    )
    sqlite_con.execute(
        "INSERT INTO etl_run_log (table_name, season_id, loader, status, started_at) VALUES (?, ?, ?, ?, datetime('now'))",
        ("dim_player", "2023-24", "test_loader", "DONE"),
    )
    sqlite_con.commit()

    with caplog.at_level(logging.INFO):
        log_checkpoint(
            sqlite_con, Stage.DIMENSIONS, ["dim_season", "dim_team"], state, runlog_tail=5
        )

    assert any("delta=" in record.message for record in caplog.records)
    assert state.status_map == {"DONE": 1}
    assert state.table_counts == {"dim_season": 2, "dim_team": 0}
    assert state.last_timestamp != 123.45
    assert any(
        "dim_season" in record.message and "delta=1" in record.message for record in caplog.records
    )


def test_log_checkpoint_with_missing_table(sqlite_con, caplog):
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

    assert any(
        "nonexistent_table" in record.message and "n/a" in record.message
        for record in caplog.records
    )
    assert state.table_counts["nonexistent_table"] is None


def test_log_checkpoint_elapsed_time(sqlite_con, caplog):
    import logging

    state = CheckpointState()
    state.last_timestamp = 100.0

    with caplog.at_level(logging.INFO):
        log_checkpoint(sqlite_con, Stage.DIMENSIONS, ["dim_season"], state, runlog_tail=5)

    assert any("elapsed_since_previous=" in record.message for record in caplog.records)


def test_log_checkpoint_first_checkpoint_no_elapsed(sqlite_con, caplog):
    import logging

    state = CheckpointState()
    with caplog.at_level(logging.INFO):
        log_checkpoint(sqlite_con, Stage.DIMENSIONS, ["dim_season"], state, runlog_tail=5)

    assert any("elapsed_since_previous=n/a" in record.message for record in caplog.records)


def test_log_checkpoint_tuple_of_tables(sqlite_con):
    state = CheckpointState()
    log_checkpoint(
        sqlite_con,
        Stage.DIMENSIONS,
        ("dim_season", "dim_team", "dim_player"),
        state,
        runlog_tail=5,
    )
    assert "dim_season" in state.table_counts
    assert "dim_team" in state.table_counts
    assert "dim_player" in state.table_counts


def test_log_checkpoint_updates_state_atomically(sqlite_con):
    state = CheckpointState()
    state.status_map = {"DONE": 0}
    state.table_counts = {"dim_season": 0}
    state.last_timestamp = 100.0

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

    assert state.last_timestamp != old_timestamp
    assert state.status_map == {"DONE": 1}
    assert state.status_map != old_status_map
    assert "dim_season" in state.table_counts
    assert "dim_team" in state.table_counts
    assert state.table_counts != old_table_counts


def test_checkpoint_integration_full_workflow(sqlite_con, caplog):
    import logging

    state = CheckpointState()

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

    assert state.status_map == {"DONE": 2}
    assert state.table_counts == {"dim_season": 2}
    delta_logs = [r for r in caplog.records if "delta=" in r.message]
    assert len(delta_logs) > 0
    assert "DONE" in delta_logs[0].message
