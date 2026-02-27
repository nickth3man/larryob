"""Tests for pipeline checkpoint runlog/status helpers."""

import sqlite3

import pytest

from src.pipeline.checkpoint import _compute_delta, _get_runlog_status_map, _log_runlog_tail


def test_get_runlog_status_map_with_data(sqlite_con):
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
    assert _get_runlog_status_map(sqlite_con) == {}


def test_get_runlog_status_map_missing_table(sqlite_con):
    sqlite_con.execute("DROP TABLE etl_run_log")
    sqlite_con.commit()
    assert _get_runlog_status_map(sqlite_con) == {}


def test_get_runlog_status_map_raises_on_other_operational_error():
    from unittest.mock import MagicMock

    mock_con = MagicMock()
    mock_con.execute.side_effect = sqlite3.OperationalError("database is locked")

    with pytest.raises(sqlite3.OperationalError, match="database is locked"):
        _get_runlog_status_map(mock_con)


def test_compute_delta_normal_case():
    previous = {"DONE": 10, "FAILED": 2}
    current = {"DONE": 15, "FAILED": 1, "RUNNING": 3}
    assert _compute_delta(previous, current) == {"DONE": 5, "FAILED": -1, "RUNNING": 3}


def test_compute_delta_no_changes():
    previous = {"DONE": 10, "FAILED": 2}
    current = {"DONE": 10, "FAILED": 2}
    assert _compute_delta(previous, current) == {}


def test_compute_delta_same_object_reference():
    status_map = {"DONE": 10, "FAILED": 2}
    assert _compute_delta(status_map, status_map) == {}


def test_compute_delta_empty_previous():
    previous = {}
    current = {"DONE": 5, "FAILED": 1}
    assert _compute_delta(previous, current) == {"DONE": 5, "FAILED": 1}


def test_compute_delta_empty_current():
    previous = {"DONE": 5, "FAILED": 1}
    current = {}
    assert _compute_delta(previous, current) == {"DONE": -5, "FAILED": -1}


def test_compute_delta_keys_only_in_current():
    previous = {"DONE": 10}
    current = {"DONE": 10, "RUNNING": 3, "PENDING": 1}
    assert _compute_delta(previous, current) == {"PENDING": 1, "RUNNING": 3}


def test_compute_delta_keys_only_in_previous():
    previous = {"DONE": 10, "RUNNING": 3}
    current = {"DONE": 10}
    assert _compute_delta(previous, current) == {"RUNNING": -3}


def test_compute_delta_zero_deltas_excluded():
    previous = {"DONE": 10, "FAILED": 2, "RUNNING": 5}
    current = {"DONE": 15, "FAILED": 2, "RUNNING": 3}
    delta = _compute_delta(previous, current)
    assert delta == {"DONE": 5, "RUNNING": -2}
    assert "FAILED" not in delta


def test_log_runlog_tail_with_data(sqlite_con, caplog):
    import logging

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

    assert any("test-checkpoint: etl_run_log tail" in record.message for record in caplog.records)
    assert any("dim_season" in record.message for record in caplog.records)
    assert any("dim_team" in record.message for record in caplog.records)


def test_log_runlog_tail_missing_table(sqlite_con, caplog):
    import logging

    sqlite_con.execute("DROP TABLE etl_run_log")
    sqlite_con.commit()

    with caplog.at_level(logging.DEBUG):
        _log_runlog_tail(sqlite_con, "test-checkpoint", limit=5)

    assert any(
        "etl_run_log missing" in record.message and record.levelno == logging.DEBUG
        for record in caplog.records
    )


def test_log_runlog_tail_with_null_row_count(sqlite_con, caplog):
    import logging

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

    assert any("dim_season" in record.message for record in caplog.records)


def test_log_runlog_tail_with_null_season_id(sqlite_con, caplog):
    import logging

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

    assert any("dim_season" in record.message for record in caplog.records)


def test_log_runlog_tail_limit_respected(sqlite_con, caplog):
    import logging

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

    checkpoint_logs = [r for r in caplog.records if "test-checkpoint: runlog id=" in r.message]
    assert len(checkpoint_logs) == 3


def test_log_runlog_tail_raises_on_other_operational_error():
    from unittest.mock import MagicMock

    mock_con = MagicMock()
    mock_con.execute.side_effect = sqlite3.OperationalError("database is locked")

    with pytest.raises(sqlite3.OperationalError, match="database is locked"):
        _log_runlog_tail(mock_con, "test-checkpoint", limit=5)
