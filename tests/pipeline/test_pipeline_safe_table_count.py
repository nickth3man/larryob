"""Tests for pipeline checkpoint safe table counting helpers."""

from unittest.mock import patch

from src.pipeline.checkpoint import _safe_table_count


def test_safe_table_count_valid_table(sqlite_con):
    """_safe_table_count should return row count for an existing table."""
    count = _safe_table_count(sqlite_con, "dim_season")
    assert count == 0

    sqlite_con.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES (?, ?, ?)",
        ("2023-24", 2023, 2024),
    )
    sqlite_con.commit()

    count = _safe_table_count(sqlite_con, "dim_season")
    assert count == 1


def test_safe_table_count_nonexistent_table(sqlite_con):
    """_safe_table_count should return None for a missing table."""
    count = _safe_table_count(sqlite_con, "nonexistent_table")
    assert count is None


def test_safe_table_count_invalid_identifier(sqlite_con):
    """_safe_table_count should return None for invalid table identifiers."""
    assert _safe_table_count(sqlite_con, "dim_season; DROP TABLE dim_season;--") is None
    assert _safe_table_count(sqlite_con, "dim-season") is None
    assert _safe_table_count(sqlite_con, "dim season") is None
    assert _safe_table_count(sqlite_con, "dim.season") is None


def test_safe_table_count_validates_with_db_identifier_check(sqlite_con):
    """_safe_table_count should use _validate_identifier from db.operations."""
    count = _safe_table_count(sqlite_con, "select")
    assert count is None


def test_safe_table_count_raises_value_error_from_validation(sqlite_con):
    """_safe_table_count should return None when _validate_identifier raises ValueError."""
    sqlite_con.execute("CREATE TABLE valid_table(id INTEGER)")
    sqlite_con.commit()

    with patch(
        "src.db.operations._validate_identifier",
        side_effect=ValueError("Invalid SQL identifier: 'valid_table'"),
    ) as mock_validate:
        count = _safe_table_count(sqlite_con, "valid_table")

    assert count is None
    mock_validate.assert_called_once_with("valid_table")
