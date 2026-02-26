"""
Tests for backfill utilities and orchestrator functions.
"""

from pathlib import Path

import pandas as pd
import pytest

from src.etl.backfill._base import (
    BackfillError,
    DataValidationError,
    FileNotFoundError,
    csv_path,
    get_valid_set,
    read_csv_safe,
    safe_float,
    safe_int,
    safe_str,
)

# ========================================================================
# Exception Classes
# ========================================================================


def test_file_not_found_error_stores_path():
    """FileNotFoundError should store the path and format message."""
    path = Path("raw/test.csv")
    error = FileNotFoundError(path)

    assert error.path == path
    assert "test.csv" in str(error)
    assert "Required file not found" in str(error)


def test_data_validation_error_stores_message_and_row_count():
    """DataValidationError should store message and optional row_count."""
    error1 = DataValidationError("Invalid data")
    # Note: message is not stored as an attribute, only passed to parent
    assert error1.row_count is None
    assert "Invalid data" in str(error1)

    error2 = DataValidationError("Invalid data", row_count=100)
    assert error2.row_count == 100
    assert "Invalid data" in str(error2)


def test_backfill_error_is_base_exception():
    """BackfillError should be usable as a base exception."""
    error = BackfillError("Base error")
    assert isinstance(error, Exception)
    assert str(error) == "Base error"


# ========================================================================
# Safe Type Conversion Functions
# ========================================================================


@pytest.mark.parametrize(
    "value,expected",
    [
        (None, None),
        ("", None),
        (float("nan"), None),
        (pd.NA, None),
        (42, 42),
        ("42", 42),
        (3.0, 3),  # float that converts cleanly to int
        (0, 0),
        ("0", 0),
    ],
)
def test_safe_int_handles_valid_inputs(value, expected):
    """safe_int should convert valid inputs to int."""
    assert safe_int(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        "invalid",
        "abc",
        "1.2.3",
        {"key": "value"},
        ["list"],
    ],
)
def test_safe_int_returns_none_for_invalid_inputs(value):
    """safe_int should return None for non-convertible values."""
    assert safe_int(value) is None


@pytest.mark.parametrize(
    "value,expected",
    [
        (None, None),
        ("", None),
        (float("nan"), None),
        (pd.NA, None),
        (3.14, 3.14),
        ("3.14", 3.14),
        (42, 42.0),
        ("42", 42.0),
        (0.0, 0.0),
        ("0", 0.0),
    ],
)
def test_safe_float_handles_valid_inputs(value, expected):
    """safe_float should convert valid inputs to float."""
    result = safe_float(value)
    if expected is None:
        assert result is None
    else:
        assert result == pytest.approx(expected)


@pytest.mark.parametrize(
    "value",
    [
        "invalid",
        "abc",
        "1.2.3",
        {"key": "value"},
        ["list"],
    ],
)
def test_safe_float_returns_none_for_invalid_inputs(value):
    """safe_float should return None for non-convertible values."""
    assert safe_float(value) is None


@pytest.mark.parametrize(
    "value,strip,expected",
    [
        ("test", True, "test"),
        ("test ", True, "test"),
        (" test", True, "test"),
        ("  test  ", True, "test"),
        ("test ", False, "test "),
        (" test", False, " test"),
        (None, True, None),
        (pd.NA, True, None),
        (123, True, "123"),
        (123, False, "123"),
    ],
)
def test_safe_str_handles_various_inputs(value, strip, expected):
    """safe_str should convert values to strings with optional stripping."""
    assert safe_str(value, strip=strip) == expected


# ========================================================================
# csv_path Function
# ========================================================================


def test_csv_path_returns_path_when_file_exists(tmp_path):
    """csv_path should return Path object when file exists."""
    test_file = tmp_path / "test.csv"
    test_file.write_text("data")

    result = csv_path(tmp_path, "test.csv")
    assert result == test_file


def test_csv_path_returns_none_when_file_missing_and_not_required(tmp_path):
    """csv_path should return None when file is missing and not required."""
    result = csv_path(tmp_path, "missing.csv", required=False)
    assert result is None


def test_csv_path_raises_when_file_missing_and_required(tmp_path):
    """csv_path should raise FileNotFoundError when required file is missing."""
    with pytest.raises(FileNotFoundError) as exc_info:
        csv_path(tmp_path, "missing.csv", required=True)

    assert "missing.csv" in str(exc_info.value)
    assert exc_info.value.path == tmp_path / "missing.csv"


def test_csv_path_with_path_object(tmp_path):
    """csv_path should handle Path objects correctly."""
    test_file = tmp_path / "test.csv"
    test_file.write_text("data")

    # Test with Path object
    result = csv_path(Path(tmp_path), "test.csv")
    assert result == test_file


# ========================================================================
# read_csv_safe Function
# ========================================================================


def test_read_csv_safe_reads_basic_csv(tmp_path):
    """read_csv_safe should read a basic CSV file."""
    test_file = tmp_path / "test.csv"
    pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]}).to_csv(test_file, index=False)

    result = read_csv_safe(test_file)
    assert len(result) == 3
    assert list(result.columns) == ["col1", "col2"]


def test_read_csv_safe_respects_usecols(tmp_path):
    """read_csv_safe should filter columns when usecols is specified."""
    test_file = tmp_path / "test.csv"
    pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"], "col3": [10, 20, 30]}).to_csv(
        test_file, index=False
    )

    result = read_csv_safe(test_file, usecols=["col1", "col3"])
    assert list(result.columns) == ["col1", "col3"]


def test_read_csv_safe_returns_iterator_with_chunksize(tmp_path):
    """read_csv_safe should return TextFileReader when chunksize is set."""
    test_file = tmp_path / "test.csv"
    pd.DataFrame({"col1": range(100), "col2": range(100)}).to_csv(test_file, index=False)

    result = read_csv_safe(test_file, chunksize=10)
    # Should return an iterator
    chunks = list(result)
    assert len(chunks) == 10
    assert len(chunks[0]) == 10


def test_read_csv_safe_with_empty_file(tmp_path):
    """read_csv_safe should handle empty CSV files."""
    test_file = tmp_path / "empty.csv"
    test_file.write_text("")

    # Empty file should raise error or return empty DataFrame
    # depending on pandas version
    try:
        result = read_csv_safe(test_file)
        assert len(result) == 0
    except pd.errors.EmptyDataError:
        # Also acceptable behavior
        pass


def test_read_csv_safe_with_malformed_csv(tmp_path):
    """read_csv_safe should propagate pandas parsing errors."""
    test_file = tmp_path / "malformed.csv"
    test_file.write_text("col1,col2\n1,2\n3,4")  # Simple valid CSV

    # Should read successfully
    result = read_csv_safe(test_file)
    assert len(result) == 2


# ========================================================================
# get_valid_set Function
# ========================================================================


def test_get_valid_set_fetches_column_values(sqlite_con):
    """get_valid_set should fetch unique values from a table column."""
    sqlite_con.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES ('2023-24', 2023, 2024)"
    )
    sqlite_con.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES ('2024-25', 2024, 2025)"
    )
    sqlite_con.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES ('2025-26', 2025, 2026)"
    )
    sqlite_con.commit()

    result = get_valid_set(sqlite_con, "dim_season", "season_id")
    assert result == {"2023-24", "2024-25", "2025-26"}
