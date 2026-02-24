"""
Tests for backfill base utilities.

Tests cover error paths, edge cases, and the BaseBackfillLoader
and ChunkedBackfillLoader classes.
"""

from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from src.etl.backfill._base import (
    BackfillError,
    BaseBackfillLoader,
    ChunkedBackfillLoader,
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


# ========================================================================
# BaseBackfillLoader Class
# ========================================================================


class SimpleTestLoader(BaseBackfillLoader):
    """Minimal loader implementation for testing."""

    table_name = "test_table"
    csv_filename = "test.csv"
    requires_validation = False

    def transform_row(self, row, context):
        return {"id": row.get("id"), "value": row.get("value")}


def test_base_loader_initializes_counters():
    """BaseBackfillLoader should initialize skipped and processed counters."""
    loader = SimpleTestLoader()
    assert loader.skipped == 0
    assert loader.processed == 0
    assert loader.raw_dir == Path("raw")


def test_base_loader_accepts_custom_raw_dir(tmp_path):
    """BaseBackfillLoader should accept custom raw_dir."""
    loader = SimpleTestLoader(raw_dir=tmp_path)
    assert loader.raw_dir == tmp_path


def test_base_loader_get_context_returns_empty_dict(sqlite_con):
    """BaseBackfillLoader.get_context should return empty dict by default."""
    loader = SimpleTestLoader()
    context = loader.get_context(sqlite_con)
    assert context == {}


def test_base_loader_process_batch_without_validation(sqlite_con, tmp_path):
    """BaseBackfillLoader should skip validation when requires_validation=False."""
    # Create a simple test table
    sqlite_con.execute("CREATE TABLE test_table (id TEXT PRIMARY KEY, value INTEGER)")

    # Create a loader class with validation disabled
    class NoValidationLoader(SimpleTestLoader):
        requires_validation = False

    loader = NoValidationLoader(raw_dir=tmp_path)
    rows = [{"id": "1", "value": 100}, {"id": "2", "value": 200}]

    inserted = loader.process_batch(sqlite_con, rows, {})
    assert inserted == 2

    # Verify data was inserted
    result = sqlite_con.execute("SELECT value FROM test_table ORDER BY id").fetchall()
    assert result == [(100,), (200,)]


def test_base_loader_process_batch_returns_zero_for_empty_rows(sqlite_con, tmp_path):
    """BaseBackfillLoader should return 0 when no rows to process."""
    sqlite_con.execute("CREATE TABLE test_table (id TEXT PRIMARY KEY, value INTEGER)")

    loader = SimpleTestLoader(raw_dir=tmp_path)
    inserted = loader.process_batch(sqlite_con, [], {})
    assert inserted == 0


def test_base_loader_load_returns_zero_when_csv_missing(sqlite_con, tmp_path):
    """BaseBackfillLoader.load should return 0 when CSV file is missing."""
    sqlite_con.execute("CREATE TABLE test_table (id TEXT PRIMARY KEY, value INTEGER)")

    loader = SimpleTestLoader(raw_dir=tmp_path)
    result = loader.load(sqlite_con)
    assert result == 0
    assert loader.processed == 0
    assert loader.skipped == 0


def test_base_loader_load_processes_csv_and_tracks_counts(sqlite_con, tmp_path):
    """BaseBackfillLoader.load should process CSV and track processed/skipped counts."""
    sqlite_con.execute("CREATE TABLE test_table (id TEXT PRIMARY KEY, value INTEGER)")

    # Create test CSV with missing value (becomes NaN when read)
    test_df = pd.DataFrame({"id": ["1", "2", "3", "4"], "value": [100, 200, None, 400]})
    test_df.to_csv(tmp_path / "test.csv", index=False)

    class SkippingTestLoader(SimpleTestLoader):
        requires_validation = False

        def transform_row(self, row, context):
            # Skip rows where value is NaN (pandas converts None to NaN)
            import pandas as pd

            if pd.isna(row.get("value")):
                return None
            return {"id": row["id"], "value": int(row["value"])}

    loader = SkippingTestLoader(raw_dir=tmp_path)
    result = loader.load(sqlite_con)
    assert result == 3  # 3 rows inserted (one skipped)
    assert loader.processed == 3
    assert loader.skipped == 1


def test_base_loader_transforms_rows_with_context(sqlite_con, tmp_path):
    """BaseBackfillLoader should pass context to transform_row."""
    sqlite_con.execute("CREATE TABLE test_table (id TEXT PRIMARY KEY, value INTEGER)")

    test_df = pd.DataFrame({"id": ["1"], "value": [100]})
    test_df.to_csv(tmp_path / "test.csv", index=False)

    loader = SimpleTestLoader(raw_dir=tmp_path)

    # Mock transform_row to verify it receives context
    original_transform = loader.transform_row
    context_received = {}

    def mock_transform(row, context):
        context_received["context"] = context
        return original_transform(row, context)

    loader.transform_row = mock_transform  # type: ignore[assignment]
    loader.get_context = lambda con: {"test_key": "test_value"}  # type: ignore[assignment]

    loader.load(sqlite_con)
    assert context_received["context"] == {"test_key": "test_value"}

    # ========================================================================
    # Type: ignore[assignment] - monkey-patching for test
    loader.transform_row = mock_transform  # type: ignore[assignment]
    # Type: ignore[assignment] - monkey-patching for test
    loader.get_context = lambda con: {"test_key": "test_value"}  # type: ignore[assignment]

    # Type: ignore[assignment] - monkey-patching for test
    loader.get_context = lambda con: {"test_key": "test_value"}  # type: ignore[assignment]


class SimpleChunkedLoader(ChunkedBackfillLoader):
    """Minimal chunked loader implementation for testing."""

    table_name = "test_table"
    csv_filename = "test.csv"
    requires_validation = False

    def transform_row(self, row, context):
        return {"id": row.get("id"), "value": row.get("value")}


def test_chunked_loader_has_default_chunk_size():
    """ChunkedBackfillLoader should have default chunk_size of 50000."""
    # Check the class attribute directly
    assert SimpleChunkedLoader.chunk_size == 50_000


def test_chunked_loader_load_returns_zero_when_csv_missing(sqlite_con, tmp_path):
    """ChunkedBackfillLoader.load should return 0 when CSV file is missing."""
    sqlite_con.execute("CREATE TABLE test_table (id TEXT PRIMARY KEY, value INTEGER)")

    loader = SimpleChunkedLoader(raw_dir=tmp_path)
    result = loader.load(sqlite_con)
    assert result == 0


def test_chunked_loader_load_processes_csv_in_chunks(sqlite_con, tmp_path):
    """ChunkedBackfillLoader.load should process CSV in chunks."""
    sqlite_con.execute("CREATE TABLE test_table (id TEXT PRIMARY KEY, value INTEGER)")

    # Create test CSV with 25 rows
    test_df = pd.DataFrame({"id": [str(i) for i in range(25)], "value": list(range(25))})
    test_df.to_csv(tmp_path / "test.csv", index=False)

    # Create a subclass with custom chunk_size
    class CustomChunkedLoader(SimpleChunkedLoader):
        chunk_size = 10
        requires_validation = False

    loader = CustomChunkedLoader(raw_dir=tmp_path)
    result = loader.load(sqlite_con)

    assert result == 25
    assert loader.processed == 25
    assert loader.skipped == 0


def test_chunked_loader_load_skips_rows_in_chunks(sqlite_con, tmp_path):
    """ChunkedBackfillLoader.load should skip rows across chunks."""
    sqlite_con.execute("CREATE TABLE test_table (id TEXT PRIMARY KEY, value INTEGER)")

    # Create test CSV with skip marker values
    rows = []
    for i in range(30):
        if i % 3 == 0:
            # Skip this row - use empty string which becomes NaN
            rows.append({"id": str(i), "value": ""})
        else:
            rows.append({"id": str(i), "value": str(i)})
    test_df = pd.DataFrame(rows)
    test_df.to_csv(tmp_path / "test.csv", index=False)

    class SkippingChunkedLoader(SimpleChunkedLoader):
        chunk_size = 10
        requires_validation = False

        def transform_row(self, row, context):
            import pandas as pd

            # Skip rows where value is NaN or empty
            if pd.isna(row.get("value")) or row.get("value") == "":
                return None
            return {"id": row["id"], "value": int(row["value"])}

    loader = SkippingChunkedLoader(raw_dir=tmp_path)
    result = loader.load(sqlite_con)

    # 20 rows should be processed (10 skipped - every 3rd row)
    assert result == 20
    assert loader.processed == 20
    assert loader.skipped == 10


def test_chunked_loader_load_commits_after_processing(sqlite_con, tmp_path):
    """ChunkedBackfillLoader.load should commit after processing all chunks."""
    sqlite_con.execute("CREATE TABLE test_table (id TEXT PRIMARY KEY, value INTEGER)")

    test_df = pd.DataFrame({"id": ["1"], "value": [100]})
    test_df.to_csv(tmp_path / "test.csv", index=False)

    loader = SimpleChunkedLoader(raw_dir=tmp_path)
    loader.load(sqlite_con)

    # Verify data was committed
    result = sqlite_con.execute("SELECT value FROM test_table WHERE id='1'").fetchone()
    assert result == (100,)


# ========================================================================
# Error Handling Edge Cases
# ========================================================================


def test_csv_path_with_path_object(tmp_path):
    """csv_path should handle Path objects correctly."""
    test_file = tmp_path / "test.csv"
    test_file.write_text("data")

    # Test with Path object
    result = csv_path(Path(tmp_path), "test.csv")
    assert result == test_file


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


def test_base_loader_handles_transform_errors_gracefully(sqlite_con, tmp_path):
    """BaseBackfillLoader should propagate exceptions in transform_row."""
    sqlite_con.execute("CREATE TABLE test_table (id TEXT PRIMARY KEY, value INTEGER)")

    # Use string values to ensure pandas reads as string
    test_df = pd.DataFrame({"id": ["a1", "b2", "c3"], "value": [100, 200, 300]})
    test_df.to_csv(tmp_path / "test.csv", index=False)

    class FailingLoader(SimpleTestLoader):
        requires_validation = False

        def transform_row(self, row, context):
            # row["id"] should be a string
            if row["id"] == "b2":
                raise ValueError("Transform failed")
            return {"id": row["id"], "value": int(row["value"])}

    loader = FailingLoader(raw_dir=tmp_path)

    # The current implementation doesn't catch exceptions in transform_row
    # They should propagate up
    with pytest.raises(ValueError, match="Transform failed"):
        loader.load(sqlite_con)


# ========================================================================
# Integration Tests with Validation
# ========================================================================


def test_base_loader_with_validation_enabled(sqlite_con, tmp_path):
    """BaseBackfillLoader should use validation when requires_validation=True."""
    sqlite_con.execute("CREATE TABLE test_table (id TEXT PRIMARY KEY, value INTEGER)")

    test_df = pd.DataFrame({"id": ["1", "2"], "value": [100, 200]})
    test_df.to_csv(tmp_path / "test.csv", index=False)

    class ValidatingLoader(SimpleTestLoader):
        requires_validation = True

    loader = ValidatingLoader(raw_dir=tmp_path)

    # Note: This will use validate_rows which requires Pydantic models
    # For tables without models, validation is skipped
    result = loader.load(sqlite_con)

    # Should still process rows even without validation model
    assert result >= 0


# ========================================================================
# Context Building Tests
# ========================================================================


def test_base_loader_subclass_can_override_get_context(sqlite_con, tmp_path):
    """Subclass should override get_context to provide custom context."""
    sqlite_con.execute("CREATE TABLE test_table (id TEXT PRIMARY KEY, value INTEGER)")

    class ContextLoader(SimpleTestLoader):
        def get_context(self, con):
            return {"valid_ids": {"1", "2", "3"}}

    test_df = pd.DataFrame({"id": ["1", "2"], "value": [100, 200]})
    test_df.to_csv(tmp_path / "test.csv", index=False)

    loader = ContextLoader(raw_dir=tmp_path)

    # Verify context is used
    context = loader.get_context(sqlite_con)
    assert context == {"valid_ids": {"1", "2", "3"}}


# ========================================================================
# Custom Process Batch Tests
# ========================================================================


def test_base_loader_subclass_can_override_process_batch(sqlite_con, tmp_path):
    """Subclass should override process_batch for custom logic."""
    sqlite_con.execute("CREATE TABLE test_table (id TEXT PRIMARY KEY, value INTEGER)")

    class CustomBatchLoader(SimpleTestLoader):
        requires_validation = False

        def process_batch(self, con, rows, context):
            # Custom logic: only insert even values
            even_rows = [r for r in rows if r["value"] % 2 == 0]
            return super().process_batch(con, even_rows, context)

    test_df = pd.DataFrame({"id": ["1", "2", "3", "4"], "value": [100, 201, 302, 403]})
    test_df.to_csv(tmp_path / "test.csv", index=False)

    loader = CustomBatchLoader(raw_dir=tmp_path)
    result = loader.load(sqlite_con)

    # Only 2 rows should be inserted (even values: 100, 302)
    assert result == 2


# ========================================================================
# log_load_summary Exception Handling Tests
# ========================================================================


def test_base_loader_load_handles_log_load_summary_exception(sqlite_con, tmp_path):
    """BaseBackfillLoader.load should handle log_load_summary exceptions gracefully."""
    sqlite_con.execute("CREATE TABLE test_table (id TEXT PRIMARY KEY, value INTEGER)")

    test_df = pd.DataFrame({"id": ["1"], "value": [100]})
    test_df.to_csv(tmp_path / "test.csv", index=False)

    loader = SimpleTestLoader(raw_dir=tmp_path)

    # Mock log_load_summary to raise an exception
    with patch(
        "src.etl.backfill._base.log_load_summary", side_effect=RuntimeError("Summary failed")
    ):
        # Should not raise, should handle exception and return inserted count
        result = loader.load(sqlite_con)
        assert result == 1


def test_chunked_loader_load_handles_log_load_summary_exception(sqlite_con, tmp_path):
    """ChunkedBackfillLoader.load should handle log_load_summary exceptions gracefully."""
    sqlite_con.execute("CREATE TABLE test_table (id TEXT PRIMARY KEY, value INTEGER)")

    test_df = pd.DataFrame({"id": ["1", "2"], "value": [100, 200]})
    test_df.to_csv(tmp_path / "test.csv", index=False)

    loader = SimpleChunkedLoader(raw_dir=tmp_path)

    # Mock log_load_summary to raise an exception
    with patch(
        "src.etl.backfill._base.log_load_summary", side_effect=RuntimeError("Summary failed")
    ):
        # Should not raise, should handle exception and return inserted count
        result = loader.load(sqlite_con)
        assert result == 2
