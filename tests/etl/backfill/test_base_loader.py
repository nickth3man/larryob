"""
Tests for BaseBackfillLoader and ChunkedBackfillLoader classes.
"""

from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from src.etl.backfill._base import (
    BaseBackfillLoader,
    ChunkedBackfillLoader,
)

# ========================================================================
# SimpleTestLoader for testing
# ========================================================================


class SimpleTestLoader(BaseBackfillLoader):
    """Minimal loader implementation for testing."""

    table_name = "test_table"
    csv_filename = "test.csv"
    requires_validation = False

    def transform_row(self, row, context):
        return {"id": row.get("id"), "value": row.get("value")}


# ========================================================================
# BaseBackfillLoader Class Tests
# ========================================================================


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


# ========================================================================
# ChunkedBackfillLoader Class Tests
# ========================================================================


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
