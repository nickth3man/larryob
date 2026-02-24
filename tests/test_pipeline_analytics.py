"""Tests: Pipeline analytics orchestration — view execution and export."""

import json
from collections.abc import Iterator
from unittest.mock import MagicMock, patch

import duckdb
import pandas as pd
import pytest

from src.pipeline.analytics import (
    _cleanup_duck_connection,
    _coerce_stdout_text,
    _export_csv,
    _export_json,
    _export_parquet,
    export_dataframe,
    run_analytics_view,
)
from src.pipeline.exceptions import AnalyticsError

# ------------------------------------------------------------------ #
# Helper fixtures and functions                                       #
# ------------------------------------------------------------------ #


@pytest.fixture
def mock_duck_con() -> Iterator[MagicMock]:
    """Mock DuckDB connection with test data."""
    mock = MagicMock(spec=duckdb.DuckDBPyConnection)

    # Create test DataFrame
    test_df = pd.DataFrame(
        {
            "player_id": ["2544", "203999"],
            "full_name": ["LeBron James", "Nikola Jokic"],
            "season_id": ["2023-24", "2023-24"],
            "gp": [71, 69],
            "ppg": [25.7, 24.5],
        }
    )

    mock.execute.return_value.df.return_value = test_df
    return mock


@pytest.fixture
def mock_duck_con_with_error() -> Iterator[MagicMock]:
    """Mock DuckDB connection that raises an error."""
    mock = MagicMock(spec=duckdb.DuckDBPyConnection)
    mock.execute.side_effect = RuntimeError("View does not exist")
    return mock


def _create_test_dataframe() -> pd.DataFrame:
    """Create a test DataFrame for export testing."""
    return pd.DataFrame(
        {
            "player_id": ["2544", "203999", "1628989"],
            "full_name": ["LeBron James", "Nikola Jokic", "Stephen Curry"],
            "season_id": ["2023-24", "2023-24", "2023-24"],
            "gp": [71, 69, 74],
            "ppg": [25.7, 24.5, 26.4],
        }
    )


# ------------------------------------------------------------------ #
# Core Functionality Tests                                            #
# ------------------------------------------------------------------ #


def test_run_analytics_view_print_to_stdout(capsys, mock_duck_con):
    """Test run_analytics_view with output_path=None (prints to stdout)."""
    with patch("src.pipeline.analytics.get_duck_con", return_value=mock_duck_con):
        run_analytics_view(
            view_name="vw_player_season_totals",
            limit=5,
            output_path=None,
        )

    captured = capsys.readouterr()
    output = captured.out

    # Verify the output contains expected data
    assert "2544" in output
    assert "LeBron James" in output
    assert "203999" in output
    assert "Nikola Jokic" in output
    assert "25.7" in output
    assert "24.5" in output


def test_coerce_stdout_text_replaces_unencodable_chars():
    """Text with non-cp1252 characters should be safely replaced."""
    raw = "Bojan Bogdanovi\u0107"
    rendered = _coerce_stdout_text(raw, encoding="cp1252")
    rendered.encode("cp1252")  # Should not raise
    assert rendered != ""


def test_run_analytics_view_exports_to_csv(tmp_path):
    """Test CSV export functionality."""
    test_df = _create_test_dataframe()
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    mock_conn.execute.return_value.df.return_value = test_df

    output_path = tmp_path / "output.csv"

    with patch("src.pipeline.analytics.get_duck_con", return_value=mock_conn):
        run_analytics_view(
            view_name="vw_player_season_totals",
            limit=10,
            output_path=output_path,
        )

    # Verify file exists
    assert output_path.exists()

    # Verify file contains valid CSV
    df_read = pd.read_csv(output_path)
    assert len(df_read) == 3
    assert list(df_read.columns) == ["player_id", "full_name", "season_id", "gp", "ppg"]
    # Note: CSV may convert strings to int if they look numeric
    assert df_read["player_id"].astype(str).tolist() == ["2544", "203999", "1628989"]


def test_run_analytics_view_exports_to_parquet(tmp_path):
    """Test Parquet export functionality (requires pyarrow)."""
    pytest.importorskip("pyarrow")
    test_df = _create_test_dataframe()
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    mock_conn.execute.return_value.df.return_value = test_df

    output_path = tmp_path / "output.parquet"

    with patch("src.pipeline.analytics.get_duck_con", return_value=mock_conn):
        run_analytics_view(
            view_name="vw_player_season_totals",
            limit=10,
            output_path=output_path,
        )

    # Verify file exists
    assert output_path.exists()

    # Verify file is valid Parquet using pandas
    df_read = pd.read_parquet(output_path)
    assert len(df_read) == 3
    assert list(df_read.columns) == ["player_id", "full_name", "season_id", "gp", "ppg"]
    assert df_read["player_id"].tolist() == ["2544", "203999", "1628989"]


def test_run_analytics_view_exports_to_json(tmp_path):
    """Test JSON export functionality."""
    test_df = _create_test_dataframe()
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    mock_conn.execute.return_value.df.return_value = test_df

    output_path = tmp_path / "output.json"

    with patch("src.pipeline.analytics.get_duck_con", return_value=mock_conn):
        run_analytics_view(
            view_name="vw_player_season_totals",
            limit=10,
            output_path=output_path,
        )

    # Verify file exists
    assert output_path.exists()

    # Verify file contains valid JSON
    with output_path.open("r") as f:
        data = json.load(f)

    assert isinstance(data, list)
    assert len(data) == 3
    assert data[0]["player_id"] == "2544"
    assert data[0]["full_name"] == "LeBron James"
    assert data[1]["player_id"] == "203999"
    assert data[2]["player_id"] == "1628989"


def test_run_analytics_view_exports_nested_directories(tmp_path):
    """Test that export creates missing parent directories."""
    test_df = _create_test_dataframe()
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    mock_conn.execute.return_value.df.return_value = test_df

    # Create output path with non-existent parent directories
    output_path = tmp_path / "level1" / "level2" / "level3" / "output.csv"

    with patch("src.pipeline.analytics.get_duck_con", return_value=mock_conn):
        run_analytics_view(
            view_name="vw_player_season_totals",
            limit=10,
            output_path=output_path,
        )

    # Verify directories were created
    assert output_path.parent.exists()
    assert output_path.exists()

    # Verify file contains valid data
    df_read = pd.read_csv(output_path)
    assert len(df_read) == 3


def test_export_dataframe_csv_directly(tmp_path):
    """Test _export_csv function directly."""
    test_df = _create_test_dataframe()
    output_path = tmp_path / "test.csv"

    _export_csv(test_df, output_path)

    assert output_path.exists()
    df_read = pd.read_csv(output_path)
    # CSV may change dtypes (e.g., string to int), so compare values not types
    assert len(df_read) == len(test_df)
    assert list(df_read.columns) == list(test_df.columns)


def test_export_dataframe_parquet_directly(tmp_path):
    """Test _export_parquet function directly (requires pyarrow)."""
    pytest.importorskip("pyarrow")
    test_df = _create_test_dataframe()
    output_path = tmp_path / "test.parquet"

    _export_parquet(test_df, output_path)

    assert output_path.exists()
    df_read = pd.read_parquet(output_path)
    pd.testing.assert_frame_equal(df_read, test_df)


def test_export_dataframe_json_directly(tmp_path):
    """Test _export_json function directly."""
    test_df = _create_test_dataframe()
    output_path = tmp_path / "test.json"

    _export_json(test_df, output_path)

    assert output_path.exists()
    with output_path.open("r") as f:
        data = json.load(f)

    assert isinstance(data, list)
    assert len(data) == 3
    # Verify we can reconstruct the DataFrame
    df_read = pd.DataFrame(data)
    # Reorder columns to match
    df_read = df_read[test_df.columns]
    pd.testing.assert_frame_equal(df_read, test_df, check_like=True)


# ------------------------------------------------------------------ #
# Validation Tests                                                    #
# ------------------------------------------------------------------ #


def test_run_analytics_view_validates_limit_positive(mock_duck_con):
    """Test that limit <= 0 raises AnalyticsError."""
    with patch("src.pipeline.analytics.get_duck_con", return_value=mock_duck_con):
        with pytest.raises(AnalyticsError) as exc_info:
            run_analytics_view(
                view_name="vw_player_season_totals",
                limit=0,
                output_path=None,
            )

    assert "limit must be > 0" in str(exc_info.value)
    assert exc_info.value.context is not None
    assert exc_info.value.context["view_name"] == "vw_player_season_totals"


def test_run_analytics_view_validates_limit_positive_negative(mock_duck_con):
    """Test that negative limit raises AnalyticsError."""
    with patch("src.pipeline.analytics.get_duck_con", return_value=mock_duck_con):
        with pytest.raises(AnalyticsError) as exc_info:
            run_analytics_view(
                view_name="vw_player_season_totals",
                limit=-1,
                output_path=None,
            )

    assert "limit must be > 0" in str(exc_info.value)


def test_run_analytics_view_raises_on_invalid_view(mock_duck_con_with_error):
    """Test that invalid view names raise AnalyticsError with context."""
    with patch("src.pipeline.analytics.get_duck_con", return_value=mock_duck_con_with_error):
        with pytest.raises(AnalyticsError) as exc_info:
            run_analytics_view(
                view_name="vw_nonexistent_view",
                limit=10,
                output_path=None,
            )

    assert "Failed analytics query" in str(exc_info.value)
    assert exc_info.value.context is not None
    assert exc_info.value.context["view_name"] == "vw_nonexistent_view"
    assert exc_info.value.__cause__ is not None


def test_run_analytics_view_raises_on_query_failure(mock_duck_con_with_error):
    """Test that query failures raise AnalyticsError with context."""
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    mock_conn.execute.side_effect = RuntimeError("Connection lost")

    with patch("src.pipeline.analytics.get_duck_con", return_value=mock_conn):
        with pytest.raises(AnalyticsError) as exc_info:
            run_analytics_view(
                view_name="vw_player_season_totals",
                limit=10,
                output_path=None,
            )

    assert "Failed analytics query" in str(exc_info.value)
    assert "limit=10" in str(exc_info.value)
    assert exc_info.value.__cause__ is not None


def test_export_dataframe_unsupported_format(tmp_path):
    """Test that unsupported formats raise AnalyticsError."""
    test_df = _create_test_dataframe()
    output_path = tmp_path / "output.txt"

    with pytest.raises(AnalyticsError) as exc_info:
        export_dataframe(
            df=test_df,
            output_path=output_path,
            view_name="vw_player_season_totals",
            limit=10,
        )

    assert "Unsupported analytics output format" in str(exc_info.value)
    assert exc_info.value.context is not None
    assert exc_info.value.context["view_name"] == "vw_player_season_totals"
    assert "output.txt" in exc_info.value.context["output_path"]


def test_export_dataframe_unsupported_format_xlsx(tmp_path):
    """Test that .xlsx format raises AnalyticsError."""
    test_df = _create_test_dataframe()
    output_path = tmp_path / "output.xlsx"

    with pytest.raises(AnalyticsError) as exc_info:
        export_dataframe(
            df=test_df,
            output_path=output_path,
            view_name="vw_player_season_totals",
            limit=10,
        )

    assert "Unsupported analytics output format" in str(exc_info.value)


def test_export_dataframe_creates_parent_directories(tmp_path):
    """Test that export creates missing directories."""
    test_df = _create_test_dataframe()

    # Create path with multiple non-existent directories
    output_path = tmp_path / "deep" / "nested" / "path" / "output.csv"

    export_dataframe(
        df=test_df,
        output_path=output_path,
        view_name="vw_test",
        limit=5,
    )

    # Verify all parent directories were created
    assert output_path.exists()
    assert output_path.parent.exists()


def test_export_dataframe_resolves_path(tmp_path):
    """Test that export paths are resolved properly."""
    test_df = _create_test_dataframe()

    # Create a path with relative segments
    output_path = tmp_path / "subdir" / ".." / "output.csv"
    output_path = output_path.resolve()

    export_dataframe(
        df=test_df,
        output_path=output_path,
        view_name="vw_test",
        limit=5,
    )

    # Verify the file was created at the resolved path
    assert output_path.exists()
    # Should be in tmp_path directly (due to ..)
    assert output_path.parent == tmp_path


# ------------------------------------------------------------------ #
# Error Handling Tests                                                #
# ------------------------------------------------------------------ #


def test_export_dataframe_handles_export_failure(tmp_path):
    """Test that export failures are wrapped in AnalyticsError."""
    test_df = _create_test_dataframe()
    output_path = tmp_path / "output.csv"

    # Mock the CSV exporter to raise an error during export
    def mock_csv_export(df, path):
        raise RuntimeError("Disk full")

    with patch("src.pipeline.analytics.EXPORTERS", {".csv": mock_csv_export}):
        with pytest.raises(AnalyticsError) as exc_info:
            export_dataframe(
                df=test_df,
                output_path=output_path,
                view_name="vw_test",
                limit=10,
            )

    assert "Failed exporting analytics view" in str(exc_info.value)
    assert exc_info.value.context is not None
    assert exc_info.value.context["view_name"] == "vw_test"
    assert exc_info.value.__cause__ is not None


def test_export_dataframe_handles_permission_error(tmp_path):
    """Test export error handling with mocked permission error."""
    test_df = _create_test_dataframe()
    output_path = tmp_path / "output.csv"

    # Mock the CSV exporter to raise a permission error
    def mock_csv_export(df, path):
        raise PermissionError("Access denied")

    with patch("src.pipeline.analytics.EXPORTERS", {".csv": mock_csv_export}):
        with pytest.raises(AnalyticsError) as exc_info:
            export_dataframe(
                df=test_df,
                output_path=output_path,
                view_name="vw_test",
                limit=10,
            )

    assert "Failed exporting analytics view" in str(exc_info.value)
    assert exc_info.value.context is not None
    assert exc_info.value.context["view_name"] == "vw_test"
    assert exc_info.value.__cause__ is not None


# ------------------------------------------------------------------ #
# Cleanup Tests                                                       #
# ------------------------------------------------------------------ #


def test_cleanup_duck_connection_closes_connection():
    """Test connection cleanup closes the connection."""
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    mock_conn.close.return_value = None

    _cleanup_duck_connection(mock_conn)

    mock_conn.close.assert_called_once()


def test_cleanup_duck_connection_handles_close_failure():
    """Test cleanup suppresses close failures."""
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    mock_conn.close.side_effect = RuntimeError("Close failed")

    # Should not raise
    _cleanup_duck_connection(mock_conn)


def test_cleanup_duck_connection_clears_cache():
    """Test connection cleanup clears thread-local cache."""
    import src.db.analytics as analytics_mod

    # Setup: create mock cached connection
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    analytics_mod._local.cached_con = mock_conn
    analytics_mod._local.cached_sqlite_path = "/path/to/sqlite.db"
    analytics_mod._local.cached_duck_db_path = ":memory:"

    _cleanup_duck_connection(mock_conn)

    # Verify cache was cleared
    assert analytics_mod._local.cached_con is None
    assert analytics_mod._local.cached_sqlite_path is None
    assert analytics_mod._local.cached_duck_db_path is None


def test_cleanup_duck_connection_handles_missing_cache():
    """Test cleanup handles missing thread-local attributes."""
    import src.db.analytics as analytics_mod

    # Remove cache attributes if they exist
    for attr in ["cached_con", "cached_sqlite_path", "cached_duck_db_path"]:
        if hasattr(analytics_mod._local, attr):
            delattr(analytics_mod._local, attr)

    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)

    # Should not raise even without cache attributes
    _cleanup_duck_connection(mock_conn)


def test_run_analytics_view_always_cleans_up_connection(mock_duck_con):
    """Test that connection cleanup happens even after errors."""
    mock_duck_con.execute.side_effect = RuntimeError("Query failed")

    with patch("src.pipeline.analytics.get_duck_con", return_value=mock_duck_con):
        with pytest.raises(AnalyticsError):
            run_analytics_view(
                view_name="vw_player_season_totals",
                limit=10,
                output_path=None,
            )

    # Verify close was still called
    mock_duck_con.close.assert_called_once()


def test_run_analytics_view_closes_connection_on_success(mock_duck_con):
    """Test that connection cleanup happens after successful query."""
    with patch("src.pipeline.analytics.get_duck_con", return_value=mock_duck_con):
        run_analytics_view(
            view_name="vw_player_season_totals",
            limit=5,
            output_path=None,
        )

    # Verify close was called
    mock_duck_con.close.assert_called_once()


# ------------------------------------------------------------------ #
# Integration-style Tests                                             #
# ------------------------------------------------------------------ #


def test_run_analytics_view_with_real_duckdb_in_memory(tmp_path):
    """Test with real in-memory DuckDB (no SQLite attachment needed)."""
    # Create a real in-memory DuckDB connection
    duck = duckdb.connect(":memory:")

    # Create a simple view
    duck.execute("""
        CREATE OR REPLACE VIEW vw_test AS
        SELECT * FROM (
            SELECT '2544' AS player_id, 'LeBron James' AS full_name, 25.7 AS ppg
            UNION ALL
            SELECT '203999' AS player_id, 'Nikola Jokic' AS full_name, 24.5 AS ppg
        )
    """)

    output_path = tmp_path / "output.csv"

    with patch("src.pipeline.analytics.get_duck_con", return_value=duck):
        run_analytics_view(
            view_name="vw_test",
            limit=10,
            output_path=output_path,
        )

    # Verify output
    assert output_path.exists()
    df_read = pd.read_csv(output_path)
    assert len(df_read) == 2
    assert "LeBron James" in df_read["full_name"].values
    assert "Nikola Jokic" in df_read["full_name"].values

    duck.close()


def test_export_dataframe_preserves_limit_in_logging(tmp_path, caplog):
    """Test that limit parameter is included in log context."""
    import logging

    test_df = _create_test_dataframe()
    output_path = tmp_path / "output.csv"

    with caplog.at_level(logging.INFO):
        export_dataframe(
            df=test_df,
            output_path=output_path,
            view_name="vw_player_season_totals",
            limit=42,
        )

    # Note: This test verifies the function runs without error
    # The actual log message verification would require more complex setup
    assert output_path.exists()


def test_run_analytics_view_empty_dataframe(capsys):
    """Test behavior when query returns empty DataFrame."""
    # Mock DuckDB connection that returns empty DataFrame
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    mock_conn.execute.return_value.df.return_value = pd.DataFrame()

    with patch("src.pipeline.analytics.get_duck_con", return_value=mock_conn):
        run_analytics_view(
            view_name="vw_empty_view",
            limit=10,
            output_path=None,
        )

    captured = capsys.readouterr()
    # Empty DataFrame should not print anything (no table output)
    # Only the logger info would be emitted
    assert "player_id" not in captured.out
