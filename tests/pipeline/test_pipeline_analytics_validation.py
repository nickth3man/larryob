"""Tests for analytics validation and export error handling."""

from unittest.mock import patch

import duckdb
import pytest

from src.pipeline.analytics import export_dataframe, run_analytics_view
from src.pipeline.exceptions import AnalyticsError


def test_run_analytics_view_validates_limit_positive(mock_duck_con):
    with patch("src.pipeline.analytics.get_duck_con", return_value=mock_duck_con):
        with pytest.raises(AnalyticsError) as exc_info:
            run_analytics_view(view_name="vw_player_season_totals", limit=0, output_path=None)

    assert "limit must be > 0" in str(exc_info.value)
    assert exc_info.value.context is not None
    assert exc_info.value.context["view_name"] == "vw_player_season_totals"


def test_run_analytics_view_validates_limit_positive_negative(mock_duck_con):
    with patch("src.pipeline.analytics.get_duck_con", return_value=mock_duck_con):
        with pytest.raises(AnalyticsError) as exc_info:
            run_analytics_view(view_name="vw_player_season_totals", limit=-1, output_path=None)
    assert "limit must be > 0" in str(exc_info.value)


def test_run_analytics_view_raises_on_invalid_view(mock_duck_con_with_error):
    with patch("src.pipeline.analytics.get_duck_con", return_value=mock_duck_con_with_error):
        with pytest.raises(AnalyticsError) as exc_info:
            run_analytics_view(view_name="vw_nonexistent_view", limit=10, output_path=None)

    assert "Failed analytics query" in str(exc_info.value)
    assert exc_info.value.context is not None
    assert exc_info.value.context["view_name"] == "vw_nonexistent_view"
    assert exc_info.value.__cause__ is not None


def test_run_analytics_view_raises_on_query_failure():
    from unittest.mock import MagicMock

    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    mock_conn.execute.side_effect = RuntimeError("Connection lost")

    with patch("src.pipeline.analytics.get_duck_con", return_value=mock_conn):
        with pytest.raises(AnalyticsError) as exc_info:
            run_analytics_view(view_name="vw_player_season_totals", limit=10, output_path=None)

    assert "Failed analytics query" in str(exc_info.value)
    assert "limit=10" in str(exc_info.value)
    assert exc_info.value.__cause__ is not None


def test_export_dataframe_unsupported_format(tmp_path, analytics_test_df):
    output_path = tmp_path / "output.txt"
    with pytest.raises(AnalyticsError) as exc_info:
        export_dataframe(
            df=analytics_test_df,
            output_path=output_path,
            view_name="vw_player_season_totals",
            limit=10,
        )

    assert "Unsupported analytics output format" in str(exc_info.value)
    assert exc_info.value.context is not None
    assert exc_info.value.context["view_name"] == "vw_player_season_totals"
    assert "output.txt" in exc_info.value.context["output_path"]


def test_export_dataframe_unsupported_format_xlsx(tmp_path, analytics_test_df):
    output_path = tmp_path / "output.xlsx"
    with pytest.raises(AnalyticsError) as exc_info:
        export_dataframe(
            df=analytics_test_df,
            output_path=output_path,
            view_name="vw_player_season_totals",
            limit=10,
        )
    assert "Unsupported analytics output format" in str(exc_info.value)


def test_export_dataframe_handles_export_failure(tmp_path, analytics_test_df):
    output_path = tmp_path / "output.csv"

    def mock_csv_export(df, path):
        raise RuntimeError("Disk full")

    with patch("src.pipeline.analytics.EXPORTERS", {".csv": mock_csv_export}):
        with pytest.raises(AnalyticsError) as exc_info:
            export_dataframe(
                df=analytics_test_df,
                output_path=output_path,
                view_name="vw_test",
                limit=10,
            )

    assert "Failed exporting analytics view" in str(exc_info.value)
    assert exc_info.value.context is not None
    assert exc_info.value.context["view_name"] == "vw_test"
    assert exc_info.value.__cause__ is not None


def test_export_dataframe_handles_permission_error(tmp_path, analytics_test_df):
    output_path = tmp_path / "output.csv"

    def mock_csv_export(df, path):
        raise PermissionError("Access denied")

    with patch("src.pipeline.analytics.EXPORTERS", {".csv": mock_csv_export}):
        with pytest.raises(AnalyticsError) as exc_info:
            export_dataframe(
                df=analytics_test_df,
                output_path=output_path,
                view_name="vw_test",
                limit=10,
            )

    assert "Failed exporting analytics view" in str(exc_info.value)
    assert exc_info.value.context is not None
    assert exc_info.value.context["view_name"] == "vw_test"
    assert exc_info.value.__cause__ is not None
