"""Tests for analytics connection cleanup and integration behavior."""

from unittest.mock import MagicMock, patch

import duckdb
import pandas as pd
import pytest

from src.pipeline.analytics import _cleanup_duck_connection, export_dataframe, run_analytics_view
from src.pipeline.exceptions import AnalyticsError


def test_cleanup_duck_connection_closes_connection():
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    mock_conn.close.return_value = None
    _cleanup_duck_connection(mock_conn)
    mock_conn.close.assert_called_once()


def test_cleanup_duck_connection_handles_close_failure():
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    mock_conn.close.side_effect = RuntimeError("Close failed")
    _cleanup_duck_connection(mock_conn)


def test_cleanup_duck_connection_clears_cache():
    import src.db.olap as olap_mod

    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    olap_mod._local.cached_con = mock_conn
    olap_mod._local.cached_sqlite_path = "/path/to/sqlite.db"
    olap_mod._local.cached_duck_db_path = ":memory:"

    _cleanup_duck_connection(mock_conn)

    assert olap_mod._local.cached_con is None
    assert olap_mod._local.cached_sqlite_path is None
    assert olap_mod._local.cached_duck_db_path is None


def test_cleanup_duck_connection_handles_missing_cache():
    import src.db.olap as olap_mod

    for attr in ["cached_con", "cached_sqlite_path", "cached_duck_db_path"]:
        if hasattr(olap_mod._local, attr):
            delattr(olap_mod._local, attr)

    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    _cleanup_duck_connection(mock_conn)


def test_run_analytics_view_always_cleans_up_connection(mock_duck_con):
    mock_duck_con.execute.side_effect = RuntimeError("Query failed")

    with patch("src.pipeline.analytics.get_duck_con", return_value=mock_duck_con):
        with pytest.raises(AnalyticsError):
            run_analytics_view(view_name="vw_player_season_totals", limit=10, output_path=None)

    mock_duck_con.close.assert_called_once()


def test_run_analytics_view_closes_connection_on_success(mock_duck_con):
    with patch("src.pipeline.analytics.get_duck_con", return_value=mock_duck_con):
        run_analytics_view(view_name="vw_player_season_totals", limit=5, output_path=None)

    mock_duck_con.close.assert_called_once()


def test_run_analytics_view_with_real_duckdb_in_memory(tmp_path):
    duck = duckdb.connect(":memory:")
    duck.execute(
        """
        CREATE OR REPLACE VIEW vw_test AS
        SELECT * FROM (
            SELECT '2544' AS player_id, 'LeBron James' AS full_name, 25.7 AS ppg
            UNION ALL
            SELECT '203999' AS player_id, 'Nikola Jokic' AS full_name, 24.5 AS ppg
        )
        """
    )

    output_path = tmp_path / "output.csv"
    with patch("src.pipeline.analytics.get_duck_con", return_value=duck):
        run_analytics_view(view_name="vw_test", limit=10, output_path=output_path)

    assert output_path.exists()
    df_read = pd.read_csv(output_path)
    assert len(df_read) == 2
    assert "LeBron James" in df_read["full_name"].values
    assert "Nikola Jokic" in df_read["full_name"].values
    duck.close()


def test_export_dataframe_preserves_limit_in_logging(tmp_path, caplog, analytics_test_df):
    import logging

    output_path = tmp_path / "output.csv"
    with caplog.at_level(logging.INFO):
        export_dataframe(
            df=analytics_test_df,
            output_path=output_path,
            view_name="vw_player_season_totals",
            limit=42,
        )

    assert output_path.exists()


def test_run_analytics_view_empty_dataframe(capsys):
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    mock_conn.execute.return_value.df.return_value = pd.DataFrame()

    with patch("src.pipeline.analytics.get_duck_con", return_value=mock_conn):
        run_analytics_view(view_name="vw_empty_view", limit=10, output_path=None)

    assert "player_id" not in capsys.readouterr().out
