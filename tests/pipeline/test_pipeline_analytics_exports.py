"""Tests for analytics view execution and export formats."""

import json
from unittest.mock import patch

import duckdb
import pandas as pd
import pytest

from src.pipeline.analytics import (
    _coerce_stdout_text,
    _export_csv,
    _export_json,
    _export_parquet,
    export_dataframe,
    run_analytics_view,
)


def test_run_analytics_view_print_to_stdout(capsys, mock_duck_con):
    with patch("src.pipeline.analytics.get_duck_con", return_value=mock_duck_con):
        run_analytics_view(view_name="vw_player_season_totals", limit=5, output_path=None)

    output = capsys.readouterr().out
    assert "2544" in output
    assert "LeBron James" in output
    assert "203999" in output
    assert "Nikola Jokic" in output
    assert "25.7" in output
    assert "24.5" in output


def test_coerce_stdout_text_replaces_unencodable_chars():
    raw = "Bojan Bogdanovi\u0107"
    rendered = _coerce_stdout_text(raw, encoding="cp1252")
    rendered.encode("cp1252")
    assert rendered != ""


def test_run_analytics_view_exports_to_csv(tmp_path, analytics_test_df):
    from unittest.mock import MagicMock

    duck_mock = MagicMock(spec=duckdb.DuckDBPyConnection)
    duck_mock.execute.return_value.df.return_value = analytics_test_df

    output_path = tmp_path / "output.csv"
    with patch("src.pipeline.analytics.get_duck_con", return_value=duck_mock):
        run_analytics_view(view_name="vw_player_season_totals", limit=10, output_path=output_path)

    assert output_path.exists()
    df_read = pd.read_csv(output_path)
    assert len(df_read) == 3
    assert list(df_read.columns) == ["player_id", "full_name", "season_id", "gp", "ppg"]
    assert df_read["player_id"].astype(str).tolist() == ["2544", "203999", "1628989"]


def test_run_analytics_view_exports_to_parquet(tmp_path, analytics_test_df):
    pytest.importorskip("pyarrow")
    from unittest.mock import MagicMock

    duck_mock = MagicMock(spec=duckdb.DuckDBPyConnection)
    duck_mock.execute.return_value.df.return_value = analytics_test_df

    output_path = tmp_path / "output.parquet"
    with patch("src.pipeline.analytics.get_duck_con", return_value=duck_mock):
        run_analytics_view(view_name="vw_player_season_totals", limit=10, output_path=output_path)

    assert output_path.exists()
    df_read = pd.read_parquet(output_path)
    assert len(df_read) == 3
    assert list(df_read.columns) == ["player_id", "full_name", "season_id", "gp", "ppg"]
    assert df_read["player_id"].tolist() == ["2544", "203999", "1628989"]


def test_run_analytics_view_exports_to_json(tmp_path, analytics_test_df):
    from unittest.mock import MagicMock

    duck_mock = MagicMock(spec=duckdb.DuckDBPyConnection)
    duck_mock.execute.return_value.df.return_value = analytics_test_df
    output_path = tmp_path / "output.json"

    with patch("src.pipeline.analytics.get_duck_con", return_value=duck_mock):
        run_analytics_view(view_name="vw_player_season_totals", limit=10, output_path=output_path)

    assert output_path.exists()
    with output_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    assert isinstance(data, list)
    assert len(data) == 3
    assert data[0]["player_id"] == "2544"
    assert data[0]["full_name"] == "LeBron James"
    assert data[1]["player_id"] == "203999"
    assert data[2]["player_id"] == "1628989"


def test_run_analytics_view_exports_nested_directories(tmp_path, analytics_test_df):
    from unittest.mock import MagicMock

    duck_mock = MagicMock(spec=duckdb.DuckDBPyConnection)
    duck_mock.execute.return_value.df.return_value = analytics_test_df

    output_path = tmp_path / "level1" / "level2" / "level3" / "output.csv"
    with patch("src.pipeline.analytics.get_duck_con", return_value=duck_mock):
        run_analytics_view(view_name="vw_player_season_totals", limit=10, output_path=output_path)

    assert output_path.parent.exists()
    assert output_path.exists()
    assert len(pd.read_csv(output_path)) == 3


def test_export_dataframe_csv_directly(tmp_path, analytics_test_df):
    output_path = tmp_path / "test.csv"
    _export_csv(analytics_test_df, output_path)
    assert output_path.exists()
    df_read = pd.read_csv(output_path)
    assert len(df_read) == len(analytics_test_df)
    assert list(df_read.columns) == list(analytics_test_df.columns)


def test_export_dataframe_parquet_directly(tmp_path, analytics_test_df):
    pytest.importorskip("pyarrow")
    output_path = tmp_path / "test.parquet"
    _export_parquet(analytics_test_df, output_path)
    assert output_path.exists()
    df_read = pd.read_parquet(output_path)
    pd.testing.assert_frame_equal(df_read, analytics_test_df)


def test_export_dataframe_json_directly(tmp_path, analytics_test_df):
    output_path = tmp_path / "test.json"
    _export_json(analytics_test_df, output_path)

    assert output_path.exists()
    with output_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    assert isinstance(data, list)
    assert len(data) == 3

    df_read = pd.DataFrame(data)[analytics_test_df.columns]
    pd.testing.assert_frame_equal(df_read, analytics_test_df, check_like=True)


def test_export_dataframe_creates_parent_directories(tmp_path, analytics_test_df):
    output_path = tmp_path / "deep" / "nested" / "path" / "output.csv"
    export_dataframe(df=analytics_test_df, output_path=output_path, view_name="vw_test", limit=5)
    assert output_path.exists()
    assert output_path.parent.exists()


def test_export_dataframe_resolves_path(tmp_path, analytics_test_df):
    output_path = (tmp_path / "subdir" / ".." / "output.csv").resolve()
    export_dataframe(df=analytics_test_df, output_path=output_path, view_name="vw_test", limit=5)
    assert output_path.exists()
    assert output_path.parent == tmp_path
