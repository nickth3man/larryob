"""Shared fixtures for pipeline test modules."""

from unittest.mock import MagicMock

import duckdb
import pandas as pd
import pytest


@pytest.fixture
def analytics_test_df() -> pd.DataFrame:
    """Small analytics result frame used by multiple tests."""
    return pd.DataFrame(
        {
            "player_id": ["2544", "203999", "1628989"],
            "full_name": ["LeBron James", "Nikola Jokic", "Stephen Curry"],
            "season_id": ["2023-24", "2023-24", "2023-24"],
            "gp": [71, 69, 74],
            "ppg": [25.7, 24.5, 26.4],
        }
    )


@pytest.fixture
def mock_duck_con(analytics_test_df: pd.DataFrame) -> MagicMock:
    """Mock DuckDB connection returning deterministic analytics data."""
    mock = MagicMock(spec=duckdb.DuckDBPyConnection)
    mock.execute.return_value.df.return_value = analytics_test_df
    return mock


@pytest.fixture
def mock_duck_con_with_error() -> MagicMock:
    """Mock DuckDB connection that always errors during execute."""
    mock = MagicMock(spec=duckdb.DuckDBPyConnection)
    mock.execute.side_effect = RuntimeError("View does not exist")
    return mock
