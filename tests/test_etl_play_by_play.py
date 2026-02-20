"""Tests: ETL play-by-play transformation."""

import sqlite3

import pandas as pd

from src.etl.play_by_play import _transform_pbp
from src.etl.utils import upsert_rows


def _make_pbp_df() -> pd.DataFrame:
    return pd.DataFrame({
        "GAME_ID": ["0022300001"] * 4,
        "EVENTNUM": [1, 2, 3, 4],
        "PERIOD": [1, 1, 1, 1],
        "PCTIMESTRING": ["11:48", "11:32", "11:15", "10:58"],
        "WCTIMESTRING": ["8:04 PM", "8:05 PM", "8:06 PM", "8:07 PM"],
        "EVENTMSGTYPE": [1, 2, 4, 5],
        "EVENTMSGACTIONTYPE": [1, 5, 1, 2],
        "PLAYER1_ID": ["2544", "203999", "2544", "1628389"],
        "PLAYER2_ID": [0, 0, 0, 0],
        "PLAYER3_ID": [0, 0, 0, 0],
        "PLAYER1_TEAM_ID": ["1610612747", "1610612744", "1610612747", "1610612748"],
        "PLAYER2_TEAM_ID": [0, 0, 0, 0],
        "HOMEDESCRIPTION": ["LeBron 2pt", None, "LeBron REBOUND", None],
        "VISITORDESCRIPTION": [None, "Jokic MISS", None, "Adebayo TURNOVER"],
        "NEUTRALDESCRIPTION": [None, None, None, None],
        "SCORE": ["2 - 0", "2 - 0", "2 - 0", "2 - 0"],
        "SCOREMARGIN": [2, 2, 2, 2],
    })


def test_transform_pbp_event_id_format() -> None:
    rows = _transform_pbp(_make_pbp_df())
    assert rows[0]["event_id"] == "0022300001_000001"
    assert rows[3]["event_id"] == "0022300001_000004"


def test_transform_pbp_player_id_cast() -> None:
    rows = _transform_pbp(_make_pbp_df())
    assert rows[0]["player1_id"] == "2544"


def test_transform_pbp_zero_player_becomes_none() -> None:
    """Player IDs of 0 (team-level events) must be stored as None."""
    rows = _transform_pbp(_make_pbp_df())
    for row in rows:
        assert row["player2_id"] is None
        assert row["player3_id"] is None


def test_transform_pbp_tie_score_margin() -> None:
    df = _make_pbp_df()
    # Cast to object so pandas 3 allows mixed str/int in the column
    df["SCOREMARGIN"] = df["SCOREMARGIN"].astype(object)
    df.loc[0, "SCOREMARGIN"] = "TIE"
    rows = _transform_pbp(df)
    assert rows[0]["score_margin"] == "TIE"


def test_transform_pbp_row_count() -> None:
    rows = _transform_pbp(_make_pbp_df())
    assert len(rows) == 4


def test_pbp_insert(sqlite_con_with_data: sqlite3.Connection) -> None:
    rows = _transform_pbp(_make_pbp_df())
    # Only player1_ids that exist in dim_player will succeed (2544 is seeded)
    # But foreign keys are ON so unknown player IDs will cause constraint errors.
    # Insert only rows with known player1_id
    safe_rows = [r for r in rows if r["player1_id"] in ("2544", "203999")]
    n = upsert_rows(sqlite_con_with_data, "fact_play_by_play", safe_rows)
    count = sqlite_con_with_data.execute(
        "SELECT COUNT(*) FROM fact_play_by_play"
    ).fetchone()[0]
    assert count == n


def test_pbp_deduplication(sqlite_con_with_data: sqlite3.Connection) -> None:
    rows = _transform_pbp(_make_pbp_df())
    safe_rows = [r for r in rows if r["player1_id"] in ("2544", "203999")]
    upsert_rows(sqlite_con_with_data, "fact_play_by_play", safe_rows)
    upsert_rows(sqlite_con_with_data, "fact_play_by_play", safe_rows)
    count = sqlite_con_with_data.execute(
        "SELECT COUNT(*) FROM fact_play_by_play"
    ).fetchone()[0]
    assert count == len(safe_rows), "Duplicate PBP events were inserted"
