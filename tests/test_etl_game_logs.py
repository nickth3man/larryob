"""Tests: ETL game log transformation and deduplication."""

import sqlite3

import pandas as pd

from src.etl.game_logs import (
    _build_game_rows,
    _build_player_rows,
    _build_team_rows,
    _parse_matchup,
)
from src.etl.utils import upsert_rows


def _make_mock_df() -> pd.DataFrame:
    return pd.DataFrame({
        "GAME_ID": ["0022300001", "0022300001", "0022300002"],
        "PLAYER_ID": ["2544", "203999", "1628389"],
        "PLAYER_NAME": ["LeBron James", "Nikola Jokic", "Bam Adebayo"],
        "TEAM_ID": ["1610612747", "1610612744", "1610612748"],
        "TEAM_ABBREVIATION": ["LAL", "GSW", "MIA"],
        "GAME_DATE": ["2023-10-24", "2023-10-24", "2023-10-25"],
        "MATCHUP": ["LAL vs. GSW", "GSW @ LAL", "MIA vs. DET"],
        "WL": ["W", "L", "W"],
        "MIN": [32.5, 36.0, 33.0],
        "FGM": [10, 8, 7],
        "FGA": [18, 14, 13],
        "FG3M": [2, 0, 0],
        "FG3A": [5, 0, 0],
        "FTM": [3, 5, 5],
        "FTA": [4, 7, 6],
        "OREB": [1, 3, 2],
        "DREB": [6, 9, 8],
        "REB": [7, 12, 10],
        "AST": [8, 7, 2],
        "STL": [1, 1, 1],
        "BLK": [0, 1, 2],
        "TOV": [3, 2, 2],
        "PF": [1, 3, 3],
        "PTS": [25, 21, 19],
        "PLUS_MINUS": [10, -10, 5],
    })


# ------------------------------------------------------------------ #
# Unit: transformers                                                  #
# ------------------------------------------------------------------ #

def test_parse_matchup_home() -> None:
    my, opp, is_home = _parse_matchup("LAL vs. GSW")
    assert my == "LAL"
    assert opp == "GSW"
    assert is_home is True


def test_parse_matchup_away() -> None:
    my, opp, is_home = _parse_matchup("GSW @ LAL")
    assert my == "GSW"
    assert opp == "LAL"
    assert is_home is False


def test_parse_matchup_malformed() -> None:
    my, opp, is_home = _parse_matchup("UNKNOWN")
    assert my is None and opp is None and is_home is False


def test_build_player_rows_count() -> None:
    df = _make_mock_df()
    rows = _build_player_rows(df)
    assert len(rows) == 3


def test_build_player_rows_null_history() -> None:
    """Rows that are missing a column must become None (not crash)."""
    df = _make_mock_df().drop(columns=["FG3M", "FG3A"])
    rows = _build_player_rows(df)
    for row in rows:
        assert row["fg3m"] is None
        assert row["fg3a"] is None


def test_build_game_rows() -> None:
    df = _make_mock_df()
    rows = _build_game_rows(df, "2023-24", "Regular Season")
    assert len(rows) == 2  # 2 distinct game IDs


def test_build_team_rows() -> None:
    df = _make_mock_df()
    rows = _build_team_rows(df)
    assert len(rows) == 3  # one per (game_id, team_id) combo


# ------------------------------------------------------------------ #
# Integration: insert + deduplication                                 #
# ------------------------------------------------------------------ #

def test_player_log_insert(sqlite_con_with_data: sqlite3.Connection) -> None:
    df = _make_mock_df()
    rows = _build_player_rows(df)
    # Only the rows whose player/game FKs exist will succeed (2544 & 203999 in fixture)
    upsert_rows(sqlite_con_with_data, "player_game_log", rows[:2])
    count = sqlite_con_with_data.execute(
        "SELECT COUNT(*) FROM player_game_log"
    ).fetchone()[0]
    assert count == 2


def test_insert_or_ignore_deduplication(sqlite_con_with_data: sqlite3.Connection) -> None:
    df = _make_mock_df()
    rows = _build_player_rows(df)[:2]
    upsert_rows(sqlite_con_with_data, "player_game_log", rows)
    upsert_rows(sqlite_con_with_data, "player_game_log", rows)  # second insert
    count = sqlite_con_with_data.execute(
        "SELECT COUNT(*) FROM player_game_log"
    ).fetchone()[0]
    assert count == 2, "Primary key constraint failed; duplicates were inserted"


def test_pts_integrity(sqlite_con_with_data: sqlite3.Connection) -> None:
    df = _make_mock_df()
    rows = _build_player_rows(df)[:1]
    upsert_rows(sqlite_con_with_data, "player_game_log", rows)
    pts = sqlite_con_with_data.execute(
        "SELECT pts FROM player_game_log WHERE player_id='2544'"
    ).fetchone()[0]
    assert pts == 25
