"""Tests: ETL game log transformation and deduplication."""

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from src.db.operations import upsert_rows
from src.etl.game_logs import (
    _build_game_rows,
    _build_player_rows,
    _build_team_rows,
    _parse_matchup,
    load_season,
)


def _make_mock_df() -> pd.DataFrame:
    """
    Create a small mock pandas DataFrame representing per-player game log records for unit tests.

    The DataFrame contains three rows (three player-game entries across two games) with typical game-log fields such as GAME_ID, PLAYER_ID, PLAYER_NAME, TEAM_ID, TEAM_ABBREVIATION, GAME_DATE, MATCHUP, WL, MIN, FGM, FGA, FG3M, FG3A, FTM, FTA, OREB, DREB, REB, AST, STL, BLK, TOV, PF, PTS, and PLUS_MINUS. Column values are suitable for testing parsing, row-building, and deduplication logic.

    Returns:
        pd.DataFrame: A DataFrame with the columns described above and three sample rows.
    """
    return pd.DataFrame(
        {
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
        }
    )


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
    assert len(rows) == 1  # game rows require resolvable home+away teams


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
    count = sqlite_con_with_data.execute("SELECT COUNT(*) FROM player_game_log").fetchone()[0]
    assert count == 2


def test_insert_or_ignore_deduplication(sqlite_con_with_data: sqlite3.Connection) -> None:
    df = _make_mock_df()
    rows = _build_player_rows(df)[:2]
    upsert_rows(sqlite_con_with_data, "player_game_log", rows)
    upsert_rows(sqlite_con_with_data, "player_game_log", rows)  # second insert
    count = sqlite_con_with_data.execute("SELECT COUNT(*) FROM player_game_log").fetchone()[0]
    assert count == 2, "Primary key constraint failed; duplicates were inserted"


def test_pts_integrity(sqlite_con_with_data: sqlite3.Connection) -> None:
    df = _make_mock_df()
    rows = _build_player_rows(df)[:1]
    upsert_rows(sqlite_con_with_data, "player_game_log", rows)
    pts = sqlite_con_with_data.execute(
        "SELECT pts FROM player_game_log WHERE player_id='2544'"
    ).fetchone()[0]
    assert pts == 25


# ------------------------------------------------------------------ #
# _build_game_rows: away-team branch                                 #
# ------------------------------------------------------------------ #


def test_build_game_rows_resolves_both_home_and_away_team_ids() -> None:
    df = _make_mock_df()
    rows = _build_game_rows(df, "2023-24", "Regular Season")
    by_game = {r["game_id"]: r for r in rows}
    game = by_game["0022300001"]
    assert game["home_team_id"] == "1610612747"
    assert game["away_team_id"] == "1610612744"


def test_build_game_rows_drops_game_when_teams_cannot_be_resolved() -> None:
    df = _make_mock_df()
    # Keep only one team for a game so home/away cannot be fully inferred.
    df_one_team = df[df["TEAM_ABBREVIATION"] == "GSW"].copy()
    rows = _build_game_rows(df_one_team, "2023-24", "Regular Season")
    assert rows == []


def test_build_game_rows_stores_season_type() -> None:
    df = _make_mock_df()
    rows = _build_game_rows(df, "2023-24", "Playoffs")
    for row in rows:
        assert row["season_type"] == "Playoffs"


def test_build_game_rows_date_truncated_to_10_chars() -> None:
    df = _make_mock_df()
    rows = _build_game_rows(df, "2023-24", "Regular Season")
    for row in rows:
        assert len(row["game_date"]) == 10


# ------------------------------------------------------------------ #
# load_season: skips when already loaded                             #
# ------------------------------------------------------------------ #


def test_load_season_skips_when_already_loaded(
    sqlite_con_with_data: sqlite3.Connection,
    monkeypatch,
    tmp_path: Path,
) -> None:
    """When etl_run_log already has a successful entry, load_season returns {}."""
    import src.db.cache.file_cache as cache_mod
    from src.db.tracking import record_run

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    record_run(
        sqlite_con_with_data,
        "player_game_log",
        "2023-24",
        "game_logs.load_season.Regular Season",
        100,
        "ok",
    )
    result = load_season(sqlite_con_with_data, "2023-24")
    assert result == {}


def test_load_season_returns_empty_dict_for_empty_api_response(
    sqlite_con_with_data: sqlite3.Connection,
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Empty API DataFrame → load_season returns {}."""
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    mock_ep = MagicMock()
    mock_ep.get_data_frames.return_value = [pd.DataFrame()]

    with patch("src.etl.game_logs.playergamelogs.PlayerGameLogs", return_value=mock_ep):
        result = load_season(sqlite_con_with_data, "2099-00")
    assert result == {}


def test_load_season_returns_counts_dict_on_success(
    sqlite_con_with_data: sqlite3.Connection,
    monkeypatch,
    tmp_path: Path,
) -> None:
    """
    Check that load_season produces a counts dictionary containing fact_game, player_game_log, and team_game_log for a valid cached season.

    Sets a temporary cache directory, saves a minimal player-game-log cache for the 2023-24 regular season, runs load_season against a test SQLite database populated with required FK fixtures, and asserts the result is a dict that includes the "fact_game" key.
    """
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    df = pd.DataFrame(
        {
            "GAME_ID": ["0022300001"],
            "PLAYER_ID": ["2544"],
            "PLAYER_NAME": ["LeBron James"],
            "TEAM_ID": ["1610612747"],
            "TEAM_ABBREVIATION": ["LAL"],
            "GAME_DATE": ["2023-10-24"],
            "MATCHUP": ["LAL vs. GSW"],
            "WL": ["W"],
            "MIN": [32.5],
            "FGM": [10],
            "FGA": [18],
            "FG3M": [2],
            "FG3A": [5],
            "FTM": [3],
            "FTA": [4],
            "OREB": [1],
            "DREB": [6],
            "REB": [7],
            "AST": [8],
            "STL": [1],
            "BLK": [0],
            "TOV": [3],
            "PF": [1],
            "PTS": [25],
            "PLUS_MINUS": [10],
        }
    )
    from src.db.cache import save_cache

    save_cache("pgl_2023-24_Regular_Season", df.to_dict(orient="records"))

    result = load_season(sqlite_con_with_data, "2023-24")

    assert isinstance(result, dict)
    assert "fact_game" in result
