"""Tests: SQLite schema integrity."""

import sqlite3


def _table_columns(con: sqlite3.Connection, table: str) -> list[str]:
    return [row[1] for row in con.execute(f"PRAGMA table_info({table})").fetchall()]


def test_all_tables_created(sqlite_con: sqlite3.Connection) -> None:
    tables = {
        row[0]
        for row in sqlite_con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    expected = {
        "dim_season", "dim_team", "dim_player",
        "fact_game", "fact_roster",
        "player_game_log", "team_game_log",
        "fact_play_by_play", "fact_player_award",
        "dim_salary_cap", "fact_salary",
    }
    assert expected.issubset(tables), f"Missing tables: {expected - tables}"


def test_player_game_log_columns(sqlite_con: sqlite3.Connection) -> None:
    cols = _table_columns(sqlite_con, "player_game_log")
    required = [
        "game_id", "player_id", "team_id", "minutes_played",
        "fgm", "fga", "fg3m", "fg3a", "ftm", "fta",
        "oreb", "dreb", "reb", "ast", "stl", "blk", "tov", "pf", "pts",
        "plus_minus", "starter",
    ]
    for col in required:
        assert col in cols, f"Missing column '{col}' in player_game_log"


def test_fact_play_by_play_columns(sqlite_con: sqlite3.Connection) -> None:
    cols = _table_columns(sqlite_con, "fact_play_by_play")
    required = [
        "event_id", "game_id", "period",
        "pc_time_string", "eventmsgtype", "eventmsgactiontype",
        "player1_id", "player2_id", "player3_id",
        "person1type", "person2type", "person3type",
        "home_description", "visitor_description", "score", "score_margin",
    ]
    for col in required:
        assert col in cols, f"Missing column '{col}' in fact_play_by_play"


def test_dim_player_columns(sqlite_con: sqlite3.Connection) -> None:
    cols = _table_columns(sqlite_con, "dim_player")
    required = [
        "player_id", "first_name", "last_name", "full_name",
        "birth_date", "height_cm", "weight_kg", "position",
        "draft_year", "is_active",
    ]
    for col in required:
        assert col in cols, f"Missing column '{col}' in dim_player"


def test_indexes_created(sqlite_con: sqlite3.Connection) -> None:
    indexes = {
        row[0]
        for row in sqlite_con.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ).fetchall()
    }
    expected_indexes = {
        "idx_pgl_player", "idx_pgl_game", "idx_pgl_player_game",
        "idx_game_date", "idx_game_home", "idx_game_away",
        "idx_pbp_game", "idx_pbp_game_period", "idx_pbp_player1",
        "idx_roster_player_dates", "idx_tgl_team",
    }
    assert expected_indexes.issubset(indexes), \
        f"Missing indexes: {expected_indexes - indexes}"


def test_schema_is_idempotent(sqlite_con: sqlite3.Connection) -> None:
    """Running DDL a second time must not raise."""
    from src.db.schema import DDL_STATEMENTS
    for ddl in DDL_STATEMENTS:
        sqlite_con.execute(ddl)  # should not raise
