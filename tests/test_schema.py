"""Tests: SQLite schema integrity."""

import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest


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
        "dim_season",
        "dim_team",
        "dim_player",
        "dim_coach",
        "fact_game",
        "fact_roster",
        "fact_team_coach_game",
        "player_game_log",
        "team_game_log",
        "fact_play_by_play",
        "fact_player_award",
        "fact_all_star",
        "fact_all_nba",
        "fact_all_nba_vote",
        "dim_salary_cap",
        "fact_salary",
        "dim_player_identifier",
        "dim_team_identifier",
        "etl_run_log",
        "dim_team_history",
        "fact_team_season",
        "dim_league_season",
        "fact_draft",
        "fact_player_season_stats",
        "fact_player_advanced_season",
        "fact_player_shooting_season",
        "fact_player_pbp_season",
    }
    assert expected == tables, f"Missing/extra tables: expected {expected}, got {tables}"


def test_player_game_log_columns(sqlite_con: sqlite3.Connection) -> None:
    cols = _table_columns(sqlite_con, "player_game_log")
    required = [
        "game_id",
        "player_id",
        "team_id",
        "minutes_played",
        "fgm",
        "fga",
        "fg3m",
        "fg3a",
        "ftm",
        "fta",
        "oreb",
        "dreb",
        "reb",
        "ast",
        "stl",
        "blk",
        "tov",
        "pf",
        "pts",
        "plus_minus",
        "starter",
    ]
    for col in required:
        assert col in cols, f"Missing column '{col}' in player_game_log"


def test_fact_play_by_play_columns(sqlite_con: sqlite3.Connection) -> None:
    cols = _table_columns(sqlite_con, "fact_play_by_play")
    required = [
        "event_id",
        "game_id",
        "period",
        "pc_time_string",
        "eventmsgtype",
        "eventmsgactiontype",
        "player1_id",
        "player2_id",
        "player3_id",
        "person1type",
        "person2type",
        "person3type",
        "home_description",
        "visitor_description",
        "score",
        "score_margin",
    ]
    for col in required:
        assert col in cols, f"Missing column '{col}' in fact_play_by_play"


def test_dim_player_columns(sqlite_con: sqlite3.Connection) -> None:
    cols = _table_columns(sqlite_con, "dim_player")
    required = [
        "player_id",
        "first_name",
        "last_name",
        "full_name",
        "birth_date",
        "height_cm",
        "weight_kg",
        "position",
        "draft_year",
        "is_active",
    ]
    for col in required:
        assert col in cols, f"Missing column '{col}' in dim_player"


def test_dim_player_migration_columns(sqlite_con: sqlite3.Connection) -> None:
    cols = _table_columns(sqlite_con, "dim_player")
    for col in ["bref_id", "college", "hof"]:
        assert col in cols, f"Missing migration column '{col}' in dim_player"


def test_fact_all_star_columns(sqlite_con: sqlite3.Connection) -> None:
    cols = _table_columns(sqlite_con, "fact_all_star")
    required = [
        "all_star_id",
        "player_id",
        "season_id",
        "team_id",
        "selection_team",
        "is_starter",
        "is_replacement",
    ]
    for col in required:
        assert col in cols, f"Missing column '{col}' in fact_all_star"


def test_fact_all_nba_columns(sqlite_con: sqlite3.Connection) -> None:
    cols = _table_columns(sqlite_con, "fact_all_nba")
    required = [
        "selection_id",
        "player_id",
        "season_id",
        "team_type",
        "team_number",
        "position",
    ]
    for col in required:
        assert col in cols, f"Missing column '{col}' in fact_all_nba"


def test_fact_all_nba_vote_columns(sqlite_con: sqlite3.Connection) -> None:
    cols = _table_columns(sqlite_con, "fact_all_nba_vote")
    required = [
        "vote_id",
        "player_id",
        "season_id",
        "team_type",
        "team_number",
        "position",
        "pts_won",
        "pts_max",
        "share",
        "first_team_votes",
        "second_team_votes",
        "third_team_votes",
    ]
    for col in required:
        assert col in cols, f"Missing column '{col}' in fact_all_nba_vote"


def test_indexes_created(sqlite_con: sqlite3.Connection) -> None:
    indexes = {
        row[0]
        for row in sqlite_con.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ).fetchall()
    }
    expected_indexes = {
        "idx_pgl_player",
        "idx_pgl_game",
        "idx_pgl_player_game",
        "idx_game_date",
        "idx_game_home",
        "idx_game_away",
        "idx_pbp_game",
        "idx_pbp_game_period",
        "idx_pbp_player1",
        "idx_roster_player_dates",
        "idx_tgl_team",
        "idx_player_bref",
        "idx_allstar_player",
        "idx_allnba_player",
        "idx_allnba_vote_player",
    }
    assert expected_indexes.issubset(indexes), f"Missing indexes: {expected_indexes - indexes}"


def test_schema_is_idempotent(sqlite_con: sqlite3.Connection) -> None:
    """Running DDL a second time must not raise."""
    from src.db.schema import DDL_STATEMENTS

    for ddl in DDL_STATEMENTS:
        sqlite_con.execute(ddl)  # should not raise


def test_init_db_creates_file_and_returns_connection(tmp_path: Path) -> None:
    from src.db.schema import init_db

    db_file = tmp_path / "test_init.db"
    con = init_db(db_file)
    try:
        assert db_file.exists()
        tables = {
            r[0]
            for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        assert "dim_season" in tables
        assert "fact_game" in tables
        assert "etl_run_log" in tables
    finally:
        con.close()


def test_init_db_is_idempotent(tmp_path: Path) -> None:
    """Calling init_db twice on the same file must not raise."""
    from src.db.schema import init_db

    db_file = tmp_path / "test_idempotent.db"
    con1 = init_db(db_file)
    con1.close()
    con2 = init_db(db_file)
    con2.close()


def test_init_db_enables_foreign_keys(tmp_path: Path) -> None:
    from src.db.schema import init_db

    db_file = tmp_path / "test_fk.db"
    con = init_db(db_file)
    try:
        result = con.execute("PRAGMA foreign_keys").fetchone()[0]
        assert result == 1
    finally:
        con.close()


def test_dim_team_columns(sqlite_con: sqlite3.Connection) -> None:
    cols = [row[1] for row in sqlite_con.execute("PRAGMA table_info(dim_team)").fetchall()]
    required = [
        "team_id",
        "abbreviation",
        "full_name",
        "city",
        "nickname",
        "conference",
        "division",
        "color_primary",
        "arena_name",
        "founded_year",
    ]
    for col in required:
        assert col in cols, f"Missing column '{col}' in dim_team"


def test_init_db_raises_for_non_duplicate_alter_errors(tmp_path: Path) -> None:
    from src.db import schema as schema_mod

    db_file = tmp_path / "test_init_error.db"
    leaked_con = sqlite3.connect(db_file)
    try:
        with patch.object(schema_mod.sqlite3, "connect", return_value=leaked_con):
            with patch.object(schema_mod, "ALTER_STATEMENTS", ["ALTER TABLE dim_team BROKEN SQL"]):
                with pytest.raises(sqlite3.OperationalError):
                    schema_mod.init_db(db_file)
    finally:
        leaked_con.close()


def test_rollback_db_executes_statements_and_returns_connection(tmp_path: Path) -> None:
    from src.db.schema import rollback_db

    db_file = tmp_path / "test_rollback.db"
    con = rollback_db(db_file)
    try:
        assert isinstance(con, sqlite3.Connection)
    finally:
        con.close()


def test_rollback_db_ignores_operational_errors(tmp_path: Path) -> None:
    from src.db import schema as schema_mod

    db_file = tmp_path / "test_rollback_error.db"
    with patch.object(schema_mod, "ROLLBACK_STATEMENTS", ["DROP TABLE definitely_missing_table"]):
        con = schema_mod.rollback_db(db_file)
    con.close()
