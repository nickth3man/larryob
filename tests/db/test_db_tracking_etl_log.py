"""Tests for src.db.tracking.etl_log — idempotency gating and run tracking."""

import logging
import sqlite3

import pytest

from src.db.tracking.etl_log import already_loaded, log_load_summary, record_run

# ------------------------------------------------------------------ #
# Fixtures                                                            #
# ------------------------------------------------------------------ #

ETL_LOG_DDL = """
CREATE TABLE etl_run_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name  TEXT NOT NULL,
    season_id   TEXT,
    loader      TEXT NOT NULL,
    started_at  TEXT NOT NULL,
    finished_at TEXT NOT NULL,
    row_count   INTEGER,
    status      TEXT NOT NULL
)
"""


@pytest.fixture
def con():
    """In-memory SQLite with the etl_run_log table created."""
    c = sqlite3.connect(":memory:")
    c.execute(ETL_LOG_DDL)
    c.commit()
    return c


@pytest.fixture
def bare_con():
    """In-memory SQLite with NO tables — simulates cold start."""
    return sqlite3.connect(":memory:")


# ------------------------------------------------------------------ #
# already_loaded                                                      #
# ------------------------------------------------------------------ #


def test_already_loaded_returns_false_when_no_table(bare_con):
    assert already_loaded(bare_con, "dim_player", "2023-24", "player_loader") is False


def test_already_loaded_returns_false_when_no_matching_row(con):
    assert already_loaded(con, "dim_player", "2023-24", "player_loader") is False


def test_already_loaded_returns_false_for_failed_status(con):
    con.execute(
        "INSERT INTO etl_run_log (table_name,season_id,loader,started_at,finished_at,row_count,status)"
        " VALUES (?,?,?,?,?,?,?)",
        ("dim_player", "2023-24", "player_loader", "T", "T", 0, "failed"),
    )
    con.commit()
    assert already_loaded(con, "dim_player", "2023-24", "player_loader") is False


def test_already_loaded_returns_true_for_ok_status(con):
    con.execute(
        "INSERT INTO etl_run_log (table_name,season_id,loader,started_at,finished_at,row_count,status)"
        " VALUES (?,?,?,?,?,?,?)",
        ("dim_player", "2023-24", "player_loader", "T", "T", 50, "ok"),
    )
    con.commit()
    assert already_loaded(con, "dim_player", "2023-24", "player_loader") is True


def test_already_loaded_season_none_only_matches_null_rows(con):
    # Insert a row with a season_id
    con.execute(
        "INSERT INTO etl_run_log (table_name,season_id,loader,started_at,finished_at,row_count,status)"
        " VALUES (?,?,?,?,?,?,?)",
        ("dim_player", "2023-24", "player_loader", "T", "T", 50, "ok"),
    )
    con.commit()
    # Querying with season_id=None should NOT match the seasoned row
    assert already_loaded(con, "dim_player", None, "player_loader") is False


def test_already_loaded_season_none_matches_null_row(con):
    con.execute(
        "INSERT INTO etl_run_log (table_name,season_id,loader,started_at,finished_at,row_count,status)"
        " VALUES (?,?,?,?,?,?,?)",
        ("dim_player", None, "player_loader", "T", "T", 50, "ok"),
    )
    con.commit()
    assert already_loaded(con, "dim_player", None, "player_loader") is True


def test_already_loaded_does_not_match_different_loader(con):
    con.execute(
        "INSERT INTO etl_run_log (table_name,season_id,loader,started_at,finished_at,row_count,status)"
        " VALUES (?,?,?,?,?,?,?)",
        ("dim_player", "2023-24", "other_loader", "T", "T", 50, "ok"),
    )
    con.commit()
    assert already_loaded(con, "dim_player", "2023-24", "player_loader") is False


# ------------------------------------------------------------------ #
# record_run                                                          #
# ------------------------------------------------------------------ #


def test_record_run_inserts_row(con):
    record_run(con, "fact_game", "2023-24", "game_loader", 100, "ok")
    row = con.execute(
        "SELECT table_name,season_id,loader,row_count,status FROM etl_run_log"
    ).fetchone()
    assert row == ("fact_game", "2023-24", "game_loader", 100, "ok")


def test_record_run_with_started_at_uses_provided_value(con):
    record_run(
        con, "fact_game", "2023-24", "game_loader", 10, "ok", started_at="2024-01-01T00:00:00"
    )
    row = con.execute("SELECT started_at FROM etl_run_log").fetchone()
    assert row[0] == "2024-01-01T00:00:00"


def test_record_run_with_no_started_at_sets_timestamp(con):
    record_run(con, "fact_game", None, "game_loader", 0, "ok")
    row = con.execute("SELECT started_at FROM etl_run_log").fetchone()
    assert row[0] is not None and len(row[0]) > 10


def test_record_run_null_row_count(con):
    record_run(con, "fact_game", None, "game_loader", None, "ok")
    row = con.execute("SELECT row_count FROM etl_run_log").fetchone()
    assert row[0] is None


def test_record_run_silently_ignores_missing_table(bare_con):
    # Should not raise even if etl_run_log does not exist
    record_run(bare_con, "fact_game", "2023-24", "game_loader", 10, "ok")


# ------------------------------------------------------------------ #
# log_load_summary                                                    #
# ------------------------------------------------------------------ #


def _make_con_with_table(ddl: str) -> sqlite3.Connection:
    c = sqlite3.connect(":memory:")
    c.execute(ddl)
    c.commit()
    return c


def test_log_load_summary_returns_row_count():
    con = _make_con_with_table("CREATE TABLE widgets (id INTEGER)")
    con.executemany("INSERT INTO widgets VALUES (?)", [(i,) for i in range(5)])
    con.commit()
    assert log_load_summary(con, "widgets") == 5


def test_log_load_summary_logs_info_when_count_above_min(caplog):
    con = _make_con_with_table("CREATE TABLE widgets (id INTEGER)")
    con.execute("INSERT INTO widgets VALUES (1)")
    con.commit()
    with caplog.at_level(logging.INFO, logger="src.db.tracking.etl_log"):
        log_load_summary(con, "widgets", min_rows=1)
    assert any("widgets" in r.message for r in caplog.records)


def test_log_load_summary_logs_warning_when_count_below_min(caplog):
    con = _make_con_with_table("CREATE TABLE widgets (id INTEGER)")
    con.commit()  # empty table
    with caplog.at_level(logging.WARNING, logger="src.db.tracking.etl_log"):
        log_load_summary(con, "widgets", min_rows=10)
    assert any(r.levelno == logging.WARNING for r in caplog.records)


def test_log_load_summary_filters_by_season_id_column():
    con = _make_con_with_table("CREATE TABLE t (season_id TEXT, v INTEGER)")
    con.execute("INSERT INTO t VALUES ('2023-24', 1)")
    con.execute("INSERT INTO t VALUES ('2022-23', 2)")
    con.commit()
    count = log_load_summary(con, "t", season_id="2023-24")
    assert count == 1


def test_log_load_summary_filters_by_game_id_join(sqlite_con_with_data):
    """When the table has game_id but no season_id, it should join to fact_game.

    sqlite_con_with_data seeds:
      - dim_season 2023-24, teams 1610612747/1610612744, player 2544
      - fact_game "0022300001" (season 2023-24)
    We add a second season + game and insert player_game_log rows for both,
    then verify that filtering by season_id returns only the 2023-24 row.
    """
    con = sqlite_con_with_data

    # Add a second season and game
    con.execute("INSERT INTO dim_season VALUES ('2022-23', 2022, 2023)")
    con.execute(
        """INSERT INTO fact_game (game_id, season_id, game_date, home_team_id, away_team_id,
           home_score, away_score, season_type, status)
           VALUES ('0021300001', '2022-23', '2023-01-01', '1610612747', '1610612744',
                   110, 95, 'Regular Season', 'Final')"""
    )

    # Insert one player_game_log row per game (different seasons)
    con.execute(
        """INSERT INTO player_game_log
           (game_id, player_id, team_id, pts, reb, ast, fgm, fga, fg3m, fg3a,
            ftm, fta, oreb, dreb, stl, blk, tov, pf, plus_minus, minutes_played)
           VALUES ('0022300001','2544','1610612747',20,6,3,8,15,2,5,2,2,1,5,1,0,2,2,5,30)"""
    )
    con.execute(
        """INSERT INTO player_game_log
           (game_id, player_id, team_id, pts, reb, ast, fgm, fga, fg3m, fg3a,
            ftm, fta, oreb, dreb, stl, blk, tov, pf, plus_minus, minutes_played)
           VALUES ('0021300001','2544','1610612747',18,4,2,7,12,1,3,3,4,1,3,0,1,1,3,4,28)"""
    )
    con.commit()

    count = log_load_summary(con, "player_game_log", season_id="2023-24")
    assert count == 1
