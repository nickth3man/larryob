"""
Shared pytest fixtures.

All fixtures use in-memory or temp-file databases so tests never touch
the production `nba_raw_data.db`.
"""

import sqlite3
from pathlib import Path

import duckdb
import pytest

from src.db.schema import DDL_STATEMENTS

# ------------------------------------------------------------------ #
# SQLite fixtures                                                     #
# ------------------------------------------------------------------ #

@pytest.fixture
def sqlite_con() -> sqlite3.Connection:
    """An in-memory SQLite db with the full NBA schema initialised."""
    con = sqlite3.connect(":memory:")
    con.execute("PRAGMA foreign_keys=ON;")
    for ddl in DDL_STATEMENTS:
        con.execute(ddl)
    con.commit()
    return con


@pytest.fixture
def sqlite_con_with_data(sqlite_con: sqlite3.Connection) -> sqlite3.Connection:
    """SQLite db pre-seeded with minimal reference rows for FK compliance."""
    con = sqlite_con
    con.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES (?, ?, ?)",
        ("2023-24", 2023, 2024),
    )
    con.execute(
        "INSERT INTO dim_team VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        ("1610612747", "LAL", "Los Angeles Lakers", "Los Angeles",
         "Lakers", "West", "Pacific", "#552583", "#FDB927", "Crypto.com Arena", 1947),
    )
    con.execute(
        "INSERT INTO dim_team VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        ("1610612744", "GSW", "Golden State Warriors", "San Francisco",
         "Warriors", "West", "Pacific", "#1D428A", "#FFC72C", "Chase Center", 1946),
    )
    con.execute(
        "INSERT INTO dim_player VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("2544", "LeBron", "James", "LeBron James",
         "1984-12-30", "Akron", "USA", 206.0, 113.0, "SF", 2003, 1, 1, 1),
    )
    con.execute(
        "INSERT INTO dim_player VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("203999", "Nikola", "Jokic", "Nikola Jokic",
         "1995-02-19", "Sombor", "Serbia", 211.0, 129.0, "C", 2014, 2, 41, 1),
    )
    con.execute(
        """INSERT INTO fact_game
           (game_id, season_id, game_date, home_team_id, away_team_id,
            home_score, away_score, season_type, status)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        ("0022300001", "2023-24", "2023-10-24",
         "1610612747", "1610612744", 120, 110, "Regular Season", "Final"),
    )
    con.commit()
    return con


# ------------------------------------------------------------------ #
# DuckDB fixture with SQLite bridge                                   #
# ------------------------------------------------------------------ #

@pytest.fixture
def duck_con_with_sqlite(sqlite_con_with_data: sqlite3.Connection, tmp_path: Path):
    """
    DuckDB connection with a *temp-file* copy of the seeded SQLite db attached.
    DuckDB's sqlite extension requires a file path, not an in-memory connection.
    """
    # Write the in-memory SQLite db to a temp file
    sqlite_file = tmp_path / "test_nba.db"
    file_con = sqlite3.connect(sqlite_file)
    sqlite_con_with_data.backup(file_con)
    file_con.close()

    duck = duckdb.connect(":memory:")
    duck.execute("INSTALL sqlite;")
    duck.execute("LOAD sqlite;")
    duck.execute(f"ATTACH '{sqlite_file}' AS nba (TYPE sqlite, READ_ONLY);")
    yield duck
    duck.close()
