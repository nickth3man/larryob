"""Tests: DuckDB analytics layer — SQLite bridge and view correctness."""

import sqlite3

import duckdb

from src.db.analytics import _VIEWS
from src.etl.utils import upsert_rows

# ------------------------------------------------------------------ #
# Helpers to seed player game log data                               #
# ------------------------------------------------------------------ #

def _seed_player_logs(con: sqlite3.Connection) -> None:
    rows = [
        {
            "game_id": "0022300001",
            "player_id": "2544",
            "team_id": "1610612747",
            "minutes_played": 35.0,
            "fgm": 10, "fga": 20,
            "fg3m": 2, "fg3a": 5,
            "ftm": 3, "fta": 4,
            "oreb": 1, "dreb": 6, "reb": 7,
            "ast": 8, "stl": 1, "blk": 0,
            "tov": 3, "pf": 1, "pts": 25,
            "plus_minus": 10, "starter": 1,
        },
    ]
    upsert_rows(con, "player_game_log", rows, conflict="ABORT")

    team_rows = [
        {
            "game_id": "0022300001",
            "team_id": "1610612747",
            "fgm": 42, "fga": 85,
            "fg3m": 12, "fg3a": 30,
            "ftm": 20, "fta": 25,
            "oreb": 8, "dreb": 35, "reb": 43,
            "ast": 24, "stl": 7, "blk": 4,
            "tov": 13, "pf": 18, "pts": 116,
            "plus_minus": 10,
        },
        {
            "game_id": "0022300001",
            "team_id": "1610612744",
            "fgm": 38, "fga": 90,
            "fg3m": 10, "fg3a": 28,
            "ftm": 20, "fta": 26,
            "oreb": 10, "dreb": 30, "reb": 40,
            "ast": 21, "stl": 6, "blk": 3,
            "tov": 14, "pf": 20, "pts": 106,
            "plus_minus": -10,
        },
    ]
    upsert_rows(con, "team_game_log", team_rows, conflict="ABORT")
    con.commit()


# ------------------------------------------------------------------ #
# Tests                                                               #
# ------------------------------------------------------------------ #

def test_sqlite_extension_loads(duck_con_with_sqlite) -> None:
    """DuckDB can successfully load the sqlite extension and attach the db."""
    result = duck_con_with_sqlite.execute("SHOW DATABASES").fetchall()
    db_names = [r[0] for r in result]
    assert "nba" in db_names


def test_can_query_dim_player(duck_con_with_sqlite) -> None:
    df = duck_con_with_sqlite.execute(
        "SELECT * FROM nba.dim_player WHERE player_id = '2544'"
    ).df()
    assert len(df) == 1
    assert df["full_name"].iloc[0] == "LeBron James"


def test_can_query_fact_game(duck_con_with_sqlite) -> None:
    df = duck_con_with_sqlite.execute(
        "SELECT * FROM nba.fact_game"
    ).df()
    assert len(df) == 1


def test_efg_pct_calculation(
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path,
) -> None:
    """eFG% = (FGM + 0.5 * FG3M) / FGA = (10 + 0.5*2) / 20 = 0.55"""
    _seed_player_logs(sqlite_con_with_data)

    sqlite_file = tmp_path / "test_efg.db"
    file_con = sqlite3.connect(sqlite_file)
    sqlite_con_with_data.backup(file_con)
    file_con.close()

    duck = duckdb.connect(":memory:")
    duck.execute("INSTALL sqlite; LOAD sqlite;")
    duck.execute(f"ATTACH '{sqlite_file}' AS nba (TYPE sqlite, READ_ONLY);")

    df = duck.execute("""
        SELECT
            (SUM(fgm) + 0.5 * SUM(fg3m)) / SUM(fga) AS efg
        FROM nba.player_game_log
        WHERE player_id = '2544'
    """).df()
    duck.close()

    assert abs(df["efg"].iloc[0] - 0.55) < 0.001


def test_ts_pct_calculation(
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path,
) -> None:
    """TS% = PTS / (2 * (FGA + 0.44 * FTA)) = 25 / (2 * (20 + 0.44*4)) = 25/43.52 ≈ 0.574"""
    _seed_player_logs(sqlite_con_with_data)

    sqlite_file = tmp_path / "test_ts.db"
    file_con = sqlite3.connect(sqlite_file)
    sqlite_con_with_data.backup(file_con)
    file_con.close()

    duck = duckdb.connect(":memory:")
    duck.execute("INSTALL sqlite; LOAD sqlite;")
    duck.execute(f"ATTACH '{sqlite_file}' AS nba (TYPE sqlite, READ_ONLY);")

    df = duck.execute("""
        SELECT
            SUM(pts) / (2.0 * (SUM(fga) + 0.44 * SUM(fta))) AS ts_pct
        FROM nba.player_game_log
        WHERE player_id = '2544'
    """).df()
    duck.close()

    expected = 25 / (2 * (20 + 0.44 * 4))
    assert abs(df["ts_pct"].iloc[0] - expected) < 0.001


def test_views_are_queryable(duck_con_with_sqlite) -> None:
    """All views must be defined; querying them must not raise."""



    # Re-attach with views via get_duck_con (using existing sqlite fixture via tmp)
    # This test just ensures the view SQL parses without error.
    for name, _ in _VIEWS:
        duck_con_with_sqlite.execute(f"SELECT 1 FROM ({_view_sql(name, duck_con_with_sqlite)}) LIMIT 0")


def test_get_duck_con_singleton(tmp_path, monkeypatch) -> None:
    """Ensure get_duck_con returns the cached connection when called twice."""
    import src.db.analytics as analytics
    from src.db.analytics import get_duck_con

    # reset singleton for test
    if not hasattr(analytics._local, "cached_con"):
        analytics._local.cached_con = None
        analytics._local.cached_sqlite_path = None
        analytics._local.cached_duck_db_path = None

    monkeypatch.setattr(analytics._local, "cached_con", None)
    monkeypatch.setattr(analytics._local, "cached_sqlite_path", None)
    monkeypatch.setattr(analytics._local, "cached_duck_db_path", None)

    sqlite_file = tmp_path / "test_singleton.db"
    import sqlite3
    con1 = sqlite3.connect(sqlite_file)
    from src.db.schema import ALTER_STATEMENTS, DDL_STATEMENTS
    for ddl in DDL_STATEMENTS:
        con1.execute(ddl)
    for alter in ALTER_STATEMENTS:
        try:
            con1.execute(alter)
        except sqlite3.OperationalError:
            pass
    con1.close()

    duck1 = get_duck_con(sqlite_path=sqlite_file)
    duck2 = get_duck_con(sqlite_path=sqlite_file)

    assert duck1 is duck2

    duck3 = get_duck_con(sqlite_path=sqlite_file, force_refresh=True)
    assert duck1 is not duck3

    duck1.close()
    duck3.close()


def _view_sql(view_name: str, con: duckdb.DuckDBPyConnection) -> str:
    """Retrieve the underlying SQL of a view for wrapping in a subquery."""

    mapping = dict(_VIEWS)
