"""Tests: ETL dimension loaders (no network calls)."""

import sqlite3

from src.etl.dimensions import (
    _map_nba_player_static,
    _map_nba_team,
    load_players_static,
    load_seasons,
    load_teams,
)


def test_load_seasons(sqlite_con: sqlite3.Connection) -> None:
    load_seasons(sqlite_con, up_to_start_year=1950)
    count = sqlite_con.execute("SELECT COUNT(*) FROM dim_season").fetchone()[0]
    assert count == 5, f"Expected 5 seasons (1946-50), got {count}"


def test_load_seasons_idempotent(sqlite_con: sqlite3.Connection) -> None:
    load_seasons(sqlite_con, up_to_start_year=1950)
    load_seasons(sqlite_con, up_to_start_year=1950)  # second call
    count = sqlite_con.execute("SELECT COUNT(*) FROM dim_season").fetchone()[0]
    assert count == 5, "Second call should not duplicate rows"


def test_season_id_format(sqlite_con: sqlite3.Connection) -> None:
    load_seasons(sqlite_con, up_to_start_year=1946)
    row = sqlite_con.execute("SELECT season_id FROM dim_season").fetchone()
    assert row[0] == "1946-47"


def test_load_teams(sqlite_con: sqlite3.Connection) -> None:
    load_seasons(sqlite_con)    # teams need no FK, but seasons must exist
    load_teams(sqlite_con)
    count = sqlite_con.execute("SELECT COUNT(*) FROM dim_team").fetchone()[0]
    assert count >= 30, f"Expected at least 30 teams, got {count}"


def test_load_players_static(sqlite_con: sqlite3.Connection) -> None:
    load_seasons(sqlite_con)
    load_teams(sqlite_con)
    load_players_static(sqlite_con)
    count = sqlite_con.execute("SELECT COUNT(*) FROM dim_player").fetchone()[0]
    assert count > 1000, f"Expected 1000+ historical players, got {count}"


def test_map_nba_team_fields() -> None:
    raw = {
        "id": 1610612747,
        "abbreviation": "LAL",
        "full_name": "Los Angeles Lakers",
        "city": "Los Angeles",
        "nickname": "Lakers",
    }
    row = _map_nba_team(raw)
    assert row["team_id"] == "1610612747"
    assert row["abbreviation"] == "LAL"
    assert row["conference"] is None


def test_map_nba_player_static_active() -> None:
    raw = {"id": 2544, "full_name": "LeBron James", "is_active": True}
    row = _map_nba_player_static(raw)
    assert row["player_id"] == "2544"
    assert row["first_name"] == "LeBron"
    assert row["last_name"] == "James"
    assert row["is_active"] == 1


def test_map_nba_player_static_inactive() -> None:
    raw = {"id": 76375, "full_name": "Kareem Abdul-Jabbar", "is_active": False}
    row = _map_nba_player_static(raw)
    assert row["is_active"] == 0
