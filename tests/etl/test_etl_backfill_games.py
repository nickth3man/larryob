"""Tests: raw backfill game loaders."""

import sqlite3
from pathlib import Path

import pandas as pd

from src.etl.backfill._games import load_games, load_schedule


def _insert_team(con: sqlite3.Connection, team_id: str, abbr: str, full_name: str) -> None:
    con.execute(
        """INSERT INTO dim_team
           (team_id, abbreviation, full_name, city, nickname)
           VALUES (?, ?, ?, 'City', 'Nick')""",
        (team_id, abbr, full_name),
    )


def test_load_games_inserts_valid_rows(sqlite_con: sqlite3.Connection, tmp_path: Path) -> None:
    sqlite_con.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES ('2023-24', 2023, 2024)"
    )
    _insert_team(sqlite_con, "1610612747", "LAL", "Los Angeles Lakers")
    _insert_team(sqlite_con, "1610612744", "GSW", "Golden State Warriors")
    sqlite_con.commit()

    pd.DataFrame(
        [
            {
                "gameId": 222300001,
                "hometeamId": 1610612747,
                "awayteamId": 1610612744,
                "gameDateTimeEst": "2024-01-15T00:00:00",
                "homeScore": 120,
                "awayScore": 110,
                "attendance": 19000,
            }
        ]
    ).to_csv(tmp_path / "Games.csv", index=False)

    load_games(sqlite_con, tmp_path)

    row = sqlite_con.execute(
        """SELECT game_id, season_id, home_team_id, away_team_id, home_score, away_score, season_type, status
           FROM fact_game"""
    ).fetchone()
    assert row == (
        "0222300001",
        "2023-24",
        "1610612747",
        "1610612744",
        120,
        110,
        "Regular Season",
        "Final",
    )


def test_load_games_skips_rows_with_unknown_team_or_season(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    sqlite_con.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES ('2023-24', 2023, 2024)"
    )
    _insert_team(sqlite_con, "1610612747", "LAL", "Los Angeles Lakers")
    sqlite_con.commit()

    pd.DataFrame(
        [
            {
                "gameId": 222300002,
                "hometeamId": 1610612747,
                "awayteamId": 1610612999,
                "gameDateTimeEst": "2024-01-16T00:00:00",
                "homeScore": 100,
                "awayScore": 90,
                "attendance": 18000,
            }
        ]
    ).to_csv(tmp_path / "Games.csv", index=False)

    load_games(sqlite_con, tmp_path)

    count = sqlite_con.execute("SELECT COUNT(*) FROM fact_game").fetchone()[0]
    assert count == 0


def test_load_schedule_sets_season_type_from_game_label(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    sqlite_con.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES ('2024-25', 2024, 2025)"
    )
    sqlite_con.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES ('2025-26', 2025, 2026)"
    )
    _insert_team(sqlite_con, "1610612747", "LAL", "Los Angeles Lakers")
    _insert_team(sqlite_con, "1610612744", "GSW", "Golden State Warriors")
    sqlite_con.commit()

    pd.DataFrame(
        [
            {
                "gameId": 1112400001,
                "gameDateTimeEst": "2024-10-10T00:00:00",
                "homeTeamId": 1610612747,
                "awayTeamId": 1610612744,
                "gameLabel": "NBA Preseason",
                "arenaName": "Arena A",
            },
            {
                "gameId": 2422400001,
                "gameDateTimeEst": "2025-04-20T00:00:00",
                "homeTeamId": 1610612747,
                "awayTeamId": 1610612744,
                "gameLabel": "NBA Playoffs",
                "arenaName": "Arena B",
            },
        ]
    ).to_csv(tmp_path / "LeagueSchedule24_25.csv", index=False)

    pd.DataFrame(
        [
            {
                "gameId": 2522500001,
                "gameDateTimeEst": "2025-10-20T00:00:00",
                "homeTeamId": 1610612747,
                "awayTeamId": 1610612744,
                "gameLabel": "NBA Play-In",
                "arenaName": "Arena C",
            },
            {
                "gameId": 2222500002,
                "gameDateTimeEst": "2025-11-01T00:00:00",
                "homeTeamId": 1610612747,
                "awayTeamId": 1610612744,
                "gameLabel": "Regular Season",
                "arenaName": "Arena D",
            },
            {
                "gameId": 2222500003,
                "gameDateTimeEst": "2025-11-03T00:00:00",
                "homeTeamId": 1610612999,
                "awayTeamId": 1610612744,
                "gameLabel": "Regular Season",
                "arenaName": "Arena E",
            },
        ]
    ).to_csv(tmp_path / "LeagueSchedule25_26.csv", index=False)

    load_schedule(sqlite_con, tmp_path)

    preseason = sqlite_con.execute(
        "SELECT season_type FROM fact_game WHERE game_id='1112400001'"
    ).fetchone()[0]
    playoffs = sqlite_con.execute(
        "SELECT season_type FROM fact_game WHERE game_id='2422400001'"
    ).fetchone()[0]
    play_in = sqlite_con.execute(
        "SELECT season_type FROM fact_game WHERE game_id='2522500001'"
    ).fetchone()[0]
    fallback = sqlite_con.execute(
        "SELECT season_type FROM fact_game WHERE game_id='2222500002'"
    ).fetchone()[0]

    assert preseason == "Preseason"
    assert playoffs == "Playoffs"
    assert play_in == "Play-In"
    assert fallback == "Regular Season"


def test_load_schedule_skips_when_files_missing(
    sqlite_con: sqlite3.Connection, tmp_path: Path
) -> None:
    load_schedule(sqlite_con, tmp_path)
    count = sqlite_con.execute("SELECT COUNT(*) FROM fact_game").fetchone()[0]
    assert count == 0
