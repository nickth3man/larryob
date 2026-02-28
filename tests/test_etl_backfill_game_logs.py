"""Tests: raw backfill game-log loaders."""

import sqlite3
from pathlib import Path

import pandas as pd

from src.etl.backfill._game_logs import load_player_game_logs, load_team_game_logs


def _seed_game_context(con: sqlite3.Connection) -> None:
    con.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES ('2023-24', 2023, 2024)"
    )
    con.execute(
        """INSERT INTO dim_team
           (team_id, abbreviation, full_name, city, nickname)
           VALUES ('1610612747', 'LAL', 'Los Angeles Lakers', 'Los Angeles', 'Lakers')"""
    )
    con.execute(
        """INSERT INTO dim_team
           (team_id, abbreviation, full_name, city, nickname)
           VALUES ('1610612744', 'GSW', 'Golden State Warriors', 'San Francisco', 'Warriors')"""
    )
    con.execute(
        """INSERT INTO dim_player
           (player_id, first_name, last_name, full_name, is_active)
           VALUES ('2544', 'LeBron', 'James', 'LeBron James', 1)"""
    )
    con.execute(
        """INSERT INTO fact_game
           (game_id, season_id, game_date, home_team_id, away_team_id, home_score, away_score, season_type, status)
           VALUES ('0222300001', '2023-24', '2024-01-15', '1610612747', '1610612744', 120, 110, 'Regular Season', 'Final')"""
    )
    con.commit()


def test_load_player_game_logs_inserts_valid_rows(
    sqlite_con: sqlite3.Connection, tmp_path: Path
) -> None:
    _seed_game_context(sqlite_con)

    pd.DataFrame([{"gameId": 222300001, "teamId": 1610612747, "home": 1}]).to_csv(
        tmp_path / "TeamStatistics.csv", index=False
    )

    pd.DataFrame(
        [
            {
                "gameId": 222300001,
                "personId": 2544,
                "home": 1,
                "numMinutes": 35.5,
                "fieldGoalsMade": 10,
                "fieldGoalsAttempted": 20,
                "threePointersMade": 2,
                "threePointersAttempted": 6,
                "freeThrowsMade": 3,
                "freeThrowsAttempted": 4,
                "reboundsOffensive": 1,
                "reboundsDefensive": 6,
                "reboundsTotal": 7,
                "assists": 8,
                "steals": 1,
                "blocks": 0,
                "turnovers": 3,
                "foulsPersonal": 2,
                "points": 25,
                "plusMinusPoints": 10,
            },
            {
                "gameId": 222300001,
                "personId": 999999,
                "home": 1,
                "numMinutes": 10.0,
                "fieldGoalsMade": 1,
                "fieldGoalsAttempted": 2,
                "threePointersMade": 0,
                "threePointersAttempted": 1,
                "freeThrowsMade": 0,
                "freeThrowsAttempted": 0,
                "reboundsOffensive": 0,
                "reboundsDefensive": 1,
                "reboundsTotal": 1,
                "assists": 1,
                "steals": 0,
                "blocks": 0,
                "turnovers": 0,
                "foulsPersonal": 1,
                "points": 2,
                "plusMinusPoints": -1,
            },
        ]
    ).to_csv(tmp_path / "PlayerStatistics.csv", index=False)

    load_player_game_logs(sqlite_con, tmp_path)

    row = sqlite_con.execute(
        """SELECT game_id, player_id, team_id, minutes_played, pts
           FROM player_game_log"""
    ).fetchone()
    assert row == ("0222300001", "2544", "1610612747", 35.5, 25)


def test_load_player_game_logs_skips_rows_without_team_lookup(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    _seed_game_context(sqlite_con)

    pd.DataFrame(
        [
            {
                "gameId": 222300001,
                "personId": 2544,
                "home": 1,
                "numMinutes": 30.0,
                "fieldGoalsMade": 8,
                "fieldGoalsAttempted": 15,
                "threePointersMade": 1,
                "threePointersAttempted": 5,
                "freeThrowsMade": 2,
                "freeThrowsAttempted": 3,
                "reboundsOffensive": 1,
                "reboundsDefensive": 4,
                "reboundsTotal": 5,
                "assists": 7,
                "steals": 1,
                "blocks": 0,
                "turnovers": 2,
                "foulsPersonal": 2,
                "points": 19,
                "plusMinusPoints": 3,
            }
        ]
    ).to_csv(tmp_path / "PlayerStatistics.csv", index=False)

    load_player_game_logs(sqlite_con, tmp_path)

    count = sqlite_con.execute("SELECT COUNT(*) FROM player_game_log").fetchone()[0]
    assert count == 0


def test_early_era_oreb_dreb_zero_pattern_not_dropped(
    sqlite_con: sqlite3.Connection, tmp_path: Path
) -> None:
    """
    Early-era rows where reb_total > 0 but oreb/dreb were not tracked (both 0)
    must NOT be dropped by validation. Instead oreb and dreb should be stored as None.
    """
    _seed_game_context(sqlite_con)

    pd.DataFrame([{"gameId": 222300001, "teamId": 1610612747, "home": 1}]).to_csv(
        tmp_path / "TeamStatistics.csv", index=False
    )

    pd.DataFrame(
        [
            {
                "gameId": 222300001,
                "personId": 2544,
                "home": 1,
                "numMinutes": 40.0,
                "fieldGoalsMade": 8,
                "fieldGoalsAttempted": 16,
                "threePointersMade": 0,
                "threePointersAttempted": 0,
                "freeThrowsMade": 4,
                "freeThrowsAttempted": 6,
                "reboundsOffensive": 0,  # early era: not tracked separately
                "reboundsDefensive": 0,  # early era: not tracked separately
                "reboundsTotal": 12,  # only total was recorded
                "assists": 5,
                "steals": 0,
                "blocks": 0,
                "turnovers": 2,
                "foulsPersonal": 3,
                "points": 20,
                "plusMinusPoints": 0,
            }
        ]
    ).to_csv(tmp_path / "PlayerStatistics.csv", index=False)

    load_player_game_logs(sqlite_con, tmp_path)

    row = sqlite_con.execute(
        "SELECT game_id, oreb, dreb, reb FROM player_game_log WHERE player_id='2544'"
    ).fetchone()
    assert row is not None, "Early-era row was dropped but should have been inserted"
    inserted_game_id, inserted_oreb, inserted_dreb, inserted_reb = row
    assert inserted_game_id == "0222300001"
    assert inserted_oreb is None, f"Expected oreb=None for early era, got {inserted_oreb}"
    assert inserted_dreb is None, f"Expected dreb=None for early era, got {inserted_dreb}"
    assert inserted_reb == 12


def test_early_era_team_oreb_dreb_zero_pattern_not_dropped(
    sqlite_con: sqlite3.Connection, tmp_path: Path
) -> None:
    """
    Team game log early-era rows where reb > 0 but oreb/dreb were not tracked
    must NOT be dropped. oreb and dreb should be stored as None.
    """
    _seed_game_context(sqlite_con)

    pd.DataFrame(
        [
            {
                "gameId": 222300001,
                "teamId": 1610612747,
                "fieldGoalsMade": 35,
                "fieldGoalsAttempted": 80,
                "threePointersMade": 0,
                "threePointersAttempted": 0,
                "freeThrowsMade": 20,
                "freeThrowsAttempted": 28,
                "reboundsOffensive": 0,  # early era: not tracked separately
                "reboundsDefensive": 0,  # early era: not tracked separately
                "reboundsTotal": 55,  # only total was recorded
                "assists": 18,
                "steals": 0,
                "blocks": 0,
                "turnovers": 12,
                "foulsPersonal": 22,
                "teamScore": 90,
                "plusMinusPoints": 5,
                "home": 1,
            }
        ]
    ).to_csv(tmp_path / "TeamStatistics.csv", index=False)

    load_team_game_logs(sqlite_con, tmp_path)

    row = sqlite_con.execute(
        "SELECT game_id, oreb, dreb, reb FROM team_game_log WHERE team_id='1610612747'"
    ).fetchone()
    assert row is not None, "Early-era team row was dropped but should have been inserted"
    inserted_game_id, inserted_oreb, inserted_dreb, inserted_reb = row
    assert inserted_game_id == "0222300001"
    assert inserted_oreb is None, f"Expected oreb=None for early era, got {inserted_oreb}"
    assert inserted_dreb is None, f"Expected dreb=None for early era, got {inserted_dreb}"
    assert inserted_reb == 55


def test_load_team_game_logs_inserts_rows_when_ids_are_valid(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    _seed_game_context(sqlite_con)

    pd.DataFrame(
        [
            {
                "gameId": 222300001,
                "teamId": 1610612747,
                "fieldGoalsMade": 42,
                "fieldGoalsAttempted": 85,
                "threePointersMade": 12,
                "threePointersAttempted": 30,
                "freeThrowsMade": 20,
                "freeThrowsAttempted": 25,
                "reboundsOffensive": 8,
                "reboundsDefensive": 35,
                "reboundsTotal": 43,
                "assists": 24,
                "steals": 7,
                "blocks": 4,
                "turnovers": 13,
                "foulsPersonal": 18,
                "teamScore": 116,
                "plusMinusPoints": 10,
            },
            {
                "gameId": 222300001,
                "teamId": 1610612999,
                "fieldGoalsMade": 40,
                "fieldGoalsAttempted": 84,
                "threePointersMade": 11,
                "threePointersAttempted": 31,
                "freeThrowsMade": 18,
                "freeThrowsAttempted": 24,
                "reboundsOffensive": 9,
                "reboundsDefensive": 33,
                "reboundsTotal": 42,
                "assists": 22,
                "steals": 6,
                "blocks": 5,
                "turnovers": 14,
                "foulsPersonal": 19,
                "teamScore": 109,
                "plusMinusPoints": -10,
            },
        ]
    ).to_csv(tmp_path / "TeamStatistics.csv", index=False)

    load_team_game_logs(sqlite_con, tmp_path)

    count = sqlite_con.execute("SELECT COUNT(*) FROM team_game_log").fetchone()[0]
    assert count == 1


def test_load_player_game_logs_accepts_early_era_rebounds(
    sqlite_con: sqlite3.Connection, tmp_path: Path
) -> None:
    """Verify early-era game logs where oreb/dreb were not tracked are accepted."""
    _seed_game_context(sqlite_con)

    pd.DataFrame([{"gameId": 222300001, "teamId": 1610612747, "home": 1}]).to_csv(
        tmp_path / "TeamStatistics.csv", index=False
    )

    # Early-era data: oreb=0, dreb=0, but reb>0 (rebounds tracked but not split)
    pd.DataFrame(
        [
            {
                "gameId": 222300001,
                "personId": 2544,
                "home": 1,
                "numMinutes": 35.5,
                "fieldGoalsMade": 10,
                "fieldGoalsAttempted": 20,
                "threePointersMade": 0,  # Pre-1979-80: 3-pointers not tracked
                "threePointersAttempted": 0,
                "freeThrowsMade": 3,
                "freeThrowsAttempted": 4,
                "reboundsOffensive": 0,  # Pre-1973-74: split rebounds not tracked
                "reboundsDefensive": 0,
                "reboundsTotal": 7,  # Total rebounds available
                "assists": 8,
                "steals": 0,  # Pre-1973-74: steals not tracked
                "blocks": 0,  # Pre-1973-74: blocks not tracked
                "turnovers": 0,  # Pre-1977-78: turnovers not tracked
                "foulsPersonal": 2,
                "points": 25,
                "plusMinusPoints": 0,
            }
        ]
    ).to_csv(tmp_path / "PlayerStatistics.csv", index=False)

    load_player_game_logs(sqlite_con, tmp_path)

    # Row should be inserted with oreb/dreb as NULL
    row = sqlite_con.execute("SELECT oreb, dreb, reb FROM player_game_log").fetchone()
    assert row is not None
    assert row[0] is None  # oreb should be NULL
    assert row[1] is None  # dreb should be NULL
    assert row[2] == 7  # reb should be preserved
