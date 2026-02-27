"""Tests: raw backfill All-NBA loaders."""

import sqlite3
from pathlib import Path

import pandas as pd

from src.etl.backfill._all_nba import load_all_nba_teams, load_all_nba_votes


def _seed_all_nba_context(con: sqlite3.Connection) -> None:
    con.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES ('2023-24', 2023, 2024)"
    )
    con.execute(
        """INSERT INTO dim_player
           (player_id, first_name, last_name, full_name, bref_id, is_active)
           VALUES ('203999', 'Nikola', 'Jokic', 'Nikola Jokic', 'jokicni01', 1)"""
    )
    con.execute(
        """INSERT INTO dim_player
           (player_id, first_name, last_name, full_name, bref_id, is_active)
           VALUES ('2544', 'LeBron', 'James', 'LeBron James', 'jamesle01', 1)"""
    )
    con.commit()


def test_load_all_nba_teams_inserts_rows_and_normalizes_columns(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    _seed_all_nba_context(sqlite_con)

    pd.DataFrame(
        [
            {
                "season": 2024,
                "lg": "NBA",
                "type": "All-Defense",
                "number_tm": "1st",
                "player": "Nikola Jokic",
                "player_id": "jokicni01",
                "position": "NA",
            },
            {
                "season": 2024,
                "lg": "NBA",
                "type": "All-NBA",
                "number_tm": "2nd",
                "player": "LeBron James",
                "player_id": "jamesle01",
                "position": "F",
            },
            {
                "season": 1990,
                "lg": "NBA",
                "type": "All-NBA",
                "number_tm": "1st",
                "player": "Nikola Jokic",
                "player_id": "jokicni01",
                "position": "C",
            },
        ]
    ).to_csv(tmp_path / "End of Season Teams.csv", index=False)

    inserted = load_all_nba_teams(sqlite_con, tmp_path)

    rows = sqlite_con.execute(
        """
        SELECT player_id, season_id, team_type, team_number, position
        FROM fact_all_nba
        ORDER BY player_id
        """
    ).fetchall()

    assert inserted == 2
    assert rows == [
        ("203999", "2023-24", "All-Defense", 1, None),
        ("2544", "2023-24", "All-NBA", 2, "F"),
    ]


def test_load_all_nba_votes_inserts_orv_rows_with_null_team_number(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    _seed_all_nba_context(sqlite_con)

    pd.DataFrame(
        [
            {
                "season": 2024,
                "lg": "nba",
                "type": "all_nba",
                "number_tm": "1T",
                "position": "C",
                "player": "Nikola Jokic",
                "player_id": "jokicni01",
                "age": 28,
                "pts_won": 495,
                "pts_max": 495,
                "share": 1.0,
                "x1st_tm": 99,
                "x2nd_tm": 0,
                "x3rd_tm": 0,
            },
            {
                "season": 2024,
                "lg": "nba",
                "type": "all_nba",
                "number_tm": "ORV",
                "position": "F",
                "player": "LeBron James",
                "player_id": "jamesle01",
                "age": 39,
                "pts_won": 12,
                "pts_max": 495,
                "share": 0.024,
                "x1st_tm": 0,
                "x2nd_tm": 1,
                "x3rd_tm": 3,
            },
            {
                "season": 2024,
                "lg": "nba",
                "type": "all_nba",
                "number_tm": "2T",
                "position": "G",
                "player": "Unknown Player",
                "player_id": "unknown01",
                "age": 30,
                "pts_won": 50,
                "pts_max": 495,
                "share": 0.101,
                "x1st_tm": 0,
                "x2nd_tm": 10,
                "x3rd_tm": 10,
            },
        ]
    ).to_csv(tmp_path / "End of Season Teams (Voting).csv", index=False)

    inserted = load_all_nba_votes(sqlite_con, tmp_path)

    rows = sqlite_con.execute(
        """
        SELECT player_id, season_id, team_type, team_number, position, pts_won, first_team_votes
        FROM fact_all_nba_vote
        ORDER BY player_id
        """
    ).fetchall()

    assert inserted == 2
    assert rows == [
        ("203999", "2023-24", "All-NBA", 1, "C", 495, 99),
        ("2544", "2023-24", "All-NBA", None, "F", 12, 0),
    ]
