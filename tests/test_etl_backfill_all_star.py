"""Tests: raw backfill All-Star selections loader."""

import sqlite3
from pathlib import Path

import pandas as pd

from src.etl.backfill._all_star import load_all_star_selections


def _seed_all_star_context(con: sqlite3.Connection) -> None:
    con.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES ('2023-24', 2023, 2024)"
    )
    con.execute(
        """INSERT INTO dim_team
           (team_id, abbreviation, full_name, city, nickname, bref_abbrev)
           VALUES ('1610612743', 'DEN', 'Denver Nuggets', 'Denver', 'Nuggets', 'DEN')"""
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


def test_load_all_star_selections_inserts_rows_and_maps_team_when_possible(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    _seed_all_star_context(sqlite_con)

    pd.DataFrame(
        [
            {
                "player": "Nikola Jokic",
                "player_id": "jokicni01",
                "team": "Denver",
                "season": 2024,
                "lg": "NBA",
                "replaced": "FALSE",
            },
            {
                "player": "LeBron James",
                "player_id": "jamesle01",
                "team": "East",
                "season": 2024,
                "lg": "NBA",
                "replaced": "TRUE",
            },
        ]
    ).to_csv(tmp_path / "All-Star Selections.csv", index=False)

    inserted = load_all_star_selections(sqlite_con, tmp_path)

    rows = sqlite_con.execute(
        """
        SELECT player_id, season_id, team_id, selection_team, is_starter, is_replacement
        FROM fact_all_star
        ORDER BY player_id
        """
    ).fetchall()

    assert inserted == 2
    assert rows == [
        ("203999", "2023-24", "1610612743", "Denver", None, 0),
        ("2544", "2023-24", None, "East", None, 1),
    ]


def test_load_all_star_selections_skips_invalid_seasons_creates_placeholder_for_unknown_player(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    _seed_all_star_context(sqlite_con)

    pd.DataFrame(
        [
            {
                "player": "Nikola Jokic",
                "player_id": "jokicni01",
                "team": "Denver",
                "season": 1990,
                "lg": "NBA",
                "replaced": "FALSE",
            },
            {
                "player": "Unknown Player",
                "player_id": "unknown01",
                "team": "East",
                "season": 2024,
                "lg": "NBA",
                "replaced": "FALSE",
            },
        ]
    ).to_csv(tmp_path / "All-Star Selections.csv", index=False)

    inserted = load_all_star_selections(sqlite_con, tmp_path)
    count = sqlite_con.execute("SELECT COUNT(*) FROM fact_all_star").fetchone()[0]

    # Invalid season is still skipped; unknown player gets a placeholder and is inserted.
    assert inserted == 1
    assert count == 1

    placeholder_pid = sqlite_con.execute(
        "SELECT player_id FROM dim_player WHERE player_id = 'placeholder_bref_unknown01'"
    ).fetchone()
    assert placeholder_pid is not None
