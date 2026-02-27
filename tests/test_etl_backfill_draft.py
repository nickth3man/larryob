"""Tests: raw backfill draft loader."""

import sqlite3
from pathlib import Path

import pandas as pd

from src.etl.backfill._draft import load_draft


def test_load_draft_inserts_rows_for_valid_seasons(
    sqlite_con: sqlite3.Connection, tmp_path: Path
) -> None:
    sqlite_con.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES ('2023-24', 2023, 2024)"
    )
    sqlite_con.commit()

    pd.DataFrame(
        [
            {
                "season": 2024,
                "round": 1,
                "overall_pick": 1,
                "tm": "ATL",
                "player_id": "risacza01",
                "player": "Zaccharie Risacher",
                "college": "JL Bourg",
                "lg": "NBA",
            },
            {
                "season": 1990,
                "round": 1,
                "overall_pick": 2,
                "tm": "BOS",
                "player_id": "oldplr01",
                "player": "Old Player",
                "college": "College",
                "lg": "NBA",
            },
        ]
    ).to_csv(tmp_path / "Draft Pick History.csv", index=False)

    load_draft(sqlite_con, tmp_path)

    row = sqlite_con.execute(
        """SELECT season_id, draft_round, overall_pick, bref_team_abbrev, bref_player_id
           FROM fact_draft"""
    ).fetchone()
    assert row == ("2023-24", 1, 1, "ATL", "risacza01")


def test_load_draft_skips_when_file_missing(sqlite_con: sqlite3.Connection, tmp_path: Path) -> None:
    load_draft(sqlite_con, tmp_path)
    count = sqlite_con.execute("SELECT COUNT(*) FROM fact_draft").fetchone()[0]
    assert count == 0
