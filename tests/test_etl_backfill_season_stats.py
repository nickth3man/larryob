"""Tests: raw backfill season-level stat loaders."""

import sqlite3
from pathlib import Path

import pandas as pd

from src.etl.backfill._season_stats import (
    load_league_season,
    load_player_season_stats,
    load_team_season,
)


def test_load_team_season_inserts_valid_rows(sqlite_con: sqlite3.Connection, tmp_path: Path) -> None:
    sqlite_con.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES ('2023-24', 2023, 2024)"
    )
    sqlite_con.commit()

    pd.DataFrame(
        [
            {
                "season": 2024,
                "abbreviation": "LAL",
                "lg": "NBA",
                "playoffs": "True",
                "w": 47,
                "l": 35,
                "pw": 45.5,
                "pl": 36.5,
                "mov": 2.1,
                "sos": 0.5,
                "srs": 2.6,
                "o_rtg": 116.0,
                "d_rtg": 113.0,
                "n_rtg": 3.0,
                "pace": 99.1,
                "ts_percent": 0.59,
                "e_fg_percent": 0.56,
                "tov_percent": 0.13,
                "orb_percent": 0.28,
                "ft_fga": 0.22,
                "opp_e_fg_percent": 0.54,
                "opp_tov_percent": 0.14,
                "drb_percent": 0.72,
                "opp_ft_fga": 0.20,
                "arena": "Crypto.com Arena",
                "attend": 770000,
                "attend_g": 18780,
            },
            {
                "season": 1990,
                "abbreviation": "BOS",
                "lg": "NBA",
                "playoffs": "False",
            },
        ]
    ).to_csv(tmp_path / "Team Summaries.csv", index=False)

    load_team_season(sqlite_con, tmp_path)

    row = sqlite_con.execute(
        "SELECT season_id, bref_abbrev, playoffs, o_rtg, d_rtg, arena FROM fact_team_season"
    ).fetchone()
    assert row == ("2023-24", "LAL", 1, 116.0, 113.0, "Crypto.com Arena")


def test_load_league_season_merges_per_game_aggregates(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    sqlite_con.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES ('2023-24', 2023, 2024)"
    )
    sqlite_con.commit()

    pd.DataFrame(
        [
            {"season": 2024, "team": "LAL", "pace": 100.0, "o_rtg": 115.0},
            {"season": 2024, "team": "GSW", "pace": 102.0, "o_rtg": 117.0},
        ]
    ).to_csv(tmp_path / "Team Summaries.csv", index=False)

    pd.DataFrame(
        [
            {
                "season": 2024,
                "pts_per_game": 114.0,
                "fga_per_game": 88.0,
                "fta_per_game": 22.0,
                "trb_per_game": 44.0,
                "ast_per_game": 27.0,
                "stl_per_game": 7.0,
                "blk_per_game": 5.0,
                "tov_per_game": 13.0,
            },
            {
                "season": 2024,
                "pts_per_game": 116.0,
                "fga_per_game": 90.0,
                "fta_per_game": 21.0,
                "trb_per_game": 43.0,
                "ast_per_game": 28.0,
                "stl_per_game": 8.0,
                "blk_per_game": 4.0,
                "tov_per_game": 12.0,
            },
        ]
    ).to_csv(tmp_path / "Team Stats Per Game.csv", index=False)

    load_league_season(sqlite_con, tmp_path)

    row = sqlite_con.execute(
        """SELECT season_id, num_teams, avg_pace, avg_ortg, avg_pts, avg_fga, avg_tov
           FROM dim_league_season WHERE season_id='2023-24'"""
    ).fetchone()
    assert row == ("2023-24", 2, 101.0, 116.0, 115.0, 89.0, 12.5)


def test_load_league_season_without_per_game_sets_null_averages(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    sqlite_con.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES ('2023-24', 2023, 2024)"
    )
    sqlite_con.commit()

    pd.DataFrame(
        [{"season": 2024, "team": "LAL", "pace": 99.0, "o_rtg": 114.0}]
    ).to_csv(tmp_path / "Team Summaries.csv", index=False)

    load_league_season(sqlite_con, tmp_path)

    row = sqlite_con.execute(
        "SELECT avg_pts, avg_fga, avg_fta FROM dim_league_season WHERE season_id='2023-24'"
    ).fetchone()
    assert row == (None, None, None)


def test_load_league_season_skips_rows_for_unseeded_seasons(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    pd.DataFrame(
        [{"season": 1990, "team": "LAL", "pace": 99.0, "o_rtg": 114.0}]
    ).to_csv(tmp_path / "Team Summaries.csv", index=False)

    load_league_season(sqlite_con, tmp_path)

    count = sqlite_con.execute("SELECT COUNT(*) FROM dim_league_season").fetchone()[0]
    assert count == 0


def test_load_player_season_stats_filters_invalid_rows(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    sqlite_con.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES ('2023-24', 2023, 2024)"
    )
    sqlite_con.commit()

    pd.DataFrame(
        [
            {
                "player_id": "jamesle01",
                "season": 2024,
                "lg": "NBA",
                "team": "LAL",
                "pos": "SF",
                "age": 39,
                "g": 70,
                "gs": 70,
                "mp": 2400,
                "fg": 700,
                "fga": 1300,
                "x3p": 150,
                "x3pa": 400,
                "ft": 300,
                "fta": 380,
                "orb": 80,
                "drb": 400,
                "trb": 480,
                "ast": 500,
                "stl": 90,
                "blk": 40,
                "tov": 200,
                "pf": 120,
                "pts": 1850,
            },
            {
                "player_id": "badrow01",
                "season": 2024,
                "team": "LAL",
                "fg": 10,
                "fga": 5,
                "x3p": 6,
                "x3pa": 4,
                "ft": 4,
                "fta": 3,
                "pts": -10,
            },
            {
                "player_id": "oldseason01",
                "season": 1990,
                "team": "LAL",
                "fg": 1,
                "fga": 2,
                "x3p": 0,
                "x3pa": 1,
                "ft": 0,
                "fta": 1,
                "pts": 2,
            },
        ]
    ).to_csv(tmp_path / "Player Totals.csv", index=False)

    load_player_season_stats(sqlite_con, tmp_path)

    count = sqlite_con.execute("SELECT COUNT(*) FROM fact_player_season_stats").fetchone()[0]
    assert count == 1
