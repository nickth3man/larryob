"""Tests: raw backfill advanced-stat loaders."""

import sqlite3
from pathlib import Path

import pandas as pd

from src.etl.backfill._advanced_stats import (
    load_player_advanced,
    load_player_pbp_season,
    load_player_shooting,
)


def test_load_player_advanced_filters_invalid_percentage_rows(
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
                "team": "LAL",
                "pos": "SF",
                "age": 39,
                "g": 70,
                "gs": 70,
                "mp": 2400,
                "per": 24.0,
                "ts_percent": 0.61,
                "x3p_ar": 0.31,
                "f_tr": 0.28,
                "orb_percent": 0.04,
                "drb_percent": 0.20,
                "trb_percent": 0.12,
                "ast_percent": 0.33,
                "stl_percent": 0.02,
                "blk_percent": 0.01,
                "tov_percent": 0.12,
                "usg_percent": 0.30,
                "ows": 7.0,
                "dws": 3.0,
                "ws": 10.0,
                "ws_48": 0.20,
                "obpm": 5.0,
                "dbpm": 1.0,
                "bpm": 6.0,
                "vorp": 5.0,
            },
            {
                "player_id": "badrow01",
                "season": 2024,
                "team": "LAL",
                "ts_percent": 2.0,
                "orb_percent": 0.10,
                "drb_percent": 0.10,
                "usg_percent": 0.10,
            },
            {
                "player_id": "oldseason01",
                "season": 1990,
                "team": "LAL",
                "ts_percent": 0.50,
                "orb_percent": 0.10,
                "drb_percent": 0.10,
                "usg_percent": 0.10,
            },
        ]
    ).to_csv(tmp_path / "Advanced.csv", index=False)

    load_player_advanced(sqlite_con, tmp_path)

    count = sqlite_con.execute("SELECT COUNT(*) FROM fact_player_advanced_season").fetchone()[0]
    assert count == 1


def test_load_player_shooting_enforces_zone_distribution_sum(
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
                "team": "LAL",
                "g": 70,
                "mp": 2400,
                "avg_dist_fga": 14.1,
                "percent_fga_from_x2p_range": 0.65,
                "percent_fga_from_x0_3_range": 0.18,
                "percent_fga_from_x3_10_range": 0.12,
                "percent_fga_from_x10_16_range": 0.10,
                "percent_fga_from_x16_3p_range": 0.25,
                "percent_fga_from_x3p_range": 0.35,
                "fg_percent_from_x2p_range": 0.56,
                "fg_percent_from_x0_3_range": 0.73,
                "fg_percent_from_x3_10_range": 0.44,
                "fg_percent_from_x10_16_range": 0.42,
                "fg_percent_from_x16_3p_range": 0.40,
                "fg_percent_from_x3p_range": 0.38,
                "percent_assisted_x2p_fg": 0.32,
                "percent_assisted_x3p_fg": 0.78,
                "percent_dunks_of_fga": 0.04,
                "num_of_dunks": 30,
                "percent_corner_3s_of_3pa": 0.11,
                "corner_3_point_percent": 0.39,
            },
            {
                "player_id": "badzone01",
                "season": 2024,
                "team": "LAL",
                "percent_fga_from_x0_3_range": 0.10,
                "percent_fga_from_x3_10_range": 0.10,
                "percent_fga_from_x10_16_range": 0.10,
                "percent_fga_from_x16_3p_range": 0.10,
                "percent_fga_from_x3p_range": 0.10,
            },
            {
                "player_id": "oldseason01",
                "season": 1990,
                "team": "LAL",
                "percent_fga_from_x0_3_range": 0.20,
                "percent_fga_from_x3_10_range": 0.20,
                "percent_fga_from_x10_16_range": 0.20,
                "percent_fga_from_x16_3p_range": 0.20,
                "percent_fga_from_x3p_range": 0.20,
            },
        ]
    ).to_csv(tmp_path / "Player Shooting.csv", index=False)

    load_player_shooting(sqlite_con, tmp_path)

    count = sqlite_con.execute("SELECT COUNT(*) FROM fact_player_shooting_season").fetchone()[0]
    assert count == 1


def test_advanced_stats_percent_fields_accept_100_scale(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """100-scale percentage values (e.g. usg_pct=28.3) must be normalised to 0-1 scale on ingest."""
    sqlite_con.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES ('2023-24', 2023, 2024)"
    )
    sqlite_con.commit()

    pd.DataFrame(
        [
            {
                "player_id": "jokicni01",
                "season": 2024,
                "team": "DEN",
                "pos": "C",
                "age": 29,
                "g": 79,
                "gs": 79,
                "mp": 2800,
                "per": 31.2,
                "ts_percent": 0.64,
                "x3p_ar": 0.11,
                "f_tr": 0.40,
                # 100-scale percentage fields — currently rejected by FactPlayerAdvancedSeasonRow
                "orb_percent": 14.2,
                "drb_percent": 28.3,
                "trb_percent": 21.0,
                "ast_percent": 42.5,
                "stl_percent": 1.8,
                "blk_percent": 3.2,
                "tov_percent": 15.6,
                "usg_percent": 28.3,
                "ows": 9.0,
                "dws": 4.5,
                "ws": 13.5,
                "ws_48": 0.232,
                "obpm": 9.0,
                "dbpm": 2.0,
                "bpm": 11.0,
                "vorp": 9.5,
            }
        ]
    ).to_csv(tmp_path / "Advanced.csv", index=False)

    load_player_advanced(sqlite_con, tmp_path)

    inserted = sqlite_con.execute("SELECT COUNT(*) FROM fact_player_advanced_season").fetchone()[0]
    assert inserted == 1

    row = sqlite_con.execute(
        "SELECT usg_pct, orb_pct, drb_pct, trb_pct, ast_pct, stl_pct, blk_pct, tov_pct "
        "FROM fact_player_advanced_season WHERE bref_player_id = 'jokicni01'"
    ).fetchone()
    assert row is not None
    # All percentage values should be normalised to 0-1 scale
    assert abs(row[0] - 0.283) < 1e-6, f"usg_pct expected ~0.283, got {row[0]}"
    assert abs(row[1] - 0.142) < 1e-6, f"orb_pct expected ~0.142, got {row[1]}"
    assert abs(row[2] - 0.283) < 1e-6, f"drb_pct expected ~0.283, got {row[2]}"
    assert abs(row[3] - 0.210) < 1e-6, f"trb_pct expected ~0.210, got {row[3]}"
    assert abs(row[4] - 0.425) < 1e-6, f"ast_pct expected ~0.425, got {row[4]}"
    assert abs(row[5] - 0.018) < 1e-6, f"stl_pct expected ~0.018, got {row[5]}"
    assert abs(row[6] - 0.032) < 1e-6, f"blk_pct expected ~0.032, got {row[6]}"
    assert abs(row[7] - 0.156) < 1e-6, f"tov_pct expected ~0.156, got {row[7]}"


def test_load_player_pbp_season_inserts_valid_rows(
    sqlite_con: sqlite3.Connection, tmp_path: Path
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
                "team": "LAL",
                "g": 70,
                "mp": 2400,
                "pg_percent": 0.00,
                "sg_percent": 0.00,
                "sf_percent": 0.80,
                "pf_percent": 0.20,
                "c_percent": 0.00,
                "on_court_plus_minus_per_100_poss": 4.2,
                "net_plus_minus_per_100_poss": 3.8,
                "bad_pass_turnover": 30,
                "lost_ball_turnover": 25,
                "shooting_foul_committed": 40,
                "offensive_foul_committed": 10,
                "shooting_foul_drawn": 50,
                "offensive_foul_drawn": 5,
                "points_generated_by_assists": 520,
                "and1": 22,
                "fga_blocked": 35,
            },
            {
                "player_id": "oldseason01",
                "season": 1990,
                "team": "LAL",
                "g": 1,
                "mp": 10,
            },
        ]
    ).to_csv(tmp_path / "Player Play By Play.csv", index=False)

    load_player_pbp_season(sqlite_con, tmp_path)

    row = sqlite_con.execute(
        """SELECT bref_player_id, season_id, team_abbrev, g, mp, and1, fga_blocked
           FROM fact_player_pbp_season"""
    ).fetchone()
    assert row == ("jamesle01", "2023-24", "LAL", 70, 2400, 22, 35)
