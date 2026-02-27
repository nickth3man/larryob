"""Tests for check_game_stat_consistency."""

import sqlite3

from src.db.operations import upsert_rows
from src.etl.validation import check_game_stat_consistency


def _seed_game_stats(
    con: sqlite3.Connection,
    *,
    game_id: str,
    team_id: str,
    team_pts: int,
    team_reb: int,
    team_ast: int,
    player_rows: list[tuple[str, int, int, int]],
) -> None:
    """Insert fact_game, team_game_log, and player_game_log rows for one test game."""
    upsert_rows(
        con,
        "fact_game",
        [
            {
                "game_id": game_id,
                "season_id": "2023-24",
                "game_date": "2023-10-25",
                "home_team_id": team_id,
                "away_team_id": "1610612747" if team_id != "1610612747" else "1610612744",
                "home_score": 110,
                "away_score": 105,
                "season_type": "Regular Season",
                "status": "Final",
            }
        ],
        conflict="REPLACE",
    )

    upsert_rows(
        con,
        "team_game_log",
        [
            {
                "game_id": game_id,
                "team_id": team_id,
                "pts": team_pts,
                "reb": team_reb,
                "ast": team_ast,
            }
        ],
        conflict="REPLACE",
    )

    rows = []
    for idx, (player_id, pts, reb, ast) in enumerate(player_rows):
        rows.append(
            {
                "game_id": game_id,
                "player_id": player_id,
                "team_id": team_id,
                "pts": pts,
                "reb": reb,
                "ast": ast,
                "starter": 1 if idx == 0 else 0,
            }
        )
    upsert_rows(con, "player_game_log", rows, conflict="REPLACE")
    con.commit()


def test_check_game_stat_consistency_pts_mismatch(sqlite_con_with_data: sqlite3.Connection):
    _seed_game_stats(
        sqlite_con_with_data,
        game_id="0022300001",
        team_id="1610612747",
        team_pts=100,
        team_reb=40,
        team_ast=20,
        player_rows=[("2544", 50, 20, 10), ("203999", 40, 20, 10)],
    )

    warnings = check_game_stat_consistency(sqlite_con_with_data, "0022300001")
    assert len(warnings) == 1
    assert "PTS mismatch" in warnings[0]
    assert "Team=100" in warnings[0]
    assert "Players=90" in warnings[0]


def test_check_game_stat_consistency_returns_no_warnings_when_stats_match(
    sqlite_con_with_data: sqlite3.Connection,
) -> None:
    _seed_game_stats(
        sqlite_con_with_data,
        game_id="0022300001",
        team_id="1610612747",
        team_pts=90,
        team_reb=30,
        team_ast=20,
        player_rows=[("2544", 50, 15, 10), ("203999", 40, 15, 10)],
    )

    warnings = check_game_stat_consistency(sqlite_con_with_data, "0022300001")
    assert warnings == []


def test_check_game_stat_consistency_reb_mismatch(sqlite_con_with_data: sqlite3.Connection) -> None:
    _seed_game_stats(
        sqlite_con_with_data,
        game_id="0022300001",
        team_id="1610612747",
        team_pts=100,
        team_reb=40,
        team_ast=25,
        player_rows=[("2544", 50, 20, 12), ("203999", 50, 15, 13)],
    )

    warnings = check_game_stat_consistency(sqlite_con_with_data, "0022300001")
    assert len(warnings) == 1
    assert "REB mismatch" in warnings[0]
    assert "Team=40" in warnings[0]
    assert "Players=35" in warnings[0]


def test_check_game_stat_consistency_ast_mismatch(sqlite_con_with_data: sqlite3.Connection) -> None:
    _seed_game_stats(
        sqlite_con_with_data,
        game_id="0022300001",
        team_id="1610612747",
        team_pts=100,
        team_reb=40,
        team_ast=25,
        player_rows=[("2544", 50, 20, 10), ("203999", 50, 20, 10)],
    )

    warnings = check_game_stat_consistency(sqlite_con_with_data, "0022300001")
    assert len(warnings) == 1
    assert "AST mismatch" in warnings[0]
    assert "Team=25" in warnings[0]
    assert "Players=20" in warnings[0]


def test_check_game_stat_consistency_multiple_mismatches(
    sqlite_con_with_data: sqlite3.Connection,
) -> None:
    _seed_game_stats(
        sqlite_con_with_data,
        game_id="0022300001",
        team_id="1610612747",
        team_pts=100,
        team_reb=40,
        team_ast=25,
        player_rows=[("2544", 40, 15, 8), ("203999", 35, 12, 7)],
    )

    warnings = check_game_stat_consistency(sqlite_con_with_data, "0022300001")
    assert len(warnings) == 3
    warning_text = " ".join(warnings)
    assert "PTS mismatch" in warning_text
    assert "REB mismatch" in warning_text
    assert "AST mismatch" in warning_text
