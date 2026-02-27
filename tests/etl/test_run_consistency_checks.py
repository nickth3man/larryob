"""Tests for run_consistency_checks orchestration and logging."""

import sqlite3

import pytest

from src.db.operations import upsert_rows
from src.etl.validation import run_consistency_checks


def _seed_fact_game(con: sqlite3.Connection, game_id: str, team_id: str) -> None:
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


def _seed_team_player_logs(
    con: sqlite3.Connection,
    *,
    game_id: str,
    team_id: str,
    team_totals: tuple[int, int, int],
    player_totals: list[tuple[str, int, int, int]],
) -> None:
    team_pts, team_reb, team_ast = team_totals
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
    for idx, (player_id, pts, reb, ast) in enumerate(player_totals):
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


def test_run_consistency_checks_processes_all_games(
    sqlite_con_with_data: sqlite3.Connection,
) -> None:
    run_consistency_checks(sqlite_con_with_data, "2023-24")


def test_run_consistency_checks_with_no_games_in_season(sqlite_con: sqlite3.Connection) -> None:
    run_consistency_checks(sqlite_con, "1900-01")


def test_run_consistency_checks_with_pts_mismatches(
    sqlite_con_with_data: sqlite3.Connection,
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging

    _seed_fact_game(sqlite_con_with_data, "0022300101", "1610612744")
    _seed_team_player_logs(
        sqlite_con_with_data,
        game_id="0022300101",
        team_id="1610612744",
        team_totals=(110, 40, 25),
        player_totals=[("2544", 55, 20, 12), ("203999", 50, 20, 13)],
    )

    with caplog.at_level(logging.WARNING):
        total_warnings = run_consistency_checks(sqlite_con_with_data, "2023-24")

    assert total_warnings == 1
    assert any("PTS mismatch" in record.message for record in caplog.records)
    assert any("Consistency check found" in record.message for record in caplog.records)


def test_run_consistency_checks_with_reb_mismatches(
    sqlite_con_with_data: sqlite3.Connection,
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging

    _seed_fact_game(sqlite_con_with_data, "0022300102", "1610612744")
    _seed_team_player_logs(
        sqlite_con_with_data,
        game_id="0022300102",
        team_id="1610612744",
        team_totals=(110, 45, 25),
        player_totals=[("2544", 55, 20, 12), ("203999", 55, 22, 13)],
    )

    with caplog.at_level(logging.WARNING):
        total_warnings = run_consistency_checks(sqlite_con_with_data, "2023-24")

    assert total_warnings == 1
    assert any("REB mismatch" in record.message for record in caplog.records)
    assert any("Consistency check found" in record.message for record in caplog.records)


def test_run_consistency_checks_with_ast_mismatches(
    sqlite_con_with_data: sqlite3.Connection,
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging

    _seed_fact_game(sqlite_con_with_data, "0022300103", "1610612744")
    _seed_team_player_logs(
        sqlite_con_with_data,
        game_id="0022300103",
        team_id="1610612744",
        team_totals=(110, 45, 28),
        player_totals=[("2544", 55, 22, 12), ("203999", 55, 23, 13)],
    )

    with caplog.at_level(logging.WARNING):
        total_warnings = run_consistency_checks(sqlite_con_with_data, "2023-24")

    assert total_warnings == 1
    assert any("AST mismatch" in record.message for record in caplog.records)
    assert any("Consistency check found" in record.message for record in caplog.records)


def test_run_consistency_checks_with_multiple_discrepancies(
    sqlite_con_with_data: sqlite3.Connection,
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging

    _seed_fact_game(sqlite_con_with_data, "0022300104", "1610612747")
    _seed_team_player_logs(
        sqlite_con_with_data,
        game_id="0022300104",
        team_id="1610612747",
        team_totals=(110, 45, 28),
        player_totals=[("2544", 50, 20, 12), ("203999", 50, 20, 12)],
    )

    with caplog.at_level(logging.WARNING):
        total_warnings = run_consistency_checks(sqlite_con_with_data, "2023-24")

    assert total_warnings == 3
    summary_logs = [r for r in caplog.records if "Consistency check found" in r.message]
    assert len(summary_logs) == 1
    assert "3 discrepancies" in summary_logs[0].message


def test_run_consistency_checks_passes_with_valid_data(
    sqlite_con_with_data: sqlite3.Connection,
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging

    _seed_fact_game(sqlite_con_with_data, "0022300105", "1610612744")
    _seed_team_player_logs(
        sqlite_con_with_data,
        game_id="0022300105",
        team_id="1610612744",
        team_totals=(110, 45, 28),
        player_totals=[("2544", 55, 22, 14), ("203999", 55, 23, 14)],
    )

    with caplog.at_level(logging.INFO):
        total_warnings = run_consistency_checks(sqlite_con_with_data, "2023-24")

    assert total_warnings == 0
    assert any("Consistency check passed" in record.message for record in caplog.records)
