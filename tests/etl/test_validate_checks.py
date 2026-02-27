"""Tests for consistency check functions."""

import sqlite3

import pytest

from src.etl.validation import check_game_stat_consistency, run_consistency_checks

# ------------------------------------------------------------------ #
# check_game_stat_consistency: basic tests                            #
# ------------------------------------------------------------------ #


def test_check_game_stat_consistency(sqlite_con_with_data: sqlite3.Connection):
    from src.db.operations.upsert import upsert_rows

    # Insert team game log with 100 pts
    upsert_rows(
        sqlite_con_with_data,
        "team_game_log",
        [
            {
                "game_id": "0022300001",
                "team_id": "1610612747",
                "pts": 100,
                "reb": 40,
                "ast": 20,
                "fgm": 0,
                "fga": 0,
                "fg3m": 0,
                "fg3a": 0,
                "ftm": 0,
                "fta": 0,
                "oreb": 0,
                "dreb": 0,
                "stl": 0,
                "blk": 0,
                "tov": 0,
                "pf": 0,
                "plus_minus": 0,
            }
        ],
    )

    # Insert player game logs summing to 90 pts (mismatch)
    upsert_rows(
        sqlite_con_with_data,
        "player_game_log",
        [
            {
                "game_id": "0022300001",
                "player_id": "2544",
                "team_id": "1610612747",
                "pts": 50,
                "reb": 20,
                "ast": 10,
                "minutes_played": 20,
                "fgm": 0,
                "fga": 0,
                "fg3m": 0,
                "fg3a": 0,
                "ftm": 0,
                "fta": 0,
                "oreb": 0,
                "dreb": 0,
                "stl": 0,
                "blk": 0,
                "tov": 0,
                "pf": 0,
                "plus_minus": 0,
                "starter": 1,
            },
            {
                "game_id": "0022300001",
                "player_id": "203999",
                "team_id": "1610612747",
                "pts": 40,
                "reb": 20,
                "ast": 10,
                "minutes_played": 20,
                "fgm": 0,
                "fga": 0,
                "fg3m": 0,
                "fg3a": 0,
                "ftm": 0,
                "fta": 0,
                "oreb": 0,
                "dreb": 0,
                "stl": 0,
                "blk": 0,
                "tov": 0,
                "pf": 0,
                "plus_minus": 0,
                "starter": 0,
            },
        ],
    )
    sqlite_con_with_data.commit()

    warnings = check_game_stat_consistency(sqlite_con_with_data, "0022300001")
    assert len(warnings) == 1
    assert "PTS mismatch" in warnings[0]
    assert "Team=100" in warnings[0]
    assert "Players=90" in warnings[0]


# ------------------------------------------------------------------ #
# check_game_stat_consistency: all stats match                        #
# ------------------------------------------------------------------ #


def test_check_game_stat_consistency_returns_no_warnings_when_stats_match(
    sqlite_con_with_data: sqlite3.Connection,
) -> None:
    from src.db.operations.upsert import upsert_rows

    upsert_rows(
        sqlite_con_with_data,
        "team_game_log",
        [
            {
                "game_id": "0022300001",
                "team_id": "1610612747",
                "pts": 90,
                "reb": 30,
                "ast": 20,
            }
        ],
    )
    upsert_rows(
        sqlite_con_with_data,
        "player_game_log",
        [
            {
                "game_id": "0022300001",
                "player_id": "2544",
                "team_id": "1610612747",
                "pts": 50,
                "reb": 15,
                "ast": 10,
            },
            {
                "game_id": "0022300001",
                "player_id": "203999",
                "team_id": "1610612747",
                "pts": 40,
                "reb": 15,
                "ast": 10,
            },
        ],
    )
    sqlite_con_with_data.commit()
    warnings = check_game_stat_consistency(sqlite_con_with_data, "0022300001")
    assert warnings == []


# ------------------------------------------------------------------ #
# check_game_stat_consistency: REB and AST mismatches                #
# ------------------------------------------------------------------ #


def test_check_game_stat_consistency_reb_mismatch(
    sqlite_con_with_data: sqlite3.Connection,
) -> None:
    """Test REB mismatch detection between player and team stats."""
    from src.db.operations.upsert import upsert_rows

    # Team game log with 40 rebounds
    upsert_rows(
        sqlite_con_with_data,
        "team_game_log",
        [
            {
                "game_id": "0022300001",
                "team_id": "1610612747",
                "pts": 100,
                "reb": 40,
                "ast": 25,
                "fgm": 0,
                "fga": 0,
                "fg3m": 0,
                "fg3a": 0,
                "ftm": 0,
                "fta": 0,
                "oreb": 0,
                "dreb": 0,
                "stl": 0,
                "blk": 0,
                "tov": 0,
                "pf": 0,
                "plus_minus": 0,
            }
        ],
    )

    # Player logs sum to 35 rebounds (mismatch)
    upsert_rows(
        sqlite_con_with_data,
        "player_game_log",
        [
            {
                "game_id": "0022300001",
                "player_id": "2544",
                "team_id": "1610612747",
                "pts": 50,
                "reb": 20,
                "ast": 12,
                "minutes_played": 20,
                "fgm": 0,
                "fga": 0,
                "fg3m": 0,
                "fg3a": 0,
                "ftm": 0,
                "fta": 0,
                "oreb": 0,
                "dreb": 0,
                "stl": 0,
                "blk": 0,
                "tov": 0,
                "pf": 0,
                "plus_minus": 0,
                "starter": 1,
            },
            {
                "game_id": "0022300001",
                "player_id": "203999",
                "team_id": "1610612747",
                "pts": 50,
                "reb": 15,
                "ast": 13,
                "minutes_played": 20,
                "fgm": 0,
                "fga": 0,
                "fg3m": 0,
                "fg3a": 0,
                "ftm": 0,
                "fta": 0,
                "oreb": 0,
                "dreb": 0,
                "stl": 0,
                "blk": 0,
                "tov": 0,
                "pf": 0,
                "plus_minus": 0,
                "starter": 0,
            },
        ],
    )
    sqlite_con_with_data.commit()

    warnings = check_game_stat_consistency(sqlite_con_with_data, "0022300001")
    assert len(warnings) == 1
    assert "REB mismatch" in warnings[0]
    assert "Team=40" in warnings[0]
    assert "Players=35" in warnings[0]


def test_check_game_stat_consistency_ast_mismatch(
    sqlite_con_with_data: sqlite3.Connection,
) -> None:
    """Test AST mismatch detection between player and team stats."""
    from src.db.operations.upsert import upsert_rows

    # Team game log with 25 assists
    upsert_rows(
        sqlite_con_with_data,
        "team_game_log",
        [
            {
                "game_id": "0022300001",
                "team_id": "1610612747",
                "pts": 100,
                "reb": 40,
                "ast": 25,
                "fgm": 0,
                "fga": 0,
                "fg3m": 0,
                "fg3a": 0,
                "ftm": 0,
                "fta": 0,
                "oreb": 0,
                "dreb": 0,
                "stl": 0,
                "blk": 0,
                "tov": 0,
                "pf": 0,
                "plus_minus": 0,
            }
        ],
    )

    # Player logs sum to 20 assists (mismatch)
    upsert_rows(
        sqlite_con_with_data,
        "player_game_log",
        [
            {
                "game_id": "0022300001",
                "player_id": "2544",
                "team_id": "1610612747",
                "pts": 50,
                "reb": 20,
                "ast": 10,
                "minutes_played": 20,
                "fgm": 0,
                "fga": 0,
                "fg3m": 0,
                "fg3a": 0,
                "ftm": 0,
                "fta": 0,
                "oreb": 0,
                "dreb": 0,
                "stl": 0,
                "blk": 0,
                "tov": 0,
                "pf": 0,
                "plus_minus": 0,
                "starter": 1,
            },
            {
                "game_id": "0022300001",
                "player_id": "203999",
                "team_id": "1610612747",
                "pts": 50,
                "reb": 20,
                "ast": 10,
                "minutes_played": 20,
                "fgm": 0,
                "fga": 0,
                "fg3m": 0,
                "fg3a": 0,
                "ftm": 0,
                "fta": 0,
                "oreb": 0,
                "dreb": 0,
                "stl": 0,
                "blk": 0,
                "tov": 0,
                "pf": 0,
                "plus_minus": 0,
                "starter": 0,
            },
        ],
    )
    sqlite_con_with_data.commit()

    warnings = check_game_stat_consistency(sqlite_con_with_data, "0022300001")
    assert len(warnings) == 1
    assert "AST mismatch" in warnings[0]
    assert "Team=25" in warnings[0]
    assert "Players=20" in warnings[0]


def test_check_game_stat_consistency_multiple_mismatches(
    sqlite_con_with_data: sqlite3.Connection,
) -> None:
    """Test detection of multiple stat mismatches (PTS, REB, AST)."""
    from src.db.operations.upsert import upsert_rows

    # Team game log
    upsert_rows(
        sqlite_con_with_data,
        "team_game_log",
        [
            {
                "game_id": "0022300001",
                "team_id": "1610612747",
                "pts": 100,
                "reb": 40,
                "ast": 25,
                "fgm": 0,
                "fga": 0,
                "fg3m": 0,
                "fg3a": 0,
                "ftm": 0,
                "fta": 0,
                "oreb": 0,
                "dreb": 0,
                "stl": 0,
                "blk": 0,
                "tov": 0,
                "pf": 0,
                "plus_minus": 0,
            }
        ],
    )

    # Player logs with mismatches in all three stats
    upsert_rows(
        sqlite_con_with_data,
        "player_game_log",
        [
            {
                "game_id": "0022300001",
                "player_id": "2544",
                "team_id": "1610612747",
                "pts": 40,
                "reb": 15,
                "ast": 8,
                "minutes_played": 20,
                "fgm": 0,
                "fga": 0,
                "fg3m": 0,
                "fg3a": 0,
                "ftm": 0,
                "fta": 0,
                "oreb": 0,
                "dreb": 0,
                "stl": 0,
                "blk": 0,
                "tov": 0,
                "pf": 0,
                "plus_minus": 0,
                "starter": 1,
            },
            {
                "game_id": "0022300001",
                "player_id": "203999",
                "team_id": "1610612747",
                "pts": 35,
                "reb": 12,
                "ast": 7,
                "minutes_played": 20,
                "fgm": 0,
                "fga": 0,
                "fg3m": 0,
                "fg3a": 0,
                "ftm": 0,
                "fta": 0,
                "oreb": 0,
                "dreb": 0,
                "stl": 0,
                "blk": 0,
                "tov": 0,
                "pf": 0,
                "plus_minus": 0,
                "starter": 0,
            },
        ],
    )
    sqlite_con_with_data.commit()

    warnings = check_game_stat_consistency(sqlite_con_with_data, "0022300001")
    assert len(warnings) == 3

    # Check all three mismatch types are present
    warning_texts = " ".join(warnings)
    assert "PTS mismatch" in warning_texts
    assert "REB mismatch" in warning_texts
    assert "AST mismatch" in warning_texts


# ------------------------------------------------------------------ #
# run_consistency_checks                                              #
# ------------------------------------------------------------------ #


def test_run_consistency_checks_processes_all_games(
    sqlite_con_with_data: sqlite3.Connection,
) -> None:
    """run_consistency_checks should complete without raising."""
    run_consistency_checks(sqlite_con_with_data, "2023-24")


def test_run_consistency_checks_with_no_games_in_season(
    sqlite_con: sqlite3.Connection,
) -> None:
    """A season with no fact_game rows should not raise."""
    run_consistency_checks(sqlite_con, "1900-01")


# ------------------------------------------------------------------ #
# run_consistency_checks: comprehensive failure path tests          #
# ------------------------------------------------------------------ #


def test_run_consistency_checks_with_pts_mismatches(
    sqlite_con_with_data: sqlite3.Connection,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test run_consistency_checks logs warnings for PTS mismatches."""
    import logging

    from src.db.operations.upsert import upsert_rows

    # Add second game to season
    upsert_rows(
        sqlite_con_with_data,
        "fact_game",
        [
            {
                "game_id": "0022300002",
                "season_id": "2023-24",
                "game_date": "2023-10-25",
                "home_team_id": "1610612744",
                "away_team_id": "1610612747",
                "home_score": 110,
                "away_score": 105,
                "season_type": "Regular Season",
                "status": "Final",
            }
        ],
    )

    # Team game log with mismatched PTS
    upsert_rows(
        sqlite_con_with_data,
        "team_game_log",
        [
            {
                "game_id": "0022300002",
                "team_id": "1610612744",
                "pts": 110,
                "reb": 40,
                "ast": 25,
                "fgm": 0,
                "fga": 0,
                "fg3m": 0,
                "fg3a": 0,
                "ftm": 0,
                "fta": 0,
                "oreb": 0,
                "dreb": 0,
                "stl": 0,
                "blk": 0,
                "tov": 0,
                "pf": 0,
                "plus_minus": 0,
            }
        ],
    )

    # Player logs with PTS mismatch
    upsert_rows(
        sqlite_con_with_data,
        "player_game_log",
        [
            {
                "game_id": "0022300002",
                "player_id": "2544",
                "team_id": "1610612744",
                "pts": 55,
                "reb": 20,
                "ast": 12,
                "minutes_played": 20,
                "fgm": 0,
                "fga": 0,
                "fg3m": 0,
                "fg3a": 0,
                "ftm": 0,
                "fta": 0,
                "oreb": 0,
                "dreb": 0,
                "stl": 0,
                "blk": 0,
                "tov": 0,
                "pf": 0,
                "plus_minus": 0,
                "starter": 1,
            },
            {
                "game_id": "0022300002",
                "player_id": "203999",
                "team_id": "1610612744",
                "pts": 50,
                "reb": 20,
                "ast": 13,
                "minutes_played": 20,
                "fgm": 0,
                "fga": 0,
                "fg3m": 0,
                "fg3a": 0,
                "ftm": 0,
                "fta": 0,
                "oreb": 0,
                "dreb": 0,
                "stl": 0,
                "blk": 0,
                "tov": 0,
                "pf": 0,
                "plus_minus": 0,
                "starter": 0,
            },
        ],
    )
    sqlite_con_with_data.commit()

    with caplog.at_level(logging.WARNING):
        total_warnings = run_consistency_checks(sqlite_con_with_data, "2023-24")

    assert total_warnings == 1
    assert any("PTS mismatch" in record.message for record in caplog.records)
    assert any("Consistency check found" in record.message for record in caplog.records)


def test_run_consistency_checks_with_reb_mismatches(
    sqlite_con_with_data: sqlite3.Connection,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test run_consistency_checks logs warnings for REB mismatches."""
    import logging

    from src.db.operations.upsert import upsert_rows

    # Add second game to season
    upsert_rows(
        sqlite_con_with_data,
        "fact_game",
        [
            {
                "game_id": "0022300002",
                "season_id": "2023-24",
                "game_date": "2023-10-25",
                "home_team_id": "1610612744",
                "away_team_id": "1610612747",
                "home_score": 110,
                "away_score": 105,
                "season_type": "Regular Season",
                "status": "Final",
            }
        ],
    )

    # Team game log with mismatched REB
    upsert_rows(
        sqlite_con_with_data,
        "team_game_log",
        [
            {
                "game_id": "0022300002",
                "team_id": "1610612744",
                "pts": 110,
                "reb": 45,
                "ast": 25,
                "fgm": 0,
                "fga": 0,
                "fg3m": 0,
                "fg3a": 0,
                "ftm": 0,
                "fta": 0,
                "oreb": 0,
                "dreb": 0,
                "stl": 0,
                "blk": 0,
                "tov": 0,
                "pf": 0,
                "plus_minus": 0,
            }
        ],
    )

    # Player logs with REB mismatch
    upsert_rows(
        sqlite_con_with_data,
        "player_game_log",
        [
            {
                "game_id": "0022300002",
                "player_id": "2544",
                "team_id": "1610612744",
                "pts": 55,
                "reb": 20,
                "ast": 12,
                "minutes_played": 20,
                "fgm": 0,
                "fga": 0,
                "fg3m": 0,
                "fg3a": 0,
                "ftm": 0,
                "fta": 0,
                "oreb": 0,
                "dreb": 0,
                "stl": 0,
                "blk": 0,
                "tov": 0,
                "pf": 0,
                "plus_minus": 0,
                "starter": 1,
            },
            {
                "game_id": "0022300002",
                "player_id": "203999",
                "team_id": "1610612744",
                "pts": 55,
                "reb": 22,
                "ast": 13,
                "minutes_played": 20,
                "fgm": 0,
                "fga": 0,
                "fg3m": 0,
                "fg3a": 0,
                "ftm": 0,
                "fta": 0,
                "oreb": 0,
                "dreb": 0,
                "stl": 0,
                "blk": 0,
                "tov": 0,
                "pf": 0,
                "plus_minus": 0,
                "starter": 0,
            },
        ],
    )
    sqlite_con_with_data.commit()

    with caplog.at_level(logging.WARNING):
        total_warnings = run_consistency_checks(sqlite_con_with_data, "2023-24")

    assert total_warnings == 1
    assert any("REB mismatch" in record.message for record in caplog.records)
    assert any("Consistency check found" in record.message for record in caplog.records)


def test_run_consistency_checks_with_ast_mismatches(
    sqlite_con_with_data: sqlite3.Connection,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test run_consistency_checks logs warnings for AST mismatches."""
    import logging

    from src.db.operations.upsert import upsert_rows

    # Add second game to season
    upsert_rows(
        sqlite_con_with_data,
        "fact_game",
        [
            {
                "game_id": "0022300002",
                "season_id": "2023-24",
                "game_date": "2023-10-25",
                "home_team_id": "1610612744",
                "away_team_id": "1610612747",
                "home_score": 110,
                "away_score": 105,
                "season_type": "Regular Season",
                "status": "Final",
            }
        ],
    )

    # Team game log with mismatched AST
    upsert_rows(
        sqlite_con_with_data,
        "team_game_log",
        [
            {
                "game_id": "0022300002",
                "team_id": "1610612744",
                "pts": 110,
                "reb": 45,
                "ast": 28,
                "fgm": 0,
                "fga": 0,
                "fg3m": 0,
                "fg3a": 0,
                "ftm": 0,
                "fta": 0,
                "oreb": 0,
                "dreb": 0,
                "stl": 0,
                "blk": 0,
                "tov": 0,
                "pf": 0,
                "plus_minus": 0,
            }
        ],
    )

    # Player logs with AST mismatch
    upsert_rows(
        sqlite_con_with_data,
        "player_game_log",
        [
            {
                "game_id": "0022300002",
                "player_id": "2544",
                "team_id": "1610612744",
                "pts": 55,
                "reb": 22,
                "ast": 12,
                "minutes_played": 20,
                "fgm": 0,
                "fga": 0,
                "fg3m": 0,
                "fg3a": 0,
                "ftm": 0,
                "fta": 0,
                "oreb": 0,
                "dreb": 0,
                "stl": 0,
                "blk": 0,
                "tov": 0,
                "pf": 0,
                "plus_minus": 0,
                "starter": 1,
            },
            {
                "game_id": "0022300002",
                "player_id": "203999",
                "team_id": "1610612744",
                "pts": 55,
                "reb": 23,
                "ast": 13,
                "minutes_played": 20,
                "fgm": 0,
                "fga": 0,
                "fg3m": 0,
                "fg3a": 0,
                "ftm": 0,
                "fta": 0,
                "oreb": 0,
                "dreb": 0,
                "stl": 0,
                "blk": 0,
                "tov": 0,
                "pf": 0,
                "plus_minus": 0,
                "starter": 0,
            },
        ],
    )
    sqlite_con_with_data.commit()

    with caplog.at_level(logging.WARNING):
        total_warnings = run_consistency_checks(sqlite_con_with_data, "2023-24")

    assert total_warnings == 1
    assert any("AST mismatch" in record.message for record in caplog.records)
    assert any("Consistency check found" in record.message for record in caplog.records)


def test_run_consistency_checks_with_multiple_discrepancies(
    sqlite_con_with_data: sqlite3.Connection,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test run_consistency_checks accumulates warnings across multiple games."""
    import logging

    from src.db.operations.upsert import upsert_rows

    # Add a game with all three types of mismatches (PTS, REB, AST)
    upsert_rows(
        sqlite_con_with_data,
        "fact_game",
        [
            {
                "game_id": "0022300002",
                "season_id": "2023-24",
                "game_date": "2023-10-25",
                "home_team_id": "1610612747",
                "away_team_id": "1610612744",
                "home_score": 110,
                "away_score": 105,
                "season_type": "Regular Season",
                "status": "Final",
            }
        ],
    )

    # Team game log
    upsert_rows(
        sqlite_con_with_data,
        "team_game_log",
        [
            {
                "game_id": "0022300002",
                "team_id": "1610612747",
                "pts": 110,
                "reb": 45,
                "ast": 28,
                "fgm": 0,
                "fga": 0,
                "fg3m": 0,
                "fg3a": 0,
                "ftm": 0,
                "fta": 0,
                "oreb": 0,
                "dreb": 0,
                "stl": 0,
                "blk": 0,
                "tov": 0,
                "pf": 0,
                "plus_minus": 0,
            }
        ],
    )

    # Player logs with all three mismatches
    upsert_rows(
        sqlite_con_with_data,
        "player_game_log",
        [
            {
                "game_id": "0022300002",
                "player_id": "2544",
                "team_id": "1610612747",
                "pts": 50,
                "reb": 20,
                "ast": 12,
                "minutes_played": 20,
                "fgm": 0,
                "fga": 0,
                "fg3m": 0,
                "fg3a": 0,
                "ftm": 0,
                "fta": 0,
                "oreb": 0,
                "dreb": 0,
                "stl": 0,
                "blk": 0,
                "tov": 0,
                "pf": 0,
                "plus_minus": 0,
                "starter": 1,
            },
            {
                "game_id": "0022300002",
                "player_id": "203999",
                "team_id": "1610612747",
                "pts": 50,
                "reb": 20,
                "ast": 12,
                "minutes_played": 20,
                "fgm": 0,
                "fga": 0,
                "fg3m": 0,
                "fg3a": 0,
                "ftm": 0,
                "fta": 0,
                "oreb": 0,
                "dreb": 0,
                "stl": 0,
                "blk": 0,
                "tov": 0,
                "pf": 0,
                "plus_minus": 0,
                "starter": 0,
            },
        ],
    )

    sqlite_con_with_data.commit()

    with caplog.at_level(logging.WARNING):
        total_warnings = run_consistency_checks(sqlite_con_with_data, "2023-24")

    # 3 mismatches: PTS, REB, and AST
    assert total_warnings == 3

    # Verify the summary log contains the count
    summary_logs = [r for r in caplog.records if "Consistency check found" in r.message]
    assert len(summary_logs) == 1
    assert "3 discrepancies" in summary_logs[0].message


def test_run_consistency_checks_passes_with_valid_data(
    sqlite_con_with_data: sqlite3.Connection,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test run_consistency_checks logs info message when all stats match."""
    import logging

    from src.db.operations.upsert import upsert_rows

    # Add a second game with matching stats
    upsert_rows(
        sqlite_con_with_data,
        "fact_game",
        [
            {
                "game_id": "0022300002",
                "season_id": "2023-24",
                "game_date": "2023-10-25",
                "home_team_id": "1610612744",
                "away_team_id": "1610612747",
                "home_score": 110,
                "away_score": 105,
                "season_type": "Regular Season",
                "status": "Final",
            }
        ],
    )

    upsert_rows(
        sqlite_con_with_data,
        "team_game_log",
        [
            {
                "game_id": "0022300002",
                "team_id": "1610612744",
                "pts": 110,
                "reb": 45,
                "ast": 28,
                "fgm": 0,
                "fga": 0,
                "fg3m": 0,
                "fg3a": 0,
                "ftm": 0,
                "fta": 0,
                "oreb": 0,
                "dreb": 0,
                "stl": 0,
                "blk": 0,
                "tov": 0,
                "pf": 0,
                "plus_minus": 0,
            }
        ],
    )

    # Player logs with matching totals
    upsert_rows(
        sqlite_con_with_data,
        "player_game_log",
        [
            {
                "game_id": "0022300002",
                "player_id": "2544",
                "team_id": "1610612744",
                "pts": 55,
                "reb": 22,
                "ast": 14,
                "minutes_played": 20,
                "fgm": 0,
                "fga": 0,
                "fg3m": 0,
                "fg3a": 0,
                "ftm": 0,
                "fta": 0,
                "oreb": 0,
                "dreb": 0,
                "stl": 0,
                "blk": 0,
                "tov": 0,
                "pf": 0,
                "plus_minus": 0,
                "starter": 1,
            },
            {
                "game_id": "0022300002",
                "player_id": "203999",
                "team_id": "1610612744",
                "pts": 55,
                "reb": 23,
                "ast": 14,
                "minutes_played": 20,
                "fgm": 0,
                "fga": 0,
                "fg3m": 0,
                "fg3a": 0,
                "ftm": 0,
                "fta": 0,
                "oreb": 0,
                "dreb": 0,
                "stl": 0,
                "blk": 0,
                "tov": 0,
                "pf": 0,
                "plus_minus": 0,
                "starter": 0,
            },
        ],
    )
    sqlite_con_with_data.commit()

    with caplog.at_level(logging.INFO):
        total_warnings = run_consistency_checks(sqlite_con_with_data, "2023-24")

    assert total_warnings == 0
    assert any("Consistency check passed" in record.message for record in caplog.records)
