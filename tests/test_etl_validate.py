import sqlite3

from src.etl.utils import load_cache, save_cache
from src.etl.validate import (
    check_game_stat_consistency,
    run_consistency_checks,
    validate_rows,
)


def test_validate_rows_player_game_log():
    rows = [
        # Valid
        {
            "game_id": "1",
            "player_id": "A",
            "team_id": "T1",
            "fgm": 5,
            "fga": 10,
            "pts": 12,
            "oreb": 1,
            "dreb": 2,
            "reb": 3,
        },
        # Invalid FGM > FGA
        {
            "game_id": "2",
            "player_id": "B",
            "team_id": "T2",
            "fgm": 10,
            "fga": 5,
            "pts": 20,
            "oreb": 0,
            "dreb": 0,
            "reb": 0,
        },
        # Invalid PTS < 0
        {
            "game_id": "3",
            "player_id": "C",
            "team_id": "T3",
            "fgm": 1,
            "fga": 2,
            "pts": -1,
            "oreb": 0,
            "dreb": 0,
            "reb": 0,
        },
        # Invalid REB sum
        {
            "game_id": "4",
            "player_id": "D",
            "team_id": "T4",
            "fgm": 1,
            "fga": 2,
            "pts": 2,
            "oreb": 1,
            "dreb": 1,
            "reb": 5,
        },
    ]

    valid = validate_rows("player_game_log", rows)
    assert len(valid) == 1
    assert valid[0]["player_id"] == "A"


def test_validate_rows_fact_game():
    rows = [
        # Valid
        {"game_id": "1", "home_score": 100, "away_score": 90, "game_date": "2024-10-22"},
        # Invalid score
        {"game_id": "2", "home_score": -5, "away_score": 90, "game_date": "2024-10-22"},
        # Invalid date format
        {"game_id": "3", "home_score": 100, "away_score": 90, "game_date": "2024/10/22"},
    ]

    valid = validate_rows("fact_game", rows)
    assert len(valid) == 1
    assert valid[0]["game_id"] == "1"


def test_validate_rows_shooting_zones():
    rows = [
        # Valid (sums to ~1.0)
        {
            "bref_player_id": "A",
            "pct_fga_0_3": 0.2,
            "pct_fga_3_10": 0.2,
            "pct_fga_10_16": 0.2,
            "pct_fga_16_3p": 0.2,
            "pct_fga_3p": 0.2,
        },
        # Invalid (sums to 0.5)
        {
            "bref_player_id": "B",
            "pct_fga_0_3": 0.1,
            "pct_fga_3_10": 0.1,
            "pct_fga_10_16": 0.1,
            "pct_fga_16_3p": 0.1,
            "pct_fga_3p": 0.1,
        },
        # Nulls (bypasses rule)
        {
            "bref_player_id": "C",
            "pct_fga_0_3": None,
            "pct_fga_3_10": None,
            "pct_fga_10_16": None,
            "pct_fga_16_3p": None,
            "pct_fga_3p": None,
        },
    ]

    valid = validate_rows("fact_player_shooting_season", rows)
    assert len(valid) == 2
    assert {r["bref_player_id"] for r in valid} == {"A", "C"}


def test_check_game_stat_consistency(sqlite_con_with_data: sqlite3.Connection):
    from src.etl.utils import upsert_rows

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


def test_cache_versioning_and_ttl(tmp_path, monkeypatch):
    # Point CACHE_DIR to temp path
    from src.etl import utils

    monkeypatch.setattr(utils, "CACHE_DIR", tmp_path)

    key = "test_key"
    data = {"hello": "world"}

    save_cache(key, data)

    # 1. Normal load works
    assert load_cache(key) == data

    # 2. Load with TTL works if fresh
    assert load_cache(key, ttl_days=1) == data

    # 3. Load with TTL fails if expired
    # Manipulate the saved timestamp to be 2 days old
    import json

    p = tmp_path / f"{key}.json"
    payload = json.loads(p.read_text())
    payload["ts"] -= 86400 * 2
    p.write_text(json.dumps(payload))

    assert load_cache(key, ttl_days=1) is None

    # 4. Version mismatch fails
    payload["ts"] += 86400 * 2  # restore time
    payload["v"] = utils.CACHE_VERSION - 1
    p.write_text(json.dumps(payload))

    assert load_cache(key) is None


# ------------------------------------------------------------------ #
# validate_rows: additional rule coverage                            #
# ------------------------------------------------------------------ #


def test_validate_rows_unknown_table_returns_all_rows():
    rows = [{"x": 1}, {"x": 2}]
    result = validate_rows("unknown_table_xyz", rows)
    assert result == rows


def test_validate_rows_team_game_log_filters_fg3m_violation():
    rows = [
        {"game_id": "1", "team_id": "A", "fg3m": 20, "fg3a": 10, "pts": 100},
    ]
    valid = validate_rows("team_game_log", rows)
    assert len(valid) == 0


def test_validate_rows_team_game_log_passes_all_none_fields():
    rows = [{"game_id": "1", "team_id": "A", "fg3m": None, "fg3a": None, "pts": None}]
    valid = validate_rows("team_game_log", rows)
    assert len(valid) == 1


def test_validate_rows_fact_salary_rejects_negative_salary():
    rows = [
        {"player_id": "A",
            "team_id": "T1", "team_id": "B", "season_id": "2023-24", "salary": -1},
    ]
    valid = validate_rows("fact_salary", rows)
    assert len(valid) == 0


def test_validate_rows_fact_salary_accepts_zero_salary():
    rows = [{"player_id": "A",
            "team_id": "T1", "team_id": "B", "season_id": "2023-24", "salary": 0}]
    valid = validate_rows("fact_salary", rows)
    assert len(valid) == 1


def test_validate_rows_fact_player_season_stats_fg_violation():
    rows = [{"bref_player_id": "A", "fg": 20, "fga": 10, "pts": 50}]
    valid = validate_rows("fact_player_season_stats", rows)
    assert len(valid) == 0


def test_validate_rows_fact_player_advanced_season_ts_pct_out_of_range():
    rows = [{"bref_player_id": "A", "ts_pct": 1.6}]
    valid = validate_rows("fact_player_advanced_season", rows)
    assert len(valid) == 0


def test_validate_rows_fact_player_advanced_season_valid():
    rows = [
        {"bref_player_id": "A", "ts_pct": 0.55, "orb_pct": 0.1, "drb_pct": 0.2, "usg_pct": 0.25}
    ]
    valid = validate_rows("fact_player_advanced_season", rows)
    assert len(valid) == 1


# ------------------------------------------------------------------ #
# check_game_stat_consistency: all stats match                       #
# ------------------------------------------------------------------ #


def test_check_game_stat_consistency_returns_no_warnings_when_stats_match(
    sqlite_con_with_data: sqlite3.Connection,
) -> None:
    from src.etl.utils import upsert_rows

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
