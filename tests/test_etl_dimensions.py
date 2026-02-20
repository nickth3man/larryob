"""Tests: ETL dimension loaders (no network calls)."""

import sqlite3

import pytest

from src.etl.dimensions import (
    _height_to_cm,
    _map_common_player_info,
    _map_nba_player_static,
    _map_nba_team,
    _normalize_position,
    _parse_birth_date,
    _weight_to_kg,
    load_players_static,
    load_seasons,
    load_teams,
)


def test_load_seasons(sqlite_con: sqlite3.Connection) -> None:
    load_seasons(sqlite_con, up_to_start_year=1950)
    count = sqlite_con.execute("SELECT COUNT(*) FROM dim_season").fetchone()[0]
    assert count == 5, f"Expected 5 seasons (1946-50), got {count}"


def test_load_seasons_idempotent(sqlite_con: sqlite3.Connection) -> None:
    load_seasons(sqlite_con, up_to_start_year=1950)
    load_seasons(sqlite_con, up_to_start_year=1950)  # second call
    count = sqlite_con.execute("SELECT COUNT(*) FROM dim_season").fetchone()[0]
    assert count == 5, "Second call should not duplicate rows"


def test_season_id_format(sqlite_con: sqlite3.Connection) -> None:
    load_seasons(sqlite_con, up_to_start_year=1946)
    row = sqlite_con.execute("SELECT season_id FROM dim_season").fetchone()
    assert row[0] == "1946-47"


def test_load_teams(sqlite_con: sqlite3.Connection) -> None:
    load_seasons(sqlite_con)    # teams need no FK, but seasons must exist
    load_teams(sqlite_con)
    count = sqlite_con.execute("SELECT COUNT(*) FROM dim_team").fetchone()[0]
    assert count >= 30, f"Expected at least 30 teams, got {count}"


def test_load_players_static(sqlite_con: sqlite3.Connection) -> None:
    load_seasons(sqlite_con)
    load_teams(sqlite_con)
    load_players_static(sqlite_con)
    count = sqlite_con.execute("SELECT COUNT(*) FROM dim_player").fetchone()[0]
    assert count > 1000, f"Expected 1000+ historical players, got {count}"


def test_map_nba_team_fields() -> None:
    raw = {
        "id": 1610612747,
        "abbreviation": "LAL",
        "full_name": "Los Angeles Lakers",
        "city": "Los Angeles",
        "nickname": "Lakers",
    }
    row = _map_nba_team(raw)
    assert row["team_id"] == "1610612747"
    assert row["abbreviation"] == "LAL"
    assert row["conference"] == "West"
    assert row["division"] == "Pacific"
    assert row["color_primary"] == "#552582"
    assert row["arena_name"] == "Crypto.com Arena"


def test_height_to_cm_valid_and_edge_cases() -> None:
    assert _height_to_cm("6-8") == pytest.approx(203.2, rel=1e-3)
    assert _height_to_cm(" 7-00 ") == pytest.approx(213.4, rel=1e-3)


def test_height_to_cm_invalid_and_none() -> None:
    assert _height_to_cm("abc") is None
    assert _height_to_cm("6-x") is None
    assert _height_to_cm(None) is None


def test_weight_to_kg_from_string_and_int() -> None:
    assert _weight_to_kg("220") == pytest.approx(99.8, rel=1e-3)
    assert _weight_to_kg(220) == pytest.approx(99.8, rel=1e-3)


def test_weight_to_kg_invalid_values() -> None:
    assert _weight_to_kg("xyz") is None
    assert _weight_to_kg(None) is None


def test_parse_birth_date() -> None:
    assert _parse_birth_date("1995-06-15T00:00:00") == "1995-06-15"
    assert _parse_birth_date("1988-01-01") == "1988-01-01"
    assert _parse_birth_date("short") is None  # len < 10
    assert _parse_birth_date(None) is None


def test_normalize_position_known_mappings() -> None:
    assert _normalize_position("PG") == "PG"
    assert _normalize_position("Guard") == "G"
    assert _normalize_position("CENTER") == "C"
    assert _normalize_position("G-F") == "G-F"


def test_normalize_position_unexpected_returns_none() -> None:
    assert _normalize_position("Two-Way Combo") is None
    assert _normalize_position(None) is None


def test_map_common_player_info_undrafted_and_active() -> None:
    raw = {
        "PERSON_ID": "201",
        "DISPLAY_FIRST_LAST": "Test Player",
        "TEAM_ID": "1610612747",
        "ROSTERSTATUS": "Active",
        "POSITION": "PG",
        "HEIGHT": "6-8",
        "WEIGHT": "220",
        "BIRTHDATE": "1995-06-15T00:00:00",
        "DRAFT_YEAR": "Undrafted",
        "DRAFT_ROUND": "0",
        "DRAFT_NUMBER": "0",
    }
    row = _map_common_player_info(raw)
    assert row["draft_year"] is None
    assert row["draft_round"] is None
    assert row["draft_number"] is None
    assert row["is_active"] == 1
    assert row["height_cm"] == pytest.approx(203.2, rel=1e-3)
    assert row["weight_kg"] == pytest.approx(99.8, rel=1e-3)
    assert row["birth_date"] == "1995-06-15"
    assert row["position"] == "PG"


def test_map_common_player_info_inactive_and_empty_draft() -> None:
    raw = {
        "PERSON_ID": "202",
        "DISPLAY_FIRST_LAST": "Other Player",
        "TEAM_ID": "1610612747",
        "ROSTERSTATUS": "Inactive",
        "POSITION": "CENTER",
        "HEIGHT": "7-00",
        "WEIGHT": 250,
        "BIRTHDATE": "1988-01-01",
        "DRAFT_YEAR": "",
        "DRAFT_ROUND": "",
        "DRAFT_NUMBER": "",
    }
    row = _map_common_player_info(raw)
    assert row["draft_year"] is None
    assert row["draft_round"] is None
    assert row["draft_number"] is None
    assert row["is_active"] == 0
    assert row["position"] == "C"


def test_map_common_player_info_missing_keys_and_short_birth_date() -> None:
    raw = {
        "PERSON_ID": "203",
        "DISPLAY_FIRST_LAST": "Partial Player",
        "TEAM_ID": "1610612747",
        "ROSTERSTATUS": "Active",
        "POSITION": "Unknown",
        "BIRTHDATE": "1995",  # too short, _parse_birth_date returns None
    }
    row = _map_common_player_info(raw)
    assert isinstance(row, dict)
    assert row.get("birth_date") is None
    assert row["position"] is None


def test_map_nba_player_static_active() -> None:
    raw = {"id": 2544, "full_name": "LeBron James", "is_active": True}
    row = _map_nba_player_static(raw)
    assert row["player_id"] == "2544"
    assert row["first_name"] == "LeBron"
    assert row["last_name"] == "James"
    assert row["is_active"] == 1


def test_map_nba_player_static_inactive() -> None:
    raw = {"id": 76375, "full_name": "Kareem Abdul-Jabbar", "is_active": False}
    row = _map_nba_player_static(raw)
    assert row["is_active"] == 0
