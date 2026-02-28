"""Tests for full-history schema additions (coach and identity crosswalk tables)."""

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from src.db.schema import init_db


@pytest.fixture
def sqlite_con(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    """Create a temporary database for testing."""
    db_path = tmp_path / "test.db"
    con = init_db(db_path)
    yield con
    con.close()


def test_full_history_tables_exist(sqlite_con: sqlite3.Connection):
    """Verify that coach and identity crosswalk tables exist."""
    names = {r[0] for r in sqlite_con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "dim_coach" in names
    assert "fact_team_coach_game" in names
    assert "dim_player_identifier" in names
    assert "dim_team_identifier" in names


def test_dim_coach_schema(sqlite_con: sqlite3.Connection):
    """Verify dim_coach table has required columns."""
    cols = {row[1]: row[2] for row in sqlite_con.execute("PRAGMA table_info(dim_coach)").fetchall()}
    assert "coach_id" in cols
    assert "full_name" in cols
    assert "first_name" in cols
    assert "last_name" in cols
    assert "first_seen_season_id" in cols
    assert "last_seen_season_id" in cols


def test_dim_player_identifier_schema(sqlite_con: sqlite3.Connection):
    """Verify dim_player_identifier table has required columns."""
    cols = {
        row[1]: row[2]
        for row in sqlite_con.execute("PRAGMA table_info(dim_player_identifier)").fetchall()
    }
    assert "source_system" in cols
    assert "source_id" in cols
    assert "player_id" in cols
    assert "match_confidence" in cols


def test_dim_team_identifier_schema(sqlite_con: sqlite3.Connection):
    """Verify dim_team_identifier table has required columns."""
    cols = {
        row[1]: row[2]
        for row in sqlite_con.execute("PRAGMA table_info(dim_team_identifier)").fetchall()
    }
    assert "source_system" in cols
    assert "source_id" in cols
    assert "team_id" in cols


def test_fact_team_coach_game_schema(sqlite_con: sqlite3.Connection):
    """Verify fact_team_coach_game table has required columns."""
    cols = {
        row[1]: row[2]
        for row in sqlite_con.execute("PRAGMA table_info(fact_team_coach_game)").fetchall()
    }
    assert "game_id" in cols
    assert "team_id" in cols
    assert "coach_id" in cols
