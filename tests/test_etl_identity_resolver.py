"""Tests for src.etl.identity.resolver — no-drop identifier resolution."""

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from src.db.schema import init_db
from src.etl.identity.resolver import (
    resolve_or_create_player,
    resolve_or_create_team,
)


@pytest.fixture
def sqlite_con(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    """Create a temporary database for testing."""
    db_path = tmp_path / "test.db"
    con = init_db(db_path)
    yield con
    con.close()


def test_resolve_or_create_player_from_existing_identifier(sqlite_con: sqlite3.Connection):
    """Verify resolution returns existing player_id when identifier exists."""
    # Setup: Create a player and identifier
    sqlite_con.execute(
        "INSERT INTO dim_player (player_id, first_name, last_name, full_name, is_active) "
        "VALUES ('12345', 'LeBron', 'James', 'LeBron James', 1)"
    )
    sqlite_con.execute(
        "INSERT INTO dim_player_identifier (source_system, source_id, player_id) "
        "VALUES ('bref', 'jamesle01', '12345')"
    )
    sqlite_con.commit()

    player_id = resolve_or_create_player(sqlite_con, "bref", "jamesle01", "LeBron James")
    assert player_id == "12345"


def test_resolve_or_create_player_creates_placeholder(sqlite_con: sqlite3.Connection):
    """Verify a placeholder player is created when identifier doesn't exist."""
    player_id = resolve_or_create_player(
        sqlite_con, source_system="bref", source_id="ackerdo01", full_name="Don Ackerman"
    )
    assert player_id is not None

    # Verify placeholder was created in dim_player
    row = sqlite_con.execute(
        "SELECT full_name, is_active FROM dim_player WHERE player_id = ?", (player_id,)
    ).fetchone()
    assert row is not None
    assert row[0] == "Don Ackerman"
    assert row[1] == 0  # is_active should be 0 for historical players

    # Verify identifier mapping was created
    id_row = sqlite_con.execute(
        "SELECT player_id FROM dim_player_identifier WHERE source_system = ? AND source_id = ?",
        ("bref", "ackerdo01"),
    ).fetchone()
    assert id_row is not None
    assert id_row[0] == player_id


def test_resolve_or_create_player_is_idempotent(sqlite_con: sqlite3.Connection):
    """Verify calling twice with same params returns same player_id."""
    player_id_1 = resolve_or_create_player(
        sqlite_con, source_system="bref", source_id="testpl01", full_name="Test Player"
    )
    player_id_2 = resolve_or_create_player(
        sqlite_con, source_system="bref", source_id="testpl01", full_name="Test Player"
    )
    assert player_id_1 == player_id_2

    # Should only have one player in dim_player
    count = sqlite_con.execute(
        "SELECT COUNT(*) FROM dim_player WHERE full_name = 'Test Player'"
    ).fetchone()[0]
    assert count == 1


def test_resolve_or_create_team_from_existing_identifier(sqlite_con: sqlite3.Connection):
    """Verify resolution returns existing team_id when identifier exists."""
    # Setup: Create a team and identifier
    sqlite_con.execute(
        "INSERT INTO dim_team (team_id, abbreviation, full_name, city, nickname) "
        "VALUES ('1610612747', 'LAL', 'Los Angeles Lakers', 'Los Angeles', 'Lakers')"
    )
    sqlite_con.execute(
        "INSERT INTO dim_team_identifier (source_system, source_id, team_id) "
        "VALUES ('bref', 'LAL', '1610612747')"
    )
    sqlite_con.commit()

    team_id = resolve_or_create_team(sqlite_con, "bref", "LAL", "Los Angeles Lakers")
    assert team_id == "1610612747"


def test_resolve_or_create_team_creates_placeholder(sqlite_con: sqlite3.Connection):
    """Verify a placeholder team is created when identifier doesn't exist."""
    team_id = resolve_or_create_team(
        sqlite_con, source_system="bref", source_id="AND", full_name="Anderson Packers"
    )
    assert team_id is not None

    # Verify placeholder was created in dim_team
    row = sqlite_con.execute(
        "SELECT full_name FROM dim_team WHERE team_id = ?", (team_id,)
    ).fetchone()
    assert row is not None
    assert row[0] == "Anderson Packers"

    # Verify identifier mapping was created
    id_row = sqlite_con.execute(
        "SELECT team_id FROM dim_team_identifier WHERE source_system = ? AND source_id = ?",
        ("bref", "AND"),
    ).fetchone()
    assert id_row is not None
    assert id_row[0] == team_id
