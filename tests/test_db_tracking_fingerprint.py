"""Tests for src.db.tracking.fingerprint — source fingerprint tracking."""

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from src.db.schema import init_db
from src.db.tracking.fingerprint import (
    get_source_fingerprint,
    record_source_fingerprint,
    should_run_loader,
)


@pytest.fixture
def sqlite_con(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    """Create a temporary database for testing."""
    db_path = tmp_path / "test.db"
    con = init_db(db_path)
    yield con
    con.close()


def test_should_run_loader_when_no_fingerprint(sqlite_con: sqlite3.Connection):
    """Verify should_run_loader returns True when no fingerprint exists."""
    assert should_run_loader(sqlite_con, "player_game_log", "2023-24", "loader", "hash-a") is True


def test_should_run_loader_when_hash_changes(sqlite_con: sqlite3.Connection):
    """Verify should_run_loader returns True when source hash changes."""
    # Record initial fingerprint
    record_source_fingerprint(sqlite_con, "player_game_log", "2023-24", "loader", "hash-a")

    # Same hash should return False (no need to re-run)
    assert should_run_loader(sqlite_con, "player_game_log", "2023-24", "loader", "hash-a") is False

    # Different hash should return True (need to re-run)
    assert should_run_loader(sqlite_con, "player_game_log", "2023-24", "loader", "hash-b") is True


def test_record_source_fingerprint_creates_entry(sqlite_con: sqlite3.Connection):
    """Verify record_source_fingerprint creates a database entry."""
    record_source_fingerprint(sqlite_con, "player_game_log", "2023-24", "loader", "hash-a")

    row = sqlite_con.execute(
        "SELECT source_hash FROM etl_source_fingerprint "
        "WHERE table_name = ? AND season_id = ? AND loader = ?",
        ("player_game_log", "2023-24", "loader"),
    ).fetchone()

    assert row is not None
    assert row[0] == "hash-a"


def test_get_source_fingerprint_returns_hash(sqlite_con: sqlite3.Connection):
    """Verify get_source_fingerprint returns the stored hash."""
    record_source_fingerprint(sqlite_con, "player_game_log", "2023-24", "loader", "hash-xyz")

    result = get_source_fingerprint(sqlite_con, "player_game_log", "2023-24", "loader")
    assert result == "hash-xyz"


def test_get_source_fingerprint_returns_none_when_missing(sqlite_con: sqlite3.Connection):
    """Verify get_source_fingerprint returns None when no fingerprint exists."""
    result = get_source_fingerprint(sqlite_con, "player_game_log", "2023-24", "loader")
    assert result is None


def test_record_source_fingerprint_updates_existing(sqlite_con: sqlite3.Connection):
    """Verify record_source_fingerprint updates an existing fingerprint."""
    record_source_fingerprint(sqlite_con, "player_game_log", "2023-24", "loader", "hash-a")
    record_source_fingerprint(sqlite_con, "player_game_log", "2023-24", "loader", "hash-b")

    row = sqlite_con.execute(
        "SELECT source_hash FROM etl_source_fingerprint "
        "WHERE table_name = ? AND season_id = ? AND loader = ?",
        ("player_game_log", "2023-24", "loader"),
    ).fetchone()

    assert row[0] == "hash-b"

    # Should only have one row
    count = sqlite_con.execute(
        "SELECT COUNT(*) FROM etl_source_fingerprint "
        "WHERE table_name = ? AND season_id = ? AND loader = ?",
        ("player_game_log", "2023-24", "loader"),
    ).fetchone()[0]
    assert count == 1
