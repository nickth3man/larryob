"""Tests: deterministic player/team identifier resolution."""

import sqlite3

from src.etl.identity.resolver import resolve_or_create_player


def test_resolve_or_create_player_creates_placeholder(sqlite_con: sqlite3.Connection) -> None:
    pid = resolve_or_create_player(
        sqlite_con, source_system="bref", source_id="ackerdo01", full_name="Don Ackerman"
    )
    assert pid
    row = sqlite_con.execute(
        "SELECT full_name FROM dim_player WHERE player_id = ?", (pid,)
    ).fetchone()
    assert row[0] == "Don Ackerman"


def test_resolve_or_create_player_is_idempotent(sqlite_con: sqlite3.Connection) -> None:
    """Calling twice with the same source_system+source_id returns the same player_id."""
    pid1 = resolve_or_create_player(
        sqlite_con, source_system="bref", source_id="ackerdo01", full_name="Don Ackerman"
    )
    pid2 = resolve_or_create_player(
        sqlite_con, source_system="bref", source_id="ackerdo01", full_name="Don Ackerman"
    )
    assert pid1 == pid2


def test_resolve_or_create_player_returns_existing_mapping(
    sqlite_con: sqlite3.Connection,
) -> None:
    """When an existing dim_player_identifier row already maps the source, return its player_id."""
    # Seed an existing player and identifier mapping
    sqlite_con.execute(
        """INSERT INTO dim_player
           (player_id, first_name, last_name, full_name, is_active)
           VALUES ('2544', 'LeBron', 'James', 'LeBron James', 1)"""
    )
    sqlite_con.execute(
        """INSERT INTO dim_player_identifier
           (source_system, source_id, player_id, match_confidence)
           VALUES ('bref', 'jamesle01', '2544', 1.0)"""
    )
    sqlite_con.commit()

    pid = resolve_or_create_player(
        sqlite_con, source_system="bref", source_id="jamesle01", full_name="LeBron James"
    )
    assert pid == "2544"
    # No duplicate dim_player_identifier row should exist
    count = sqlite_con.execute(
        "SELECT COUNT(*) FROM dim_player_identifier WHERE source_system='bref' AND source_id='jamesle01'"
    ).fetchone()[0]
    assert count == 1


def test_resolve_or_create_player_inserts_dim_player_identifier_row(
    sqlite_con: sqlite3.Connection,
) -> None:
    """A new placeholder also inserts a row in dim_player_identifier."""
    pid = resolve_or_create_player(
        sqlite_con, source_system="bref", source_id="oldtimer01", full_name="Old Timer"
    )
    row = sqlite_con.execute(
        """SELECT player_id, match_confidence
           FROM dim_player_identifier
           WHERE source_system='bref' AND source_id='oldtimer01'"""
    ).fetchone()
    assert row is not None
    assert row[0] == pid
    assert row[1] == 0.0


def test_resolve_or_create_player_synthetic_id_format(
    sqlite_con: sqlite3.Connection,
) -> None:
    """Placeholder player_id is deterministic and clearly marked."""
    pid = resolve_or_create_player(
        sqlite_con, source_system="bref", source_id="testpl01", full_name="Test Player"
    )
    assert pid == "placeholder_bref_testpl01"
