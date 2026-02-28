"""Tests for src.db.tracking.fingerprint — source hash-based loader gating."""

import sqlite3

from src.db.tracking.fingerprint import (
    save_loader_fingerprint,
    should_run_loader,
)

# ------------------------------------------------------------------ #
# should_run_loader                                                   #
# ------------------------------------------------------------------ #


def test_should_run_loader_returns_true_when_no_hash_stored(sqlite_con):
    """First run: no fingerprint record exists yet — loader must run."""
    assert should_run_loader(sqlite_con, "player_game_log", "2023-24", "loader", "hash-a") is True


def test_should_run_loader_returns_false_when_same_hash(sqlite_con):
    """Same hash persisted — loader should be skipped."""
    save_loader_fingerprint(sqlite_con, "player_game_log", "2023-24", "loader", "hash-a")
    assert should_run_loader(sqlite_con, "player_game_log", "2023-24", "loader", "hash-a") is False


def test_should_run_loader_returns_true_when_hash_changes(sqlite_con):
    """Hash changed since last run — loader must re-run."""
    save_loader_fingerprint(sqlite_con, "player_game_log", "2023-24", "loader", "hash-a")
    assert should_run_loader(sqlite_con, "player_game_log", "2023-24", "loader", "hash-b") is True


def test_should_run_loader_is_scoped_by_table_season_loader(sqlite_con):
    """Fingerprints are keyed by (table_name, season_id, loader) — different combos are independent."""
    save_loader_fingerprint(sqlite_con, "player_game_log", "2023-24", "loader_a", "hash-x")
    # Different loader — should still be treated as first run
    assert should_run_loader(sqlite_con, "player_game_log", "2023-24", "loader_b", "hash-x") is True
    # Different season — should still be treated as first run
    assert should_run_loader(sqlite_con, "player_game_log", "2022-23", "loader_a", "hash-x") is True
    # Different table — should still be treated as first run
    assert should_run_loader(sqlite_con, "team_game_log", "2023-24", "loader_a", "hash-x") is True


# ------------------------------------------------------------------ #
# save_loader_fingerprint (upsert behaviour)                          #
# ------------------------------------------------------------------ #


def test_save_loader_fingerprint_stores_hash(sqlite_con):
    """After saving, the hash is persisted in etl_source_fingerprint."""
    save_loader_fingerprint(sqlite_con, "player_game_log", "2023-24", "loader", "hash-a")
    row = sqlite_con.execute(
        "SELECT source_hash FROM etl_source_fingerprint"
        " WHERE table_name = ? AND season_id = ? AND loader = ?",
        ("player_game_log", "2023-24", "loader"),
    ).fetchone()
    assert row is not None
    assert row[0] == "hash-a"


def test_save_loader_fingerprint_upserts_on_repeated_call(sqlite_con):
    """Calling save twice with different hashes updates the stored record (upsert)."""
    save_loader_fingerprint(sqlite_con, "player_game_log", "2023-24", "loader", "hash-a")
    save_loader_fingerprint(sqlite_con, "player_game_log", "2023-24", "loader", "hash-b")

    rows = sqlite_con.execute(
        "SELECT source_hash FROM etl_source_fingerprint"
        " WHERE table_name = ? AND season_id = ? AND loader = ?",
        ("player_game_log", "2023-24", "loader"),
    ).fetchall()
    # Only one row due to PRIMARY KEY constraint
    assert len(rows) == 1
    assert rows[0][0] == "hash-b"


def test_save_loader_fingerprint_sets_updated_at(sqlite_con):
    """The updated_at column is populated as a non-empty string."""
    save_loader_fingerprint(sqlite_con, "player_game_log", "2023-24", "loader", "hash-a")
    row = sqlite_con.execute(
        "SELECT updated_at FROM etl_source_fingerprint"
        " WHERE table_name = ? AND season_id = ? AND loader = ?",
        ("player_game_log", "2023-24", "loader"),
    ).fetchone()
    assert row is not None
    assert len(row[0]) > 10  # ISO timestamp like "2026-02-28T..."


# ------------------------------------------------------------------ #
# Graceful handling without the table (bare connection)               #
# ------------------------------------------------------------------ #


def test_should_run_loader_returns_true_when_table_missing():
    """If etl_source_fingerprint doesn't exist yet, treat it as a first run."""
    bare_con = sqlite3.connect(":memory:")
    assert should_run_loader(bare_con, "player_game_log", "2023-24", "loader", "hash-a") is True
    bare_con.close()
