"""Tests for scripts.completion_audit — completeness contract enforcement."""

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from scripts.completion_audit import evaluate_completion


@pytest.fixture
def sqlite_con(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    """Create a temporary database for testing."""
    db_path = tmp_path / "test.db"
    con = sqlite3.connect(db_path)
    # Create minimal schema
    con.execute("CREATE TABLE dim_season (season_id TEXT PRIMARY KEY)")
    con.execute("CREATE TABLE fact_game (game_id TEXT PRIMARY KEY, season_type TEXT)")
    con.execute("CREATE TABLE dim_player (player_id TEXT PRIMARY KEY)")
    con.execute("CREATE TABLE dim_team (team_id TEXT PRIMARY KEY)")
    con.execute("CREATE TABLE fact_salary (salary_id INTEGER PRIMARY KEY, season_id TEXT)")
    con.execute(
        "CREATE TABLE dim_player_identifier (source_system TEXT, source_id TEXT, player_id TEXT, PRIMARY KEY (source_system, source_id))"
    )
    con.commit()
    yield con
    con.close()


def test_evaluate_completion_includes_missing_game_types(sqlite_con: sqlite3.Connection):
    """Verify evaluate_completion reports missing required game types."""
    data = evaluate_completion(sqlite_con)

    assert "missing_required_game_types" in data
    # All game types should be missing in empty DB
    assert len(data["missing_required_game_types"]) == 4


def test_evaluate_completion_includes_season_range(sqlite_con: sqlite3.Connection):
    """Verify evaluate_completion reports season range compliance."""
    data = evaluate_completion(sqlite_con)

    assert "season_range" in data
    assert data["season_range"]["expected_start"] == "1946-47"


def test_evaluate_completion_counts_unresolved_entities(sqlite_con: sqlite3.Connection):
    """Verify evaluate_completion reports unresolved entity counts."""
    # Add a player without identifier crosswalk
    sqlite_con.execute("INSERT INTO dim_player (player_id) VALUES ('12345')")
    sqlite_con.commit()

    data = evaluate_completion(sqlite_con)

    assert "unresolved_entities" in data
    # Player 12345 has no identifier crosswalk entry
    assert data["unresolved_entities"]["players_without_identifier"] >= 1


def test_evaluate_completion_with_full_season_range(sqlite_con: sqlite3.Connection):
    """Verify evaluate_completion passes when all seasons present."""
    # Add all seasons from 1946-47 to 2024-25
    for start_year in range(1946, 2025):
        season_id = f"{start_year}-{str(start_year + 1)[-2:]}"
        sqlite_con.execute("INSERT INTO dim_season (season_id) VALUES (?)", (season_id,))
    sqlite_con.commit()

    data = evaluate_completion(sqlite_con)

    assert data["season_range"]["complete"] is True


def test_evaluate_completion_with_required_game_types(sqlite_con: sqlite3.Connection):
    """Verify evaluate_completion reports game types present."""
    # Add games for each required type
    for i, game_type in enumerate(["Preseason", "Regular Season", "Play-In", "Playoffs"]):
        sqlite_con.execute(
            "INSERT INTO fact_game (game_id, season_type) VALUES (?, ?)",
            (f"00{i}", game_type),
        )
    sqlite_con.commit()

    data = evaluate_completion(sqlite_con)

    assert "missing_required_game_types" in data
    assert len(data["missing_required_game_types"]) == 0
