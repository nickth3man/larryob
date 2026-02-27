"""Tests: backfill salary history loader."""

import sqlite3
from pathlib import Path

import pandas as pd

from src.etl.backfill._salary_history import load_salary_history

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _seed_player(con: sqlite3.Connection, player_id: str, full_name: str) -> None:
    first, *rest = full_name.split()
    last = rest[-1] if rest else ""
    con.execute(
        """INSERT OR IGNORE INTO dim_player
           (player_id, first_name, last_name, full_name, is_active)
           VALUES (?, ?, ?, ?, 1)""",
        (player_id, first, last, full_name),
    )


def _seed_team(con: sqlite3.Connection, team_id: str, abbreviation: str) -> None:
    con.execute(
        """INSERT OR IGNORE INTO dim_team
           (team_id, abbreviation, full_name, city, nickname)
           VALUES (?, ?, ?, 'City', 'Nickname')""",
        (team_id, abbreviation, f"{abbreviation} Team"),
    )


def _write_salary_csv(
    tmp_path: Path, rows: list[dict], filename: str = "open_salaries.csv"
) -> Path:
    path = tmp_path / filename
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


# ---------------------------------------------------------------------------
# File-path validation
# ---------------------------------------------------------------------------


def test_load_salary_history_missing_file_returns_zero(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """When the CSV does not exist, return 0 without error."""
    result = load_salary_history(sqlite_con, raw_dir=tmp_path)
    assert result == 0


def test_load_salary_history_explicit_missing_file_returns_zero(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """Explicit open_file path that doesn't exist → 0."""
    missing = tmp_path / "no_such_file.csv"
    result = load_salary_history(sqlite_con, open_file=missing)
    assert result == 0


# ---------------------------------------------------------------------------
# Valid rows
# ---------------------------------------------------------------------------


def test_load_salary_history_inserts_valid_rows(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """A parseable row with known player, team, and salary is inserted."""
    _seed_player(sqlite_con, "2544", "LeBron James")
    _seed_team(sqlite_con, "1610612747", "LAL")
    sqlite_con.execute(
        "INSERT OR IGNORE INTO dim_season (season_id, start_year, end_year) VALUES ('2023-24', 2023, 2024)"
    )
    sqlite_con.commit()

    csv_file = _write_salary_csv(
        tmp_path,
        [
            {
                "player_name": "LeBron James",
                "season_id": "2023-24",
                "team_abbrev": "LAL",
                "salary": 47607350,
            }
        ],
    )
    result = load_salary_history(sqlite_con, open_file=csv_file)

    assert result == 1
    row = sqlite_con.execute(
        "SELECT player_id, team_id, season_id, salary FROM fact_salary"
    ).fetchone()
    assert row == ("2544", "1610612747", "2023-24", 47607350)


def test_load_salary_history_parses_string_salary(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """Dollar-formatted string like "$5,000,000" is parsed and inserted."""
    _seed_player(sqlite_con, "201939", "Stephen Curry")
    _seed_team(sqlite_con, "1610612744", "GSW")
    sqlite_con.execute(
        "INSERT OR IGNORE INTO dim_season (season_id, start_year, end_year) VALUES ('2023-24', 2023, 2024)"
    )
    sqlite_con.commit()

    csv_file = _write_salary_csv(
        tmp_path,
        [
            {
                "player_name": "Stephen Curry",
                "season_id": "2023-24",
                "team_abbrev": "GSW",
                "salary": "$5,000,000",
            }
        ],
    )
    result = load_salary_history(sqlite_con, open_file=csv_file)

    assert result == 1
    salary = sqlite_con.execute("SELECT salary FROM fact_salary").fetchone()[0]
    assert salary == 5_000_000


def test_load_salary_history_integer_end_year_season(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """Integer end-year convention (e.g. 2024) → normalised to '2023-24'."""
    _seed_player(sqlite_con, "203999", "Nikola Jokic")
    _seed_team(sqlite_con, "1610612743", "DEN")
    sqlite_con.execute(
        "INSERT OR IGNORE INTO dim_season (season_id, start_year, end_year) VALUES ('2023-24', 2023, 2024)"
    )
    sqlite_con.commit()

    csv_file = _write_salary_csv(
        tmp_path,
        [{"player_name": "Nikola Jokic", "season": 2024, "team": "DEN", "salary": 47607350}],
    )
    result = load_salary_history(sqlite_con, open_file=csv_file)

    assert result == 1
    season_id = sqlite_con.execute("SELECT season_id FROM fact_salary").fetchone()[0]
    assert season_id == "2023-24"


# ---------------------------------------------------------------------------
# Skip conditions
# ---------------------------------------------------------------------------


def test_load_salary_history_bad_season_skipped(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """Row with unparseable season_id is skipped."""
    _seed_player(sqlite_con, "2544", "LeBron James")
    _seed_team(sqlite_con, "1610612747", "LAL")
    sqlite_con.commit()

    csv_file = _write_salary_csv(
        tmp_path,
        [
            {
                "player_name": "LeBron James",
                "season_id": "not-a-season",
                "team_abbrev": "LAL",
                "salary": 1000000,
            }
        ],
    )
    result = load_salary_history(sqlite_con, open_file=csv_file)

    assert result == 0
    count = sqlite_con.execute("SELECT COUNT(*) FROM fact_salary").fetchone()[0]
    assert count == 0


def test_load_salary_history_unknown_player_skipped(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """Row for a player not in dim_player is skipped."""
    _seed_team(sqlite_con, "1610612747", "LAL")
    sqlite_con.commit()

    csv_file = _write_salary_csv(
        tmp_path,
        [
            {
                "player_name": "Ghost Player",
                "season_id": "2023-24",
                "team_abbrev": "LAL",
                "salary": 1000000,
            }
        ],
    )
    result = load_salary_history(sqlite_con, open_file=csv_file)

    assert result == 0


def test_load_salary_history_unknown_team_skipped(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """Row for a team abbreviation not in dim_team is skipped."""
    _seed_player(sqlite_con, "2544", "LeBron James")
    sqlite_con.commit()

    csv_file = _write_salary_csv(
        tmp_path,
        [
            {
                "player_name": "LeBron James",
                "season_id": "2023-24",
                "team_abbrev": "ZZZ",
                "salary": 1000000,
            }
        ],
    )
    result = load_salary_history(sqlite_con, open_file=csv_file)

    assert result == 0


def test_load_salary_history_zero_salary_skipped(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """Row with salary of 0 is skipped."""
    _seed_player(sqlite_con, "2544", "LeBron James")
    _seed_team(sqlite_con, "1610612747", "LAL")
    sqlite_con.commit()

    csv_file = _write_salary_csv(
        tmp_path,
        [
            {
                "player_name": "LeBron James",
                "season_id": "2023-24",
                "team_abbrev": "LAL",
                "salary": 0,
            }
        ],
    )
    result = load_salary_history(sqlite_con, open_file=csv_file)

    assert result == 0


def test_load_salary_history_dollar_zero_salary_skipped(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """Row with '$0' string salary is also skipped."""
    _seed_player(sqlite_con, "2544", "LeBron James")
    _seed_team(sqlite_con, "1610612747", "LAL")
    sqlite_con.commit()

    csv_file = _write_salary_csv(
        tmp_path,
        [
            {
                "player_name": "LeBron James",
                "season_id": "2023-24",
                "team_abbrev": "LAL",
                "salary": "$0",
            }
        ],
    )
    result = load_salary_history(sqlite_con, open_file=csv_file)

    assert result == 0


def test_load_salary_history_mixed_rows(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """Only valid rows among a mixed batch are inserted."""
    _seed_player(sqlite_con, "2544", "LeBron James")
    _seed_team(sqlite_con, "1610612747", "LAL")
    sqlite_con.execute(
        "INSERT OR IGNORE INTO dim_season (season_id, start_year, end_year) VALUES ('2023-24', 2023, 2024)"
    )
    sqlite_con.commit()

    csv_file = _write_salary_csv(
        tmp_path,
        [
            # Valid
            {
                "player_name": "LeBron James",
                "season_id": "2023-24",
                "team_abbrev": "LAL",
                "salary": 47607350,
            },
            # Bad season
            {
                "player_name": "LeBron James",
                "season_id": "bad",
                "team_abbrev": "LAL",
                "salary": 1000000,
            },
            # Unknown player
            {
                "player_name": "Nobody Here",
                "season_id": "2023-24",
                "team_abbrev": "LAL",
                "salary": 1000000,
            },
            # Zero salary
            {
                "player_name": "LeBron James",
                "season_id": "2022-23",
                "team_abbrev": "LAL",
                "salary": 0,
            },
        ],
    )
    result = load_salary_history(sqlite_con, open_file=csv_file)

    assert result == 1


def test_load_salary_history_idempotent(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """Running twice inserts once (INSERT OR REPLACE doesn't raise)."""
    _seed_player(sqlite_con, "2544", "LeBron James")
    _seed_team(sqlite_con, "1610612747", "LAL")
    sqlite_con.execute(
        "INSERT OR IGNORE INTO dim_season (season_id, start_year, end_year) VALUES ('2023-24', 2023, 2024)"
    )
    sqlite_con.commit()

    csv_file = _write_salary_csv(
        tmp_path,
        [
            {
                "player_name": "LeBron James",
                "season_id": "2023-24",
                "team_abbrev": "LAL",
                "salary": 47607350,
            }
        ],
    )

    first = load_salary_history(sqlite_con, open_file=csv_file)
    second = load_salary_history(sqlite_con, open_file=csv_file)

    assert first == 1
    assert second == 1  # REPLACE re-inserts
    count = sqlite_con.execute("SELECT COUNT(*) FROM fact_salary").fetchone()[0]
    assert count == 1
