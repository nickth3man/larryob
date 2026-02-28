"""Tests: raw backfill awards loader."""

import sqlite3
from pathlib import Path

import pandas as pd

from src.etl.backfill._awards import _bref_to_player_id, _eos_award_name, load_awards


def _seed_award_context(con: sqlite3.Connection) -> None:
    con.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES ('2023-24', 2023, 2024)"
    )
    con.execute(
        """INSERT INTO dim_player
           (player_id, first_name, last_name, full_name, bref_id, is_active)
           VALUES ('2544', 'LeBron', 'James', 'LeBron James', 'jamesle01', 1)"""
    )
    con.commit()


def test_eos_award_name_maps_known_awards() -> None:
    result = _eos_award_name("Most Valuable Player Voting")
    assert result == "MVP"


def test_eos_award_name_returns_none_for_unmapped_value() -> None:
    result = _eos_award_name("Some Other Award")
    assert result is None


def test_bref_to_player_id_builds_lookup(sqlite_con: sqlite3.Connection) -> None:
    sqlite_con.execute(
        """INSERT INTO dim_player
           (player_id, first_name, last_name, full_name, bref_id, is_active)
           VALUES ('203999', 'Nikola', 'Jokic', 'Nikola Jokic', 'jokicni01', 1)"""
    )
    sqlite_con.commit()

    mapping = _bref_to_player_id(sqlite_con)
    assert mapping == {"jokicni01": "203999"}


def test_load_awards_prefers_eos_voting_when_present(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    _seed_award_context(sqlite_con)

    pd.DataFrame(
        [
            {
                "season": 2024,
                "player_id": "jamesle01",
                "award": "Most Valuable Player",
                "pts_won": 700,
                "pts_max": 1000,
            }
        ]
    ).to_csv(tmp_path / "Player Award Shares.csv", index=False)

    pd.DataFrame([{"season": 2024, "player_id": "jamesle01"}]).to_csv(
        tmp_path / "All-Star Selections.csv", index=False
    )

    pd.DataFrame(
        [
            {
                "season": 2024,
                "player_id": "jamesle01",
                "type": "all_nba",
                "number_tm": "1st",
                "pts_won": 120,
                "pts_max": 125,
            }
        ]
    ).to_csv(tmp_path / "End of Season Teams (Voting).csv", index=False)

    pd.DataFrame(
        [
            {
                "season": 2024,
                "player_id": "jamesle01",
                "type": "all_nba",
                "number_tm": "2nd",
            }
        ]
    ).to_csv(tmp_path / "End of Season Teams.csv", index=False)

    load_awards(sqlite_con, tmp_path)

    count = sqlite_con.execute("SELECT COUNT(*) FROM fact_player_award").fetchone()[0]
    assert count == 3


def test_load_awards_maps_award_share_name_to_canonical_short_code(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    _seed_award_context(sqlite_con)

    pd.DataFrame(
        [
            {
                "season": 2024,
                "player_id": "jamesle01",
                "award": "Most Valuable Player",
                "pts_won": 700,
                "pts_max": 1000,
            }
        ]
    ).to_csv(tmp_path / "Player Award Shares.csv", index=False)

    load_awards(sqlite_con, tmp_path)

    award_name = sqlite_con.execute("SELECT award_name FROM fact_player_award").fetchone()[0]
    assert award_name == "MVP"


def test_load_awards_uses_eos_fallback_when_voting_file_missing(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    _seed_award_context(sqlite_con)

    pd.DataFrame(
        [
            {
                "season": 2024,
                "player_id": "jamesle01",
                "type": "all_defense",
                "number_tm": "1st",
            }
        ]
    ).to_csv(tmp_path / "End of Season Teams.csv", index=False)

    load_awards(sqlite_con, tmp_path)

    row = sqlite_con.execute(
        """SELECT award_name, award_type, votes_received, votes_possible
           FROM fact_player_award"""
    ).fetchone()
    assert row == ("All-Defense 1st", "team_inclusion", None, None)


def test_load_awards_skips_when_source_files_missing(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    _seed_award_context(sqlite_con)

    load_awards(sqlite_con, tmp_path)

    count = sqlite_con.execute("SELECT COUNT(*) FROM fact_player_award").fetchone()[0]
    assert count == 0


def test_load_awards_skips_invalid_season_creates_placeholder_for_unknown_player(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    _seed_award_context(sqlite_con)

    pd.DataFrame(
        [
            {
                "season": 1990,
                "player_id": "jamesle01",
                "award": "MVP",
                "pts_won": 10,
                "pts_max": 100,
            },
            {
                "season": 2024,
                "player_id": "unknown01",
                "award": "MVP",
                "pts_won": 10,
                "pts_max": 100,
            },
        ]
    ).to_csv(tmp_path / "Player Award Shares.csv", index=False)

    pd.DataFrame(
        [
            {"season": 1990, "player_id": "jamesle01"},
            {"season": 2024, "player_id": "unknown01"},
        ]
    ).to_csv(tmp_path / "All-Star Selections.csv", index=False)

    pd.DataFrame(
        [
            {
                "season": 1990,
                "player_id": "jamesle01",
                "type": "all_nba",
                "number_tm": "1st",
                "pts_won": 50,
                "pts_max": 100,
            },
            {
                "season": 2024,
                "player_id": "unknown01",
                "type": "all_nba",
                "number_tm": "1st",
                "pts_won": 50,
                "pts_max": 100,
            },
        ]
    ).to_csv(tmp_path / "End of Season Teams (Voting).csv", index=False)

    pd.DataFrame(
        [
            {"season": 1990, "player_id": "jamesle01", "type": "all_nba", "number_tm": "2nd"},
            {"season": 2024, "player_id": "unknown01", "type": "all_nba", "number_tm": "2nd"},
        ]
    ).to_csv(tmp_path / "End of Season Teams.csv", index=False)

    load_awards(sqlite_con, tmp_path)

    # Invalid-season rows are still skipped; the three unknown01 rows with valid
    # season each create/reuse a placeholder and are inserted.
    count = sqlite_con.execute("SELECT COUNT(*) FROM fact_player_award").fetchone()[0]
    assert count == 3  # award share + all-star + EOS voting (fallback not used when voting present)

    placeholder_pid = sqlite_con.execute(
        "SELECT player_id FROM dim_player WHERE player_id = 'placeholder_bref_unknown01'"
    ).fetchone()
    assert placeholder_pid is not None


def test_load_awards_eos_fallback_skips_invalid_season_creates_placeholder_for_unknown(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    _seed_award_context(sqlite_con)

    pd.DataFrame(
        [
            {"season": 1990, "player_id": "jamesle01", "type": "all_nba", "number_tm": "2nd"},
            {"season": 2024, "player_id": "unknown01", "type": "all_nba", "number_tm": "2nd"},
        ]
    ).to_csv(tmp_path / "End of Season Teams.csv", index=False)

    load_awards(sqlite_con, tmp_path)

    # Invalid-season row is skipped; unknown01 with valid season gets a placeholder.
    count = sqlite_con.execute("SELECT COUNT(*) FROM fact_player_award").fetchone()[0]
    assert count == 1

    placeholder_pid = sqlite_con.execute(
        "SELECT player_id FROM dim_player WHERE player_id = 'placeholder_bref_unknown01'"
    ).fetchone()
    assert placeholder_pid is not None
