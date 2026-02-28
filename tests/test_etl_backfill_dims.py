"""Tests: raw backfill dimension enrichers."""

import sqlite3
from pathlib import Path
from sqlite3 import IntegrityError
from unittest.mock import patch

import pandas as pd
import pytest

from src.etl.backfill import _dims as dims_mod
from src.etl.backfill._dims import (
    _enrich_from_career_info,
    _enrich_from_players_csv,
    enrich_dim_player,
    enrich_dim_team,
    load_team_history,
)


def test_load_team_history_inserts_rows_when_csv_present(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    sqlite_con.execute(
        """INSERT INTO dim_team
           (team_id, abbreviation, full_name, city, nickname)
           VALUES ('1610612747', 'LAL', 'Los Angeles Lakers', 'Los Angeles', 'Lakers')"""
    )
    sqlite_con.commit()

    pd.DataFrame(
        [
            {
                "teamId": 1610612747,
                "teamCity": "Los Angeles",
                "teamName": "Lakers",
                "teamAbbrev": "LAL",
                "seasonFounded": 1948,
                "seasonActiveTill": 2026,
                "league": "NBA",
            }
        ]
    ).to_csv(tmp_path / "TeamHistories.csv", index=False)

    load_team_history(sqlite_con, tmp_path)

    row = sqlite_con.execute(
        """SELECT team_id, team_city, team_name, team_abbrev, season_founded, season_active_till, league
           FROM dim_team_history"""
    ).fetchone()
    assert row == ("1610612747", "Los Angeles", "Lakers", "LAL", 1948, 2026, "NBA")


def test_load_team_history_skips_when_csv_missing(
    sqlite_con: sqlite3.Connection, tmp_path: Path
) -> None:
    load_team_history(sqlite_con, tmp_path)
    count = sqlite_con.execute("SELECT COUNT(*) FROM dim_team_history").fetchone()[0]
    assert count == 0


def test_load_team_history_inserts_only_dim_team_ids(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """Only team IDs present in dim_team (FK target) are inserted.

    SQLite behaviour (PRAGMA foreign_keys=ON): executemany with INSERT OR IGNORE
    raises IntegrityError on a FK violation even though the conflict clause is
    IGNORE. The IGNORE clause only suppresses UNIQUE/NOT NULL conflicts; FK
    failures propagate as hard errors when foreign_keys is enabled.

    The codebase's upsert_rows catches OperationalError but not IntegrityError,
    so callers must only pass rows whose FK references exist in dim_team.
    """
    sqlite_con.execute(
        """INSERT INTO dim_team
           (team_id, abbreviation, full_name, city, nickname)
           VALUES ('1610612747', 'LAL', 'Los Angeles Lakers', 'Los Angeles', 'Lakers')"""
    )
    sqlite_con.commit()

    # Build a CSV that contains a valid row AND one with an unknown team_id.
    pd.DataFrame(
        [
            {
                "teamId": 1610612747,
                "teamCity": "Los Angeles",
                "teamName": "Lakers",
                "teamAbbrev": "LAL",
                "seasonFounded": 1948,
                "seasonActiveTill": 2026,
                "league": "NBA",
            },
            {
                # team_id '9999000002' is absent from dim_team; FK violation.
                "teamId": 9999000002,
                "teamCity": "Nowhere",
                "teamName": "Ghosts",
                "teamAbbrev": "GHO",
                "seasonFounded": 1900,
                "seasonActiveTill": 1910,
                "league": "BAA",
            },
        ]
    ).to_csv(tmp_path / "TeamHistories.csv", index=False)

    # With foreign_keys=ON, passing an unknown team_id raises IntegrityError.
    with pytest.raises(IntegrityError, match="FOREIGN KEY constraint failed"):
        load_team_history(sqlite_con, tmp_path)


def test_load_team_history_keeps_historical_franchises(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    """Historical team IDs absent from the 30-team static set are preserved.

    ID '9999000001' is not present in src/etl/data/team_metadata.json.
    The old code (which filtered to valid_team_ids from that file) would have
    dropped this row; the new code (no filter) must keep it.
    """
    # Seed the historical franchise into dim_team so the FK constraint is
    # satisfied. This mirrors what Task 3 (seed historical teams) does in
    # the real pipeline.
    sqlite_con.execute(
        """INSERT INTO dim_team
           (team_id, abbreviation, full_name, city, nickname)
           VALUES ('9999000001', 'ROC', 'Rochester Royals', 'Rochester', 'Royals')"""
    )
    sqlite_con.commit()

    pd.DataFrame(
        [
            {
                "teamId": 9999000001,
                "teamCity": "Rochester",
                "teamName": "Royals",
                "teamAbbrev": "ROC",
                "seasonFounded": 1945,
                "seasonActiveTill": 1957,
                "league": "BAA",
            }
        ]
    ).to_csv(tmp_path / "TeamHistories.csv", index=False)

    load_team_history(sqlite_con, raw_dir=tmp_path)

    count = sqlite_con.execute("SELECT COUNT(*) FROM dim_team_history").fetchone()[0]
    assert count == 1


def test_enrich_dim_team_updates_latest_abbreviation(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    sqlite_con.execute(
        """INSERT INTO dim_team
           (team_id, abbreviation, full_name, city, nickname)
           VALUES ('1610612747', 'LAL', 'Los Angeles Lakers', 'Los Angeles', 'Lakers')"""
    )
    sqlite_con.commit()

    pd.DataFrame(
        [
            {"team": "Los Angeles Lakers", "abbreviation": "LAK", "season": 2023},
            {"team": "Los Angeles Lakers", "abbreviation": "LAL", "season": 2024},
        ]
    ).to_csv(tmp_path / "Team Abbrev.csv", index=False)

    enrich_dim_team(sqlite_con, tmp_path)

    bref_abbrev = sqlite_con.execute(
        "SELECT bref_abbrev FROM dim_team WHERE team_id='1610612747'"
    ).fetchone()[0]
    assert bref_abbrev == "LAL"


def test_enrich_from_players_csv_updates_missing_bio_fields(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    sqlite_con.execute(
        """INSERT INTO dim_player
           (player_id, first_name, last_name, full_name, is_active)
           VALUES ('2544', 'LeBron', 'James', 'LeBron James', 1)"""
    )
    sqlite_con.commit()

    pd.DataFrame(
        [
            {
                "personId": 2544,
                "height": "6-9",
                "bodyWeight": 250,
                "lastAttended": "St. Vincent-St. Mary HS",
                "draftYear": 2003,
                "draftRound": 1,
                "draftNumber": 1,
            }
        ]
    ).to_csv(tmp_path / "Players.csv", index=False)

    _enrich_from_players_csv(sqlite_con, tmp_path)

    row = sqlite_con.execute(
        """SELECT college, draft_year, draft_round, draft_number, height_cm, weight_kg
           FROM dim_player WHERE player_id='2544'"""
    ).fetchone()
    assert row[:4] == ("St. Vincent-St. Mary HS", 2003, 1, 1)
    assert round(row[4], 2) == 205.74
    assert round(row[5], 2) == 113.40


def test_enrich_from_players_csv_handles_invalid_height_weight_and_missing_person_id(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    sqlite_con.execute(
        """INSERT INTO dim_player
           (player_id, first_name, last_name, full_name, is_active)
           VALUES ('201939', 'Stephen', 'Curry', 'Stephen Curry', 1)"""
    )
    sqlite_con.execute(
        """INSERT INTO dim_player
           (player_id, first_name, last_name, full_name, is_active)
           VALUES ('202691', 'Klay', 'Thompson', 'Klay Thompson', 1)"""
    )
    sqlite_con.execute(
        """INSERT INTO dim_player
           (player_id, first_name, last_name, full_name, is_active)
           VALUES ('202695', 'Kawhi', 'Leonard', 'Kawhi Leonard', 1)"""
    )
    sqlite_con.execute(
        """INSERT INTO dim_player
           (player_id, first_name, last_name, full_name, is_active)
           VALUES ('202696', 'Nikola', 'Vucevic', 'Nikola Vucevic', 1)"""
    )
    sqlite_con.commit()

    pd.DataFrame(
        [
            {
                "personId": 201939,
                "height": "80",
                "bodyWeight": "bad-weight",
                "lastAttended": "Davidson",
                "draftYear": 2009,
                "draftRound": 1,
                "draftNumber": 7,
            },
            {
                "personId": 202691,
                "height": "6-x",
                "bodyWeight": None,
                "lastAttended": "Washington State",
                "draftYear": 2011,
                "draftRound": 1,
                "draftNumber": 11,
            },
            {
                "personId": 202695,
                "height": "abc",
                "bodyWeight": 225,
                "lastAttended": "San Diego State",
                "draftYear": 2011,
                "draftRound": 1,
                "draftNumber": 15,
            },
            {
                "personId": 202696,
                "height": None,
                "bodyWeight": 260,
                "lastAttended": "USC",
                "draftYear": 2011,
                "draftRound": 1,
                "draftNumber": 16,
            },
            {
                "personId": None,
                "height": "6-10",
                "bodyWeight": 240,
                "lastAttended": "Skip",
                "draftYear": 2020,
                "draftRound": 2,
                "draftNumber": 50,
            },
        ]
    ).to_csv(tmp_path / "Players.csv", index=False)

    _enrich_from_players_csv(sqlite_con, tmp_path)

    row_numeric_height = sqlite_con.execute(
        "SELECT height_cm, weight_kg FROM dim_player WHERE player_id='201939'"
    ).fetchone()
    row_invalid_height = sqlite_con.execute(
        "SELECT height_cm, weight_kg FROM dim_player WHERE player_id='202691'"
    ).fetchone()
    row_invalid_float = sqlite_con.execute(
        "SELECT height_cm, weight_kg FROM dim_player WHERE player_id='202695'"
    ).fetchone()
    row_none_height = sqlite_con.execute(
        "SELECT height_cm, weight_kg FROM dim_player WHERE player_id='202696'"
    ).fetchone()
    assert row_numeric_height == (203.2, None)
    assert row_invalid_height == (None, None)
    assert row_invalid_float == (None, 102.0582)
    assert row_none_height == (None, 117.93392)


def test_enrich_from_career_info_matches_normalized_name(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    sqlite_con.execute(
        """INSERT INTO dim_player
           (player_id, first_name, last_name, full_name, birth_date, is_active)
           VALUES ('203999', 'Nikola', 'Jokic', 'Nikola Jokic', '1995-02-19', 1)"""
    )
    sqlite_con.commit()

    pd.DataFrame(
        [
            {
                "player_id": "jokicni01",
                "player": "Nikolá Jokić",
                "birth_date": "1995-02-19",
                "ht_in_in": 83,
                "wt": 284,
                "colleges": "Mega Basket",
                "hof": "False",
            }
        ]
    ).to_csv(tmp_path / "Player Career Info.csv", index=False)

    _enrich_from_career_info(sqlite_con, tmp_path)

    row = sqlite_con.execute(
        """SELECT bref_id, college, hof, height_cm, weight_kg
           FROM dim_player WHERE player_id='203999'"""
    ).fetchone()
    assert row[0] == "jokicni01"
    assert row[1] == "Mega Basket"
    assert row[2] == 0
    assert round(row[3], 2) == 210.82
    assert round(row[4], 2) == 128.82


def test_enrich_from_career_info_resolves_duplicate_name_by_birth_date(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    sqlite_con.execute(
        """INSERT INTO dim_player
           (player_id, first_name, last_name, full_name, birth_date, is_active)
           VALUES ('1000', 'John', 'Smith', 'John Smith', '1990-01-01', 1)"""
    )
    sqlite_con.execute(
        """INSERT INTO dim_player
           (player_id, first_name, last_name, full_name, birth_date, is_active)
           VALUES ('1001', 'John', 'Smith', 'John Smith', '1992-02-02', 1)"""
    )
    sqlite_con.commit()

    pd.DataFrame(
        [
            {
                "player_id": "smithjo02",
                "player": "John Smith",
                "birth_date": "1992-02-02",
                "ht_in_in": 78,
                "wt": 220,
                "colleges": "State U",
                "hof": "0",
            },
            {
                "player_id": "unmatched01",
                "player": "No Match",
                "birth_date": "1980-01-01",
                "ht_in_in": 75,
                "wt": 200,
                "colleges": "Unknown",
                "hof": "False",
            },
        ]
    ).to_csv(tmp_path / "Player Career Info.csv", index=False)

    _enrich_from_career_info(sqlite_con, tmp_path)

    matched = sqlite_con.execute(
        "SELECT bref_id, college FROM dim_player WHERE player_id='1001'"
    ).fetchone()
    unmatched = sqlite_con.execute(
        "SELECT bref_id FROM dim_player WHERE player_id='1000'"
    ).fetchone()[0]
    assert matched == ("smithjo02", "State U")
    assert unmatched is None


def test_enrich_dim_player_calls_players_source_only(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    with patch.object(dims_mod, "_enrich_from_players_csv") as players_patch:
        with patch.object(dims_mod, "_enrich_from_career_info") as career_patch:
            enrich_dim_player(sqlite_con, tmp_path)

    players_patch.assert_called_once_with(sqlite_con, tmp_path)
    career_patch.assert_not_called()
