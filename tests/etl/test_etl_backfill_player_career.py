"""Tests: raw backfill player career enrichment loader."""

import sqlite3
from pathlib import Path

import pandas as pd

from src.etl.backfill._player_career import enrich_player_career


def test_enrich_player_career_updates_bref_hof_and_bio(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    sqlite_con.execute(
        """INSERT INTO dim_player
           (player_id, first_name, last_name, full_name, birth_date, is_active)
           VALUES ('203999', 'Nikola', 'Jokic', 'Nikola Jokic', '1995-02-19', 1)"""
    )
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
                "player": "Nikolá Jokić",
                "player_id": "jokicni01",
                "ht_in_in": 83,
                "wt": 284,
                "birth_date": "1995-02-19",
                "colleges": "Mega Basket",
                "hof": "FALSE",
            },
            {
                "player": "John Smith",
                "player_id": "smithjo02",
                "ht_in_in": 78,
                "wt": 220,
                "birth_date": "1992-02-02",
                "colleges": "State U",
                "hof": "TRUE",
            },
            {
                "player": "No Match",
                "player_id": "nomatch01",
                "ht_in_in": 75,
                "wt": 200,
                "birth_date": "1980-01-01",
                "colleges": "Unknown",
                "hof": "FALSE",
            },
        ]
    ).to_csv(tmp_path / "Player Career Info.csv", index=False)

    updated = enrich_player_career(sqlite_con, tmp_path)

    jokic = sqlite_con.execute(
        """
        SELECT bref_id, college, hof, height_cm, weight_kg
        FROM dim_player
        WHERE player_id = '203999'
        """
    ).fetchone()
    smith_1992 = sqlite_con.execute(
        """
        SELECT bref_id, college, hof
        FROM dim_player
        WHERE player_id = '1001'
        """
    ).fetchone()
    smith_1990 = sqlite_con.execute(
        "SELECT bref_id FROM dim_player WHERE player_id = '1000'"
    ).fetchone()

    assert updated == 2
    assert jokic[0] == "jokicni01"
    assert jokic[1] == "Mega Basket"
    assert jokic[2] == 0
    assert round(jokic[3], 2) == 210.82
    assert round(jokic[4], 2) == 128.82
    assert smith_1992 == ("smithjo02", "State U", 1)
    assert smith_1990 == (None,)


def test_enrich_player_career_skips_when_file_missing(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    sqlite_con.execute(
        """INSERT INTO dim_player
           (player_id, first_name, last_name, full_name, is_active)
           VALUES ('2544', 'LeBron', 'James', 'LeBron James', 1)"""
    )
    sqlite_con.commit()

    updated = enrich_player_career(sqlite_con, tmp_path)
    bref_id = sqlite_con.execute(
        "SELECT bref_id FROM dim_player WHERE player_id = '2544'"
    ).fetchone()[0]

    assert updated == 0
    assert bref_id is None
