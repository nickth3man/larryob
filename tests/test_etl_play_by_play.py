"""Tests: ETL play-by-play transformation."""

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from src.db.operations import upsert_rows
from src.etl.transform.play_by_play import (
    _fetch_pbp,
    _transform_pbp,
    load_game,
    load_games,
    load_season_pbp,
)


def _make_pbp_df() -> pd.DataFrame:
    """
    Create a sample play-by-play pandas DataFrame containing four events for game "0022300001".

    Returns:
        pd.DataFrame: A DataFrame with 4 rows and the following columns:
            GAME_ID, EVENTNUM, PERIOD, PCTIMESTRING, WCTIMESTRING,
            EVENTMSGTYPE, EVENTMSGACTIONTYPE, PLAYER1_ID, PLAYER2_ID,
            PLAYER3_ID, PLAYER1_TEAM_ID, PLAYER2_TEAM_ID,
            HOMEDESCRIPTION, VISITORDESCRIPTION, NEUTRALDESCRIPTION,
            SCORE, SCOREMARGIN. PLAYER IDs are strings; zero-valued player IDs indicate team-level events.
    """
    return pd.DataFrame(
        {
            "GAME_ID": ["0022300001"] * 4,
            "EVENTNUM": [1, 2, 3, 4],
            "PERIOD": [1, 1, 1, 1],
            "PCTIMESTRING": ["11:48", "11:32", "11:15", "10:58"],
            "WCTIMESTRING": ["8:04 PM", "8:05 PM", "8:06 PM", "8:07 PM"],
            "EVENTMSGTYPE": [1, 2, 4, 5],
            "EVENTMSGACTIONTYPE": [1, 5, 1, 2],
            "PLAYER1_ID": ["2544", "203999", "2544", "1628389"],
            "PLAYER2_ID": [0, 0, 0, 0],
            "PLAYER3_ID": [0, 0, 0, 0],
            "PLAYER1_TEAM_ID": ["1610612747", "1610612744", "1610612747", "1610612748"],
            "PLAYER2_TEAM_ID": [0, 0, 0, 0],
            "HOMEDESCRIPTION": ["LeBron 2pt", None, "LeBron REBOUND", None],
            "VISITORDESCRIPTION": [None, "Jokic MISS", None, "Adebayo TURNOVER"],
            "NEUTRALDESCRIPTION": [None, None, None, None],
            "SCORE": ["2 - 0", "2 - 0", "2 - 0", "2 - 0"],
            "SCOREMARGIN": [2, 2, 2, 2],
        }
    )


def test_transform_pbp_event_id_format() -> None:
    rows = _transform_pbp(_make_pbp_df())
    assert rows[0]["event_id"] == "0022300001_000001"
    assert rows[3]["event_id"] == "0022300001_000004"


def test_transform_pbp_player_id_cast() -> None:
    rows = _transform_pbp(_make_pbp_df())
    assert rows[0]["player1_id"] == "2544"


def test_transform_pbp_zero_player_becomes_none() -> None:
    """Player IDs of 0 (team-level events) must be stored as None."""
    rows = _transform_pbp(_make_pbp_df())
    for row in rows:
        assert row["player2_id"] is None
        assert row["player3_id"] is None


def test_transform_pbp_tie_score_margin() -> None:
    df = _make_pbp_df()
    # Cast to object so pandas 3 allows mixed str/int in the column
    df["SCOREMARGIN"] = df["SCOREMARGIN"].astype(object)
    df.loc[0, "SCOREMARGIN"] = "TIE"
    rows = _transform_pbp(df)
    assert rows[0]["score_margin"] == "TIE"


def test_transform_pbp_row_count() -> None:
    rows = _transform_pbp(_make_pbp_df())
    assert len(rows) == 4


def test_pbp_insert(sqlite_con_with_data: sqlite3.Connection) -> None:
    rows = _transform_pbp(_make_pbp_df())
    # Only player1_ids that exist in dim_player will succeed (2544 is seeded)
    # But foreign keys are ON so unknown player IDs will cause constraint errors.
    # Insert only rows with known player1_id
    safe_rows = [r for r in rows if r["player1_id"] in ("2544", "203999")]
    n = upsert_rows(sqlite_con_with_data, "fact_play_by_play", safe_rows)
    count = sqlite_con_with_data.execute("SELECT COUNT(*) FROM fact_play_by_play").fetchone()[0]
    assert count == n


def test_pbp_deduplication(sqlite_con_with_data: sqlite3.Connection) -> None:
    rows = _transform_pbp(_make_pbp_df())
    safe_rows = [r for r in rows if r["player1_id"] in ("2544", "203999")]
    upsert_rows(sqlite_con_with_data, "fact_play_by_play", safe_rows)
    upsert_rows(sqlite_con_with_data, "fact_play_by_play", safe_rows)
    count = sqlite_con_with_data.execute("SELECT COUNT(*) FROM fact_play_by_play").fetchone()[0]
    assert count == len(safe_rows), "Duplicate PBP events were inserted"


# ------------------------------------------------------------------ #
# _fetch_pbp: cache path                                             #
# ------------------------------------------------------------------ #


def test_fetch_pbp_loads_from_cache(monkeypatch, tmp_path: Path) -> None:
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    from src.db.cache import save_cache

    records = _make_pbp_df().to_dict(orient="records")
    save_cache("pbp_0022300001", records)

    df = _fetch_pbp("0022300001")
    assert len(df) == 4


# ------------------------------------------------------------------ #
# load_game                                                           #
# ------------------------------------------------------------------ #


def test_load_game_from_cache_inserts_events(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    df = _make_pbp_df()
    # Keep only rows with player1_ids that exist in the fixture (2544, 203999)
    df = df[df["PLAYER1_ID"].isin(["2544", "203999"])]
    from src.db.cache import save_cache

    save_cache("pbp_0022300001", df.to_dict(orient="records"))

    n = load_game(sqlite_con_with_data, "0022300001")
    assert n >= 1


def test_load_game_returns_zero_for_empty_api_response(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    mock_ep = MagicMock()
    mock_ep.get_data_frames.return_value = [pd.DataFrame()]
    with patch("src.etl.transform.play_by_play.playbyplayv2.PlayByPlayV2", return_value=mock_ep):
        n = load_game(sqlite_con_with_data, "0022300001")
    assert n == 0


# ------------------------------------------------------------------ #
# load_games                                                          #
# ------------------------------------------------------------------ #


def test_load_games_handles_exception_per_game(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    with patch("src.etl.transform.play_by_play.load_game", side_effect=RuntimeError("API error")):
        with patch("src.etl.extract.api_client.time.sleep"):
            total = load_games(sqlite_con_with_data, ["0022300001"])
    assert total == 0


def test_load_games_sums_counts_across_games(
    monkeypatch,
    sqlite_con_with_data: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    with patch("src.etl.transform.play_by_play.load_game", return_value=5):
        with patch("src.etl.extract.api_client.time.sleep"):
            total = load_games(sqlite_con_with_data, ["001", "002", "003"])
    assert total == 15


# ------------------------------------------------------------------ #
# load_season_pbp                                                     #
# ------------------------------------------------------------------ #


def test_load_season_pbp_skips_when_already_loaded(
    sqlite_con_with_data: sqlite3.Connection,
    monkeypatch,
    tmp_path: Path,
) -> None:
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)
    from src.db.tracking import record_run

    record_run(
        sqlite_con_with_data, "fact_play_by_play", "2023-24", "play_by_play.load_season", 500, "ok"
    )
    result = load_season_pbp(sqlite_con_with_data, "2023-24")
    assert result == 0


def test_load_season_pbp_processes_games(
    sqlite_con_with_data: sqlite3.Connection,
    monkeypatch,
    tmp_path: Path,
) -> None:
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    with patch("src.etl.transform.play_by_play.load_games", return_value=10) as mock_lg:
        with patch("src.etl.transform.play_by_play.log_load_summary", return_value=10):
            result = load_season_pbp(sqlite_con_with_data, "2023-24")
    assert result == 10
    mock_lg.assert_called_once()


# ------------------------------------------------------------------ #
# load_season_pbp: source parameter                                   #
# ------------------------------------------------------------------ #


def test_load_season_pbp_source_api_skips_bulk(
    sqlite_con_with_data: sqlite3.Connection,
    monkeypatch,
    tmp_path: Path,
) -> None:
    """source='api' must not call load_bulk_pbp_season at all."""
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    with patch("src.etl.transform.play_by_play.load_games", return_value=5):
        with patch("src.etl.transform.play_by_play.log_load_summary"):
            with patch("src.etl.load.bulk.load_bulk_pbp_season") as mock_bulk:
                result = load_season_pbp(sqlite_con_with_data, "2023-24", source="api")

    assert result == 5
    mock_bulk.assert_not_called()


def test_load_season_pbp_source_bulk_calls_bulk_only(
    sqlite_con_with_data: sqlite3.Connection,
    monkeypatch,
    tmp_path: Path,
) -> None:
    """source='bulk' must call load_bulk_pbp_season and skip load_games."""
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    with patch("src.etl.transform.play_by_play.load_games") as mock_api:
        with patch("src.etl.transform.play_by_play.log_load_summary"):
            # Patch the import-time name inside the function's local scope
            with patch("src.etl.load.bulk.load_bulk_pbp_season", return_value=42):
                result = load_season_pbp(sqlite_con_with_data, "2023-24", source="bulk")

    assert result == 42
    mock_api.assert_not_called()


def test_load_season_pbp_source_auto_deduplicates(
    sqlite_con_with_data: sqlite3.Connection,
    monkeypatch,
    tmp_path: Path,
) -> None:
    """
    source='auto': after bulk load, game_ids already in fact_play_by_play
    must be excluded from the API call.
    """
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)

    # The fixture seeds fact_game with game_id "0022300001" for season "2023-24".
    # Pre-populate fact_play_by_play with that game so it is skipped by auto mode.
    from src.db.operations import upsert_rows as _upsert

    row = {
        "event_id": "0022300001_000001",
        "game_id": "0022300001",
        "period": 1,
        "pc_time_string": "12:00",
        "wc_time_string": "8:00 PM",
        "eventmsgtype": 12,
        "eventmsgactiontype": 0,
        "player1_id": None,
        "player2_id": None,
        "player3_id": None,
        "person1type": None,
        "person2type": None,
        "person3type": None,
        "team1_id": None,
        "team2_id": None,
        "home_description": None,
        "visitor_description": None,
        "neutral_description": None,
        "score": None,
        "score_margin": None,
    }
    _upsert(sqlite_con_with_data, "fact_play_by_play", [row])

    with patch("src.etl.transform.play_by_play.load_games", return_value=0) as mock_api:
        with patch("src.etl.transform.play_by_play.log_load_summary"):
            with patch("src.etl.load.bulk.load_bulk_pbp_season", return_value=1):
                load_season_pbp(sqlite_con_with_data, "2023-24", source="auto")

    # load_games was called, but game "0022300001" should NOT be in its args
    # because it was already present after the bulk load.
    call_args = mock_api.call_args
    game_ids_passed = call_args[0][1]  # positional arg: (con, game_ids, ...)
    assert "0022300001" not in game_ids_passed
