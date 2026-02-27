"""Tests: ETL shared utilities (caching, backoff, upsert, run-log helpers)."""

import json
import logging
import sqlite3
from pathlib import Path

import pytest

from src.db.cache import cache_path, load_cache, save_cache
from src.db.operations import transaction, upsert_rows
from src.db.tracking import already_loaded, log_load_summary, record_run
from src.etl.config import CacheConfig
from src.etl.logging import setup_logging

CACHE_VERSION = CacheConfig.CACHE_VERSION


def test_utils_compat_module_removed() -> None:
    import importlib

    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("src.etl.utils")

# ------------------------------------------------------------------ #
# Helpers                                                             #
# ------------------------------------------------------------------ #


def _make_schema(con: sqlite3.Connection) -> None:
    """Minimal schema for run-log and upsert tests."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS etl_run_log (
            id          INTEGER PRIMARY KEY,
            table_name  TEXT NOT NULL,
            season_id   TEXT,
            loader      TEXT NOT NULL,
            started_at  TEXT NOT NULL,
            finished_at TEXT,
            row_count   INTEGER,
            status      TEXT NOT NULL
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS fruits (
            id   INTEGER PRIMARY KEY,
            name TEXT UNIQUE
        )
    """)
    con.commit()


# ------------------------------------------------------------------ #
# setup_logging                                                       #
# ------------------------------------------------------------------ #


def test_setup_logging_adds_stream_handler_to_root() -> None:
    setup_logging(level="WARNING")
    root = logging.getLogger()
    assert root.level == logging.WARNING
    assert any(isinstance(h, logging.StreamHandler) for h in root.handlers)


def test_setup_logging_replaces_existing_handlers() -> None:
    setup_logging(level="DEBUG")
    handler_count_first = len(logging.getLogger().handlers)
    setup_logging(level="DEBUG")
    handler_count_second = len(logging.getLogger().handlers)
    assert handler_count_first == handler_count_second


def test_setup_logging_with_file_adds_two_handlers(tmp_path: Path) -> None:
    log_file = tmp_path / "test.log"
    setup_logging(level="INFO", log_file=log_file)
    root = logging.getLogger()
    handler_types = [type(h).__name__ for h in root.handlers]
    assert "StreamHandler" in handler_types
    assert "FileHandler" in handler_types
    # Clean up file handlers to avoid ResourceWarning
    for h in root.handlers[:]:
        if isinstance(h, logging.FileHandler):
            h.close()
            root.removeHandler(h)


# ------------------------------------------------------------------ #
# cache_path                                                          #
# ------------------------------------------------------------------ #


def test_cache_path_returns_json_path(monkeypatch, tmp_path: Path) -> None:
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)
    result = cache_path("my_key")
    assert result == tmp_path / "my_key.json"


# ------------------------------------------------------------------ #
# save_cache / load_cache                                             #
# ------------------------------------------------------------------ #


def test_save_and_load_cache_roundtrip(monkeypatch, tmp_path: Path) -> None:
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)
    save_cache("roundtrip", {"x": 1})
    result = load_cache("roundtrip")
    assert result == {"x": 1}


def test_load_cache_returns_none_for_missing_file(monkeypatch, tmp_path: Path) -> None:
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)
    assert load_cache("nonexistent_key") is None


def test_load_cache_returns_none_for_stale_ttl(monkeypatch, tmp_path: Path) -> None:
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)
    save_cache("ttl_key", [1, 2, 3])
    p = tmp_path / "ttl_key.json"
    payload = json.loads(p.read_text())
    payload["ts"] -= 86400 * 5  # 5 days ago
    p.write_text(json.dumps(payload))
    assert load_cache("ttl_key", ttl_days=1) is None


def test_load_cache_returns_data_when_ttl_not_expired(monkeypatch, tmp_path: Path) -> None:
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)
    save_cache("fresh_key", "hello")
    assert load_cache("fresh_key", ttl_days=30) == "hello"


def test_load_cache_returns_none_for_wrong_version(monkeypatch, tmp_path: Path) -> None:
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)
    save_cache("ver_key", "data")
    p = tmp_path / "ver_key.json"
    payload = json.loads(p.read_text())
    payload["v"] = CACHE_VERSION - 1
    p.write_text(json.dumps(payload))
    assert load_cache("ver_key") is None


def test_load_cache_returns_none_for_corrupt_json(monkeypatch, tmp_path: Path) -> None:
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)
    (tmp_path / "bad.json").write_text("{not valid json}", encoding="utf-8")
    assert load_cache("bad") is None


def test_load_cache_returns_none_for_old_format_when_version_2(monkeypatch, tmp_path: Path) -> None:
    """Files with no 'v'/'ts' keys (old v1 format) must return None when CACHE_VERSION >= 2."""
    import src.db.cache.file_cache as cache_mod

    monkeypatch.setattr(cache_mod, "CACHE_DIR", tmp_path)
    (tmp_path / "oldformat.json").write_text(json.dumps({"key": "value"}), encoding="utf-8")
    assert load_cache("oldformat") is None


# ------------------------------------------------------------------ #
# upsert_rows                                                         #
# ------------------------------------------------------------------ #


def _create_and_close_memory_db():
    """Helper to create and properly close an in-memory database."""
    con = sqlite3.connect(":memory:")
    try:
        _make_schema(con)
        yield con
    finally:
        con.close()


def test_upsert_rows_returns_zero_for_empty_list() -> None:
    con = sqlite3.connect(":memory:")
    try:
        _make_schema(con)
        result = upsert_rows(con, "fruits", [])
        assert result == 0
    finally:
        con.close()


def test_upsert_rows_inserts_rows_and_returns_count() -> None:
    con = sqlite3.connect(":memory:")
    try:
        _make_schema(con)
        rows = [{"name": "apple"}, {"name": "banana"}]
        inserted = upsert_rows(con, "fruits", rows)
        assert inserted == 2
        count = con.execute("SELECT COUNT(*) FROM fruits").fetchone()[0]
        assert count == 2
    finally:
        con.close()


def test_upsert_rows_conflict_ignore_skips_duplicates() -> None:
    con = sqlite3.connect(":memory:")
    try:
        _make_schema(con)
        upsert_rows(con, "fruits", [{"name": "apple"}])
        upsert_rows(con, "fruits", [{"name": "apple"}])
        count = con.execute("SELECT COUNT(*) FROM fruits").fetchone()[0]
        assert count == 1
    finally:
        con.close()


def test_upsert_rows_conflict_replace_overwrites() -> None:
    con = sqlite3.connect(":memory:")
    try:
        _make_schema(con)
        upsert_rows(con, "fruits", [{"name": "apple"}])
        upsert_rows(con, "fruits", [{"name": "apple"}], conflict="REPLACE")
        count = con.execute("SELECT COUNT(*) FROM fruits").fetchone()[0]
        assert count == 1
    finally:
        con.close()


def test_upsert_rows_autocommit_false_does_not_commit() -> None:
    con = sqlite3.connect(":memory:")
    try:
        _make_schema(con)
        upsert_rows(con, "fruits", [{"name": "pear"}], autocommit=False)
        con.rollback()
        count = con.execute("SELECT COUNT(*) FROM fruits").fetchone()[0]
        assert count == 0
    finally:
        con.close()


def test_upsert_rows_returns_zero_when_target_table_missing() -> None:
    con = sqlite3.connect(":memory:")
    try:
        result = upsert_rows(con, "missing_table_xyz", [{"name": "apple"}])
        assert result == 0
    finally:
        con.close()


def test_upsert_rows_raises_for_non_missing_operational_error() -> None:
    con = sqlite3.connect(":memory:")
    try:
        _make_schema(con)
        with pytest.raises(sqlite3.OperationalError, match="no column named"):
            upsert_rows(con, "fruits", [{"missing_column": "apple"}])
    finally:
        con.close()


# ------------------------------------------------------------------ #
# already_loaded                                                      #
# ------------------------------------------------------------------ #


def test_already_loaded_returns_false_when_no_record(sqlite_con: sqlite3.Connection) -> None:
    assert already_loaded(sqlite_con, "player_game_log", "2023-24", "test_loader") is False


def test_already_loaded_returns_true_after_record_run(sqlite_con: sqlite3.Connection) -> None:
    record_run(sqlite_con, "player_game_log", "2023-24", "test_loader", 100, "ok")
    assert already_loaded(sqlite_con, "player_game_log", "2023-24", "test_loader") is True


def test_already_loaded_returns_false_for_error_status(sqlite_con: sqlite3.Connection) -> None:
    record_run(sqlite_con, "player_game_log", "2023-24", "test_loader", 0, "error")
    assert already_loaded(sqlite_con, "player_game_log", "2023-24", "test_loader") is False


def test_already_loaded_with_none_season_id(sqlite_con: sqlite3.Connection) -> None:
    assert already_loaded(sqlite_con, "dim_season", None, "dimensions.load_seasons") is False
    record_run(sqlite_con, "dim_season", None, "dimensions.load_seasons", 79, "ok")
    assert already_loaded(sqlite_con, "dim_season", None, "dimensions.load_seasons") is True


def test_already_loaded_returns_false_when_table_missing() -> None:
    con = sqlite3.connect(":memory:")
    try:
        result = already_loaded(con, "nonexistent_table", "2023-24", "loader")
        assert result is False
    finally:
        con.close()


# ------------------------------------------------------------------ #
# record_run                                                          #
# ------------------------------------------------------------------ #


def test_record_run_inserts_row_to_etl_run_log(sqlite_con: sqlite3.Connection) -> None:
    record_run(sqlite_con, "fact_game", "2023-24", "game_logs.load_season", 500, "ok")
    row = sqlite_con.execute(
        "SELECT table_name, season_id, loader, row_count, status FROM etl_run_log"
    ).fetchone()
    assert row == ("fact_game", "2023-24", "game_logs.load_season", 500, "ok")


def test_record_run_uses_provided_started_at(sqlite_con: sqlite3.Connection) -> None:
    record_run(
        sqlite_con,
        "fact_game",
        None,
        "loader",
        0,
        "ok",
        started_at="2024-01-01T00:00:00+00:00",
    )
    row = sqlite_con.execute("SELECT started_at FROM etl_run_log").fetchone()
    assert row[0] == "2024-01-01T00:00:00+00:00"


def test_record_run_silently_ignores_missing_table() -> None:
    con = sqlite3.connect(":memory:")
    try:
        record_run(con, "fact_game", "2023-24", "loader", 0, "ok")
    finally:
        con.close()


# ------------------------------------------------------------------ #
# log_load_summary                                                    #
# ------------------------------------------------------------------ #


def test_log_load_summary_returns_row_count(sqlite_con: sqlite3.Connection) -> None:
    for i in range(5):
        sqlite_con.execute(
            "INSERT INTO dim_season (season_id, start_year, end_year) VALUES (?, ?, ?)",
            (f"200{i}-0{i + 1}", 2000 + i, 2001 + i),
        )
    sqlite_con.commit()
    count = log_load_summary(sqlite_con, "dim_season")
    assert count == 5


def test_log_load_summary_warns_when_count_below_min_rows(
    sqlite_con: sqlite3.Connection,
    caplog,
) -> None:
    import logging

    with caplog.at_level(logging.WARNING):
        count = log_load_summary(sqlite_con, "dim_season", min_rows=9999)
    assert count == 0
    assert any("Expected minimum" in r.message for r in caplog.records)


def test_log_load_summary_with_season_filter(sqlite_con: sqlite3.Connection) -> None:
    sqlite_con.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES ('2023-24', 2023, 2024)"
    )
    sqlite_con.execute(
        "INSERT INTO dim_team (team_id, abbreviation, full_name, city, nickname) "
        "VALUES ('1610612747','LAL','Los Angeles Lakers','Los Angeles','Lakers')"
    )
    sqlite_con.execute(
        "INSERT INTO fact_game (game_id, season_id, game_date, home_team_id, away_team_id, "
        "home_score, away_score, season_type, status) "
        "VALUES ('001', '2023-24', '2023-10-24', '1610612747', '1610612747', 100, 90, 'Regular Season', 'Final')"
    )
    sqlite_con.commit()
    count = log_load_summary(sqlite_con, "fact_game", season_id="2023-24")
    assert count == 1


# ------------------------------------------------------------------ #
# transaction context manager                                         #
# ------------------------------------------------------------------ #


def test_transaction_commits_on_success(sqlite_con: sqlite3.Connection) -> None:
    with transaction(sqlite_con):
        sqlite_con.execute(
            "INSERT INTO dim_season (season_id, start_year, end_year) VALUES ('2030-31', 2030, 2031)"
        )
    row = sqlite_con.execute(
        "SELECT season_id FROM dim_season WHERE season_id='2030-31'"
    ).fetchone()
    assert row is not None


def test_transaction_rolls_back_on_exception(sqlite_con: sqlite3.Connection) -> None:
    with pytest.raises(ValueError):
        with transaction(sqlite_con):
            sqlite_con.execute(
                "INSERT INTO dim_season (season_id, start_year, end_year) VALUES ('2031-32', 2031, 2032)"
            )
            raise ValueError("intentional rollback")
    row = sqlite_con.execute(
        "SELECT season_id FROM dim_season WHERE season_id='2031-32'"
    ).fetchone()
    assert row is None
