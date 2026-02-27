"""Tests for src.db.cache.file_cache — hit/miss, TTL, versioning, atomicity."""

import json
import os
import time

import pytest

# ------------------------------------------------------------------ #
# Helpers                                                             #
# ------------------------------------------------------------------ #


def _write_raw_cache(path, payload: dict):
    """Write a raw JSON payload directly (bypassing save_cache) for fixture setup."""
    path.write_text(json.dumps(payload), encoding="utf-8")


# ------------------------------------------------------------------ #
# Fixtures — redirect cache dir to a temp directory                  #
# ------------------------------------------------------------------ #


@pytest.fixture(autouse=True)
def isolated_cache(tmp_path, monkeypatch):
    """Point the cache at a throwaway temp directory for every test."""
    monkeypatch.setenv("LARRYOB_CACHE_DIR", str(tmp_path))
    # Re-import so module-level CACHE_DIR picks up the env override.
    import importlib

    import src.db.cache.file_cache as fc_mod

    importlib.reload(fc_mod)
    yield fc_mod
    # Reload again to restore defaults for other test modules.
    monkeypatch.delenv("LARRYOB_CACHE_DIR", raising=False)
    importlib.reload(fc_mod)


# ------------------------------------------------------------------ #
# cache_path                                                          #
# ------------------------------------------------------------------ #


def test_cache_path_builds_json_filename(isolated_cache, tmp_path):
    p = isolated_cache.cache_path("my_key")
    assert p.parent == tmp_path
    assert p.name == "my_key.json"


# ------------------------------------------------------------------ #
# Round-trip                                                          #
# ------------------------------------------------------------------ #


def test_save_and_load_roundtrip(isolated_cache):
    fc = isolated_cache
    fc.save_cache("key1", {"data": [1, 2, 3]})
    result = fc.load_cache("key1")
    assert result == {"data": [1, 2, 3]}


def test_save_and_load_list(isolated_cache):
    fc = isolated_cache
    fc.save_cache("key_list", [10, 20, 30])
    assert fc.load_cache("key_list") == [10, 20, 30]


def test_save_and_load_primitive(isolated_cache):
    fc = isolated_cache
    fc.save_cache("key_str", "hello")
    assert fc.load_cache("key_str") == "hello"


# ------------------------------------------------------------------ #
# Cache miss                                                          #
# ------------------------------------------------------------------ #


def test_load_cache_returns_none_for_missing_key(isolated_cache):
    assert isolated_cache.load_cache("nonexistent") is None


# ------------------------------------------------------------------ #
# Cache version                                                       #
# ------------------------------------------------------------------ #


def test_load_cache_returns_none_for_wrong_version(isolated_cache, tmp_path):
    fc = isolated_cache
    wrong_version = fc.CACHE_VERSION + 99
    payload = {"v": wrong_version, "ts": time.time(), "data": "stale"}
    _write_raw_cache(fc.cache_path("old_key"), payload)
    assert fc.load_cache("old_key") is None


def test_load_cache_returns_data_for_correct_version(isolated_cache):
    fc = isolated_cache
    fc.save_cache("right_version", 42)
    assert fc.load_cache("right_version") == 42


# ------------------------------------------------------------------ #
# TTL                                                                 #
# ------------------------------------------------------------------ #


def test_load_cache_respects_ttl_fresh(isolated_cache):
    fc = isolated_cache
    fc.save_cache("fresh_key", "value")
    result = fc.load_cache("fresh_key", ttl_days=1.0)
    assert result == "value"


def test_load_cache_returns_none_for_expired_ttl(isolated_cache, tmp_path):
    fc = isolated_cache
    # Write a cache entry timestamped 3 days ago
    stale_ts = time.time() - 3 * 86400
    payload = {"v": fc.CACHE_VERSION, "ts": stale_ts, "data": "old"}
    _write_raw_cache(fc.cache_path("stale_key"), payload)
    assert fc.load_cache("stale_key", ttl_days=1.0) is None


def test_load_cache_returns_data_when_no_ttl_given(isolated_cache, tmp_path):
    fc = isolated_cache
    stale_ts = time.time() - 365 * 86400  # 1 year ago
    payload = {"v": fc.CACHE_VERSION, "ts": stale_ts, "data": "ancient"}
    _write_raw_cache(fc.cache_path("ancient_key"), payload)
    # No ttl_days argument — should not expire
    assert fc.load_cache("ancient_key") == "ancient"


# ------------------------------------------------------------------ #
# Corrupted file                                                      #
# ------------------------------------------------------------------ #


def test_load_cache_returns_none_for_corrupted_json(isolated_cache, tmp_path, caplog):
    import logging

    fc = isolated_cache
    bad_path = fc.cache_path("bad_json")
    bad_path.write_text("NOT_JSON{{{{", encoding="utf-8")
    with caplog.at_level(logging.WARNING, logger="src.db.cache.file_cache"):
        result = fc.load_cache("bad_json")
    assert result is None
    assert any("corrupt" in r.message.lower() or "bad_json" in r.message for r in caplog.records)


# ------------------------------------------------------------------ #
# Atomicity                                                           #
# ------------------------------------------------------------------ #


def test_save_cache_is_atomic(isolated_cache, tmp_path, monkeypatch):
    """save_cache should use os.replace so no partial writes are visible."""
    fc = isolated_cache
    calls = []
    original_replace = os.replace

    def spy_replace(src, dst):
        calls.append((src, dst))
        return original_replace(src, dst)

    monkeypatch.setattr(os, "replace", spy_replace)
    fc.save_cache("atomic_key", {"x": 1})
    assert len(calls) == 1
    src, dst = calls[0]
    # Source should be a temp file in the same directory
    assert os.path.dirname(src) == str(tmp_path)
    assert str(dst).endswith("atomic_key.json")


# ------------------------------------------------------------------ #
# Old-format cache file (no envelope)                                #
# ------------------------------------------------------------------ #


def test_load_cache_returns_none_for_bare_json_without_envelope(isolated_cache, tmp_path):
    """Files written without the v/ts/data envelope should be a cache miss."""
    fc = isolated_cache
    fc.cache_path("bare").write_text(json.dumps({"just": "data"}), encoding="utf-8")
    assert fc.load_cache("bare") is None
