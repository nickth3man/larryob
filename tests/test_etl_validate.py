import sqlite3
import pytest
from pathlib import Path
from src.etl.validate import validate_rows, check_game_stat_consistency
from src.etl.utils import CACHE_VERSION, load_cache, save_cache
import time

def test_validate_rows_player_game_log():
    rows = [
        # Valid
        {"game_id": "1", "player_id": "A", "fgm": 5, "fga": 10, "pts": 12, "oreb": 1, "dreb": 2, "reb": 3},
        # Invalid FGM > FGA
        {"game_id": "2", "player_id": "B", "fgm": 10, "fga": 5, "pts": 20, "oreb": 0, "dreb": 0, "reb": 0},
        # Invalid PTS < 0
        {"game_id": "3", "player_id": "C", "fgm": 1, "fga": 2, "pts": -1, "oreb": 0, "dreb": 0, "reb": 0},
        # Invalid REB sum
        {"game_id": "4", "player_id": "D", "fgm": 1, "fga": 2, "pts": 2, "oreb": 1, "dreb": 1, "reb": 5},
    ]
    
    valid = validate_rows("player_game_log", rows)
    assert len(valid) == 1
    assert valid[0]["player_id"] == "A"


def test_validate_rows_fact_game():
    rows = [
        # Valid
        {"game_id": "1", "home_score": 100, "away_score": 90, "game_date": "2024-10-22"},
        # Invalid score
        {"game_id": "2", "home_score": -5, "away_score": 90, "game_date": "2024-10-22"},
        # Invalid date format
        {"game_id": "3", "home_score": 100, "away_score": 90, "game_date": "2024/10/22"},
    ]
    
    valid = validate_rows("fact_game", rows)
    assert len(valid) == 1
    assert valid[0]["game_id"] == "1"


def test_validate_rows_shooting_zones():
    rows = [
        # Valid (sums to ~1.0)
        {"bref_player_id": "A", "pct_fga_0_3": 0.2, "pct_fga_3_10": 0.2, "pct_fga_10_16": 0.2, "pct_fga_16_3p": 0.2, "pct_fga_3p": 0.2},
        # Invalid (sums to 0.5)
        {"bref_player_id": "B", "pct_fga_0_3": 0.1, "pct_fga_3_10": 0.1, "pct_fga_10_16": 0.1, "pct_fga_16_3p": 0.1, "pct_fga_3p": 0.1},
        # Nulls (bypasses rule)
        {"bref_player_id": "C", "pct_fga_0_3": None, "pct_fga_3_10": None, "pct_fga_10_16": None, "pct_fga_16_3p": None, "pct_fga_3p": None},
    ]
    
    valid = validate_rows("fact_player_shooting_season", rows)
    assert len(valid) == 2
    assert set(r["bref_player_id"] for r in valid) == {"A", "C"}


def test_check_game_stat_consistency(sqlite_con_with_data: sqlite3.Connection):
    from src.etl.utils import upsert_rows
    
    # Insert team game log with 100 pts
    upsert_rows(sqlite_con_with_data, "team_game_log", [{
        "game_id": "0022300001", "team_id": "1610612747", "pts": 100, "reb": 40, "ast": 20
    }])
    
    # Insert player game logs summing to 90 pts (mismatch)
    upsert_rows(sqlite_con_with_data, "player_game_log", [
        {"game_id": "0022300001", "player_id": "2544", "team_id": "1610612747", "pts": 50, "reb": 20, "ast": 10},
        {"game_id": "0022300001", "player_id": "203999", "team_id": "1610612747", "pts": 40, "reb": 20, "ast": 10},
    ])
    sqlite_con_with_data.commit()
    
    warnings = check_game_stat_consistency(sqlite_con_with_data, "0022300001")
    assert len(warnings) == 1
    assert "PTS mismatch" in warnings[0]
    assert "Team=100" in warnings[0]
    assert "Players=90" in warnings[0]


def test_cache_versioning_and_ttl(tmp_path, monkeypatch):
    # Point CACHE_DIR to temp path
    from src.etl import utils
    monkeypatch.setattr(utils, "CACHE_DIR", tmp_path)
    
    key = "test_key"
    data = {"hello": "world"}
    
    save_cache(key, data)
    
    # 1. Normal load works
    assert load_cache(key) == data
    
    # 2. Load with TTL works if fresh
    assert load_cache(key, ttl_days=1) == data
    
    # 3. Load with TTL fails if expired
    # Manipulate the saved timestamp to be 2 days old
    import json
    p = tmp_path / f"{key}.json"
    payload = json.loads(p.read_text())
    payload["ts"] -= 86400 * 2
    p.write_text(json.dumps(payload))
    
    assert load_cache(key, ttl_days=1) is None
    
    # 4. Version mismatch fails
    payload["ts"] += 86400 * 2  # restore time
    payload["v"] = utils.CACHE_VERSION - 1
    p.write_text(json.dumps(payload))
    
    assert load_cache(key) is None