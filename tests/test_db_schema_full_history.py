"""Tests for full-history schema additions."""

from src.db.schema import init_db


def test_full_history_tables_exist(tmp_path):
    """Verify that coach and identity crosswalk tables are created."""
    db_path = tmp_path / "test.db"
    con = init_db(db_path)
    try:
        names = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert "dim_coach" in names
        assert "fact_team_coach_game" in names
        assert "dim_player_identifier" in names
        assert "dim_team_identifier" in names
    finally:
        con.close()
