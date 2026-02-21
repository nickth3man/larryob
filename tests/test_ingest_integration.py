import sqlite3
from unittest.mock import patch

import ingest


def test_ingest_dims_only(tmp_path, monkeypatch):
    """Smoke test for ingest pipeline with --dims-only flag."""
    db_file = tmp_path / "test_ingest.db"

    # Mock init_db to use our temp file
    from src.db.schema import init_db
    con = init_db(db_file)

    # Set up CLI args
    test_args = ["ingest.py", "--dims-only", "--seasons", "2023-24"]

    # Mock network calls
    with patch("sys.argv", test_args):
        with patch("src.etl.dimensions.nba_teams_static.get_teams", return_value=[]):
            with patch("src.etl.dimensions.nba_players_static.get_players", return_value=[]):
                with patch("ingest.init_db", return_value=con):
                    ingest.main()

    # Verify tables exist and connection closed successfully
    con = sqlite3.connect(db_file)
    tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    assert "dim_team" in tables
    assert "dim_player" in tables
    assert "dim_season" in tables
    con.close()


def test_ingest_full_pipeline_mocked(tmp_path, monkeypatch):
    """Smoke test for full ingest pipeline with mocked external calls."""
    db_file = tmp_path / "test_ingest_full.db"

    # Mock init_db to use our temp file
    from src.db.schema import init_db
    con = init_db(db_file)

    # Set up CLI args
    test_args = [
        "ingest.py",
        "--seasons", "2023-24",
        "--awards",
        "--salaries",
        "--rosters",
        "--pbp-limit", "1"
    ]

    # Mock network calls to avoid actual HTTP requests
    with patch("sys.argv", test_args):
        with patch("src.db.schema.init_db", return_value=con):
            with patch("ingest.init_db", return_value=con):
                with patch("ingest.run_dimensions"):
                    with patch("ingest.load_all_awards"):
                        with patch("ingest.load_salaries_for_seasons"):
                            with patch("ingest.load_rosters_for_seasons"):
                                with patch("ingest.load_multiple_seasons"):
                                    with patch("ingest.load_season_pbp"):
                                        ingest.main()

    # Verify tables exist
    con = sqlite3.connect(db_file)
    tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    assert "fact_game" in tables
    con.close()

    # Clean up singleton connections from any duckdb imports that might have happened
    try:
        import src.db.analytics as analytics
        cached_con = getattr(analytics._local, "cached_con", None)
        if cached_con is not None:
            try:
                cached_con.close()
            except Exception:
                pass
            setattr(analytics._local, "cached_con", None)
            setattr(analytics._local, "cached_sqlite_path", None)
            setattr(analytics._local, "cached_duck_db_path", None)
    except Exception:
        pass
