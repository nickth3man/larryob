import sqlite3
from unittest.mock import patch

import pytest

import src.pipeline.cli as ingest


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
                with patch("src.pipeline.cli.init_db", return_value=con):
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
            with patch("src.pipeline.cli.init_db", return_value=con):
                with patch("src.pipeline.stages.run_dimensions"):
                    with patch("src.pipeline.executor.load_all_awards"):
                        with patch("src.pipeline.executor.load_salaries_for_seasons"):
                            with patch("src.pipeline.executor.load_rosters_for_seasons"):
                                with patch("src.pipeline.stages.load_multiple_seasons"):
                                    with patch("src.pipeline.stages.load_season_pbp"):
                                        with patch("src.pipeline.stages.run_consistency_checks", return_value=0) as reconcile:
                                            ingest.main()
                                            reconcile.assert_called_once_with(con, "2023-24")

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


def test_ingest_reconciliation_discrepancy_raises_by_default(tmp_path):
    db_file = tmp_path / "test_ingest_reconcile_fail.db"
    from src.db.schema import init_db
    con = init_db(db_file)

    test_args = ["ingest.py", "--seasons", "2023-24"]
    with patch("sys.argv", test_args):
        with patch("src.pipeline.cli.init_db", return_value=con):
            with patch("src.pipeline.stages.run_dimensions"):
                with patch("src.pipeline.stages.load_multiple_seasons"):
                    with patch("src.pipeline.stages.run_consistency_checks", return_value=2):
                        with pytest.raises(RuntimeError, match="Reconciliation checks found 2"):
                            ingest.main()


def test_ingest_reconciliation_warn_only_continues(tmp_path):
    db_file = tmp_path / "test_ingest_reconcile_warn.db"
    from src.db.schema import init_db
    con = init_db(db_file)

    test_args = ["ingest.py", "--seasons", "2023-24", "--reconciliation-warn-only"]
    with patch("sys.argv", test_args):
        with patch("src.pipeline.cli.init_db", return_value=con):
            with patch("src.pipeline.stages.run_dimensions"):
                with patch("src.pipeline.stages.load_multiple_seasons"):
                    with patch("src.pipeline.stages.run_consistency_checks", return_value=3) as reconcile:
                        ingest.main()
                        reconcile.assert_called_once_with(con, "2023-24")


def test_ingest_skip_reconciliation_bypasses_checks(tmp_path):
    db_file = tmp_path / "test_ingest_skip_reconcile.db"
    from src.db.schema import init_db
    con = init_db(db_file)

    test_args = ["ingest.py", "--seasons", "2023-24", "--skip-reconciliation"]
    with patch("sys.argv", test_args):
        with patch("src.pipeline.cli.init_db", return_value=con):
            with patch("src.pipeline.stages.run_dimensions"):
                with patch("src.pipeline.stages.load_multiple_seasons"):
                    with patch("src.pipeline.stages.run_consistency_checks") as reconcile:
                        ingest.main()
                        reconcile.assert_not_called()


def test_ingest_analytics_only_requires_view() -> None:
    with patch("sys.argv", ["ingest.py", "--analytics-only"]):
        with pytest.raises(SystemExit):
            ingest.main()


def test_ingest_analytics_only_runs_query_without_init_db() -> None:
    with patch("sys.argv", ["ingest.py", "--analytics-only", "--analytics-view", "vw_team_standings"]):
        with patch("src.pipeline.cli.run_analytics_view") as query_view:
            with patch("src.pipeline.cli.init_db") as init_db_patch:
                ingest.main()
                query_view.assert_called_once()
                init_db_patch.assert_not_called()


def test_ingest_metrics_summary_and_export_hooks(tmp_path):
    db_file = tmp_path / "test_ingest_metrics.db"
    from src.db.schema import init_db

    con = init_db(db_file)
    args = [
        "ingest.py",
        "--dims-only",
        "--metrics",
        "--metrics-summary",
        "--metrics-export-endpoint",
        "http://localhost:9999/metrics",
    ]

    with patch("sys.argv", args):
        with patch("src.pipeline.cli.init_db", return_value=con):
            with patch("src.pipeline.stages.run_dimensions"):
                with patch("src.pipeline.executor.log_metrics_summary") as log_summary:
                    with patch("src.pipeline.executor.export_metrics") as export_metrics_mock:
                        ingest.main()
                        log_summary.assert_called_once()
                        export_metrics_mock.assert_called_once_with("http://localhost:9999/metrics")


def test_ingest_raw_backfill_fail_fast_raises(tmp_path):
    db_file = tmp_path / "test_ingest_raw_backfill_fail_fast.db"
    from src.db.schema import init_db

    con = init_db(db_file)
    args = ["ingest.py", "--raw-backfill", "--raw-backfill-fail-fast", "--seasons", "2023-24"]
    with patch("sys.argv", args):
        with patch("src.pipeline.cli.init_db", return_value=con):
            with patch("src.pipeline.stages.run_dimensions"):
                with patch(
                    "src.pipeline.stages.run_raw_backfill",
                    return_value={"ok": [], "skipped": [], "failed": ["games"], "details": []},
                ):
                    with pytest.raises(RuntimeError, match="Raw backfill failed in fail-fast mode"):
                        ingest.main()
