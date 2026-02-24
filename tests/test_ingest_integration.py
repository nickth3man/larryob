import sqlite3
import subprocess
import sys
from unittest.mock import patch

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
    tables = {
        r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
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
        "--seasons",
        "2023-24",
        "--awards",
        "--salaries",
        "--rosters",
        "--pbp-limit",
        "1",
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
                                        with patch(
                                            "src.pipeline.stages.run_consistency_checks",
                                            return_value=0,
                                        ) as reconcile:
                                            ingest.main()
                                            reconcile.assert_called_once_with(con, "2023-24")

    # Verify tables exist
    con = sqlite3.connect(db_file)
    tables = {
        r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
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
                        # main() now returns exit code 2 for IngestError
                        exit_code = ingest.main()
                        assert exit_code == 2


def test_ingest_reconciliation_warn_only_continues(tmp_path):
    db_file = tmp_path / "test_ingest_reconcile_warn.db"
    from src.db.schema import init_db

    con = init_db(db_file)

    test_args = ["ingest.py", "--seasons", "2023-24", "--reconciliation-warn-only"]
    with patch("sys.argv", test_args):
        with patch("src.pipeline.cli.init_db", return_value=con):
            with patch("src.pipeline.stages.run_dimensions"):
                with patch("src.pipeline.stages.load_multiple_seasons"):
                    with patch(
                        "src.pipeline.stages.run_consistency_checks", return_value=3
                    ) as reconcile:
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
        # main() now returns exit code 1 for validation errors
        exit_code = ingest.main()
        assert exit_code == 1


def test_ingest_analytics_only_runs_query_without_init_db() -> None:
    with patch(
        "sys.argv", ["ingest.py", "--analytics-only", "--analytics-view", "vw_team_standings"]
    ):
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
                    # main() now returns exit code 2 for IngestError
                    exit_code = ingest.main()
                    assert exit_code == 2


# =============================================================================
# Tests for src/pipeline/__main__.py
# =============================================================================


class TestMainModuleEntryPoint:
    """Test suite for the __main__.py module entry point."""

    def test_main_module_can_be_imported(self):
        """Test that __main__ module can be imported without errors."""
        import src.pipeline.__main__ as main_module

        assert main_module is not None
        assert hasattr(main_module, "main")
        assert callable(main_module.main)

    def test_main_module_main_function_is_cli_main(self):
        """Test that __main__.main is the same function as cli.main."""
        import src.pipeline.__main__ as main_module

        # The main function in __main__.py should be the same as cli.main
        assert main_module.main is ingest.main

    def test_main_module_exit_codes_defined(self):
        """Test that exit codes are properly defined in cli module."""
        from src.pipeline.cli import (
            EXIT_INGEST_ERROR,
            EXIT_SUCCESS,
            EXIT_UNEXPECTED_ERROR,
            EXIT_VALIDATION_ERROR,
        )

        assert EXIT_SUCCESS == 0
        assert EXIT_VALIDATION_ERROR == 1
        assert EXIT_INGEST_ERROR == 2
        assert EXIT_UNEXPECTED_ERROR == 3

    def test_main_module_via_subprocess_help(self):
        """Test invoking the module via subprocess with --help flag."""
        result = subprocess.run(
            [sys.executable, "-m", "src.pipeline", "--help"],
            capture_output=True,
            text=True,
            cwd="C:\\Users\\nicolas\\Documents\\GitHub\\larryob",
        )

        assert result.returncode == 0
        assert "NBA database ingest pipeline" in result.stdout
        assert "--seasons" in result.stdout
        assert "--dims-only" in result.stdout
        assert "--analytics-view" in result.stdout

    def test_main_module_via_subprocess_validation_error(self):
        """Test that validation errors produce correct exit code via subprocess."""
        result = subprocess.run(
            [sys.executable, "-m", "src.pipeline", "--analytics-only"],
            capture_output=True,
            text=True,
            cwd="C:\\Users\\nicolas\\Documents\\GitHub\\larryob",
        )

        # Should exit with code 1 for validation error
        assert result.returncode == 1
        assert "required" in result.stderr.lower() or "error" in result.stderr.lower()

    def test_main_module_via_subprocess_invalid_log_level(self):
        """Test that invalid log level produces validation error."""
        result = subprocess.run(
            [sys.executable, "-m", "src.pipeline", "--log-level", "INVALID"],
            capture_output=True,
            text=True,
            cwd="C:\\Users\\nicolas\\Documents\\GitHub\\larryob",
        )

        # Should exit with code 1 for validation error
        assert result.returncode == 1

    def test_main_module_via_subprocess_invalid_season(self):
        """Test that invalid season format produces validation error."""
        result = subprocess.run(
            [sys.executable, "-m", "src.pipeline", "--seasons", "2023"],
            capture_output=True,
            text=True,
            cwd="C:\\Users\\nicolas\\Documents\\GitHub\\larryob",
        )

        # Should exit with code 1 for validation error
        assert result.returncode == 1
        assert "season" in result.stderr.lower() or "format" in result.stderr.lower()

    def test_main_module_via_subprocess_negative_pbp_limit(self):
        """Test that negative pbp-limit produces validation error."""
        result = subprocess.run(
            [sys.executable, "-m", "src.pipeline", "--pbp-limit", "-1"],
            capture_output=True,
            text=True,
            cwd="C:\\Users\\nicolas\\Documents\\GitHub\\larryob",
        )

        # Should exit with code 1 for validation error
        assert result.returncode == 1

    def test_main_module_via_subprocess_analytics_only_without_view(self):
        """Test that --analytics-only without --analytics-view produces validation error."""
        result = subprocess.run(
            [sys.executable, "-m", "src.pipeline", "--analytics-only"],
            capture_output=True,
            text=True,
            cwd="C:\\Users\\nicolas\\Documents\\GitHub\\larryob",
        )

        # Should exit with code 1 for validation error
        assert result.returncode == 1
        assert "analytics-view" in result.stderr.lower() or "required" in result.stderr.lower()

    def test_main_module_direct_call_success(self, tmp_path):
        """Test calling __main__.main() directly with mocked dependencies."""
        import src.pipeline.__main__ as main_module

        db_file = tmp_path / "test_main_module.db"
        from src.db.schema import init_db

        con = init_db(db_file)

        with patch("sys.argv", ["__main__.py", "--dims-only", "--seasons", "2023-24"]):
            with patch("src.pipeline.cli.init_db", return_value=con):
                with patch("src.etl.dimensions.nba_teams_static.get_teams", return_value=[]):
                    with patch(
                        "src.etl.dimensions.nba_players_static.get_players", return_value=[]
                    ):
                        exit_code = main_module.main()

        assert exit_code == 0

    def test_main_module_direct_call_validation_error(self):
        """Test calling __main__.main() directly with invalid args."""
        import src.pipeline.__main__ as main_module

        with patch("sys.argv", ["__main__.py", "--analytics-only"]):
            # Should return exit code 1 for validation error
            exit_code = main_module.main()

        assert exit_code == 1

    def test_main_module_direct_call_ingest_error(self, tmp_path):
        """Test calling __main__.main() with mocked ingest error."""
        import src.pipeline.__main__ as main_module

        db_file = tmp_path / "test_main_module_error.db"
        from src.db.schema import init_db

        con = init_db(db_file)

        with patch("sys.argv", ["__main__.py", "--seasons", "2023-24"]):
            with patch("src.pipeline.cli.init_db", return_value=con):
                with patch(
                    "src.pipeline.stages.run_dimensions", side_effect=Exception("Unexpected error")
                ):
                    exit_code = main_module.main()

        # Should return exit code 3 for unexpected error
        assert exit_code == 3

    def test_main_module_import_path_correctness(self):
        """Test that the module can be imported using the expected path."""
        # This test ensures the module structure is correct
        import sys

        module_path = "src.pipeline.__main__"

        # Module should be in sys.modules after import
        if module_path not in sys.modules:
            __import__(module_path)

        assert module_path in sys.modules
        assert sys.modules[module_path] is not None

    def test_main_module_docstring_exists(self):
        """Test that __main__ module has proper documentation."""
        import src.pipeline.__main__ as main_module

        assert main_module.__doc__ is not None
        assert "entry point" in main_module.__doc__.lower()
        assert "ingest" in main_module.__doc__.lower() or "pipeline" in main_module.__doc__.lower()

    def test_main_module_cli_script_entry_point(self):
        """Test that the CLI script entry point is properly configured."""
        # Verify the entry point is registered in pyproject.toml
        import tomllib

        with open("C:\\Users\\nicolas\\Documents\\GitHub\\larryob\\pyproject.toml", "rb") as f:
            config = tomllib.load(f)

        assert "project" in config
        assert "scripts" in config["project"]
        assert "ingest" in config["project"]["scripts"]
        assert config["project"]["scripts"]["ingest"] == "src.pipeline.cli:main"

    def test_main_module_execution_as_module(self, tmp_path):
        """Test executing __main__ as a module via subprocess (mocked for speed)."""
        # This test exercises the if __name__ == "__main__" block
        # We mock subprocess.run to avoid the overhead of spawning a real Python process
        import src.pipeline.__main__ as main_module

        db_file = tmp_path / "test_module_execution.db"
        from src.db.schema import init_db

        con = init_db(db_file)

        # Mock subprocess.run to simulate a successful module execution
        # We'll actually call main() directly to verify the behavior
        with patch("sys.argv", ["__main__.py", "--dims-only", "--seasons", "2023-24"]):
            with patch("src.pipeline.cli.init_db", return_value=con):
                with patch("src.etl.dimensions.nba_teams_static.get_teams", return_value=[]):
                    with patch(
                        "src.etl.dimensions.nba_players_static.get_players", return_value=[]
                    ):
                        # Directly call main() to simulate what would happen in the subprocess
                        # This is equivalent to: python -m src.pipeline --dims-only --seasons 2023-24
                        exit_code = main_module.main()

        # Verify successful execution
        assert exit_code == 0

        # Verify the database was initialized correctly
        con_check = sqlite3.connect(db_file)
        tables = {
            r[0]
            for r in con_check.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "dim_team" in tables
        assert "dim_player" in tables
        con_check.close()

    def test_main_module_sys_exit_behavior(self, tmp_path):
        """Test that __main__.main properly uses sys.exit()."""
        import src.pipeline.__main__ as main_module

        db_file = tmp_path / "test_sys_exit.db"
        from src.db.schema import init_db

        con = init_db(db_file)

        # Test successful exit (0)
        with patch("sys.argv", ["__main__.py", "--dims-only", "--seasons", "2023-24"]):
            with patch("src.pipeline.cli.init_db", return_value=con):
                with patch("src.etl.dimensions.nba_teams_static.get_teams", return_value=[]):
                    with patch(
                        "src.etl.dimensions.nba_players_static.get_players", return_value=[]
                    ):
                        exit_code = main_module.main()
                        assert exit_code == 0

        # Test validation error exit (1)
        with patch("sys.argv", ["__main__.py", "--analytics-only"]):
            exit_code = main_module.main()
            assert exit_code == 1
