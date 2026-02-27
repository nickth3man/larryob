"""Tests: raw backfill orchestrator."""

import sqlite3
from pathlib import Path
from unittest.mock import patch

from src.etl.backfill import _orchestrator as orchestrator_mod
from src.etl.backfill._orchestrator import run_raw_backfill


def test_run_raw_backfill_handles_missing_input_files(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    summary = run_raw_backfill(sqlite_con, tmp_path)

    team_history_count = sqlite_con.execute("SELECT COUNT(*) FROM dim_team_history").fetchone()[0]
    draft_count = sqlite_con.execute("SELECT COUNT(*) FROM fact_draft").fetchone()[0]
    assert team_history_count == 0
    assert draft_count == 0
    assert len(summary["failed"]) == 0


def test_run_raw_backfill_logs_and_continues_on_loader_error(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    with patch.object(orchestrator_mod, "load_team_history", side_effect=RuntimeError("boom")):
        with patch.object(orchestrator_mod, "enrich_dim_team") as enrich_patch:
            summary = run_raw_backfill(sqlite_con, tmp_path)

    enrich_patch.assert_called_once_with(sqlite_con, tmp_path)
    assert summary["failed"] == ["team_history"]


def test_run_raw_backfill_fail_fast_stops_on_first_error(
    sqlite_con: sqlite3.Connection,
    tmp_path: Path,
) -> None:
    with patch.object(orchestrator_mod, "load_team_history", side_effect=RuntimeError("boom")):
        with patch.object(orchestrator_mod, "enrich_dim_team") as enrich_patch:
            summary = run_raw_backfill(sqlite_con, tmp_path, fail_fast=True)

    enrich_patch.assert_not_called()
    assert summary["failed"] == ["team_history"]


def test_loader_registry_includes_database_completion_loaders() -> None:
    loader_names = [config.name for config in orchestrator_mod._LOADERS]

    assert "player_career" in loader_names
    assert "all_star" in loader_names
    assert "all_nba" in loader_names
    assert "all_nba_votes" in loader_names

    assert loader_names.index("player_career") > loader_names.index("dim_player_enrich")
    assert loader_names.index("all_star") > loader_names.index("awards")
