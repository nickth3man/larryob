"""Tests for src.pipeline.stages — stage runners and reconciliation logic."""

import sqlite3
from unittest.mock import MagicMock, call, patch

import pytest

from src.pipeline.exceptions import ReconciliationError
from src.pipeline.models import IngestConfig
from src.pipeline.stages import (
    run_dimensions_stage,
    run_game_logs_stage,
    run_pbp_stage,
    run_raw_backfill_stage,
    run_reconciliation,
)


# ------------------------------------------------------------------ #
# Helpers                                                             #
# ------------------------------------------------------------------ #


def _config(**kwargs) -> IngestConfig:
    defaults = dict(
        seasons=("2023-24",),
        dims_only=False,
        enrich_bio=False,
        awards=False,
        salaries=False,
        rosters=False,
        include_playoffs=False,
        pbp_limit=0,
        raw_backfill=False,
        raw_backfill_fail_fast=False,
        skip_reconciliation=False,
        reconciliation_warn_only=False,
        metrics_enabled=False,
        metrics_summary=False,
        metrics_export_endpoint=None,
        runlog_tail=12,
    )
    defaults.update(kwargs)
    return IngestConfig(**defaults)


# ------------------------------------------------------------------ #
# run_dimensions_stage                                                #
# ------------------------------------------------------------------ #


def test_run_dimensions_stage_calls_run_all():
    con = MagicMock(spec=sqlite3.Connection)
    cfg = _config(dims_only=False, enrich_bio=False)
    with patch("src.pipeline.stages.run_dimensions") as mock_rd:
        run_dimensions_stage(con, cfg)
    mock_rd.assert_called_once_with(con, full_players=True, enrich_bio=False)


def test_run_dimensions_stage_dims_only_passes_full_players_false():
    con = MagicMock(spec=sqlite3.Connection)
    cfg = _config(dims_only=True, enrich_bio=False)
    with patch("src.pipeline.stages.run_dimensions") as mock_rd:
        run_dimensions_stage(con, cfg)
    mock_rd.assert_called_once_with(con, full_players=False, enrich_bio=False)


def test_run_dimensions_stage_enrich_bio_passes_through():
    con = MagicMock(spec=sqlite3.Connection)
    cfg = _config(dims_only=False, enrich_bio=True)
    with patch("src.pipeline.stages.run_dimensions") as mock_rd:
        run_dimensions_stage(con, cfg)
    mock_rd.assert_called_once_with(con, full_players=True, enrich_bio=True)


# ------------------------------------------------------------------ #
# run_game_logs_stage                                                 #
# ------------------------------------------------------------------ #


def test_run_game_logs_stage_regular_season_only():
    con = MagicMock(spec=sqlite3.Connection)
    cfg = _config(include_playoffs=False, seasons=("2023-24",))
    with patch("src.pipeline.stages.load_multiple_seasons") as mock_load:
        run_game_logs_stage(con, cfg)
    mock_load.assert_called_once_with(con, ["2023-24"], season_types=["Regular Season"])


def test_run_game_logs_stage_includes_playoffs():
    con = MagicMock(spec=sqlite3.Connection)
    cfg = _config(include_playoffs=True, seasons=("2023-24",))
    with patch("src.pipeline.stages.load_multiple_seasons") as mock_load:
        run_game_logs_stage(con, cfg)
    mock_load.assert_called_once_with(
        con, ["2023-24"], season_types=["Regular Season", "Playoffs"]
    )


def test_run_game_logs_stage_multiple_seasons():
    con = MagicMock(spec=sqlite3.Connection)
    cfg = _config(seasons=("2022-23", "2023-24"))
    with patch("src.pipeline.stages.load_multiple_seasons") as mock_load:
        run_game_logs_stage(con, cfg)
    mock_load.assert_called_once_with(
        con, ["2022-23", "2023-24"], season_types=["Regular Season"]
    )


# ------------------------------------------------------------------ #
# run_raw_backfill_stage                                              #
# ------------------------------------------------------------------ #


def test_run_raw_backfill_stage_calls_run_raw_backfill():
    con = MagicMock(spec=sqlite3.Connection)
    cfg = _config(raw_backfill=True, raw_backfill_fail_fast=False)
    summary = {"ok": ["loader_a"], "skipped": [], "failed": []}
    with patch("src.pipeline.stages.run_raw_backfill", return_value=summary) as mock_rb:
        result = run_raw_backfill_stage(con, cfg)
    assert result == summary
    mock_rb.assert_called_once()


def test_run_raw_backfill_stage_passes_fail_fast():
    con = MagicMock(spec=sqlite3.Connection)
    cfg = _config(raw_backfill=True, raw_backfill_fail_fast=True)
    with patch("src.pipeline.stages.run_raw_backfill", return_value={}) as mock_rb:
        run_raw_backfill_stage(con, cfg)
    _, call_kwargs = mock_rb.call_args
    assert call_kwargs.get("fail_fast") is True


# ------------------------------------------------------------------ #
# run_pbp_stage                                                       #
# ------------------------------------------------------------------ #


def test_run_pbp_stage_calls_load_season_pbp_for_each_season():
    con = MagicMock(spec=sqlite3.Connection)
    cfg = _config(seasons=("2022-23", "2023-24"), pbp_limit=5)
    with patch("src.pipeline.stages.load_season_pbp") as mock_pbp:
        run_pbp_stage(con, cfg)
    assert mock_pbp.call_count == 2
    mock_pbp.assert_any_call(con, "2022-23", limit=5)
    mock_pbp.assert_any_call(con, "2023-24", limit=5)


def test_run_pbp_stage_passes_correct_limit():
    con = MagicMock(spec=sqlite3.Connection)
    cfg = _config(seasons=("2023-24",), pbp_limit=100)
    with patch("src.pipeline.stages.load_season_pbp") as mock_pbp:
        run_pbp_stage(con, cfg)
    mock_pbp.assert_called_once_with(con, "2023-24", limit=100)


# ------------------------------------------------------------------ #
# run_reconciliation                                                  #
# ------------------------------------------------------------------ #


def test_run_reconciliation_no_warnings_completes_silently():
    con = MagicMock(spec=sqlite3.Connection)
    cfg = _config(seasons=("2023-24",), reconciliation_warn_only=False)
    with patch("src.pipeline.stages.run_consistency_checks", return_value=0):
        run_reconciliation(con, cfg)  # should not raise


def test_run_reconciliation_warn_only_logs_instead_of_raising(caplog):
    import logging

    con = MagicMock(spec=sqlite3.Connection)
    cfg = _config(seasons=("2023-24",), reconciliation_warn_only=True)
    with patch("src.pipeline.stages.run_consistency_checks", return_value=3):
        with caplog.at_level(logging.WARNING, logger="src.pipeline.stages"):
            run_reconciliation(con, cfg)  # should not raise
    assert any(r.levelno == logging.WARNING for r in caplog.records)


def test_run_reconciliation_raises_when_not_warn_only():
    con = MagicMock(spec=sqlite3.Connection)
    cfg = _config(seasons=("2023-24",), reconciliation_warn_only=False)
    with patch("src.pipeline.stages.run_consistency_checks", return_value=2):
        with pytest.raises(ReconciliationError):
            run_reconciliation(con, cfg)


def test_run_reconciliation_accumulates_warnings_across_seasons():
    """Warnings from multiple seasons are summed before the single raise."""
    con = MagicMock(spec=sqlite3.Connection)
    cfg = _config(seasons=("2022-23", "2023-24"), reconciliation_warn_only=False)
    # 1 warning per season = total 2 → should raise
    with patch("src.pipeline.stages.run_consistency_checks", return_value=1):
        with pytest.raises(ReconciliationError) as exc_info:
            run_reconciliation(con, cfg)
    assert exc_info.value.warning_count == 2


def test_run_reconciliation_multiple_seasons_zero_warnings():
    con = MagicMock(spec=sqlite3.Connection)
    cfg = _config(seasons=("2022-23", "2023-24"), reconciliation_warn_only=False)
    with patch("src.pipeline.stages.run_consistency_checks", return_value=0):
        run_reconciliation(con, cfg)  # no raise
