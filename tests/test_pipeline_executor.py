"""Tests for src.pipeline.executor — stage plan construction and orchestration."""

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from src.pipeline.exceptions import IngestError
from src.pipeline.executor import (
    _build_stage_plan,
    _execute_raw_backfill_stage,
    _execute_stage,
    finalize_metrics,
    run_ingest_pipeline,
    set_metrics_env,
)
from src.pipeline.models import CheckpointState, IngestConfig, Stage

# ------------------------------------------------------------------ #
# Helpers                                                             #
# ------------------------------------------------------------------ #


def _config(**kwargs) -> IngestConfig:
    defaults = dict(
        seasons=("2023-24",),
        dims_only=False,
        awards=False,
        salaries=False,
        rosters=False,
        raw_backfill=False,
        pbp_limit=0,
        skip_reconciliation=False,
        reconciliation_warn_only=False,
        raw_backfill_fail_fast=False,
        metrics_enabled=False,
        metrics_summary=False,
        metrics_export_endpoint=None,
        runlog_tail=12,
    )
    defaults.update(kwargs)
    return IngestConfig(**defaults)


# ------------------------------------------------------------------ #
# _build_stage_plan                                                   #
# ------------------------------------------------------------------ #


def test_build_stage_plan_default_has_dimensions_and_game_logs():
    cfg = _config()
    plan = _build_stage_plan(cfg)
    stages = [s for s, *_ in plan]
    assert Stage.DIMENSIONS in stages
    assert Stage.GAME_LOGS in stages


def test_build_stage_plan_dims_only_excludes_game_logs():
    cfg = _config(dims_only=True)
    plan = _build_stage_plan(cfg)
    stages = [s for s, *_ in plan]
    assert Stage.GAME_LOGS not in stages


def test_build_stage_plan_awards_flag_adds_awards_stage():
    cfg = _config(awards=True)
    plan = _build_stage_plan(cfg)
    stages = [s for s, *_ in plan]
    assert Stage.AWARDS in stages


def test_build_stage_plan_salaries_flag_adds_salaries_stage():
    cfg = _config(salaries=True)
    plan = _build_stage_plan(cfg)
    stages = [s for s, *_ in plan]
    assert Stage.SALARIES in stages


def test_build_stage_plan_rosters_flag_adds_rosters_stage():
    cfg = _config(rosters=True)
    plan = _build_stage_plan(cfg)
    stages = [s for s, *_ in plan]
    assert Stage.ROSTERS in stages


def test_build_stage_plan_raw_backfill_flag_adds_raw_backfill_stage():
    cfg = _config(raw_backfill=True)
    plan = _build_stage_plan(cfg)
    stages = [s for s, *_ in plan]
    assert Stage.RAW_BACKFILL in stages


def test_build_stage_plan_raw_backfill_with_dims_only_excluded():
    cfg = _config(raw_backfill=True, dims_only=True)
    plan = _build_stage_plan(cfg)
    stages = [s for s, *_ in plan]
    assert Stage.RAW_BACKFILL not in stages


def test_build_stage_plan_dimensions_always_first():
    cfg = _config(awards=True, salaries=True, rosters=True)
    plan = _build_stage_plan(cfg)
    assert plan[0][0] is Stage.DIMENSIONS


def test_build_stage_plan_all_optional_stages():
    cfg = _config(awards=True, salaries=True, rosters=True, raw_backfill=True)
    plan = _build_stage_plan(cfg)
    stages = [s for s, *_ in plan]
    assert Stage.DIMENSIONS in stages
    assert Stage.RAW_BACKFILL in stages
    assert Stage.AWARDS in stages
    assert Stage.SALARIES in stages
    assert Stage.ROSTERS in stages
    assert Stage.GAME_LOGS in stages


# ------------------------------------------------------------------ #
# _execute_stage                                                      #
# ------------------------------------------------------------------ #


def test_execute_stage_calls_stage_fn():
    con = MagicMock(spec=sqlite3.Connection)
    state = CheckpointState()
    cfg = _config()
    fn = MagicMock()
    with patch("src.pipeline.executor.log_checkpoint"):
        _execute_stage(con, Stage.AWARDS, ("fact_player_award",), state, cfg, fn)
    fn.assert_called_once_with(con)


def test_execute_stage_passes_args_and_kwargs():
    con = MagicMock(spec=sqlite3.Connection)
    state = CheckpointState()
    cfg = _config()
    fn = MagicMock()
    with patch("src.pipeline.executor.log_checkpoint"):
        _execute_stage(con, Stage.AWARDS, (), state, cfg, fn, "arg1", key="val")
    fn.assert_called_once_with(con, "arg1", key="val")


def test_execute_stage_reraises_exception():
    con = MagicMock(spec=sqlite3.Connection)
    state = CheckpointState()
    cfg = _config()

    def boom(con):
        raise ValueError("stage failed")

    with patch("src.pipeline.executor.log_checkpoint"):
        with pytest.raises(ValueError, match="stage failed"):
            _execute_stage(con, Stage.AWARDS, (), state, cfg, boom)


# ------------------------------------------------------------------ #
# _execute_raw_backfill_stage                                         #
# ------------------------------------------------------------------ #


def test_execute_raw_backfill_stage_logs_and_checkpoints_on_success():
    con = MagicMock(spec=sqlite3.Connection)
    state = CheckpointState()
    cfg = _config(raw_backfill=True)
    summary = {"ok": ["loader_a"], "skipped": [], "failed": []}
    with (
        patch("src.pipeline.executor.run_raw_backfill_stage", return_value=summary),
        patch("src.pipeline.executor.log_checkpoint") as mock_ckpt,
    ):
        _execute_raw_backfill_stage(con, state, cfg)
    mock_ckpt.assert_called_once()


def test_execute_raw_backfill_stage_warns_on_failures_no_fail_fast():
    con = MagicMock(spec=sqlite3.Connection)
    state = CheckpointState()
    cfg = _config(raw_backfill=True, raw_backfill_fail_fast=False)
    summary = {"ok": [], "skipped": [], "failed": ["bad_loader"]}
    with (
        patch("src.pipeline.executor.run_raw_backfill_stage", return_value=summary),
        patch("src.pipeline.executor.log_checkpoint"),
    ):
        # Should not raise
        _execute_raw_backfill_stage(con, state, cfg)


def test_execute_raw_backfill_stage_raises_on_failures_with_fail_fast():
    con = MagicMock(spec=sqlite3.Connection)
    state = CheckpointState()
    cfg = _config(raw_backfill=True, raw_backfill_fail_fast=True)
    summary = {"ok": [], "skipped": [], "failed": ["bad_loader"]}
    with (
        patch("src.pipeline.executor.run_raw_backfill_stage", return_value=summary),
        patch("src.pipeline.executor.log_checkpoint"),
    ):
        with pytest.raises(IngestError):
            _execute_raw_backfill_stage(con, state, cfg)


# ------------------------------------------------------------------ #
# finalize_metrics                                                    #
# ------------------------------------------------------------------ #


def test_finalize_metrics_noop_when_disabled():
    with (
        patch("src.pipeline.executor.log_metrics_summary") as mock_summary,
        patch("src.pipeline.executor.export_metrics") as mock_export,
    ):
        finalize_metrics(metrics_enabled=False, show_summary=True, export_endpoint="http://x")
    mock_summary.assert_not_called()
    mock_export.assert_not_called()


def test_finalize_metrics_logs_summary_when_enabled():
    with (
        patch("src.pipeline.executor.log_metrics_summary") as mock_summary,
        patch("src.pipeline.executor.export_metrics"),
    ):
        finalize_metrics(metrics_enabled=True, show_summary=True, export_endpoint=None)
    mock_summary.assert_called_once()


def test_finalize_metrics_exports_when_endpoint_given():
    with (
        patch("src.pipeline.executor.log_metrics_summary"),
        patch("src.pipeline.executor.export_metrics") as mock_export,
    ):
        finalize_metrics(
            metrics_enabled=True, show_summary=False, export_endpoint="http://endpoint"
        )
    mock_export.assert_called_once_with("http://endpoint")


def test_finalize_metrics_no_export_when_no_endpoint():
    with (
        patch("src.pipeline.executor.log_metrics_summary"),
        patch("src.pipeline.executor.export_metrics") as mock_export,
    ):
        finalize_metrics(metrics_enabled=True, show_summary=False, export_endpoint=None)
    mock_export.assert_not_called()


# ------------------------------------------------------------------ #
# set_metrics_env                                                     #
# ------------------------------------------------------------------ #


def test_set_metrics_env_sets_env_when_true(monkeypatch):
    import os

    monkeypatch.delenv("LARRYOB_METRICS_ENABLED", raising=False)
    set_metrics_env(True)
    assert os.environ.get("LARRYOB_METRICS_ENABLED") == "true"


def test_set_metrics_env_does_not_set_when_false(monkeypatch):
    import os

    monkeypatch.delenv("LARRYOB_METRICS_ENABLED", raising=False)
    set_metrics_env(False)
    assert "LARRYOB_METRICS_ENABLED" not in os.environ


# ------------------------------------------------------------------ #
# run_ingest_pipeline — integration-style smoke tests                 #
# ------------------------------------------------------------------ #


def test_run_ingest_pipeline_dims_only_skips_game_logs():
    """Verify dims_only=True path doesn't call game-log stages."""
    con = MagicMock(spec=sqlite3.Connection)
    cfg = _config(dims_only=True)
    with (
        patch("src.pipeline.executor.run_dimensions_stage") as mock_dims,
        patch("src.pipeline.executor.run_game_logs_stage") as mock_gl,
        patch("src.pipeline.executor.log_checkpoint"),
    ):
        run_ingest_pipeline(con, cfg)
    mock_dims.assert_called_once()
    mock_gl.assert_not_called()


def test_run_ingest_pipeline_calls_game_logs_by_default():
    con = MagicMock(spec=sqlite3.Connection)
    cfg = _config(skip_reconciliation=True)
    with (
        patch("src.pipeline.executor.run_dimensions_stage"),
        patch("src.pipeline.executor.run_game_logs_stage") as mock_gl,
        patch("src.pipeline.executor.log_checkpoint"),
    ):
        run_ingest_pipeline(con, cfg)
    mock_gl.assert_called_once()


def test_run_ingest_pipeline_reraises_stage_failure():
    con = MagicMock(spec=sqlite3.Connection)
    cfg = _config(dims_only=True)
    with (
        patch("src.pipeline.executor.run_dimensions_stage", side_effect=RuntimeError("fail")),
        patch("src.pipeline.executor.log_checkpoint"),
    ):
        with pytest.raises(RuntimeError, match="fail"):
            run_ingest_pipeline(con, cfg)
