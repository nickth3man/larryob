"""Tests for src.etl.raw_backfill — orchestrator summary structure and fail-fast."""

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.etl.raw_backfill import RAW_DIR, run_raw_backfill

# ------------------------------------------------------------------ #
# Helpers                                                             #
# ------------------------------------------------------------------ #


def _bare_con() -> sqlite3.Connection:
    return sqlite3.connect(":memory:")


# ------------------------------------------------------------------ #
# RAW_DIR default                                                     #
# ------------------------------------------------------------------ #


def test_raw_dir_default_value():
    assert RAW_DIR == Path("raw")


# ------------------------------------------------------------------ #
# Summary structure                                                   #
# ------------------------------------------------------------------ #


def test_run_raw_backfill_returns_dict_with_expected_keys():
    """run_raw_backfill should always return a dict with expected summary keys."""
    con = _bare_con()
    # All loaders will either skip (no CSV files present) or fail gracefully
    result = run_raw_backfill(con, raw_dir=Path("/nonexistent_raw_dir"))
    assert isinstance(result, dict)
    assert set(result.keys()) == {"ok", "skipped", "failed", "details"}


def test_run_raw_backfill_missing_dir_produces_no_unexpected_keys():
    """With no raw dir, the summary should only contain the expected keys."""
    con = _bare_con()
    result = run_raw_backfill(con, raw_dir=Path("/nonexistent_raw_dir"))
    assert set(result.keys()) == {"ok", "skipped", "failed", "details"}
    # All loaders should be accounted for across ok, skipped, and failed
    total = len(result["ok"]) + len(result["skipped"]) + len(result["failed"])
    assert total > 0  # at least some loaders ran


def test_run_raw_backfill_lists_are_strings():
    """ok, skipped, and failed should all be lists of strings."""
    con = _bare_con()
    result = run_raw_backfill(con, raw_dir=Path("/nonexistent_raw_dir"))
    for key in ("ok", "skipped", "failed"):
        assert isinstance(result[key], list)
        for item in result[key]:
            assert isinstance(item, str), f"Expected str in {key}, got {type(item)}"


# ------------------------------------------------------------------ #
# fail_fast behaviour                                                 #
# ------------------------------------------------------------------ #


def test_run_raw_backfill_fail_fast_stops_after_first_error(tmp_path):
    """With fail_fast=True, the run should stop after the first loader failure.

    We patch _run_single_loader so every call reports an error, and verify that
    not all loaders run (i.e., we stop early).
    """
    import src.etl.backfill._orchestrator as orchestrator_mod

    call_count = []

    def fake_loader(con, config, raw_dir, idx, total):
        call_count.append(config.loader_name)
        # Return a mock result with status="error"
        result = MagicMock()
        result.status = "error"
        result.loader = config.loader_name
        return result

    con = _bare_con()
    with patch.object(orchestrator_mod, "_run_single_loader", side_effect=fake_loader):
        run_raw_backfill(con, raw_dir=tmp_path, fail_fast=True)

    # Only one loader should have been called before stopping
    assert len(call_count) == 1


def test_run_raw_backfill_no_fail_fast_runs_all_loaders(tmp_path):
    """Without fail_fast, all loaders run even if some fail."""
    import src.etl.backfill._orchestrator as orchestrator_mod

    call_count = []

    def fake_loader(con, config, raw_dir, idx, total):
        call_count.append(config.loader_name)
        result = MagicMock()
        result.status = "error"
        result.loader = config.loader_name
        return result

    con = _bare_con()
    total_loaders = len(orchestrator_mod._LOADERS)
    with patch.object(orchestrator_mod, "_run_single_loader", side_effect=fake_loader):
        run_raw_backfill(con, raw_dir=tmp_path, fail_fast=False)

    assert len(call_count) == total_loaders


# ------------------------------------------------------------------ #
# Summary aggregation                                                 #
# ------------------------------------------------------------------ #


def test_run_raw_backfill_ok_summary_counts_correctly(tmp_path):
    """When a loader succeeds, it should appear in ok, not skipped or failed."""
    import src.etl.backfill._orchestrator as orchestrator_mod

    first = True

    def fake_loader(con, config, raw_dir, idx, total):
        nonlocal first
        result = MagicMock()
        if first:
            result.status = "ok"
            first = False
        else:
            result.status = "skipped"
        result.loader = config.loader_name
        return result

    con = _bare_con()
    with patch.object(orchestrator_mod, "_run_single_loader", side_effect=fake_loader):
        summary = run_raw_backfill(con, raw_dir=tmp_path, fail_fast=False)

    assert len(summary["ok"]) == 1
    assert len(summary["failed"]) == 0


def test_run_raw_backfill_details_key_present(tmp_path):
    """The summary dict should include a structured 'details' payload."""
    import src.etl.backfill._orchestrator as orchestrator_mod

    def fake_loader(con, config, raw_dir, idx, total):
        return orchestrator_mod.LoaderResult(
            loader=config.loader_name,
            status="skipped",
            table=config.table_name,
            before_row_count=0,
            after_row_count=0,
            delta_row_count=0,
            elapsed_sec=0.01,
            error=None,
        )

    con = _bare_con()
    with patch.object(orchestrator_mod, "_run_single_loader", side_effect=fake_loader):
        summary = run_raw_backfill(con, raw_dir=tmp_path, fail_fast=False)

    assert "details" in summary
    assert isinstance(summary["details"], list)
    assert summary["details"], "Expected at least one loader detail entry"
    expected_detail_keys = {
        "loader",
        "status",
        "table",
        "before_row_count",
        "after_row_count",
        "delta_row_count",
        "elapsed_sec",
        "error",
    }
    for detail in summary["details"]:
        assert isinstance(detail, dict)
        assert expected_detail_keys.issubset(detail.keys())
