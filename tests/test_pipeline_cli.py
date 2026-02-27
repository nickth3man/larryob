"""Tests for src.pipeline.cli — argument parsing and validate_arguments."""

import pytest

from src.pipeline._cli_validators import _validate_log_level as _validate_log_level_from_module
from src.pipeline.cli import create_argument_parser, validate_arguments
from src.pipeline.constants import DEFAULT_SEASONS

# ------------------------------------------------------------------ #
# Helpers                                                             #
# ------------------------------------------------------------------ #


def _parse(args: list[str]):
    """Parse args and return the Namespace (no validation)."""
    parser = create_argument_parser()
    return parser.parse_args(args)


def _parse_and_validate(args: list[str]):
    """Parse and run validate_arguments. Raises SystemExit on error."""
    parser = create_argument_parser()
    ns = parser.parse_args(args)
    validate_arguments(parser, ns)
    return ns


# ------------------------------------------------------------------ #
# Defaults                                                            #
# ------------------------------------------------------------------ #


def test_default_seasons_are_set():
    ns = _parse([])
    assert ns.seasons == list(DEFAULT_SEASONS)


def test_default_dims_only_is_false():
    ns = _parse([])
    assert ns.dims_only is False


def test_default_pbp_limit_is_zero():
    ns = _parse([])
    assert ns.pbp_limit == 0


def test_default_runlog_tail_is_twelve():
    ns = _parse([])
    assert ns.runlog_tail == 12


def test_default_log_level_is_info():
    ns = _parse([])
    assert ns.log_level == "INFO"


def test_default_awards_false():
    ns = _parse([])
    assert ns.awards is False


def test_default_salaries_false():
    ns = _parse([])
    assert ns.salaries is False


def test_default_rosters_false():
    ns = _parse([])
    assert ns.rosters is False


def test_cli_validators_module_exposes_validate_log_level() -> None:
    assert _validate_log_level_from_module("INFO") == "INFO"


# ------------------------------------------------------------------ #
# Feature flags                                                       #
# ------------------------------------------------------------------ #


def test_dims_only_flag():
    ns = _parse(["--dims-only"])
    assert ns.dims_only is True


def test_awards_flag():
    ns = _parse(["--awards"])
    assert ns.awards is True


def test_salaries_flag():
    ns = _parse(["--salaries"])
    assert ns.salaries is True


def test_rosters_flag():
    ns = _parse(["--rosters"])
    assert ns.rosters is True


def test_include_playoffs_flag():
    ns = _parse(["--include-playoffs"])
    assert ns.include_playoffs is True


def test_enrich_bio_flag():
    ns = _parse(["--enrich-bio"])
    assert ns.enrich_bio is True


def test_skip_reconciliation_flag():
    ns = _parse(["--skip-reconciliation"])
    assert ns.skip_reconciliation is True


def test_reconciliation_warn_only_flag():
    ns = _parse(["--reconciliation-warn-only"])
    assert ns.reconciliation_warn_only is True


def test_metrics_flag():
    ns = _parse(["--metrics"])
    assert ns.metrics is True


def test_raw_backfill_flag():
    ns = _parse(["--raw-backfill"])
    assert ns.raw_backfill is True


# ------------------------------------------------------------------ #
# --seasons                                                           #
# ------------------------------------------------------------------ #


def test_seasons_single():
    ns = _parse(["--seasons", "2023-24"])
    assert ns.seasons == ["2023-24"]


def test_seasons_multiple_space_separated():
    ns = _parse(["--seasons", "2022-23", "2023-24"])
    assert ns.seasons == ["2022-23", "2023-24"]


# ------------------------------------------------------------------ #
# --pbp-limit                                                         #
# ------------------------------------------------------------------ #


def test_pbp_limit_set():
    ns = _parse(["--pbp-limit", "10"])
    assert ns.pbp_limit == 10


def test_pbp_limit_zero_is_valid():
    ns = _parse_and_validate(["--pbp-limit", "0"])
    assert ns.pbp_limit == 0


def test_pbp_limit_negative_fails_validation():
    with pytest.raises(SystemExit):
        _parse_and_validate(["--pbp-limit", "-1"])


# ------------------------------------------------------------------ #
# --analytics flags                                                   #
# ------------------------------------------------------------------ #


def test_analytics_only_requires_analytics_view():
    with pytest.raises(SystemExit):
        _parse_and_validate(["--analytics-only"])


def test_analytics_only_with_view_passes():
    ns = _parse_and_validate(["--analytics-only", "--analytics-view", "v_player_stats"])
    assert ns.analytics_only is True
    assert ns.analytics_view == "v_player_stats"


def test_analytics_limit_must_be_positive():
    with pytest.raises(SystemExit):
        _parse_and_validate(["--analytics-view", "v_x", "--analytics-limit", "0"])


# ------------------------------------------------------------------ #
# --log-level                                                         #
# ------------------------------------------------------------------ #


def test_log_level_debug_is_valid():
    ns = _parse_and_validate(["--log-level", "DEBUG"])
    assert ns.log_level == "DEBUG"


def test_log_level_warning_is_valid():
    ns = _parse_and_validate(["--log-level", "WARNING"])
    assert ns.log_level == "WARNING"


def test_log_level_invalid_fails():
    with pytest.raises(SystemExit):
        _parse_and_validate(["--log-level", "VERBOSE"])


# ------------------------------------------------------------------ #
# --runlog-tail                                                       #
# ------------------------------------------------------------------ #


def test_runlog_tail_positive_is_valid():
    ns = _parse_and_validate(["--runlog-tail", "5"])
    assert ns.runlog_tail == 5


def test_runlog_tail_zero_fails():
    with pytest.raises(SystemExit):
        _parse_and_validate(["--runlog-tail", "0"])


# ------------------------------------------------------------------ #
# --pbp-source / --salary-source                                      #
# ------------------------------------------------------------------ #


def test_pbp_source_default():
    ns = _parse([])
    assert ns.pbp_source == "auto"


def test_salary_source_default():
    ns = _parse([])
    assert ns.salary_source == "auto"


# ------------------------------------------------------------------ #
# validate_arguments — season format                                  #
# ------------------------------------------------------------------ #


def test_valid_season_format_passes():
    ns = _parse_and_validate(["--seasons", "2023-24"])
    assert ns.seasons == ["2023-24"]


def test_invalid_season_format_fails():
    with pytest.raises(SystemExit):
        _parse_and_validate(["--seasons", "2023"])
