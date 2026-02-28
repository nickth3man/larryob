"""Tests for run_from_parsed_args execution and error handling paths."""

from unittest.mock import MagicMock, patch

from src.pipeline.cli import create_argument_parser
from src.pipeline.cli.commands import (
    EXIT_INGEST_ERROR,
    EXIT_SUCCESS,
    EXIT_UNEXPECTED_ERROR,
    EXIT_VALIDATION_ERROR,
)
from src.pipeline.cli.runner import run_from_parsed_args
from src.pipeline.exceptions import IngestError
from src.pipeline.models import IngestConfig


def _parse(argv: list[str]):
    parser = create_argument_parser()
    args = parser.parse_args(argv)
    return parser, args


def _config(**kwargs) -> IngestConfig:
    defaults = dict(
        seasons=("2023-24",),
        metrics_enabled=False,
        metrics_summary=False,
        metrics_export_endpoint=None,
        runlog_tail=12,
    )
    defaults.update(kwargs)
    return IngestConfig(**defaults)  # ty: ignore[invalid-argument-type]


def test_run_from_parsed_args_returns_validation_error_when_validate_arguments_exits():
    parser, args = _parse([])
    with patch("src.pipeline.cli.runner.validate_arguments", side_effect=SystemExit):
        assert run_from_parsed_args(parser, args) == EXIT_VALIDATION_ERROR


def test_run_from_parsed_args_returns_validation_error_when_config_build_fails():
    parser, args = _parse([])
    with (
        patch("src.pipeline.cli.runner.validate_arguments"),
        patch("src.pipeline.cli.runner.setup_logging"),
        patch(
            "src.pipeline.cli.runner.IngestConfig.from_args", side_effect=ValueError("bad config")
        ),
        patch("src.pipeline.cli.runner.finalize_metrics") as mock_finalize,
    ):
        assert run_from_parsed_args(parser, args) == EXIT_VALIDATION_ERROR
    mock_finalize.assert_not_called()


def test_run_from_parsed_args_runs_analytics_after_successful_ingest():
    parser, args = _parse(["--analytics-view", "v_player_stats"])
    con = MagicMock()
    config = _config(analytics_view="v_player_stats", analytics_limit=20, analytics_output=None)
    with (
        patch("src.pipeline.cli.runner.setup_logging"),
        patch("src.pipeline.cli.runner._log_config_summary"),
        patch("src.pipeline.cli.runner.IngestConfig.from_args", return_value=config),
        patch("src.pipeline.cli.runner.init_db", return_value=con),
        patch("src.pipeline.cli.runner.run_ingest_pipeline"),
        patch("src.pipeline.cli.runner.run_analytics_view") as mock_analytics,
        patch("src.pipeline.cli.runner.finalize_metrics") as mock_finalize,
    ):
        assert run_from_parsed_args(parser, args) == EXIT_SUCCESS

    mock_analytics.assert_called_once_with(
        view_name="v_player_stats",
        limit=20,
        output_path=None,
    )
    con.close.assert_called_once()
    mock_finalize.assert_called_once_with(False, False, None)


def test_run_from_parsed_args_returns_ingest_error_and_logs_context():
    parser, args = _parse([])
    con = MagicMock()
    error = IngestError("ingest failed", context={"stage": "game_logs"})
    config = _config()

    with (
        patch("src.pipeline.cli.runner.setup_logging"),
        patch("src.pipeline.cli.runner._log_config_summary"),
        patch("src.pipeline.cli.runner.IngestConfig.from_args", return_value=config),
        patch("src.pipeline.cli.runner.init_db", return_value=con),
        patch("src.pipeline.cli.runner.run_ingest_pipeline", side_effect=error),
        patch("src.pipeline.cli.runner.finalize_metrics") as mock_finalize,
        patch("src.pipeline.cli.runner.logger.debug") as mock_debug,
    ):
        assert run_from_parsed_args(parser, args) == EXIT_INGEST_ERROR

    mock_debug.assert_called_once_with("Error context: %s", {"stage": "game_logs"})
    con.close.assert_called_once()
    mock_finalize.assert_called_once_with(False, False, None)


def test_run_from_parsed_args_returns_unexpected_error_for_non_ingest_exception():
    parser, args = _parse([])
    con = MagicMock()
    config = _config()
    with (
        patch("src.pipeline.cli.runner.setup_logging"),
        patch("src.pipeline.cli.runner._log_config_summary"),
        patch("src.pipeline.cli.runner.IngestConfig.from_args", return_value=config),
        patch("src.pipeline.cli.runner.init_db", return_value=con),
        patch("src.pipeline.cli.runner.run_ingest_pipeline", side_effect=RuntimeError("boom")),
        patch("src.pipeline.cli.runner.finalize_metrics") as mock_finalize,
    ):
        assert run_from_parsed_args(parser, args) == EXIT_UNEXPECTED_ERROR

    con.close.assert_called_once()
    mock_finalize.assert_called_once_with(False, False, None)


def test_run_from_parsed_args_returns_validation_error_when_analytics_only_has_no_view():
    parser, args = _parse(["--analytics-only"])
    parser.error = MagicMock(return_value=None)
    config = _config(analytics_only=True, analytics_view=None)

    with (
        patch("src.pipeline.cli.runner.validate_arguments"),
        patch("src.pipeline.cli.runner.setup_logging"),
        patch("src.pipeline.cli.runner._log_config_summary"),
        patch("src.pipeline.cli.runner.IngestConfig.from_args", return_value=config),
        patch("src.pipeline.cli.runner.finalize_metrics") as mock_finalize,
    ):
        assert run_from_parsed_args(parser, args) == EXIT_VALIDATION_ERROR

    parser.error.assert_called_once_with("--analytics-only requires --analytics-view")
    mock_finalize.assert_called_once_with(False, False, None)
