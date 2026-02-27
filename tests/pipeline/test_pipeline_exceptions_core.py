"""Core tests for pipeline exception hierarchy types."""

import sys
from pathlib import Path

from src.pipeline.exceptions import (
    AnalyticsError,
    IngestError,
    ReconciliationError,
    ValidationError,
)

# Add src to path for direct import parity with existing exception tests.
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))


def test_ingest_error_basic_initialization():
    error = IngestError("Something went wrong")
    assert error.message == "Something went wrong"
    assert error.context is None
    assert str(error) == "Something went wrong"


def test_ingest_error_with_context():
    context = {"season_id": "2023-24", "player_id": "2544"}
    error = IngestError("Player not found", context=context)
    assert error.message == "Player not found"
    assert error.context == context


def test_ingest_error_repr_without_context():
    error = IngestError("Database connection failed")
    repr_str = repr(error)
    assert repr_str == "IngestError('Database connection failed')"
    assert "context=" not in repr_str


def test_ingest_error_repr_with_context():
    context = {"table": "dim_player", "row_count": 0}
    error = IngestError("No data found", context=context)
    repr_str = repr(error)
    assert (
        repr_str == "IngestError('No data found', context={'table': 'dim_player', 'row_count': 0})"
    )
    assert "context=" in repr_str


def test_ingest_error_repr_with_complex_context():
    context = {
        "season_id": "2023-24",
        "missing_players": ["2544", "203999"],
        "metadata": {"source": "api", "attempt": 3},
    }
    error = IngestError("Batch import failed", context=context)
    repr_str = repr(error)
    assert "IngestError('Batch import failed', context=" in repr_str
    assert "'season_id': '2023-24'" in repr_str
    assert "'missing_players': ['2544', '203999']" in repr_str


def test_ingest_error_is_runtime_error():
    error = IngestError("Test error")
    assert isinstance(error, RuntimeError)
    assert isinstance(error, IngestError)


def test_ingest_error_can_be_caught_as_base():
    errors = [
        ReconciliationError(5, seasons=["2023-24"]),
        AnalyticsError("View not found", view_name="vw_test"),
        ValidationError("Invalid season", argument="season_id", value="20234"),
    ]
    for error in errors:
        assert isinstance(error, IngestError)


def test_reconciliation_error_basic_initialization():
    error = ReconciliationError(10)
    assert error.warning_count == 10
    assert error.seasons is None
    assert "10 discrepancy warning" in str(error)
    assert "--reconciliation-warn-only" in str(error)


def test_reconciliation_error_with_seasons():
    seasons = ["2023-24", "2022-23"]
    error = ReconciliationError(5, seasons=seasons)
    assert error.warning_count == 5
    assert error.seasons == seasons
    assert error.context is not None
    assert error.context["warning_count"] == 5
    assert error.context["seasons"] == seasons


def test_reconciliation_error_repr_without_seasons():
    error = ReconciliationError(3)
    repr_str = repr(error)
    assert "ReconciliationError(" in repr_str
    assert "3 discrepancy warning" in repr_str
    assert "context=" in repr_str


def test_reconciliation_error_repr_with_seasons():
    error = ReconciliationError(7, seasons=["2023-24", "2022-23"])
    repr_str = repr(error)
    assert "ReconciliationError(" in repr_str
    assert "context=" in repr_str


def test_reconciliation_error_message_format():
    error = ReconciliationError(1)
    message = str(error)
    assert "Reconciliation checks found 1 discrepancy warning" in message
    assert "Re-run with --reconciliation-warn-only" in message


def test_reconciliation_error_plural_warnings():
    error = ReconciliationError(5)
    message = str(error)
    assert "5 discrepancy warning" in message
    assert "warning(s)" in message


def test_analytics_error_basic_initialization():
    error = AnalyticsError("Query failed")
    assert error.message == "Query failed"
    assert error.view_name is None
    assert error.output_path is None
    assert error.context is None


def test_analytics_error_with_view_name():
    error = AnalyticsError("View does not exist", view_name="vw_nonexistent")
    assert error.view_name == "vw_nonexistent"
    assert error.context is not None
    assert error.context["view_name"] == "vw_nonexistent"
    assert "output_path" not in error.context


def test_analytics_error_with_output_path():
    error = AnalyticsError("Export failed", output_path="/data/output.csv")
    assert error.output_path == "/data/output.csv"
    assert error.context is not None
    assert error.context["output_path"] == "/data/output.csv"


def test_analytics_error_with_both_view_and_path():
    error = AnalyticsError(
        "Failed to export view",
        view_name="vw_player_totals",
        output_path="/output.csv",
    )
    assert error.view_name == "vw_player_totals"
    assert error.output_path == "/output.csv"
    assert error.context is not None
    assert error.context["view_name"] == "vw_player_totals"
    assert error.context["output_path"] == "/output.csv"


def test_analytics_error_repr_without_context():
    error = AnalyticsError("Generic analytics error")
    repr_str = repr(error)
    assert repr_str == "AnalyticsError('Generic analytics error')"
    assert "context=" not in repr_str


def test_analytics_error_repr_with_view_name():
    error = AnalyticsError("View not found", view_name="vw_test")
    repr_str = repr(error)
    assert "AnalyticsError(" in repr_str
    assert "context=" in repr_str
    assert "vw_test" in repr_str


def test_analytics_error_repr_with_both_parameters():
    error = AnalyticsError("Export failed", view_name="vw_data", output_path="/path/to/output.csv")
    repr_str = repr(error)
    assert "AnalyticsError(" in repr_str
    assert "context=" in repr_str
    assert "vw_data" in repr_str


def test_validation_error_basic_initialization():
    error = ValidationError("Invalid input")
    assert error.message == "Invalid input"
    assert error.argument is None
    assert error.value is None
    assert error.context is None


def test_validation_error_with_argument():
    error = ValidationError("Invalid season format", argument="season_id")
    assert error.argument == "season_id"
    assert error.value is None
    assert error.context is not None
    assert error.context["argument"] == "season_id"
    assert "value" not in error.context


def test_validation_error_with_value():
    error = ValidationError("Invalid season ID", value="20234")
    assert error.argument is None
    assert error.value == "20234"
    assert error.context is not None
    assert error.context["value"] == "20234"
    assert "argument" not in error.context


def test_validation_error_with_both_argument_and_value():
    error = ValidationError("Season must be YYYY-YY format", argument="season_id", value="20234")
    assert error.argument == "season_id"
    assert error.value == "20234"
    assert error.context is not None
    assert error.context["argument"] == "season_id"
    assert error.context["value"] == "20234"


def test_validation_error_repr_without_context():
    error = ValidationError("Validation failed")
    repr_str = repr(error)
    assert repr_str == "ValidationError('Validation failed')"
    assert "context=" not in repr_str


def test_validation_error_repr_with_argument_only():
    error = ValidationError("Invalid argument", argument="log_level")
    repr_str = repr(error)
    assert "ValidationError(" in repr_str
    assert "context=" in repr_str
    assert "log_level" in repr_str


def test_validation_error_repr_with_value_only():
    error = ValidationError("Invalid value", value="DEBUG")
    repr_str = repr(error)
    assert "ValidationError(" in repr_str
    assert "context=" in repr_str
    assert "DEBUG" in repr_str


def test_validation_error_repr_with_both_parameters():
    error = ValidationError("Invalid log level", argument="log_level", value="TRACE")
    repr_str = repr(error)
    assert "ValidationError(" in repr_str
    assert "context=" in repr_str
    assert "log_level" in repr_str
    assert "TRACE" in repr_str


def test_validation_error_with_none_value():
    error = ValidationError("Invalid input", argument="test", value=None)
    assert error.argument == "test"
    assert error.value is None
    assert error.context is not None
    assert "value" not in error.context


def test_validation_error_with_zero_value():
    error = ValidationError("Value cannot be zero", argument="limit", value=0)
    assert error.argument == "limit"
    assert error.value == 0
    assert error.context is not None
    assert error.context["value"] == 0


def test_validation_error_with_false_value():
    error = ValidationError("Flag cannot be False", argument="enabled", value=False)
    assert error.argument == "enabled"
    assert error.value is False
    assert error.context is not None
    assert error.context["value"] is False


def test_validation_error_with_empty_string_value():
    error = ValidationError("Value cannot be empty", argument="name", value="")
    assert error.argument == "name"
    assert error.value == ""
    assert error.context is not None
    assert error.context["value"] == ""


def test_validation_error_with_complex_value():
    seasons = ["2023-24", "2022-23", "2021-22"]
    error = ValidationError("Invalid seasons list", argument="seasons", value=seasons)
    assert error.argument == "seasons"
    assert error.value == seasons
    assert error.context is not None
    assert error.context["value"] == seasons
