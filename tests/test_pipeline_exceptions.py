"""Tests: Pipeline exception hierarchy — all exception types and their methods."""

import sys
from pathlib import Path

import pytest

# Add src to path for direct import (avoiding __init__.py which triggers heavy imports)
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

# Now import the exceptions module directly
from src.pipeline.exceptions import (  # noqa: E402
    AnalyticsError,
    IngestError,
    ReconciliationError,
    ValidationError,
)

# ------------------------------------------------------------------ #
# IngestError (Base Exception) Tests                                  #
# ------------------------------------------------------------------ #


def test_ingest_error_basic_initialization():
    """Test IngestError can be initialized with a message."""
    error = IngestError("Something went wrong")
    assert error.message == "Something went wrong"
    assert error.context is None
    assert str(error) == "Something went wrong"


def test_ingest_error_with_context():
    """Test IngestError with context dictionary."""
    context = {"season_id": "2023-24", "player_id": "2544"}
    error = IngestError("Player not found", context=context)
    assert error.message == "Player not found"
    assert error.context == context


def test_ingest_error_repr_without_context():
    """Test IngestError.__repr__() without context."""
    error = IngestError("Database connection failed")
    repr_str = repr(error)
    assert repr_str == "IngestError('Database connection failed')"
    assert "context=" not in repr_str


def test_ingest_error_repr_with_context():
    """Test IngestError.__repr__() with context dictionary."""
    context = {"table": "dim_player", "row_count": 0}
    error = IngestError("No data found", context=context)
    repr_str = repr(error)
    assert (
        repr_str == "IngestError('No data found', context={'table': 'dim_player', 'row_count': 0})"
    )
    assert "context=" in repr_str


def test_ingest_error_repr_with_complex_context():
    """Test IngestError.__repr__() with nested context."""
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
    """Test IngestError inherits from RuntimeError."""
    error = IngestError("Test error")
    assert isinstance(error, RuntimeError)
    assert isinstance(error, IngestError)


def test_ingest_error_can_be_caught_as_base():
    """Test that all subclasses can be caught as IngestError."""
    errors = [
        ReconciliationError(5, seasons=["2023-24"]),
        AnalyticsError("View not found", view_name="vw_test"),
        ValidationError("Invalid season", argument="season_id", value="20234"),
    ]

    for error in errors:
        assert isinstance(error, IngestError)


# ------------------------------------------------------------------ #
# ReconciliationError Tests                                           #
# ------------------------------------------------------------------ #


def test_reconciliation_error_basic_initialization():
    """Test ReconciliationError with warning_count."""
    error = ReconciliationError(10)
    assert error.warning_count == 10
    assert error.seasons is None
    assert "10 discrepancy warning" in str(error)
    assert "--reconciliation-warn-only" in str(error)


def test_reconciliation_error_with_seasons():
    """Test ReconciliationError with seasons list."""
    seasons = ["2023-24", "2022-23"]
    error = ReconciliationError(5, seasons=seasons)
    assert error.warning_count == 5
    assert error.seasons == seasons
    assert error.context is not None
    assert error.context["warning_count"] == 5
    assert error.context["seasons"] == seasons


def test_reconciliation_error_repr_without_seasons():
    """Test ReconciliationError.__repr__() without seasons."""
    error = ReconciliationError(3)
    repr_str = repr(error)
    # Should include both message and context
    assert "ReconciliationError(" in repr_str
    assert "3 discrepancy warning" in repr_str
    assert "context=" in repr_str


def test_reconciliation_error_repr_with_seasons():
    """Test ReconciliationError.__repr__() with seasons."""
    error = ReconciliationError(7, seasons=["2023-24", "2022-23"])
    repr_str = repr(error)
    assert "ReconciliationError(" in repr_str
    assert "context=" in repr_str


def test_reconciliation_error_message_format():
    """Test ReconciliationError message includes guidance."""
    error = ReconciliationError(1)
    message = str(error)
    assert "Reconciliation checks found 1 discrepancy warning" in message
    assert "Re-run with --reconciliation-warn-only" in message


def test_reconciliation_error_plural_warnings():
    """Test ReconciliationError uses warning(s) format for multiple warnings."""
    error = ReconciliationError(5)
    message = str(error)
    # The message uses "warning(s)" format, not strict plural
    assert "5 discrepancy warning" in message
    assert "warning(s)" in message


# ------------------------------------------------------------------ #
# AnalyticsError Tests                                                #
# ------------------------------------------------------------------ #


def test_analytics_error_basic_initialization():
    """Test AnalyticsError with message only."""
    error = AnalyticsError("Query failed")
    assert error.message == "Query failed"
    assert error.view_name is None
    assert error.output_path is None
    assert error.context is None


def test_analytics_error_with_view_name():
    """Test AnalyticsError with view_name."""
    error = AnalyticsError("View does not exist", view_name="vw_nonexistent")
    assert error.view_name == "vw_nonexistent"
    assert error.context is not None
    assert error.context["view_name"] == "vw_nonexistent"
    assert "output_path" not in error.context


def test_analytics_error_with_output_path():
    """Test AnalyticsError with output_path."""
    error = AnalyticsError("Export failed", output_path="/data/output.csv")
    assert error.output_path == "/data/output.csv"
    assert error.context is not None
    assert error.context["output_path"] == "/data/output.csv"


def test_analytics_error_with_both_view_and_path():
    """Test AnalyticsError with both view_name and output_path."""
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
    """Test AnalyticsError.__repr__() without optional parameters."""
    error = AnalyticsError("Generic analytics error")
    repr_str = repr(error)
    assert repr_str == "AnalyticsError('Generic analytics error')"
    assert "context=" not in repr_str


def test_analytics_error_repr_with_view_name():
    """Test AnalyticsError.__repr__() with view_name."""
    error = AnalyticsError("View not found", view_name="vw_test")
    repr_str = repr(error)
    assert "AnalyticsError(" in repr_str
    assert "context=" in repr_str
    assert "vw_test" in repr_str


def test_analytics_error_repr_with_both_parameters():
    """Test AnalyticsError.__repr__() with both view_name and output_path."""
    error = AnalyticsError(
        "Export failed",
        view_name="vw_data",
        output_path="/path/to/output.csv",
    )
    repr_str = repr(error)
    assert "AnalyticsError(" in repr_str
    assert "context=" in repr_str
    assert "vw_data" in repr_str


# ------------------------------------------------------------------ #
# ValidationError Tests                                                #
# ------------------------------------------------------------------ #


def test_validation_error_basic_initialization():
    """Test ValidationError with message only."""
    error = ValidationError("Invalid input")
    assert error.message == "Invalid input"
    assert error.argument is None
    assert error.value is None
    assert error.context is None


def test_validation_error_with_argument():
    """Test ValidationError with argument parameter."""
    error = ValidationError("Invalid season format", argument="season_id")
    assert error.argument == "season_id"
    assert error.value is None
    assert error.context is not None
    assert error.context["argument"] == "season_id"
    assert error.context is not None and "value" not in error.context


def test_validation_error_with_value():
    """Test ValidationError with value parameter."""
    error = ValidationError("Invalid season ID", value="20234")
    assert error.argument is None
    assert error.value == "20234"
    assert error.context is not None
    assert error.context["value"] == "20234"
    assert "argument" not in error.context


def test_validation_error_with_both_argument_and_value():
    """Test ValidationError with both argument and value."""
    error = ValidationError(
        "Season must be YYYY-YY format",
        argument="season_id",
        value="20234",
    )
    assert error.argument == "season_id"
    assert error.value == "20234"
    assert error.context is not None
    assert error.context["argument"] == "season_id"
    assert error.context["value"] == "20234"


def test_validation_error_repr_without_context():
    """Test ValidationError.__repr__() without optional parameters."""
    error = ValidationError("Validation failed")
    repr_str = repr(error)
    assert repr_str == "ValidationError('Validation failed')"
    assert "context=" not in repr_str


def test_validation_error_repr_with_argument_only():
    """Test ValidationError.__repr__() with argument only."""
    error = ValidationError("Invalid argument", argument="log_level")
    repr_str = repr(error)
    assert "ValidationError(" in repr_str
    assert "context=" in repr_str
    assert "log_level" in repr_str


def test_validation_error_repr_with_value_only():
    """Test ValidationError.__repr__() with value only."""
    error = ValidationError("Invalid value", value="DEBUG")
    repr_str = repr(error)
    assert "ValidationError(" in repr_str
    assert "context=" in repr_str
    assert "DEBUG" in repr_str


def test_validation_error_repr_with_both_parameters():
    """Test ValidationError.__repr__() with both argument and value."""
    error = ValidationError(
        "Invalid log level",
        argument="log_level",
        value="TRACE",
    )
    repr_str = repr(error)
    assert "ValidationError(" in repr_str
    assert "context=" in repr_str
    assert "log_level" in repr_str
    assert "TRACE" in repr_str


def test_validation_error_with_none_value():
    """Test ValidationError with value=None (no context added)."""
    error = ValidationError("Invalid input", argument="test", value=None)
    assert error.argument == "test"
    assert error.value is None
    # When value is None, it shouldn't be in context
    assert error.context is not None and "value" not in error.context


def test_validation_error_with_zero_value():
    """Test ValidationError with value=0 (should be in context)."""
    error = ValidationError("Value cannot be zero", argument="limit", value=0)
    assert error.argument == "limit"
    assert error.value == 0
    assert error.context is not None
    assert error.context["value"] == 0


def test_validation_error_with_false_value():
    """Test ValidationError with value=False (should be in context)."""
    error = ValidationError("Flag cannot be False", argument="enabled", value=False)
    assert error.argument == "enabled"
    assert error.value is False
    assert error.context is not None
    assert error.context["value"] is False


def test_validation_error_with_empty_string_value():
    """Test ValidationError with value='' (should be in context)."""
    error = ValidationError("Value cannot be empty", argument="name", value="")
    assert error.argument == "name"
    assert error.value == ""
    assert error.context is not None
    assert error.context["value"] == ""


def test_validation_error_with_complex_value():
    """Test ValidationError with complex value type."""
    seasons = ["2023-24", "2022-23", "2021-22"]
    error = ValidationError("Invalid seasons list", argument="seasons", value=seasons)
    assert error.argument == "seasons"
    assert error.value == seasons
    assert error.context is not None
    assert error.context["value"] == seasons


# ------------------------------------------------------------------ #
# Cross-Exception Behavior Tests                                      #
# ------------------------------------------------------------------ #


def test_all_exceptions_have_slots():
    """Test that all exceptions use __slots__ for memory efficiency."""
    # Verify __slots__ is defined on all exception classes
    assert hasattr(IngestError, "__slots__")
    assert hasattr(ReconciliationError, "__slots__")
    assert hasattr(AnalyticsError, "__slots__")
    assert hasattr(ValidationError, "__slots__")

    # Verify expected slots are present
    assert "message" in IngestError.__slots__
    assert "context" in IngestError.__slots__
    assert "warning_count" in ReconciliationError.__slots__
    assert "seasons" in ReconciliationError.__slots__
    assert "view_name" in AnalyticsError.__slots__
    assert "output_path" in AnalyticsError.__slots__
    assert "argument" in ValidationError.__slots__
    assert "value" in ValidationError.__slots__


def test_all_exceptions_can_be_raised_and_caught():
    """Test that all exceptions can be raised and caught properly."""
    # Test catching as base class
    with pytest.raises(IngestError):
        raise ReconciliationError(5)

    with pytest.raises(IngestError):
        raise AnalyticsError("test")

    with pytest.raises(IngestError):
        raise ValidationError("test")

    # Test catching specific types
    with pytest.raises(ReconciliationError):
        raise ReconciliationError(5)

    with pytest.raises(AnalyticsError):
        raise AnalyticsError("test")

    with pytest.raises(ValidationError):
        raise ValidationError("test")


def test_exception_context_preserves_original_message():
    """Test that exception context doesn't alter the original message."""
    errors = [
        IngestError("Base error", context={"key": "value"}),
        ReconciliationError(10, seasons=["2023-24"]),
        AnalyticsError("Analytics error", view_name="vw_test"),
        ValidationError("Validation error", argument="test", value=123),
    ]

    for error in errors:
        message = str(error)
        # The message should always contain the core error text
        assert len(message) > 0
        assert error.message in message or error.__class__.__name__ in message


def test_exception_context_is_serializable():
    """Test that exception contexts can be serialized for logging."""
    errors = [
        IngestError("Test", context={"str": "value", "int": 42, "list": [1, 2, 3]}),
        ReconciliationError(5, seasons=["2023-24", "2022-23"]),
        AnalyticsError("Test", view_name="vw_test", output_path="/path/to/file.csv"),
        ValidationError("Test", argument="season", value="2023-24"),
    ]

    for error in errors:
        if error.context:
            # Should be able to convert to dict for JSON serialization
            context_dict = dict(error.context)
            assert isinstance(context_dict, dict)
            # All values should be basic types
            for key, value in context_dict.items():
                assert isinstance(key, str)
                # Values can be str, int, list, None, etc.
                assert value is None or isinstance(value, (str, int, list, bool))


# ------------------------------------------------------------------ #
# Parametrized Tests for Exception Repr                               #
# ------------------------------------------------------------------ #


@pytest.mark.parametrize(
    ("error_class", "args", "kwargs", "expected_in_repr"),
    [
        # IngestError
        (IngestError, ("Test message",), {}, "IngestError('Test message')"),
        (
            IngestError,
            ("Test message",),
            {"context": {"key": "value"}},
            "context=",
        ),
        # ReconciliationError
        (
            ReconciliationError,
            (5,),
            {},
            "ReconciliationError(",
        ),
        (
            ReconciliationError,
            (10,),
            {"seasons": ["2023-24"]},
            "context=",
        ),
        # AnalyticsError
        (AnalyticsError, ("Test",), {}, "AnalyticsError('Test')"),
        (
            AnalyticsError,
            ("Test",),
            {"view_name": "vw_test"},
            "context=",
        ),
        (
            AnalyticsError,
            ("Test",),
            {"output_path": "/test.csv"},
            "context=",
        ),
        # ValidationError
        (ValidationError, ("Test",), {}, "ValidationError('Test')"),
        (
            ValidationError,
            ("Test",),
            {"argument": "season_id"},
            "context=",
        ),
        (
            ValidationError,
            ("Test",),
            {"value": "20234"},
            "context=",
        ),
    ],
)
def test_exception_repr_variants(error_class, args, kwargs, expected_in_repr):
    """Test exception repr() for various initialization patterns."""
    error = error_class(*args, **kwargs)
    repr_str = repr(error)
    assert expected_in_repr in repr_str
    assert error.__class__.__name__ in repr_str
