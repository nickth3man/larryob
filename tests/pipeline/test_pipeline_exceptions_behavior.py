"""Cross-exception behavior tests for pipeline exception types."""

import sys
from pathlib import Path

import pytest

from src.pipeline.exceptions import (
    AnalyticsError,
    IngestError,
    ReconciliationError,
    ValidationError,
)

# Add src to path for direct import parity with existing exception tests.
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))


def test_all_exceptions_have_slots():
    assert hasattr(IngestError, "__slots__")
    assert hasattr(ReconciliationError, "__slots__")
    assert hasattr(AnalyticsError, "__slots__")
    assert hasattr(ValidationError, "__slots__")

    assert "message" in IngestError.__slots__
    assert "context" in IngestError.__slots__
    assert "warning_count" in ReconciliationError.__slots__
    assert "seasons" in ReconciliationError.__slots__
    assert "view_name" in AnalyticsError.__slots__
    assert "output_path" in AnalyticsError.__slots__
    assert "argument" in ValidationError.__slots__
    assert "value" in ValidationError.__slots__


def test_all_exceptions_can_be_raised_and_caught():
    with pytest.raises(IngestError):
        raise ReconciliationError(5)
    with pytest.raises(IngestError):
        raise AnalyticsError("test")
    with pytest.raises(IngestError):
        raise ValidationError("test")

    with pytest.raises(ReconciliationError):
        raise ReconciliationError(5)
    with pytest.raises(AnalyticsError):
        raise AnalyticsError("test")
    with pytest.raises(ValidationError):
        raise ValidationError("test")


def test_exception_context_preserves_original_message():
    errors = [
        IngestError("Base error", context={"key": "value"}),
        ReconciliationError(10, seasons=["2023-24"]),
        AnalyticsError("Analytics error", view_name="vw_test"),
        ValidationError("Validation error", argument="test", value=123),
    ]

    for error in errors:
        message = str(error)
        assert len(message) > 0
        assert error.message in message or error.__class__.__name__ in message


def test_exception_context_is_serializable():
    errors = [
        IngestError("Test", context={"str": "value", "int": 42, "list": [1, 2, 3]}),
        ReconciliationError(5, seasons=["2023-24", "2022-23"]),
        AnalyticsError("Test", view_name="vw_test", output_path="/path/to/file.csv"),
        ValidationError("Test", argument="season", value="2023-24"),
    ]

    for error in errors:
        if error.context:
            context_dict = dict(error.context)
            assert isinstance(context_dict, dict)
            for key, value in context_dict.items():
                assert isinstance(key, str)
                assert value is None or isinstance(value, (str, int, list, bool))


@pytest.mark.parametrize(
    ("error_class", "args", "kwargs", "expected_in_repr"),
    [
        (IngestError, ("Test message",), {}, "IngestError('Test message')"),
        (IngestError, ("Test message",), {"context": {"key": "value"}}, "context="),
        (ReconciliationError, (5,), {}, "ReconciliationError("),
        (ReconciliationError, (10,), {"seasons": ["2023-24"]}, "context="),
        (AnalyticsError, ("Test",), {}, "AnalyticsError('Test')"),
        (AnalyticsError, ("Test",), {"view_name": "vw_test"}, "context="),
        (AnalyticsError, ("Test",), {"output_path": "/test.csv"}, "context="),
        (ValidationError, ("Test",), {}, "ValidationError('Test')"),
        (ValidationError, ("Test",), {"argument": "season_id"}, "context="),
        (ValidationError, ("Test",), {"value": "20234"}, "context="),
    ],
)
def test_exception_repr_variants(error_class, args, kwargs, expected_in_repr):
    error = error_class(*args, **kwargs)
    repr_str = repr(error)
    assert expected_in_repr in repr_str
    assert error.__class__.__name__ in repr_str
