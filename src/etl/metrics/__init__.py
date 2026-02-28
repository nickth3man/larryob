"""Metrics collection facade."""

import logging
from typing import Any

from . import reporter as _reporter
from .calculator import (
    ETLTimer,
    record_api_call,
    record_api_latency,
    record_etl_duration,
    record_etl_rows,
    record_retry,
    reset_metrics,
)

logger = logging.getLogger(__name__)


def get_metrics_summary() -> dict[str, Any]:
    """Get summary of collected metrics."""
    return _reporter.get_metrics_summary()


def log_metrics_summary() -> None:
    """Log summary of collected metrics."""
    _reporter.logger = logger
    _reporter.log_metrics_summary()


def export_metrics(endpoint: str | None = None, timeout_seconds: float = 5.0) -> bool:
    """Export metrics summary to endpoint."""
    _reporter.logger = logger
    return _reporter.export_metrics(endpoint=endpoint, timeout_seconds=timeout_seconds)


__all__ = [
    "ETLTimer",
    "record_etl_rows",
    "record_etl_duration",
    "record_api_call",
    "record_retry",
    "record_api_latency",
    "get_metrics_summary",
    "reset_metrics",
    "log_metrics_summary",
    "export_metrics",
]
