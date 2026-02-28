"""Metrics recording primitives and timer context manager."""

import logging
import time
from collections import defaultdict
from threading import Lock
from typing import Any

from ..config import MetricsConfig

logger = logging.getLogger(__name__)

_metrics_lock = Lock()
_metrics: dict[str, Any] = {
    "etl_rows_loaded": defaultdict(int),
    "api_calls": defaultdict(int),
    "api_success": defaultdict(int),
    "api_failures": defaultdict(int),
    "api_retries": defaultdict(int),
    "api_latency_ms": defaultdict(list),
    "etl_duration_seconds": defaultdict(list),
}


def _check_enabled() -> bool:
    """Check if metrics collection is enabled."""
    return MetricsConfig.enabled()


def record_etl_rows(table: str, season_id: str | None, row_count: int) -> None:
    """Record loaded row counts."""
    if not _check_enabled():
        return

    key = (table, season_id) if season_id else (table, None)
    with _metrics_lock:
        _metrics["etl_rows_loaded"][key] += row_count

    logger.debug("Metrics: ETL loaded %d rows into %s (season=%s)", row_count, table, season_id)


def record_etl_duration(table: str, season_id: str | None, duration_seconds: float) -> None:
    """Record ETL duration in seconds."""
    if not _check_enabled():
        return

    key = (table, season_id) if season_id else (table, None)
    with _metrics_lock:
        _metrics["etl_duration_seconds"][key].append(duration_seconds)

    logger.debug("Metrics: ETL took %.2fs for %s (season=%s)", duration_seconds, table, season_id)


def record_api_call(label: str, success: bool, attempt: int) -> None:
    """Record an API call attempt."""
    if not _check_enabled():
        return

    with _metrics_lock:
        _metrics["api_calls"][label] += 1
        if success:
            _metrics["api_success"][label] += 1
        else:
            _metrics["api_failures"][label] += 1

    logger.debug("Metrics: API call %s (attempt=%d, success=%s)", label, attempt, success)


def record_retry(label: str, attempt: int, exc: Exception) -> None:
    """Record a retry event."""
    if not _check_enabled():
        return

    with _metrics_lock:
        _metrics["api_retries"][label] += 1

    logger.debug("Metrics: Retry %d for %s: %s", attempt, label, exc)


def record_api_latency(label: str, latency_ms: float) -> None:
    """Record API call latency in milliseconds."""
    if not _check_enabled():
        return

    with _metrics_lock:
        _metrics["api_latency_ms"][label].append(latency_ms)

    logger.debug("Metrics: API latency for %s: %.2fms", label, latency_ms)


def reset_metrics() -> None:
    """Clear all collected metrics."""
    with _metrics_lock:
        _metrics["etl_rows_loaded"].clear()
        _metrics["api_calls"].clear()
        _metrics["api_success"].clear()
        _metrics["api_failures"].clear()
        _metrics["api_retries"].clear()
        _metrics["api_latency_ms"].clear()
        _metrics["etl_duration_seconds"].clear()


class ETLTimer:
    """Context manager for timing ETL operations."""

    def __init__(self, table: str, season_id: str | None = None):
        self.table = table
        self.season_id = season_id
        self.start_time: float | None = None

    def __enter__(self) -> "ETLTimer":
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.start_time is not None:
            duration = time.time() - self.start_time
            record_etl_duration(self.table, self.season_id, duration)
