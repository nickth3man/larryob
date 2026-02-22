"""
Metrics collection for NBA data pipeline.

Provides simple in-memory metrics tracking for ETL operations.
Can be extended to export to Prometheus, StatsD, or other monitoring systems.
Enable via LARRYOB_METRICS_ENABLED environment variable.

Usage
-----
    from src.etl.metrics import record_etl_rows, record_api_call

    record_etl_rows("player_game_log", "2023-24", 1234)
    record_api_call("PlayerGameLogs(2023-24)", success=True, attempt=1)
"""

import logging
import time
from collections import defaultdict
from threading import Lock
from typing import Any

from .config import MetricsConfig

logger = logging.getLogger(__name__)

# Thread-safe metrics storage
_metrics_lock = Lock()
_metrics: dict[str, Any] = {
    "etl_rows_loaded": defaultdict(int),  # (table, season_id) -> count
    "api_calls": defaultdict(int),  # label -> total_calls
    "api_success": defaultdict(int),  # label -> successful_calls
    "api_failures": defaultdict(int),  # label -> failed_calls
    "api_retries": defaultdict(int),  # label -> retry_count
    "api_latency_ms": defaultdict(list),  # label -> list of latencies
    "etl_duration_seconds": defaultdict(list),  # (table, season_id) -> list of durations
}


def _check_enabled() -> bool:
    """Check if metrics collection is enabled."""
    return MetricsConfig.enabled()


def record_etl_rows(table: str, season_id: str | None, row_count: int) -> None:
    """
    Record the number of rows loaded into a table.

    Parameters
    ----------
    table : str
        Name of the table (e.g., "player_game_log").
    season_id : str | None
        Season identifier (e.g., "2023-24") or None for non-seasonal tables.
    row_count : int
        Number of rows loaded.
    """
    if not _check_enabled():
        return

    key = (table, season_id) if season_id else (table, None)
    with _metrics_lock:
        _metrics["etl_rows_loaded"][key] += row_count

    logger.debug("Metrics: ETL loaded %d rows into %s (season=%s)", row_count, table, season_id)


def record_etl_duration(table: str, season_id: str | None, duration_seconds: float) -> None:
    """
    Record the duration of an ETL operation.

    Parameters
    ----------
    table : str
        Name of the table.
    season_id : str | None
        Season identifier or None for non-seasonal tables.
    duration_seconds : float
        Duration of the operation in seconds.
    """
    if not _check_enabled():
        return

    key = (table, season_id) if season_id else (table, None)
    with _metrics_lock:
        _metrics["etl_duration_seconds"][key].append(duration_seconds)

    logger.debug("Metrics: ETL took %.2fs for %s (season=%s)", duration_seconds, table, season_id)


def record_api_call(label: str, success: bool, attempt: int) -> None:
    """
    Record an API call attempt.

    Parameters
    ----------
    label : str
        Descriptive label for the API call (e.g., "PlayerGameLogs(2023-24)").
    success : bool
        Whether the call succeeded.
    attempt : int
        Attempt number (1-indexed).
    """
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
    """
    Record a retry attempt.

    Parameters
    ----------
    label : str
        Descriptive label for the API call.
    attempt : int
        Attempt number that failed.
    exc : Exception
        The exception that caused the retry.
    """
    if not _check_enabled():
        return

    with _metrics_lock:
        _metrics["api_retries"][label] += 1

    logger.debug("Metrics: Retry %d for %s: %s", attempt, label, exc)


def record_api_latency(label: str, latency_ms: float) -> None:
    """
    Record API call latency.

    Parameters
    ----------
    label : str
        Descriptive label for the API call.
    latency_ms : float
        Latency in milliseconds.
    """
    if not _check_enabled():
        return

    with _metrics_lock:
        _metrics["api_latency_ms"][label].append(latency_ms)

    logger.debug("Metrics: API latency for %s: %.2fms", label, latency_ms)


def get_metrics_summary() -> dict[str, Any]:
    """
    Get a summary of all collected metrics.

    Returns
    -------
    dict[str, Any]
        Dictionary containing:
        - etl_rows_loaded: Dict of (table, season) -> row_count
        - api_calls: Dict of label -> total_calls
        - api_success: Dict of label -> successful_calls
        - api_failures: Dict of label -> failed_calls
        - api_retries: Dict of label -> retry_count
        - api_latency_summary: Dict of label -> (min, max, avg) latency in ms
        - etl_duration_summary: Dict of (table, season) -> (min, max, avg) duration in seconds
    """
    with _metrics_lock:
        summary: dict[str, Any] = {
            "etl_rows_loaded": dict(_metrics["etl_rows_loaded"]),
            "api_calls": dict(_metrics["api_calls"]),
            "api_success": dict(_metrics["api_success"]),
            "api_failures": dict(_metrics["api_failures"]),
            "api_retries": dict(_metrics["api_retries"]),
        }

        # Calculate latency summaries
        latency_summary = {}
        for label, latencies in _metrics["api_latency_ms"].items():
            if latencies:
                latency_summary[label] = {
                    "min_ms": round(min(latencies), 2),
                    "max_ms": round(max(latencies), 2),
                    "avg_ms": round(sum(latencies) / len(latencies), 2),
                    "count": len(latencies),
                }
        summary["api_latency_summary"] = latency_summary

        # Calculate duration summaries
        duration_summary = {}
        for key, durations in _metrics["etl_duration_seconds"].items():
            if durations:
                duration_summary[str(key)] = {
                    "min_s": round(min(durations), 2),
                    "max_s": round(max(durations), 2),
                    "avg_s": round(sum(durations) / len(durations), 2),
                    "count": len(durations),
                }
        summary["etl_duration_summary"] = duration_summary

        return summary


def reset_metrics() -> None:
    """Clear all collected metrics. Useful for testing."""
    with _metrics_lock:
        _metrics["etl_rows_loaded"].clear()
        _metrics["api_calls"].clear()
        _metrics["api_success"].clear()
        _metrics["api_failures"].clear()
        _metrics["api_retries"].clear()
        _metrics["api_latency_ms"].clear()
        _metrics["etl_duration_seconds"].clear()


def log_metrics_summary() -> None:
    """Log a summary of collected metrics at INFO level."""
    if not _check_enabled():
        return

    summary = get_metrics_summary()

    logger.info("=== Metrics Summary ===")

    if summary["etl_rows_loaded"]:
        logger.info("ETL Rows Loaded:")
        for key, count in summary["etl_rows_loaded"].items():
            logger.info("  %s: %s rows", key, f"{count:,}")

    if summary["api_calls"]:
        logger.info("API Calls:")
        for label, total in summary["api_calls"].items():
            success = summary["api_success"].get(label, 0)
            failures = summary["api_failures"].get(label, 0)
            retries = summary["api_retries"].get(label, 0)
            logger.info(
                "  %s: %d calls (%d success, %d failures, %d retries)",
                label,
                total,
                success,
                failures,
                retries,
            )

    if summary["api_latency_summary"]:
        logger.info("API Latency:")
        for label, stats in summary["api_latency_summary"].items():
            logger.info(
                "  %s: avg=%.2fms, min=%.2fms, max=%.2fms (%d samples)",
                label,
                stats["avg_ms"],
                stats["min_ms"],
                stats["max_ms"],
                stats["count"],
            )

    if summary["etl_duration_summary"]:
        logger.info("ETL Duration:")
        for key, stats in summary["etl_duration_summary"].items():
            logger.info(
                "  %s: avg=%.2fs, min=%.2fs, max=%.2fs (%d runs)",
                key,
                stats["avg_s"],
                stats["min_s"],
                stats["max_s"],
                stats["count"],
            )


def export_metrics(endpoint: str | None = None, timeout_seconds: float = 5.0) -> bool:
    """
    Export metrics summary to an HTTP endpoint via POST JSON.

    Returns True when a POST request is sent and succeeds with a 2xx status code.
    Returns False when metrics are disabled, endpoint is not configured, or request fails.
    """
    if not _check_enabled():
        return False

    target = endpoint or MetricsConfig.export_endpoint()
    if not target:
        return False

    payload = get_metrics_summary()
    try:
        import requests

        response = requests.post(target, json=payload, timeout=timeout_seconds)
        response.raise_for_status()
    except Exception as exc:
        logger.warning("Metrics export failed (%s): %s", target, exc)
        return False

    logger.info("Metrics exported to %s", target)
    return True


class ETLTimer:
    """
    Context manager for timing ETL operations.

    Automatically records duration when the context exits.

    Example
    -------
    >>> with ETLTimer("player_game_log", "2023-24"):
    ...     load_season(con, "2023-24")
    """

    def __init__(self, table: str, season_id: str | None = None):
        """
        Initialize the timer.

        Parameters
        ----------
        table : str
            Name of the table.
        season_id : str | None
            Season identifier or None for non-seasonal tables.
        """
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
