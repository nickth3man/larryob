"""Metrics summary and export helpers."""

import logging
from typing import Any

from .calculator import _check_enabled, _metrics, _metrics_lock

logger = logging.getLogger(__name__)


def get_metrics_summary() -> dict[str, Any]:
    """Get a summary of all collected metrics."""
    with _metrics_lock:
        summary: dict[str, Any] = {
            "etl_rows_loaded": dict(_metrics["etl_rows_loaded"]),
            "api_calls": dict(_metrics["api_calls"]),
            "api_success": dict(_metrics["api_success"]),
            "api_failures": dict(_metrics["api_failures"]),
            "api_retries": dict(_metrics["api_retries"]),
        }
        latency_snapshot = {k: list(v) for k, v in _metrics["api_latency_ms"].items()}
        duration_snapshot = {k: list(v) for k, v in _metrics["etl_duration_seconds"].items()}

    latency_summary = {}
    for label, latencies in latency_snapshot.items():
        if latencies:
            latency_summary[label] = {
                "min_ms": round(min(latencies), 2),
                "max_ms": round(max(latencies), 2),
                "avg_ms": round(sum(latencies) / len(latencies), 2),
                "count": len(latencies),
            }
    summary["api_latency_summary"] = latency_summary

    duration_summary = {}
    for key, durations in duration_snapshot.items():
        if durations:
            duration_summary[str(key)] = {
                "min_s": round(min(durations), 2),
                "max_s": round(max(durations), 2),
                "avg_s": round(sum(durations) / len(durations), 2),
                "count": len(durations),
            }
    summary["etl_duration_summary"] = duration_summary

    return summary


def log_metrics_summary() -> None:
    """Log a summary of collected metrics."""
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
    """Export metrics summary to an HTTP endpoint."""
    if not _check_enabled():
        return False

    from ..config import MetricsConfig

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
