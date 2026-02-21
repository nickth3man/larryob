"""Tests for metrics collection (src.etl.metrics)."""

import os

import pytest

from src.etl.metrics import (
    ETLTimer,
    get_metrics_summary,
    record_api_call,
    record_api_latency,
    record_etl_duration,
    record_etl_rows,
    record_retry,
    reset_metrics,
)


@pytest.fixture(autouse=True)
def reset_metrics_before_each_test() -> None:
    """Reset metrics before each test."""
    reset_metrics()


class TestMetricsRecording:
    """Test metrics recording functions."""

    def test_record_etl_rows(self) -> None:
        """Should record ETL row counts."""
        # Enable metrics for testing
        os.environ["LARRYOB_METRICS_ENABLED"] = "true"

        record_etl_rows("player_game_log", "2023-24", 1234)
        record_etl_rows("player_game_log", "2023-24", 567)  # Add to same key

        summary = get_metrics_summary()
        assert summary["etl_rows_loaded"][("player_game_log", "2023-24")] == 1801

        os.environ.pop("LARRYOB_METRICS_ENABLED")

    def test_record_etl_rows_different_tables(self) -> None:
        """Should record rows for different tables separately."""
        os.environ["LARRYOB_METRICS_ENABLED"] = "true"

        record_etl_rows("player_game_log", "2023-24", 100)
        record_etl_rows("team_game_log", "2023-24", 50)
        record_etl_rows("player_game_log", "2024-25", 200)

        summary = get_metrics_summary()
        assert summary["etl_rows_loaded"][("player_game_log", "2023-24")] == 100
        assert summary["etl_rows_loaded"][("team_game_log", "2023-24")] == 50
        assert summary["etl_rows_loaded"][("player_game_log", "2024-25")] == 200

        os.environ.pop("LARRYOB_METRICS_ENABLED")

    def test_record_api_call_success(self) -> None:
        """Should record successful API call."""
        os.environ["LARRYOB_METRICS_ENABLED"] = "true"

        record_api_call("PlayerGameLogs(2023-24)", success=True, attempt=1)

        summary = get_metrics_summary()
        assert summary["api_calls"]["PlayerGameLogs(2023-24)"] == 1
        assert summary["api_success"]["PlayerGameLogs(2023-24)"] == 1
        # Key might not exist if no failures recorded
        assert summary.get("api_failures", {}).get("PlayerGameLogs(2023-24)", 0) == 0

        os.environ.pop("LARRYOB_METRICS_ENABLED")

    def test_record_api_call_failure(self) -> None:
        """Should record failed API call."""
        os.environ["LARRYOB_METRICS_ENABLED"] = "true"

        record_api_call("PlayerGameLogs(2023-24)", success=False, attempt=3)

        summary = get_metrics_summary()
        assert summary["api_calls"]["PlayerGameLogs(2023-24)"] == 1
        assert summary["api_failures"]["PlayerGameLogs(2023-24)"] == 1
        # Key might not exist if no successes recorded
        assert summary.get("api_success", {}).get("PlayerGameLogs(2023-24)", 0) == 0

        os.environ.pop("LARRYOB_METRICS_ENABLED")

    def test_record_retry(self) -> None:
        """Should record retry attempts."""
        os.environ["LARRYOB_METRICS_ENABLED"] = "true"

        record_retry("PlayerGameLogs(2023-24)", 2, ValueError("Rate limit"))

        summary = get_metrics_summary()
        assert summary["api_retries"]["PlayerGameLogs(2023-24)"] == 1

        os.environ.pop("LARRYOB_METRICS_ENABLED")

    def test_record_api_latency(self) -> None:
        """Should record API call latency."""
        os.environ["LARRYOB_METRICS_ENABLED"] = "true"

        record_api_latency("PlayerGameLogs(2023-24)", 123.45)
        record_api_latency("PlayerGameLogs(2023-24)", 234.56)

        summary = get_metrics_summary()
        latency_summary = summary["api_latency_summary"]["PlayerGameLogs(2023-24)"]
        assert latency_summary["min_ms"] == 123.45
        assert latency_summary["max_ms"] == 234.56
        # Round to 1 decimal place to match rounding in metrics.py
        assert latency_summary["avg_ms"] == 179.0  # (123.45 + 234.56) / 2, rounded
        assert latency_summary["count"] == 2

        os.environ.pop("LARRYOB_METRICS_ENABLED")

    def test_record_etl_duration(self) -> None:
        """Should record ETL operation duration."""
        os.environ["LARRYOB_METRICS_ENABLED"] = "true"

        record_etl_duration("player_game_log", "2023-24", 45.5)
        record_etl_duration("player_game_log", "2023-24", 55.5)

        summary = get_metrics_summary()
        duration_summary = summary["etl_duration_summary"]["('player_game_log', '2023-24')"]
        assert duration_summary["min_s"] == 45.5
        assert duration_summary["max_s"] == 55.5
        assert duration_summary["avg_s"] == 50.5  # (45.5 + 55.5) / 2
        assert duration_summary["count"] == 2

        os.environ.pop("LARRYOB_METRICS_ENABLED")


class TestMetricsDisabled:
    """Test that metrics don't record when disabled."""

    def test_metrics_disabled(self) -> None:
        """Should not record metrics when disabled."""
        os.environ.pop("LARRYOB_METRICS_ENABLED", None)

        record_etl_rows("player_game_log", "2023-24", 1234)
        record_api_call("test", success=True, attempt=1)

        summary = get_metrics_summary()
        assert len(summary["etl_rows_loaded"]) == 0
        assert len(summary["api_calls"]) == 0


class TestETLTimer:
    """Test ETLTimer context manager."""

    def test_etl_timer_records_duration(self) -> None:
        """Should record operation duration."""
        os.environ["LARRYOB_METRICS_ENABLED"] = "true"

        import time

        with ETLTimer("player_game_log", "2023-24"):
            time.sleep(0.01)  # Sleep for at least 10ms

        summary = get_metrics_summary()
        duration_summary = summary["etl_duration_summary"]["('player_game_log', '2023-24')"]
        assert duration_summary["count"] == 1
        assert duration_summary["min_s"] >= 0.01
        assert duration_summary["max_s"] >= 0.01

        os.environ.pop("LARRYOB_METRICS_ENABLED")

    def test_etl_timer_exception(self) -> None:
        """Should still record duration even if operation raises."""
        os.environ["LARRYOB_METRICS_ENABLED"] = "true"

        import time

        try:
            with ETLTimer("player_game_log", "2023-24"):
                time.sleep(0.01)
                raise ValueError("Test exception")
        except ValueError:
            pass  # Expected

        # Duration should still be recorded
        summary = get_metrics_summary()
        duration_summary = summary["etl_duration_summary"]["('player_game_log', '2023-24')"]
        assert duration_summary["count"] == 1
        assert duration_summary["min_s"] >= 0.01

        os.environ.pop("LARRYOB_METRICS_ENABLED")


class TestResetMetrics:
    """Test metrics reset functionality."""

    def test_reset_metrics(self) -> None:
        """Should clear all metrics."""
        os.environ["LARRYOB_METRICS_ENABLED"] = "true"

        record_etl_rows("player_game_log", "2023-24", 1234)
        record_api_call("test", success=True, attempt=1)

        reset_metrics()

        summary = get_metrics_summary()
        assert len(summary["etl_rows_loaded"]) == 0
        assert len(summary["api_calls"]) == 0

        os.environ.pop("LARRYOB_METRICS_ENABLED")
