"""Tests for metrics collection (src.etl.metrics)."""

import os
from unittest.mock import MagicMock, patch

import pytest

from src.etl._etl_timer import ETLTimer
from src.etl.metrics import (
    export_metrics,
    get_metrics_summary,
    log_metrics_summary,
    record_api_call,
    record_api_latency,
    record_etl_duration,
    record_etl_rows,
    record_retry,
    reset_metrics,
)


def test_etl_timer_module_exports_context_manager() -> None:
    assert hasattr(ETLTimer, "__enter__")
    assert hasattr(ETLTimer, "__exit__")


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


class TestMetricsExport:
    """Test metrics summary/export lifecycle helpers."""

    def test_export_metrics_returns_false_when_disabled(self) -> None:
        os.environ.pop("LARRYOB_METRICS_ENABLED", None)
        assert export_metrics("http://localhost:9999/metrics") is False

    def test_export_metrics_posts_summary_when_enabled(self) -> None:
        os.environ["LARRYOB_METRICS_ENABLED"] = "true"
        os.environ["LARRYOB_METRICS_ENDPOINT"] = "http://localhost:9999/metrics"
        record_etl_rows("player_game_log", "2023-24", 10)

        with patch("requests.post") as post_mock:
            response = MagicMock()
            response.raise_for_status.return_value = None
            post_mock.return_value = response
            assert export_metrics() is True
            post_mock.assert_called_once()

        os.environ.pop("LARRYOB_METRICS_ENABLED")
        os.environ.pop("LARRYOB_METRICS_ENDPOINT")

    def test_export_metrics_returns_false_when_no_endpoint(self) -> None:
        """Should return False when metrics enabled but no endpoint configured."""
        os.environ["LARRYOB_METRICS_ENABLED"] = "true"
        # Ensure no endpoint is configured
        os.environ.pop("LARRYOB_METRICS_ENDPOINT", None)

        assert export_metrics() is False

        os.environ.pop("LARRYOB_METRICS_ENABLED")

    def test_export_metrics_handles_http_errors(self) -> None:
        """Should return False when HTTP request fails."""
        os.environ["LARRYOB_METRICS_ENABLED"] = "true"
        os.environ["LARRYOB_METRICS_ENDPOINT"] = "http://localhost:9999/metrics"
        record_etl_rows("player_game_log", "2023-24", 10)

        with patch("requests.post") as post_mock:
            post_mock.side_effect = Exception("Connection refused")

            assert export_metrics() is False

        os.environ.pop("LARRYOB_METRICS_ENABLED")
        os.environ.pop("LARRYOB_METRICS_ENDPOINT")

    def test_export_metrics_with_custom_endpoint(self) -> None:
        """Should use custom endpoint parameter when provided."""
        os.environ["LARRYOB_METRICS_ENABLED"] = "true"
        record_etl_rows("player_game_log", "2023-24", 10)

        with patch("requests.post") as post_mock:
            response = MagicMock()
            response.raise_for_status.return_value = None
            post_mock.return_value = response

            custom_endpoint = "http://custom.example.com/metrics"
            assert export_metrics(endpoint=custom_endpoint) is True

            # Verify the custom endpoint was used
            post_mock.assert_called_once()
            call_args = post_mock.call_args
            assert call_args[0][0] == custom_endpoint

        os.environ.pop("LARRYOB_METRICS_ENABLED")

    def test_log_metrics_summary_noop_when_disabled(self) -> None:
        os.environ.pop("LARRYOB_METRICS_ENABLED", None)
        # Should not raise even with no metrics enabled
        log_metrics_summary()

    def test_log_metrics_summary_with_etl_rows(self) -> None:
        """Should log ETL rows summary when metrics are enabled."""
        os.environ["LARRYOB_METRICS_ENABLED"] = "true"

        record_etl_rows("player_game_log", "2023-24", 1234)
        record_etl_rows("team_game_log", "2023-24", 500)

        # Should not raise, should log the summary
        with patch("src.etl.metrics.logger") as logger_mock:
            log_metrics_summary()
            # Verify logger.info was called with the summary header
            logger_mock.info.assert_called()

        os.environ.pop("LARRYOB_METRICS_ENABLED")

    def test_log_metrics_summary_with_api_calls(self) -> None:
        """Should log API calls summary when metrics are enabled."""
        os.environ["LARRYOB_METRICS_ENABLED"] = "true"

        record_api_call("PlayerGameLogs(2023-24)", success=True, attempt=1)
        record_api_call("PlayerGameLogs(2023-24)", success=False, attempt=2)
        record_retry("PlayerGameLogs(2023-24)", 2, ValueError("Rate limit"))

        # Should not raise, should log the summary
        with patch("src.etl.metrics.logger") as logger_mock:
            log_metrics_summary()
            # Verify logger.info was called with the summary header
            logger_mock.info.assert_called()

        os.environ.pop("LARRYOB_METRICS_ENABLED")

    def test_log_metrics_summary_with_latency(self) -> None:
        """Should log API latency summary when metrics are enabled."""
        os.environ["LARRYOB_METRICS_ENABLED"] = "true"

        record_api_latency("PlayerGameLogs(2023-24)", 123.45)
        record_api_latency("PlayerGameLogs(2023-24)", 234.56)

        # Should not raise, should log the summary
        with patch("src.etl.metrics.logger") as logger_mock:
            log_metrics_summary()
            # Verify logger.info was called with the summary header
            logger_mock.info.assert_called()

        os.environ.pop("LARRYOB_METRICS_ENABLED")

    def test_log_metrics_summary_with_durations(self) -> None:
        """Should log ETL duration summary when metrics are enabled."""
        os.environ["LARRYOB_METRICS_ENABLED"] = "true"

        record_etl_duration("player_game_log", "2023-24", 45.5)
        record_etl_duration("player_game_log", "2023-24", 55.5)

        # Should not raise, should log the summary
        with patch("src.etl.metrics.logger") as logger_mock:
            log_metrics_summary()
            # Verify logger.info was called with the summary header
            logger_mock.info.assert_called()

        os.environ.pop("LARRYOB_METRICS_ENABLED")

    def test_log_metrics_summary_comprehensive(self) -> None:
        """Should log comprehensive summary with all metric types."""
        os.environ["LARRYOB_METRICS_ENABLED"] = "true"

        # Record all types of metrics
        record_etl_rows("player_game_log", "2023-24", 1234)
        record_api_call("PlayerGameLogs(2023-24)", success=True, attempt=1)
        record_api_call("PlayerGameLogs(2023-24)", success=False, attempt=2)
        record_retry("PlayerGameLogs(2023-24)", 2, ValueError("Rate limit"))
        record_api_latency("PlayerGameLogs(2023-24)", 123.45)
        record_etl_duration("player_game_log", "2023-24", 45.5)

        # Should not raise, should log the complete summary
        with patch("src.etl.metrics.logger") as logger_mock:
            log_metrics_summary()
            # Verify logger.info was called multiple times for different sections
            assert logger_mock.info.call_count > 0

        os.environ.pop("LARRYOB_METRICS_ENABLED")


class TestMetricsDisabledRecording:
    """Test that individual recording functions respect disabled state."""

    def test_record_etl_duration_when_disabled(self) -> None:
        """Should not record duration when metrics disabled."""
        os.environ.pop("LARRYOB_METRICS_ENABLED", None)

        record_etl_duration("player_game_log", "2023-24", 45.5)

        summary = get_metrics_summary()
        assert len(summary["etl_duration_summary"]) == 0

    def test_record_retry_when_disabled(self) -> None:
        """Should not record retry when metrics disabled."""
        os.environ.pop("LARRYOB_METRICS_ENABLED", None)

        record_retry("PlayerGameLogs(2023-24)", 2, ValueError("Rate limit"))

        summary = get_metrics_summary()
        assert len(summary["api_retries"]) == 0

    def test_record_api_latency_when_disabled(self) -> None:
        """Should not record latency when metrics disabled."""
        os.environ.pop("LARRYOB_METRICS_ENABLED", None)

        record_api_latency("PlayerGameLogs(2023-24)", 123.45)

        summary = get_metrics_summary()
        assert len(summary["api_latency_summary"]) == 0


class TestETLTimerEdgeCases:
    """Test ETLTimer context manager edge cases."""

    def test_etl_timer_without_season_id(self) -> None:
        """Should record duration for non-seasonal tables."""
        os.environ["LARRYOB_METRICS_ENABLED"] = "true"

        import time

        with ETLTimer("dim_player"):
            time.sleep(0.01)

        summary = get_metrics_summary()
        # Should have recorded duration with (table, None) key
        assert len(summary["etl_duration_summary"]) == 1
        # Key is string representation of tuple
        duration_key = "('dim_player', None)"
        assert duration_key in summary["etl_duration_summary"]

        os.environ.pop("LARRYOB_METRICS_ENABLED")

    def test_etl_timer_when_metrics_disabled(self) -> None:
        """Should not record duration when metrics disabled."""
        os.environ.pop("LARRYOB_METRICS_ENABLED", None)

        import time

        with ETLTimer("player_game_log", "2023-24"):
            time.sleep(0.01)

        summary = get_metrics_summary()
        assert len(summary["etl_duration_summary"]) == 0

    def test_etl_timer_with_zero_duration(self) -> None:
        """Should handle zero or near-zero duration gracefully."""
        os.environ["LARRYOB_METRICS_ENABLED"] = "true"

        # Very fast operation - might have zero duration
        with ETLTimer("player_game_log", "2023-24"):
            pass  # No operation, immediate return

        summary = get_metrics_summary()
        assert len(summary["etl_duration_summary"]) == 1

        os.environ.pop("LARRYOB_METRICS_ENABLED")
