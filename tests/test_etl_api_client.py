"""Tests for unified API client (src.etl.api_client)."""

import time
from unittest.mock import MagicMock, patch

import pytest

from src.etl.extract.api_client import APICaller, get_api_caller


class TestAPICaller:
    """Test unified API client with rate limiting."""

    def test_init_default(self, monkeypatch) -> None:
        """Default initialization should use config defaults."""
        # Unset env vars to test true defaults
        monkeypatch.delenv("LARRYOB_API_DELAY_SECONDS", raising=False)
        monkeypatch.delenv("LARRYOB_API_MAX_RETRIES", raising=False)
        monkeypatch.delenv("LARRYOB_INTER_CALL_SLEEP", raising=False)
        caller = APICaller()
        assert caller._base_sleep == 3.0
        assert caller._max_retries == 5
        assert caller._inter_call_sleep == 2.0

    def test_init_custom(self) -> None:
        """Custom values should override defaults."""
        caller = APICaller(base_sleep=1.0, max_retries=3, inter_call_sleep=0.5)
        assert caller._base_sleep == 1.0
        assert caller._max_retries == 3
        assert caller._inter_call_sleep == 0.5

    @patch("src.etl.extract.api_client.time.sleep")
    def test_call_with_backoff_success(self, mock_sleep: MagicMock) -> None:
        """Successful call should sleep once and return result."""
        caller = APICaller(base_sleep=0.1, max_retries=3)

        def fn():
            return "success"

        result = caller.call_with_backoff(fn, label="test")
        assert result == "success"
        assert mock_sleep.call_count == 1  # Sleep after success

    @patch("src.etl.extract.api_client.time.sleep")
    def test_call_with_backoff_retry_then_success(self, mock_sleep: MagicMock) -> None:
        """Should retry on failure and succeed."""
        caller = APICaller(base_sleep=0.1, max_retries=3)

        call_count = 0

        def fn():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ValueError("Temporary failure")
            return "success"

        result = caller.call_with_backoff(fn, label="test")
        assert result == "success"
        assert call_count == 2
        # First attempt failed (retry sleep), second succeeded (normal sleep)
        assert mock_sleep.call_count == 2

    @patch("src.etl.extract.api_client.time.sleep")
    def test_call_with_backoff_exhausted_retries(self, mock_sleep: MagicMock) -> None:
        """Should raise after max retries exhausted."""
        caller = APICaller(base_sleep=0.1, max_retries=2)

        def fn():
            raise ValueError("Persistent failure")

        with pytest.raises(ValueError, match="Persistent failure"):
            caller.call_with_backoff(fn, label="test")

        # Should sleep after first failed attempt (retry sleep)
        # but NOT after final failure (exception raised before sleep)
        assert mock_sleep.call_count == 1

    @patch("src.etl.extract.api_client.time.sleep")
    def test_call_with_backoff_custom_delay(self, mock_sleep: MagicMock) -> None:
        """Custom delay should override instance default."""
        caller = APICaller(base_sleep=1.0, max_retries=3)

        def fn():
            return "success"

        caller.call_with_backoff_custom_delay(fn, base_sleep=0.5, label="test")
        # Check that the custom delay was used (approximately)
        mock_sleep.assert_called_once()
        call_args = mock_sleep.call_args
        assert call_args[0][0] == 0.5

    def test_sleep_between_calls(self) -> None:
        """Should sleep for inter_call_sleep duration."""
        caller = APICaller(inter_call_sleep=0.01, base_sleep=0.01)

        start = time.time()
        caller.sleep_between_calls()
        elapsed = time.time() - start

        assert elapsed >= 0.01
        assert elapsed < 0.1  # Should not take much longer


class TestGetAPICaller:
    """Test singleton API caller getter."""

    def test_get_api_caller_singleton(self) -> None:
        """Should return the same instance on subsequent calls."""
        caller1 = get_api_caller()
        caller2 = get_api_caller()
        assert caller1 is caller2

    def test_get_api_caller_default_config(self, monkeypatch) -> None:
        """Default instance should use config defaults."""
        # Unset env vars to test true defaults
        monkeypatch.delenv("LARRYOB_API_DELAY_SECONDS", raising=False)
        monkeypatch.delenv("LARRYOB_API_MAX_RETRIES", raising=False)
        monkeypatch.delenv("LARRYOB_INTER_CALL_SLEEP", raising=False)
        # Reset singleton to ensure fresh instance with new env
        import src.etl.extract.api_client as api_client_mod

        api_client_mod._default_api_caller = None
        caller = get_api_caller()
        assert caller._base_sleep == 3.0
        assert caller._max_retries == 5
