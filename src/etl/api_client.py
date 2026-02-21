"""
Unified API client with adaptive rate limiting for NBA data sources.

Replaces scattered time.sleep() and call_with_backoff() patterns across loaders.
Provides a single APICaller class that:
- Handles exponential backoff retries
- Adapts pacing from recent success/failure patterns
- Integrates with metrics collection
"""

import logging
import time
from collections.abc import Callable
from typing import TypeVar

from .config import APIConfig
from .metrics import record_api_call, record_api_latency, record_retry

logger = logging.getLogger(__name__)

T = TypeVar("T")


class APICaller:
    """
    Unified API client with rate limiting and retry logic.

    All external API calls should go through this class.
    """

    def __init__(
        self,
        base_sleep: float | None = None,
        max_retries: int | None = None,
        inter_call_sleep: float | None = None,
    ):
        """
        Initialize API caller with configurable limits.

        Parameters
        ----------
        base_sleep : float | None
            Base delay after successful calls.
        max_retries : int | None
            Maximum retry attempts.
        inter_call_sleep : float | None
            Minimum sleep between iterative calls in loops.
        """
        self._base_sleep = base_sleep if base_sleep is not None else APIConfig.base_sleep()
        self._max_retries = max_retries if max_retries is not None else APIConfig.max_retries()
        self._inter_call_sleep = inter_call_sleep if inter_call_sleep is not None else APIConfig.inter_call_sleep()

        # Adaptive pacing state.
        self._adaptive_sleep = max(0.0, self._base_sleep)
        self._adaptive_min_sleep = min(self._adaptive_sleep, 0.5)
        self._adaptive_max_sleep = max(self._adaptive_sleep * 8, 30.0)
        self._success_streak = 0

    def _note_success(self, used_sleep: float) -> None:
        self._success_streak += 1
        if self._success_streak >= 3:
            self._adaptive_sleep = max(
                self._adaptive_min_sleep,
                min(self._adaptive_sleep, used_sleep) * 0.9,
            )

    def _note_failure(self, wait: float) -> None:
        self._success_streak = 0
        self._adaptive_sleep = min(
            self._adaptive_max_sleep,
            max(self._adaptive_sleep * 1.6, wait / 2.0),
        )

    def call_with_backoff(
        self,
        fn: Callable[[], T],
        *,
        label: str = "",
        base_sleep: float | None = None,
    ) -> T:
        """
        Call a function with exponential backoff retry on any exception.
        """
        sleep_time = base_sleep if base_sleep is not None else self._adaptive_sleep

        for attempt in range(1, self._max_retries + 1):
            try:
                started = time.time()
                result = fn()
                record_api_latency(label, (time.time() - started) * 1000.0)

                time.sleep(sleep_time)
                self._note_success(sleep_time)
                record_api_call(label, success=True, attempt=attempt)
                return result
            except Exception as exc:
                wait = max(sleep_time * (2 ** attempt), self._adaptive_sleep)
                logger.warning(
                    "Attempt %d/%d failed for %r: %s - retrying in %.0fs",
                    attempt,
                    self._max_retries,
                    label,
                    exc,
                    wait,
                )
                record_retry(label, attempt, exc)
                self._note_failure(wait)
                if attempt == self._max_retries:
                    record_api_call(label, success=False, attempt=attempt)
                    raise
                time.sleep(wait)

        raise RuntimeError("Unexpected exit from retry loop")

    def call_with_backoff_custom_delay(
        self,
        fn: Callable[[], T],
        *,
        label: str = "",
        base_sleep: float,
        max_retries: int | None = None,
    ) -> T:
        """
        Call with custom delay parameters.
        """
        retries = max_retries if max_retries is not None else self._max_retries

        for attempt in range(1, retries + 1):
            try:
                started = time.time()
                result = fn()
                record_api_latency(label, (time.time() - started) * 1000.0)
                time.sleep(base_sleep)
                self._note_success(base_sleep)
                record_api_call(label, success=True, attempt=attempt)
                return result
            except Exception as exc:
                wait = base_sleep * (2 ** attempt)
                logger.warning(
                    "Attempt %d/%d failed for %r: %s - retrying in %.0fs",
                    attempt,
                    retries,
                    label,
                    exc,
                    wait,
                )
                record_retry(label, attempt, exc)
                self._note_failure(wait)
                if attempt == retries:
                    record_api_call(label, success=False, attempt=attempt)
                    raise
                time.sleep(wait)

        raise RuntimeError("Unexpected exit from retry loop")

    def sleep_between_calls(self) -> None:
        """
        Sleep between successive API calls in a loop.
        """
        pause = max(self._inter_call_sleep, self._adaptive_sleep * 0.5)
        time.sleep(pause)


_default_api_caller: APICaller | None = None


def get_api_caller() -> APICaller:
    """Get the default singleton API caller instance."""
    global _default_api_caller
    if _default_api_caller is None:
        _default_api_caller = APICaller()
    return _default_api_caller
