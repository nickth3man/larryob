"""
Unified API client with centralized rate limiting for NBA data sources.

Replaces scattered time.sleep() and call_with_backoff() patterns across loaders.
Provides a single APICaller class that:
- Handles exponential backoff retries
- Enforces configurable rate limits
- Integrates with metrics collection
- Supports both nba_api and HTTP scraping (requests)
"""

import logging
import time
from collections.abc import Callable
from typing import Any, TypeVar

from .config import APIConfig
from .metrics import record_api_call, record_retry

logger = logging.getLogger(__name__)

T = TypeVar("T")


class APICaller:
    """
    Unified API client with rate limiting and retry logic.

    Replaces the scattered call_with_backoff() pattern and manual time.sleep()
    calls across loaders. All external API calls should go through this class.
    """

    def __init__(
        self,
        base_sleep: float | None = None,
        max_retries: int | None = None,
        inter_call_sleep: float | None = None,
    ):
        """
        Initialize API caller with configurable rate limits.

        Parameters
        ----------
        base_sleep : float | None
            Base sleep delay between API calls in seconds. Defaults to APIConfig.base_sleep().
        max_retries : int | None
            Maximum number of retry attempts. Defaults to APIConfig.max_retries().
        inter_call_sleep : float | None
            Sleep between successive API calls in a loop. Defaults to APIConfig.inter_call_sleep().
        """
        self._base_sleep = base_sleep if base_sleep is not None else APIConfig.base_sleep()
        self._max_retries = max_retries if max_retries is not None else APIConfig.max_retries()
        self._inter_call_sleep = inter_call_sleep if inter_call_sleep is not None else APIConfig.inter_call_sleep()

    def call_with_backoff(
        self,
        fn: Callable[[], T],
        *,
        label: str = "",
        base_sleep: float | None = None,
    ) -> T:
        """
        Call a function with exponential backoff retry on any exception.

        This is the unified replacement for the scattered call_with_backoff()
        implementations across game_logs.py, play_by_play.py, dimensions.py, etc.

        Parameters
        ----------
        fn : Callable[[], T]
            Zero-arg callable to execute (typically an nba_api endpoint call).
        label : str
            Descriptive label for logging and metrics (e.g., "PlayerGameLogs(2023-24)").
        base_sleep : float | None
            Override the default base sleep for this call.

        Returns
        -------
        T
            The result of the callable.

        Raises
        ------
        Exception
            The last exception if all retries are exhausted.
        """
        sleep_time = base_sleep if base_sleep is not None else self._base_sleep

        for attempt in range(1, self._max_retries + 1):
            try:
                result = fn()
                # Sleep after successful call to respect rate limits
                time.sleep(sleep_time)
                record_api_call(label, success=True, attempt=attempt)
                return result
            except Exception as exc:
                wait = sleep_time * (2 ** attempt)
                logger.warning(
                    "Attempt %d/%d failed for %r: %s — retrying in %.0fs",
                    attempt, self._max_retries, label, exc, wait,
                )
                record_retry(label, attempt, exc)
                if attempt == self._max_retries:
                    record_api_call(label, success=False, attempt=attempt)
                    raise
                time.sleep(wait)

        # This should never be reached, but mypy needs it
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
        Call with custom delay parameters (for endpoints with different rate limits).

        Use this for endpoints that require different rate limiting than the default.
        For example, play-by-play endpoints may allow faster calls than box score endpoints.

        Parameters
        ----------
        fn : Callable[[], T]
            Zero-arg callable to execute.
        label : str
            Descriptive label for logging and metrics.
        base_sleep : float
            Custom base sleep delay for this call.
        max_retries : int | None
            Override default max retries for this call.

        Returns
        -------
        T
            The result of the callable.
        """
        retries = max_retries if max_retries is not None else self._max_retries

        for attempt in range(1, retries + 1):
            try:
                result = fn()
                time.sleep(base_sleep)
                record_api_call(label, success=True, attempt=attempt)
                return result
            except Exception as exc:
                wait = base_sleep * (2 ** attempt)
                logger.warning(
                    "Attempt %d/%d failed for %r: %s — retrying in %.0fs",
                    attempt, retries, label, exc, wait,
                )
                record_retry(label, attempt, exc)
                if attempt == retries:
                    record_api_call(label, success=False, attempt=attempt)
                    raise
                time.sleep(wait)

        raise RuntimeError("Unexpected exit from retry loop")

    def sleep_between_calls(self) -> None:
        """
        Sleep between successive API calls in a loop.

        This replaces the scattered time.sleep() calls in loaders that iterate
        over players/teams/seasons. Use this when making multiple calls in sequence.

        Example
        -------
        >>> for player_id in player_ids:
        ...     result = api.call_with_backoff(lambda: fetch_player(player_id), label=f"player({player_id})")
        ...     api.sleep_between_calls()  # Delay before next iteration
        """
        time.sleep(self._inter_call_sleep)


# Default singleton instance for convenience
_default_api_caller: APICaller | None = None


def get_api_caller() -> APICaller:
    """Get the default singleton API caller instance."""
    global _default_api_caller
    if _default_api_caller is None:
        _default_api_caller = APICaller()
    return _default_api_caller


# Backward compatibility functions for gradual migration
def call_with_backoff(
    fn: Callable[[], Any],
    *,
    base_sleep: float = 3.0,
    max_retries: int = 5,
    label: str = "",
) -> Any:
    """
    Backward-compatible wrapper for existing call_with_backoff() usage.

    This function maintains the same signature as the original in utils.py
    but delegates to the new APICaller class. Existing code will continue to work.

    .. deprecated::
        Use APICaller.call_with_backoff() or get_api_caller() instead.
        This function will be removed in a future version.
    """
    caller = APICaller(base_sleep=base_sleep, max_retries=max_retries)
    return caller.call_with_backoff(fn, label=label, base_sleep=base_sleep)
