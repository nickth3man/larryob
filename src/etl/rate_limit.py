"""
Adaptive rate limiting and HTTP fetching for Basketball-Reference scraping.

This module provides rate limiting utilities designed for respectful
access to Basketball-Reference.com (non-commercial, personal project use).
"""

import logging
import os
import time

import requests

logger = logging.getLogger(__name__)

# Basketball-Reference constants
BREF_BASE = "https://www.basketball-reference.com"
BREF_HEADERS = {"User-Agent": "Mozilla/5.0 (personal research project, non-commercial)"}


class BBRRateLimitExceeded(RuntimeError):
    """Raised when Basketball-Reference asks for an excessive Retry-After delay."""

    def __init__(self, url: str, retry_after: int, max_allowed: int) -> None:
        """
        Initialize the exception with the URL and retry-after limits that triggered the rate-limit error.
        
        Parameters:
            url (str): The requested URL that caused the rate-limit response.
            retry_after (int): The Retry-After value (in seconds) observed from the server.
            max_allowed (int): The configured maximum allowed Retry-After (in seconds); exceeded value is considered fatal.
        
        Attributes:
            url (str): Same as the `url` parameter.
            retry_after (int): Same as the `retry_after` parameter.
            max_allowed (int): Same as the `max_allowed` parameter.
        """
        self.url = url
        self.retry_after = retry_after
        self.max_allowed = max_allowed
        super().__init__(
            f"BBref rate limit exceeded: url={url} retry_after={retry_after}s max_allowed={max_allowed}s"
        )


def _bref_delay_seconds() -> float:
    """
    Read the configured initial per-request delay for Basketball-Reference throttling.
    
    Returns:
        Initial delay in seconds (float): the value of the environment variable
        LARRYOB_BREF_DELAY_SECONDS converted to float; defaults to 1.5 when unset.
    """
    return float(os.getenv("LARRYOB_BREF_DELAY_SECONDS", "1.5"))


def _bref_max_retries() -> int:
    """
    Read the configured maximum number of retry attempts for Basketball-Reference HTTP fetches from the environment.
    
    Reads the LARRYOB_BREF_MAX_RETRIES environment variable and returns its integer value; if unset, returns 3.
     
    Returns:
        int: Maximum number of retries to perform for fetch attempts.
    """
    return int(os.getenv("LARRYOB_BREF_MAX_RETRIES", "3"))


def _bref_max_retry_after_seconds() -> int:
    """
    Return the configured maximum allowed Retry-After duration in seconds.
    
    Reads the environment variable LARRYOB_BREF_MAX_RETRY_AFTER_SECONDS and parses it as an integer; defaults to 300 if unset or empty.
    
    Returns:
        max_seconds (int): Maximum allowed Retry-After value in seconds.
    """
    return int(os.getenv("LARRYOB_BREF_MAX_RETRY_AFTER_SECONDS", "300"))


class _AdaptiveBRefThrottle:
    """
    Adaptive, process-wide request throttle for Basketball-Reference.

    - Starts cautiously.
    - Backs off aggressively on 429 / transient failures.
    - Slowly ramps up after sustained success.
    """

    def __init__(self) -> None:
        """
        Initialize the adaptive throttle's state.
        
        Sets conservative bounds and starting delay (bounded by min_delay and
        the environment-configured default), initializes the next-allowed timestamp
        to 0, and zeroes the success and rate-limit streak counters.
        """
        self.min_delay = 0.4
        self.max_delay = 30.0
        self.delay = max(self.min_delay, _bref_delay_seconds())
        self.next_allowed_at = 0.0
        self.success_streak = 0
        self.rate_limit_streak = 0

    def _sleep_until_allowed(self) -> None:
        """
        Block until the throttle's next allowed request time is reached.
        
        If the current monotonic time is earlier than self.next_allowed_at, sleep for the remaining seconds; otherwise return immediately.
        """
        now = time.monotonic()
        if now < self.next_allowed_at:
            time.sleep(self.next_allowed_at - now)

    def before_request(self) -> None:
        """
        Ensure the caller waits until the throttle permits the next request.
        
        Blocks execution until the throttle's next allowed timestamp has been reached.
        """
        self._sleep_until_allowed()

    def on_success(self) -> None:
        """
        Record a successful request and adjust throttle state accordingly.
        
        Increments the consecutive success counter, resets the consecutive rate-limit counter,
        reduces the current delay by 10% when the success streak is at least 3 (clamped to min_delay),
        and schedules the next allowed request time to now plus the current delay.
        """
        self.success_streak += 1
        self.rate_limit_streak = 0
        if self.success_streak >= 3:
            self.delay = max(self.min_delay, self.delay * 0.9)
        self.next_allowed_at = time.monotonic() + self.delay

    def on_transient_error(self) -> None:
        """
        Record a transient error by resetting success streak, increasing the current delay, and scheduling the next allowed request time.
        
        This method sets success_streak to 0, raises delay (clamped between min_delay and max_delay) to back off from subsequent requests, and updates next_allowed_at to now plus the new delay.
        """
        self.success_streak = 0
        self.delay = min(self.max_delay, max(self.delay * 1.4, self.delay + 0.5))
        self.next_allowed_at = time.monotonic() + self.delay

    def on_rate_limit(self, retry_after: int | None) -> int:
        """
        Adjust throttle state after receiving a rate-limit signal and compute how long to wait before the next request.
        
        This resets the success streak, increments the rate-limit streak, determines a wait duration (uses the positive `retry_after` when provided, otherwise uses twice the current delay), clamps that duration between `min_delay` and `max_delay`, updates the current `delay` (increasing it toward the computed wait but not above `max_delay`), and sets `next_allowed_at` to the current time plus the computed wait.
        
        Parameters:
            retry_after (int | None): Server-provided Retry-After value in seconds; if `None` or non-positive, the method uses twice the current delay instead.
        
        Returns:
            int: Number of seconds to wait before the next request.
        """
        self.success_streak = 0
        self.rate_limit_streak += 1
        requested_wait = (
            retry_after if retry_after is not None and retry_after > 0 else int(self.delay * 2)
        )
        wait = int(max(self.min_delay, min(self.max_delay, float(requested_wait))))
        self.delay = min(self.max_delay, max(self.delay * 1.8, float(wait)))
        self.next_allowed_at = time.monotonic() + wait
        return wait

    def inter_season_pause(self) -> float:
        """
        Return a short additional pause to insert between requests after recent rate-limiting activity.
        
        Returns:
            pause_seconds (float): A pause in seconds — zero if no recent rate limits, otherwise the smaller of 5.0 and the current throttle delay.
        """
        if self.rate_limit_streak == 0:
            return 0.0
        return min(5.0, self.delay)


# Process-wide singleton throttle instance
_BREF_THROTTLE = _AdaptiveBRefThrottle()


def fetch_html(url: str, max_retries: int | None = None) -> str | None:
    """
    Fetch the HTML content of a Basketball-Reference page using the module's adaptive throttle.
    
    Attempts an HTTP GET for the given URL, applying the process-wide adaptive throttle and retry logic; on 429 responses it respects Retry-After (subject to the configured maximum) and uses exponential backoff between attempts.
    
    Parameters:
        max_retries (int | None): Override for the number of fetch attempts; if None the environment-configured default is used.
    
    Returns:
        str | None: The response body as UTF-8 text on success, `None` when the fetch fails persistently or encounters a non-retryable client error (e.g., 404 or other 4xx).
    
    Raises:
        BBRRateLimitExceeded: If the server's `Retry-After` value exceeds the configured maximum allowed retry-after.
    """
    retries = max_retries if max_retries is not None else _bref_max_retries()
    max_retry_after = _bref_max_retry_after_seconds()
    for attempt in range(retries):
        try:
            _BREF_THROTTLE.before_request()
            resp = requests.get(url, headers=BREF_HEADERS, timeout=20)
            if resp.status_code == 429:
                try:
                    retry_after = int(resp.headers.get("Retry-After", 0))
                except (ValueError, TypeError):
                    retry_after = None
                if retry_after is not None and retry_after > max_retry_after:
                    raise BBRRateLimitExceeded(url, retry_after, max_retry_after)
                wait = _BREF_THROTTLE.on_rate_limit(retry_after)
                logger.warning(
                    "BBref rate-limited (%s): attempt=%d/%d retry_after=%s adaptive_wait=%ds next_delay=%.2fs",
                    url,
                    attempt + 1,
                    retries,
                    retry_after,
                    wait,
                    _BREF_THROTTLE.delay,
                )
                continue
            # Historical team abbreviations often 404; treat as terminal miss.
            if resp.status_code == 404:
                _BREF_THROTTLE.on_success()
                logger.debug("BBref page not found (404): %s", url)
                return None
            # Do not retry other non-rate-limited client errors.
            if 400 <= resp.status_code < 500:
                _BREF_THROTTLE.on_transient_error()
                logger.debug("BBref client error %s for %s; skipping.", resp.status_code, url)
                return None
            resp.raise_for_status()
            resp.encoding = "utf-8"
            _BREF_THROTTLE.on_success()
            return resp.text
        except requests.RequestException as exc:
            if attempt < retries - 1:
                _BREF_THROTTLE.on_transient_error()
                logger.debug("BBref fetch error (%s), retry %d: %s", url, attempt + 1, exc)
            else:
                _BREF_THROTTLE.on_transient_error()
                logger.warning("BBref fetch failed (%s): %s", url, exc)
    return None
