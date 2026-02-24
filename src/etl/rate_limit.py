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
        self.url = url
        self.retry_after = retry_after
        self.max_allowed = max_allowed
        super().__init__(
            f"BBref rate limit exceeded: url={url} retry_after={retry_after}s max_allowed={max_allowed}s"
        )


def _bref_delay_seconds() -> float:
    return float(os.getenv("LARRYOB_BREF_DELAY_SECONDS", "1.5"))


def _bref_max_retries() -> int:
    return int(os.getenv("LARRYOB_BREF_MAX_RETRIES", "3"))


def _bref_max_retry_after_seconds() -> int:
    return int(os.getenv("LARRYOB_BREF_MAX_RETRY_AFTER_SECONDS", "300"))


class _AdaptiveBRefThrottle:
    """
    Adaptive, process-wide request throttle for Basketball-Reference.

    - Starts cautiously.
    - Backs off aggressively on 429 / transient failures.
    - Slowly ramps up after sustained success.
    """

    def __init__(self) -> None:
        self.min_delay = 0.4
        self.max_delay = 30.0
        self.delay = max(self.min_delay, _bref_delay_seconds())
        self.next_allowed_at = 0.0
        self.success_streak = 0
        self.rate_limit_streak = 0

    def _sleep_until_allowed(self) -> None:
        now = time.monotonic()
        if now < self.next_allowed_at:
            time.sleep(self.next_allowed_at - now)

    def before_request(self) -> None:
        self._sleep_until_allowed()

    def on_success(self) -> None:
        self.success_streak += 1
        self.rate_limit_streak = 0
        if self.success_streak >= 3:
            self.delay = max(self.min_delay, self.delay * 0.9)
        self.next_allowed_at = time.monotonic() + self.delay

    def on_transient_error(self) -> None:
        self.success_streak = 0
        self.delay = min(self.max_delay, max(self.delay * 1.4, self.delay + 0.5))
        self.next_allowed_at = time.monotonic() + self.delay

    def on_rate_limit(self, retry_after: int | None) -> int:
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
        if self.rate_limit_streak == 0:
            return 0.0
        return min(5.0, self.delay)


# Process-wide singleton throttle instance
_BREF_THROTTLE = _AdaptiveBRefThrottle()


def fetch_html(url: str, max_retries: int | None = None) -> str | None:
    """
    Fetch URL with exponential backoff on 429 Too Many Requests.
    Returns response text or None on persistent error.
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
