"""Tests for src.etl.rate_limit — adaptive throttle state machine and fetch_html."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from src.etl.rate_limit import (
    BBRRateLimitExceeded,
    _AdaptiveBRefThrottle,
    fetch_html,
)

# ------------------------------------------------------------------ #
# _AdaptiveBRefThrottle — initial state                              #
# ------------------------------------------------------------------ #


def test_throttle_initial_delay_above_min():
    t = _AdaptiveBRefThrottle()
    assert t.delay >= t.min_delay


def test_throttle_initial_streaks_are_zero():
    t = _AdaptiveBRefThrottle()
    assert t.success_streak == 0
    assert t.rate_limit_streak == 0


# ------------------------------------------------------------------ #
# on_success                                                          #
# ------------------------------------------------------------------ #


def test_on_success_increments_streak():
    t = _AdaptiveBRefThrottle()
    t.on_success()
    assert t.success_streak == 1


def test_on_success_resets_rate_limit_streak():
    t = _AdaptiveBRefThrottle()
    t.rate_limit_streak = 5
    t.on_success()
    assert t.rate_limit_streak == 0


def test_on_success_reduces_delay_after_three_in_a_row():
    t = _AdaptiveBRefThrottle()
    t.delay = 2.0
    t.on_success()
    t.on_success()
    initial_delay = t.delay
    t.on_success()  # 3rd consecutive success — should reduce
    assert t.delay < initial_delay


def test_on_success_never_drops_below_min_delay():
    t = _AdaptiveBRefThrottle()
    t.delay = t.min_delay
    for _ in range(20):
        t.on_success()
    assert t.delay >= t.min_delay


def test_on_success_advances_next_allowed_at():
    t = _AdaptiveBRefThrottle()
    with patch("src.etl.rate_limit.time") as mock_time:
        mock_time.monotonic.return_value = 100.0
        t.on_success()
        assert t.next_allowed_at > 100.0


# ------------------------------------------------------------------ #
# on_transient_error                                                  #
# ------------------------------------------------------------------ #


def test_on_transient_error_resets_success_streak():
    t = _AdaptiveBRefThrottle()
    t.success_streak = 10
    t.on_transient_error()
    assert t.success_streak == 0


def test_on_transient_error_increases_delay():
    t = _AdaptiveBRefThrottle()
    old_delay = t.delay
    t.on_transient_error()
    assert t.delay > old_delay


def test_on_transient_error_caps_at_max_delay():
    t = _AdaptiveBRefThrottle()
    t.delay = t.max_delay
    t.on_transient_error()
    assert t.delay <= t.max_delay


# ------------------------------------------------------------------ #
# on_rate_limit                                                       #
# ------------------------------------------------------------------ #


def test_on_rate_limit_resets_success_streak():
    t = _AdaptiveBRefThrottle()
    t.success_streak = 7
    t.on_rate_limit(60)
    assert t.success_streak == 0


def test_on_rate_limit_increments_rate_limit_streak():
    t = _AdaptiveBRefThrottle()
    t.on_rate_limit(30)
    assert t.rate_limit_streak == 1


def test_on_rate_limit_returns_wait_seconds():
    t = _AdaptiveBRefThrottle()
    wait = t.on_rate_limit(60)
    assert isinstance(wait, int)
    assert wait > 0


def test_on_rate_limit_uses_retry_after_when_provided():
    t = _AdaptiveBRefThrottle()
    wait = t.on_rate_limit(45)
    # Wait should be 45 (clamped to max_delay if needed)
    assert wait <= t.max_delay
    assert wait >= t.min_delay


def test_on_rate_limit_fallback_when_none():
    t = _AdaptiveBRefThrottle()
    t.delay = 5.0
    wait = t.on_rate_limit(None)
    assert wait >= 1  # fallback = 2 * delay, clamped


def test_on_rate_limit_fallback_when_zero():
    t = _AdaptiveBRefThrottle()
    t.delay = 4.0
    wait = t.on_rate_limit(0)
    assert wait >= 1


def test_on_rate_limit_advances_next_allowed_at():
    t = _AdaptiveBRefThrottle()
    with patch("src.etl.rate_limit.time") as mock_time:
        mock_time.monotonic.return_value = 100.0
        t.on_rate_limit(10)
        assert t.next_allowed_at > 100.0


# ------------------------------------------------------------------ #
# inter_season_pause                                                  #
# ------------------------------------------------------------------ #


def test_inter_season_pause_zero_when_no_rate_limits():
    t = _AdaptiveBRefThrottle()
    assert t.inter_season_pause() == 0.0


def test_inter_season_pause_nonzero_after_rate_limit():
    t = _AdaptiveBRefThrottle()
    t.on_rate_limit(60)
    assert t.inter_season_pause() > 0.0


def test_inter_season_pause_capped_at_five_seconds():
    t = _AdaptiveBRefThrottle()
    t.delay = 100.0  # force a large delay
    t.rate_limit_streak = 3
    assert t.inter_season_pause() <= 5.0


# ------------------------------------------------------------------ #
# BBRRateLimitExceeded                                               #
# ------------------------------------------------------------------ #


def test_bbrrate_limit_exceeded_attributes():
    exc = BBRRateLimitExceeded("http://example.com", retry_after=600, max_allowed=300)
    assert exc.url == "http://example.com"
    assert exc.retry_after == 600
    assert exc.max_allowed == 300
    assert "600" in str(exc)


# ------------------------------------------------------------------ #
# fetch_html — success path                                           #
# ------------------------------------------------------------------ #


def _mock_response(status_code: int, text: str = "<html/>", headers: dict | None = None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = text
    resp.headers = headers or {}
    resp.raise_for_status = MagicMock()
    resp.encoding = "utf-8"
    return resp


def test_fetch_html_returns_text_on_200():
    with (
        patch("src.etl.rate_limit.requests.get") as mock_get,
        patch("src.etl.rate_limit._BREF_THROTTLE") as mock_throttle,
    ):
        mock_throttle.before_request = MagicMock()
        mock_throttle.on_success = MagicMock()
        mock_get.return_value = _mock_response(200, "<html>ok</html>")
        result = fetch_html("http://example.com", max_retries=1)
    assert result == "<html>ok</html>"


def test_fetch_html_returns_none_on_404():
    with (
        patch("src.etl.rate_limit.requests.get") as mock_get,
        patch("src.etl.rate_limit._BREF_THROTTLE") as mock_throttle,
    ):
        mock_throttle.before_request = MagicMock()
        mock_throttle.on_success = MagicMock()
        mock_get.return_value = _mock_response(404)
        result = fetch_html("http://example.com/missing", max_retries=1)
    assert result is None


# ------------------------------------------------------------------ #
# fetch_html — 429 handling                                           #
# ------------------------------------------------------------------ #


def test_fetch_html_retries_on_429_and_succeeds():
    responses = [
        _mock_response(429, headers={"Retry-After": "1"}),
        _mock_response(200, "<html>ok</html>"),
    ]
    with (
        patch("src.etl.rate_limit.requests.get", side_effect=responses),
        patch("src.etl.rate_limit._BREF_THROTTLE") as mock_throttle,
    ):
        mock_throttle.before_request = MagicMock()
        mock_throttle.on_success = MagicMock()
        mock_throttle.on_rate_limit = MagicMock(return_value=1)
        result = fetch_html("http://example.com", max_retries=3)
    assert result == "<html>ok</html>"


def test_fetch_html_raises_on_excessive_retry_after():
    with (
        patch("src.etl.rate_limit.requests.get") as mock_get,
        patch("src.etl.rate_limit._BREF_THROTTLE") as mock_throttle,
        patch("src.etl.rate_limit._bref_max_retry_after_seconds", return_value=300),
    ):
        mock_throttle.before_request = MagicMock()
        mock_get.return_value = _mock_response(429, headers={"Retry-After": "600"})
        with pytest.raises(BBRRateLimitExceeded) as exc_info:
            fetch_html("http://example.com", max_retries=3)
    assert exc_info.value.retry_after == 600


# ------------------------------------------------------------------ #
# fetch_html — 4xx non-retryable                                      #
# ------------------------------------------------------------------ #


def test_fetch_html_returns_none_on_other_4xx():
    with (
        patch("src.etl.rate_limit.requests.get") as mock_get,
        patch("src.etl.rate_limit._BREF_THROTTLE") as mock_throttle,
    ):
        mock_throttle.before_request = MagicMock()
        mock_throttle.on_transient_error = MagicMock()
        mock_get.return_value = _mock_response(403)
        result = fetch_html("http://example.com", max_retries=3)
    assert result is None


# ------------------------------------------------------------------ #
# fetch_html — persistent network error                               #
# ------------------------------------------------------------------ #


def test_fetch_html_returns_none_after_all_retries_fail():
    with (
        patch("src.etl.rate_limit.requests.get", side_effect=requests.RequestException("timeout")),
        patch("src.etl.rate_limit._BREF_THROTTLE") as mock_throttle,
    ):
        mock_throttle.before_request = MagicMock()
        mock_throttle.on_transient_error = MagicMock()
        result = fetch_html("http://example.com", max_retries=3)
    assert result is None
