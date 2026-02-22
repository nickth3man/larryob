"""Tests: ETL salary loaders — pure-logic helpers and mock-boundary integrations."""

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from src.etl.salaries import (
    _SALARY_CAP_BY_SEASON,
    BBRRateLimitExceeded,
    _get_html,
    _normalize_name,
    _parse_salary,
    _season_team_map,
    load_salary_cap,
)


@pytest.fixture(autouse=True)
def reset_bref_throttle_state():
    import src.etl.salaries as salaries_mod

    salaries_mod._BREF_THROTTLE.delay = salaries_mod._bref_delay_seconds()
    salaries_mod._BREF_THROTTLE.next_allowed_at = 0.0
    salaries_mod._BREF_THROTTLE.success_streak = 0
    salaries_mod._BREF_THROTTLE.rate_limit_streak = 0


# ------------------------------------------------------------------ #
# _normalize_name                                                     #
# ------------------------------------------------------------------ #


def test_normalize_name_strips_accents() -> None:
    assert _normalize_name("Nikola Jokić") == "nikola jokic"


def test_normalize_name_lowercases_and_strips() -> None:
    assert _normalize_name("  LeBron James  ") == "lebron james"


def test_normalize_name_removes_non_alpha_characters() -> None:
    assert _normalize_name("D'Angelo Russell") == "dangelo russell"


def test_normalize_name_handles_hyphenated_names() -> None:
    result = _normalize_name("Karl-Anthony Towns")
    assert result == "karlanthony towns"


def test_normalize_name_handles_empty_string() -> None:
    assert _normalize_name("") == ""


# ------------------------------------------------------------------ #
# _parse_salary                                                       #
# ------------------------------------------------------------------ #


def test_parse_salary_converts_formatted_string() -> None:
    assert _parse_salary("$12,345,678") == 12_345_678


def test_parse_salary_returns_none_for_non_string_input() -> None:
    assert _parse_salary(None) is None
    assert _parse_salary(12_000_000) is None


def test_parse_salary_returns_none_for_empty_after_stripping() -> None:
    assert _parse_salary("$") is None
    assert _parse_salary("N/A") is None


def test_parse_salary_handles_plain_number_string() -> None:
    assert _parse_salary("5000000") == 5_000_000


def test_parse_salary_handles_zero() -> None:
    assert _parse_salary("$0") == 0


# ------------------------------------------------------------------ #
# _get_html                                                           #
# ------------------------------------------------------------------ #


def test_get_html_returns_text_on_success() -> None:
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = "<html>ok</html>"
    with patch("src.etl.salaries.requests.get", return_value=mock_resp) as mock_get:
        result = _get_html("http://example.com")
    assert result == "<html>ok</html>"
    mock_get.assert_called_once()


def test_get_html_returns_none_on_persistent_error() -> None:
    import requests as req_mod

    with patch("src.etl.salaries.requests.get", side_effect=req_mod.RequestException("timeout")):
        with patch("src.etl.salaries.time.sleep"):
            result = _get_html("http://example.com", max_retries=2)
    assert result is None


def test_get_html_retries_on_429_then_succeeds() -> None:
    rate_limited = MagicMock()
    rate_limited.status_code = 429
    rate_limited.headers = {"Retry-After": "1"}

    ok = MagicMock()
    ok.status_code = 200
    ok.text = "content"

    with patch("src.etl.salaries.requests.get", side_effect=[rate_limited, ok]):
        with patch("src.etl.salaries.time.sleep"):
            result = _get_html("http://example.com", max_retries=3)
    assert result == "content"


def test_get_html_handles_invalid_retry_after_header() -> None:
    rate_limited = MagicMock()
    rate_limited.status_code = 429
    rate_limited.headers = {"Retry-After": "not-a-number"}

    ok = MagicMock()
    ok.status_code = 200
    ok.text = "ok"

    with patch("src.etl.salaries.requests.get", side_effect=[rate_limited, ok]):
        with patch("src.etl.salaries.time.sleep"):
            result = _get_html("http://example.com", max_retries=3)
    assert result == "ok"


def test_get_html_returns_none_on_404() -> None:
    not_found = MagicMock()
    not_found.status_code = 404
    with patch("src.etl.salaries.requests.get", return_value=not_found):
        result = _get_html("http://example.com")
    assert result is None


def test_get_html_skips_extreme_retry_after() -> None:
    rate_limited = MagicMock()
    rate_limited.status_code = 429
    rate_limited.headers = {"Retry-After": "3600"}
    with patch("src.etl.salaries.requests.get", return_value=rate_limited):
        with patch("src.etl.salaries.time.sleep") as mocked_sleep:
            try:
                _get_html("http://example.com")
            except BBRRateLimitExceeded:
                pass
            else:  # pragma: no cover - defensive
                raise AssertionError("Expected BBRRateLimitExceeded to be raised")
    mocked_sleep.assert_not_called()


# ------------------------------------------------------------------ #
# load_salary_cap                                                     #
# ------------------------------------------------------------------ #


def test_load_salary_cap_seeds_all_historical_seasons(sqlite_con: sqlite3.Connection) -> None:
    inserted = load_salary_cap(sqlite_con)
    count = sqlite_con.execute("SELECT COUNT(*) FROM dim_salary_cap").fetchone()[0]
    assert count == len(_SALARY_CAP_BY_SEASON)
    assert inserted == len(_SALARY_CAP_BY_SEASON)


def test_load_salary_cap_is_idempotent(sqlite_con: sqlite3.Connection) -> None:
    load_salary_cap(sqlite_con)
    load_salary_cap(sqlite_con)
    count = sqlite_con.execute("SELECT COUNT(*) FROM dim_salary_cap").fetchone()[0]
    assert count == len(_SALARY_CAP_BY_SEASON)


def test_load_salary_cap_values_are_positive(sqlite_con: sqlite3.Connection) -> None:
    load_salary_cap(sqlite_con)
    min_cap = sqlite_con.execute("SELECT MIN(cap_amount) FROM dim_salary_cap").fetchone()[0]
    assert min_cap > 0


def test_load_salary_cap_known_season_value(sqlite_con: sqlite3.Connection) -> None:
    load_salary_cap(sqlite_con)
    cap = sqlite_con.execute(
        "SELECT cap_amount FROM dim_salary_cap WHERE season_id='2023-24'"
    ).fetchone()[0]
    assert cap == 136_021_000


def test_load_salary_cap_first_season_is_1984_85(sqlite_con: sqlite3.Connection) -> None:
    load_salary_cap(sqlite_con)
    seasons = [
        r[0]
        for r in sqlite_con.execute(
            "SELECT season_id FROM dim_salary_cap ORDER BY season_id"
        ).fetchall()
    ]
    assert seasons[0] == "1984-85"


def test_season_team_map_uses_fact_team_season_when_present(sqlite_con: sqlite3.Connection) -> None:
    sqlite_con.execute(
        "INSERT INTO dim_season (season_id, start_year, end_year) VALUES ('1960-61', 1960, 1961)"
    )
    sqlite_con.execute(
        """
        INSERT INTO dim_team (
            team_id, abbreviation, full_name, city, nickname,
            conference, division, color_primary, color_secondary, arena_name, founded_year
        ) VALUES (
            '1610612747', 'LAL', 'Los Angeles Lakers', 'Los Angeles', 'Lakers',
            'West', 'Pacific', '#552583', '#FDB927', 'Crypto.com Arena', 1947
        )
        """
    )
    sqlite_con.execute(
        """
        INSERT INTO dim_team_history (
            team_id, team_city, team_name, team_abbrev,
            season_founded, season_active_till, league
        ) VALUES (
            '1610612747', 'Minneapolis', 'Lakers', 'MNL',
            1948, 1960, 'NBA'
        )
        """
    )
    sqlite_con.execute(
        """
        INSERT INTO fact_team_season (season_id, bref_abbrev, lg, playoffs)
        VALUES ('1960-61', 'MNL', 'NBA', 1)
        """
    )
    sqlite_con.commit()

    mapping, source = _season_team_map(sqlite_con, "1960-61")
    assert source == "fact_team_season"
    assert mapping == {"MNL": "1610612747"}
