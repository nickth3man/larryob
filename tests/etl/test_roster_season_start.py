"""Tests for roster season start date helpers."""

from src.etl.roster import _season_start_date


def test_season_start_date_returns_october_first() -> None:
    assert _season_start_date("2023-24") == "2023-10-01"


def test_season_start_date_uses_start_year() -> None:
    assert _season_start_date("1999-00") == "1999-10-01"


def test_season_start_date_recent_season() -> None:
    assert _season_start_date("2024-25") == "2024-10-01"


def test_season_start_date_with_current_season() -> None:
    assert _season_start_date("2024-25") == "2024-10-01"


def test_season_start_date_with_historical_season() -> None:
    assert _season_start_date("1980-81") == "1980-10-01"


def test_season_start_date_with_future_season() -> None:
    assert _season_start_date("2027-28") == "2027-10-01"


def test_season_start_date_with_old_format() -> None:
    assert _season_start_date("1999-00") == "1999-10-01"
