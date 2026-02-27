"""Salary data ETL package."""

from .extractor import _abbr_to_bref, _season_team_map
from .loader import load_player_salaries, load_salaries_for_seasons, load_salary_cap
from .transformer import _SALARY_CAP_BY_SEASON, _normalize_name

# Backward-compatible alias expected by plan verification/import checks.
extract_salaries = load_salaries_for_seasons

__all__ = [
    "load_salary_cap",
    "load_player_salaries",
    "load_salaries_for_seasons",
    "extract_salaries",
    "_SALARY_CAP_BY_SEASON",
    "_normalize_name",
    "_season_team_map",
    "_abbr_to_bref",
]
