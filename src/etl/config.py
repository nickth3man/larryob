"""
Centralized configuration for NBA data pipeline.

Environment variables can override defaults:
- LARRYOB_API_DELAY_SECONDS: Base delay between API calls (default: 3.0)
- LARRYOB_API_MAX_RETRIES: Maximum retry attempts (default: 5)
- LARRYOB_CACHE_DIR: Override cache directory path
- LARRYOB_METRICS_ENABLED: Enable metrics collection (default: false)
"""

import os
from pathlib import Path
from typing import Any

# -----------------------------------------------------------------------------
# API Rate Limiting Configuration
# -----------------------------------------------------------------------------


class APIConfig:
    """Centralized API rate limiting and retry configuration."""

    @staticmethod
    def base_sleep() -> float:
        """Base sleep delay between API calls in seconds."""
        return float(os.getenv("LARRYOB_API_DELAY_SECONDS", "3.0"))

    @staticmethod
    def max_retries() -> int:
        """Maximum number of retry attempts for failed API calls."""
        return int(os.getenv("LARRYOB_API_MAX_RETRIES", "5"))

    @staticmethod
    def inter_call_sleep() -> float:
        """Sleep between successive API calls in a loop."""
        return float(os.getenv("LARRYOB_INTER_CALL_SLEEP", "2.0"))


# -----------------------------------------------------------------------------
# Cache Configuration
# -----------------------------------------------------------------------------


class CacheConfig:
    """Cache directory and version configuration."""

    CACHE_VERSION = 2  # Bump when ETL output shape changes

    @staticmethod
    def cache_dir() -> Path:
        """Get cache directory path (overrides .cache/ default)."""
        override = os.getenv("LARRYOB_CACHE_DIR")
        if override:
            return Path(override)
        # Default to .cache/ relative to project root
        return Path(__file__).parent.parent.parent / ".cache"


# -----------------------------------------------------------------------------
# Team Metadata (previously in dimensions.py)
# -----------------------------------------------------------------------------

_TEAM_METADATA: dict[str, dict] = {
    "1610612737": {
        "conference": "East",
        "division": "Southeast",
        "arena_name": "State Farm Arena",
        "color_primary": "#E03A3E",
        "color_secondary": "#C1D32F",
        "founded_year": 1949,
    },
    "1610612738": {
        "conference": "East",
        "division": "Atlantic",
        "arena_name": "TD Garden",
        "color_primary": "#007A33",
        "color_secondary": "#BA9653",
        "founded_year": 1946,
    },
    "1610612739": {
        "conference": "East",
        "division": "Central",
        "arena_name": "Rocket Mortgage FieldHouse",
        "color_primary": "#860038",
        "color_secondary": "#FDBB30",
        "founded_year": 1970,
    },
    "1610612740": {
        "conference": "West",
        "division": "Southwest",
        "arena_name": "Smoothie King Center",
        "color_primary": "#0C2C56",
        "color_secondary": "#B4975A",
        "founded_year": 2002,
    },
    "1610612741": {
        "conference": "East",
        "division": "Central",
        "arena_name": "United Center",
        "color_primary": "#CE1141",
        "color_secondary": "#000000",
        "founded_year": 1966,
    },
    "1610612742": {
        "conference": "West",
        "division": "Southwest",
        "arena_name": "American Airlines Center",
        "color_primary": "#002B5C",
        "color_secondary": "#00471B",
        "founded_year": 1980,
    },
    "1610612743": {
        "conference": "West",
        "division": "Northwest",
        "arena_name": "Ball Arena",
        "color_primary": "#0E2240",
        "color_secondary": "#FEC524",
        "founded_year": 1976,
    },
    "1610612744": {
        "conference": "West",
        "division": "Pacific",
        "arena_name": "Chase Center",
        "color_primary": "#1D428A",
        "color_secondary": "#FFC52F",
        "founded_year": 1946,
    },
    "1610612745": {
        "conference": "West",
        "division": "Southwest",
        "arena_name": "Toyota Center",
        "color_primary": "#CE1141",
        "color_secondary": "#C4CED4",
        "founded_year": 1967,
    },
    "1610612746": {
        "conference": "West",
        "division": "Pacific",
        "arena_name": "Crypto.com Arena",
        "color_primary": "#C60C30",
        "color_secondary": "#EF3B24",
        "founded_year": 1970,
    },
    "1610612747": {
        "conference": "West",
        "division": "Pacific",
        "arena_name": "Crypto.com Arena",
        "color_primary": "#552582",
        "color_secondary": "#FDB927",
        "founded_year": 1948,
    },
    "1610612748": {
        "conference": "East",
        "division": "Southeast",
        "arena_name": "Kaseya Center",
        "color_primary": "#98002E",
        "color_secondary": "#000000",
        "founded_year": 1988,
    },
    "1610612749": {
        "conference": "East",
        "division": "Central",
        "arena_name": "Fiserv Forum",
        "color_primary": "#00471B",
        "color_secondary": "#EEE1C6",
        "founded_year": 1968,
    },
    "1610612750": {
        "conference": "West",
        "division": "Northwest",
        "arena_name": "Target Center",
        "color_primary": "#0C2340",
        "color_secondary": "#9EA2A2",
        "founded_year": 1989,
    },
    "1610612751": {
        "conference": "East",
        "division": "Atlantic",
        "arena_name": "Barclays Center",
        "color_primary": "#000000",
        "color_secondary": "#FFFFFF",
        "founded_year": 1976,
    },
    "1610612752": {
        "conference": "East",
        "division": "Atlantic",
        "arena_name": "Madison Square Garden",
        "color_primary": "#006BB6",
        "color_secondary": "#F58426",
        "founded_year": 1946,
    },
    "1610612753": {
        "conference": "East",
        "division": "Southeast",
        "arena_name": "Kia Center",
        "color_primary": "#0077C0",
        "color_secondary": "#000000",
        "founded_year": 1989,
    },
    "1610612754": {
        "conference": "East",
        "division": "Central",
        "arena_name": "Gainbridge Fieldhouse",
        "color_primary": "#002D62",
        "color_secondary": "#FDBB30",
        "founded_year": 1976,
    },
    "1610612755": {
        "conference": "East",
        "division": "Atlantic",
        "arena_name": "Wells Fargo Center",
        "color_primary": "#006BB6",
        "color_secondary": "#ED174C",
        "founded_year": 1949,
    },
    "1610612756": {
        "conference": "West",
        "division": "Pacific",
        "arena_name": "Footprint Center",
        "color_primary": "#1D1160",
        "color_secondary": "#E56020",
        "founded_year": 1968,
    },
    "1610612757": {
        "conference": "West",
        "division": "Northwest",
        "arena_name": "Moda Center",
        "color_primary": "#E03A3E",
        "color_secondary": "#000000",
        "founded_year": 1970,
    },
    "1610612758": {
        "conference": "West",
        "division": "Pacific",
        "arena_name": "Golden 1 Center",
        "color_primary": "#5A2D81",
        "color_secondary": "#888888",
        "founded_year": 1948,
    },
    "1610612759": {
        "conference": "West",
        "division": "Southwest",
        "arena_name": "Frost Bank Center",
        "color_primary": "#000000",
        "color_secondary": "#C4CED4",
        "founded_year": 1976,
    },
    "1610612760": {
        "conference": "West",
        "division": "Northwest",
        "arena_name": "Paycom Center",
        "color_primary": "#007AC1",
        "color_secondary": "#EF3B24",
        "founded_year": 2008,
    },
    "1610612761": {
        "conference": "East",
        "division": "Atlantic",
        "arena_name": "Scotiabank Arena",
        "color_primary": "#CE1141",
        "color_secondary": "#000000",
        "founded_year": 1995,
    },
    "1610612762": {
        "conference": "West",
        "division": "Northwest",
        "arena_name": "Delta Center",
        "color_primary": "#002B5C",
        "color_secondary": "#00471B",
        "founded_year": 1974,
    },
    "1610612763": {
        "conference": "West",
        "division": "Southwest",
        "arena_name": "FedExForum",
        "color_primary": "#12173F",
        "color_secondary": "#6ECEB2",
        "founded_year": 1995,
    },
    "1610612764": {
        "conference": "East",
        "division": "Southeast",
        "arena_name": "Capital One Arena",
        "color_primary": "#002B5C",
        "color_secondary": "#E31837",
        "founded_year": 1961,
    },
    "1610612765": {
        "conference": "East",
        "division": "Central",
        "arena_name": "Little Caesars Arena",
        "color_primary": "#C8102E",
        "color_secondary": "#1D42BA",
        "founded_year": 1948,
    },
    "1610612766": {
        "conference": "East",
        "division": "Southeast",
        "arena_name": "Spectrum Center",
        "color_primary": "#1D1160",
        "color_secondary": "#00788C",
        "founded_year": 1988,
    },
}


def get_team_metadata(team_id: str) -> dict[str, Any] | None:
    """Get metadata for a team by ID."""
    return _TEAM_METADATA.get(team_id)


# -----------------------------------------------------------------------------
# Salary Cap Data (previously in salaries.py)
# -----------------------------------------------------------------------------

_SALARY_CAP_BY_SEASON: dict[str, int] = {
    "1984-85": 3_600_000,
    "1985-86": 4_233_000,
    "1986-87": 4_945_000,
    "1987-88": 6_164_000,
    "1988-89": 7_232_000,
    "1989-90": 9_802_000,
    "1990-91": 11_871_000,
    "1991-92": 12_500_000,
    "1992-93": 14_000_000,
    "1993-94": 15_175_000,
    "1994-95": 15_964_000,
    "1995-96": 23_000_000,
    "1997-98": 26_900_000,
    "1998-99": 30_000_000,
    "1999-00": 34_000_000,
    "2000-01": 35_500_000,
    "2001-02": 42_500_000,
    "2002-03": 40_271_000,
    "2003-04": 43_870_000,
    "2004-05": 43_870_000,
    "2005-06": 49_500_000,
    "2006-07": 53_135_000,
    "2007-08": 55_630_000,
    "2008-09": 58_680_000,
    "2009-10": 57_700_000,
    "2010-11": 58_044_000,
    "2011-12": 58_044_000,
    "2012-13": 58_044_000,
    "2013-14": 58_679_000,
    "2014-15": 63_065_000,
    "2015-16": 70_000_000,
    "2016-17": 94_143_000,
    "2017-18": 99_093_000,
    "2018-19": 101_869_000,
    "2019-20": 109_140_000,
    "2020-21": 109_140_000,
    "2021-22": 112_414_000,
    "2022-23": 123_655_000,
    "2023-24": 136_021_000,
    "2024-25": 140_588_000,
    "2025-26": 150_267_000,
}


def get_salary_cap(season_id: str) -> int | None:
    """Get salary cap amount for a season."""
    return _SALARY_CAP_BY_SEASON.get(season_id)


def get_all_salary_caps() -> dict[str, int]:
    """Get all salary caps as a dictionary (for backward compatibility)."""
    return _SALARY_CAP_BY_SEASON.copy()


# -----------------------------------------------------------------------------
# Basketball-Reference Abbreviation Mapping (previously in salaries.py)
# -----------------------------------------------------------------------------

_ABBR_TO_BREF: dict[str, str] = {
    "ATL": "ATL",
    "BKN": "BRK",
    "BOS": "BOS",
    "CHA": "CHO",
    "CHI": "CHI",
    "CLE": "CLE",
    "DAL": "DAL",
    "DEN": "DEN",
    "DET": "DET",
    "GSW": "GSW",
    "HOU": "HOU",
    "IND": "IND",
    "LAC": "LAC",
    "LAL": "LAL",
    "MEM": "MEM",
    "MIA": "MIA",
    "MIL": "MIL",
    "MIN": "MIN",
    "NOP": "NOP",
    "NYK": "NYK",
    "OKC": "OKC",
    "ORL": "ORL",
    "PHI": "PHI",
    "PHX": "PHO",
    "POR": "POR",
    "SAC": "SAC",
    "SAS": "SAS",
    "TOR": "TOR",
    "UTA": "UTA",
    "WAS": "WAS",
}


def nba_abbr_to_bref(abbr: str) -> str | None:
    """Convert NBA abbreviation to Basketball-Reference abbreviation."""
    return _ABBR_TO_BREF.get(abbr)


# -----------------------------------------------------------------------------
# Metrics Configuration
# -----------------------------------------------------------------------------


class MetricsConfig:
    """Metrics collection configuration."""

    @staticmethod
    def enabled() -> bool:
        """Check if metrics collection is enabled."""
        return os.getenv("LARRYOB_METRICS_ENABLED", "false").lower() in ("true", "1", "yes")

    @staticmethod
    def export_endpoint() -> str | None:
        """Get metrics export endpoint URL (optional)."""
        return os.getenv("LARRYOB_METRICS_ENDPOINT")
