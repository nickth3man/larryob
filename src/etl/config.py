"""
Centralized configuration for NBA data pipeline.

Environment variables can override defaults:
- LARRYOB_API_DELAY_SECONDS: Base delay between API calls (default: 3.0)
- LARRYOB_API_MAX_RETRIES: Maximum retry attempts (default: 5)
- LARRYOB_CACHE_DIR: Override cache directory path
- LARRYOB_METRICS_ENABLED: Enable metrics collection (default: false)
"""

import json
import os
from pathlib import Path
from typing import Any

# -----------------------------------------------------------------------------
# Data Directory Path
# -----------------------------------------------------------------------------

_DATA_DIR = Path(__file__).parent / "data"


def _load_json(filename: str) -> dict:
    """
    Load and parse a JSON file from the module's data directory.
    
    Parameters:
        filename (str): Name of the JSON file located in the module data directory (relative path).
    
    Returns:
        dict: The parsed JSON object.
    """
    filepath = _DATA_DIR / filename
    with open(filepath, encoding="utf-8") as f:
        return json.load(f)


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
# Team Metadata (loaded from JSON)
# -----------------------------------------------------------------------------

_TEAM_METADATA: dict[str, dict] = _load_json("team_metadata.json")


def get_team_metadata(team_id: str) -> dict[str, Any] | None:
    """
    Retrieve metadata for a team given its identifier.
    
    Parameters:
        team_id (str): Team identifier key used in the loaded team metadata.
    
    Returns:
        dict[str, Any] | None: The team's metadata dictionary if present, `None` if the team_id is not found.
    """
    return _TEAM_METADATA.get(team_id)


# -----------------------------------------------------------------------------
# Salary Cap Data (loaded from JSON)
# -----------------------------------------------------------------------------

_SALARY_CAP_BY_SEASON: dict[str, int] = _load_json("salary_cap.json")


def get_salary_cap(season_id: str) -> int | None:
    """
    Retrieve the salary cap for a given NBA season.
    
    @param season_id: Season identifier used as key in the salary cap data (e.g., "2024-25").
    @returns: `int` salary cap in dollars for the specified season, `None` if the season is not present.
    """
    return _SALARY_CAP_BY_SEASON.get(season_id)


def get_all_salary_caps() -> dict[str, int]:
    """Get all salary caps as a dictionary (for backward compatibility)."""
    return _SALARY_CAP_BY_SEASON.copy()


# -----------------------------------------------------------------------------
# Basketball-Reference Abbreviation Mapping (loaded from JSON)
# -----------------------------------------------------------------------------

_ABBR_TO_BREF: dict[str, str] = _load_json("abbr_mappings.json")


def nba_abbr_to_bref(abbr: str) -> str | None:
    """
    Map an NBA team abbreviation to its Basketball-Reference equivalent.
    
    Parameters:
        abbr (str): NBA team abbreviation to convert (case-sensitive).
    
    Returns:
        str | None: Basketball-Reference abbreviation if found, None otherwise.
    """
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
