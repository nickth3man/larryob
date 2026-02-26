"""
Input validation helpers for the ingest pipeline.

All functions here are pure (no I/O, no side effects) and raise typed exceptions
from `exceptions.py`. They are intentionally kept free of imports from other
pipeline modules so that `models.py` can safely import `_normalize_seasons`.

Design Decisions
----------------
- All functions are pure (no I/O, no side effects)
- Regex patterns are reused from constants.py for consistency
- Validation errors include the invalid value for debugging
- Season normalization preserves order while de-duplicating

Usage
-----
    seasons = _normalize_seasons(["2023-24", "2024-25"])
    _validate_seasons(seasons)
    safe_name = validate_view_name("vw_player_totals")
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from pathlib import Path

from src.db.operations import _validate_identifier as _validate_sql_identifier
from src.pipeline.constants import _SEASON_ID_PATTERN, _VALID_IDENTIFIER
from src.pipeline.exceptions import AnalyticsError, ValidationError

#: Supported analytics export file extensions
SUPPORTED_ANALYTICS_EXTENSIONS: frozenset[str] = frozenset({".csv", ".parquet", ".json"})


def _normalize_seasons(raw_seasons: Sequence[str]) -> list[str]:
    """Normalize seasons by trimming and de-duplicating while preserving order.

    Args:
        raw_seasons: Raw season strings from CLI or config.

    Returns:
        List of cleaned, de-duplicated season strings in original order.

    Examples:
        >>> _normalize_seasons(["2023-24", " 2024-25 ", "2023-24"])
        ['2023-24', '2024-25']
        >>> _normalize_seasons(["", "2023-24"])
        ['2023-24']
    """
    normalized: list[str] = []
    seen: set[str] = set()
    for season in raw_seasons:
        cleaned = season.strip()
        if not cleaned or cleaned in seen:
            continue
        normalized.append(cleaned)
        seen.add(cleaned)
    return normalized


def validate_view_name(name: str) -> str:
    """Validate and return a safe analytics view name.

    Performs two-stage validation:
    1. Regex check against valid identifier pattern
    2. SQL identifier validation for extra safety

    Args:
        name: The view name to validate.

    Returns:
        The validated view name (unchanged).

    Raises:
        AnalyticsError: If the view name is invalid.

    Examples:
        >>> validate_view_name("vw_player_totals")
        'vw_player_totals'
        >>> validate_view_name("invalid-name")
        AnalyticsError: Invalid analytics view name: 'invalid-name'
    """
    if not _VALID_IDENTIFIER.fullmatch(name):
        raise AnalyticsError(f"Invalid analytics view name: {name!r}", view_name=name)
    _validate_sql_identifier(name)
    return name


def _validate_log_level(level: str) -> str:
    """Validate and normalize log level string.

    Args:
        level: Log level string (case-insensitive).

    Returns:
        Uppercase log level string.

    Raises:
        ValidationError: If the log level is not recognized.

    Examples:
        >>> _validate_log_level("info")
        'INFO'
        >>> _validate_log_level("INVALID")
        ValidationError: Invalid --log-level 'INVALID'...
    """
    candidate = level.upper()
    if candidate not in logging.getLevelNamesMapping():
        raise ValidationError(
            f"Invalid --log-level {level!r}. "
            "Expected one of DEBUG, INFO, WARNING, ERROR, CRITICAL.",
            argument="--log-level",
            value=level,
        )
    return candidate


def _validate_analytics_output_path(path: Path) -> None:
    """Validate analytics output extension early for friendlier CLI errors.

    Args:
        path: Output file path.

    Raises:
        ValidationError: If the file extension is not supported.

    Examples:
        >>> _validate_analytics_output_path(Path("output.csv"))  # OK
        >>> _validate_analytics_output_path(Path("output.xlsx"))
        ValidationError: Unsupported analytics output format...
    """
    suffix = path.suffix.lower()
    if suffix not in SUPPORTED_ANALYTICS_EXTENSIONS:
        raise ValidationError(
            f"Unsupported analytics output format: {path} "
            f"(expected one of {', '.join(sorted(SUPPORTED_ANALYTICS_EXTENSIONS))})",
            argument="--analytics-output",
            value=str(path),
        )


def _validate_seasons(seasons: Sequence[str]) -> list[str]:
    """Validate normalized season IDs and return a cleaned copy.

    Args:
        seasons: Normalized season strings to validate.

    Returns:
        List of validated season strings.

    Raises:
        ValidationError: If no seasons provided or any season is malformed.

    Examples:
        >>> _validate_seasons(["2023-24", "2024-25"])
        ['2023-24', '2024-25']
        >>> _validate_seasons([])
        ValidationError: At least one season must be provided...
        >>> _validate_seasons(["2023"])
        ValidationError: Invalid --seasons values ['2023']...
    """
    if not seasons:
        raise ValidationError(
            "At least one season must be provided via --seasons",
            argument="--seasons",
            value=None,
        )

    invalid = [s for s in seasons if not _SEASON_ID_PATTERN.fullmatch(s)]
    if invalid:
        raise ValidationError(
            f"Invalid --seasons values {invalid}. Expected format YYYY-YY (e.g. 2023-24).",
            argument="--seasons",
            value=invalid,
        )
    return list(seasons)
