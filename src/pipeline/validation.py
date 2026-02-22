"""
Input validation helpers for the ingest pipeline.

All functions here are pure (no I/O, no side effects) and raise typed exceptions
from `exceptions.py`. They are intentionally kept free of imports from other
pipeline modules so that `models.py` can safely import `_normalize_seasons`.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from pathlib import Path

from src.etl.utils import _validate_identifier as _validate_sql_identifier
from src.pipeline.constants import _SEASON_ID_PATTERN, _VALID_IDENTIFIER
from src.pipeline.exceptions import AnalyticsError, ValidationError


def _normalize_seasons(raw_seasons: Sequence[str]) -> list[str]:
    """Normalize seasons by trimming and de-duplicating while preserving order."""
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

    Args:
        name: The view name to validate.

    Returns:
        The validated view name.

    Raises:
        AnalyticsError: If the view name is invalid.
    """
    if not _VALID_IDENTIFIER.fullmatch(name):
        raise AnalyticsError(f"Invalid analytics view name: {name!r}")
    _validate_sql_identifier(name)
    return name


def _validate_log_level(level: str) -> str:
    """Validate and normalize log level string."""
    candidate = level.upper()
    if candidate not in logging.getLevelNamesMapping():
        raise ValidationError(
            f"Invalid --log-level {level!r}. "
            "Expected one of DEBUG, INFO, WARNING, ERROR, CRITICAL."
        )
    return candidate


def _validate_analytics_output_path(path: Path) -> None:
    """Validate analytics output extension early for friendlier CLI errors."""
    suffix = path.suffix.lower()
    if suffix not in {".csv", ".parquet", ".json"}:
        raise ValidationError(
            f"Unsupported analytics output format: {path} "
            "(expected .csv, .parquet, or .json)"
        )


def _validate_seasons(seasons: Sequence[str]) -> list[str]:
    """Validate normalized season IDs and return a cleaned copy."""
    if not seasons:
        raise ValidationError("At least one season must be provided via --seasons")

    invalid = [s for s in seasons if not _SEASON_ID_PATTERN.fullmatch(s)]
    if invalid:
        raise ValidationError(
            "Invalid --seasons values "
            f"{invalid}. Expected format YYYY-YY (e.g. 2023-24)."
        )
    return list(seasons)
