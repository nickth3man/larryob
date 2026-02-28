"""
Full-history completeness contract constants.

This module defines the NBA lineage contract: the mandatory data scope
for a complete historical database from 1946-47 to present.

Design Decisions
----------------
- NBA_LINEAGE_FIRST_START_YEAR: 1946 marks the BAA founding year
- REQUIRED_GAME_TYPES: All game types that must be covered for completeness
- full_history_seasons(): Generates season IDs for the full lineage range

Usage
-----
    from src.pipeline.completeness import full_history_seasons, REQUIRED_GAME_TYPES

    seasons = full_history_seasons(2025)  # ("1946-47", ..., "2025-26")
"""

from __future__ import annotations

#: First year of NBA/BAA lineage (1946-47 season)
NBA_LINEAGE_FIRST_START_YEAR = 1946

#: Game types required for full-history completeness
REQUIRED_GAME_TYPES: tuple[str, ...] = ("Preseason", "Regular Season", "Play-In", "Playoffs")


def _season_id(start_year: int) -> str:
    """Convert a start year to season ID format (e.g., 2023 -> '2023-24')."""
    return f"{start_year}-{str(start_year + 1)[-2:]}"


def full_history_seasons(up_to_start_year: int) -> tuple[str, ...]:
    """
    Generate season IDs from NBA lineage start to the given end year.

    Args:
        up_to_start_year: The final season's start year (inclusive)

    Returns:
        Tuple of season IDs in "YYYY-YY" format

    Example:
        >>> full_history_seasons(1947)
        ('1946-47', '1947-48')
    """
    return tuple(_season_id(y) for y in range(NBA_LINEAGE_FIRST_START_YEAR, up_to_start_year + 1))
