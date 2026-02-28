"""
Pipeline-level constants: default seasons, stage-to-table mappings, and type aliases.

This module defines the core constants used across the pipeline package.


Design Decisions
----------------
- All regex patterns are compiled at module load for performance
- Table lists are frozen to prevent accidental mutation during pipeline execution
- StageFn uses ParamSpec for better type safety when possible (Python 3.10+)

Usage
-----
    from src.pipeline.constants import DEFAULT_SEASONS, DIMENSIONS_TABLES
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any

# =============================================================================
# Default Configuration
# =============================================================================
from src.pipeline.completeness import full_history_seasons

#: Default seasons to ingest when --seasons is not provided
DEFAULT_SEASONS: tuple[str, ...] = full_history_seasons(2025)

# =============================================================================
# Validation Patterns (compiled at module load)
# =============================================================================

#: Matches season IDs in format "YYYY-YY" (e.g., "2023-24")
_SEASON_ID_PATTERN: re.Pattern[str] = re.compile(r"^\d{4}-\d{2}$")

#: Matches valid SQL identifiers (letters, digits, underscores; must start with letter/underscore)
_VALID_IDENTIFIER: re.Pattern[str] = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# =============================================================================
# Type Aliases
# =============================================================================

#: Type alias for stage execution functions
StageFn = Callable[..., Any]

# =============================================================================
# Stage-to-Table Mappings
# =============================================================================

#: Tables affected by the DIMENSIONS stage (seed reference data)
DIMENSIONS_TABLES: tuple[str, ...] = ("dim_season", "dim_team", "dim_player")

#: Tables populated during RAW_BACKFILL (from Basketball-Reference CSVs)
RAW_BACKFILL_TABLES: tuple[str, ...] = (
    "dim_team_history",
    "fact_game",
    "player_game_log",
    "team_game_log",
    "fact_team_season",
    "fact_player_season_stats",
    "fact_player_advanced_season",
    "fact_player_shooting_season",
    "fact_player_pbp_season",
)

#: Tables populated during AWARDS stage
AWARDS_TABLES: tuple[str, ...] = ("fact_player_award",)

#: Tables populated during SALARIES stage
SALARIES_TABLES: tuple[str, ...] = ("dim_salary_cap", "fact_salary")

#: Tables populated during ROSTERS stage
ROSTERS_TABLES: tuple[str, ...] = ("fact_roster",)

#: Tables updated during GAME_LOGS stage (box scores)
GAME_LOGS_TABLES: tuple[str, ...] = ("fact_game", "player_game_log", "team_game_log")

#: Tables populated during PBP (play-by-play) stage
PBP_TABLES: tuple[str, ...] = ("fact_play_by_play",)
