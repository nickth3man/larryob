"""
Pipeline-level constants: default seasons, stage-to-table mappings, and type aliases.

Nothing in this module imports from other pipeline modules — it is the dependency root.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any

DEFAULT_SEASONS: list[str] = ["2023-24", "2024-25"]

_SEASON_ID_PATTERN: re.Pattern[str] = re.compile(r"^\d{4}-\d{2}$")
_VALID_IDENTIFIER: re.Pattern[str] = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

StageFn = Callable[..., Any]

DIMENSIONS_TABLES: list[str] = ["dim_season", "dim_team", "dim_player"]
RAW_BACKFILL_TABLES: list[str] = [
    "dim_team_history",
    "fact_game",
    "player_game_log",
    "team_game_log",
    "fact_team_season",
    "fact_player_season_stats",
    "fact_player_advanced_season",
    "fact_player_shooting_season",
    "fact_player_pbp_season",
]
AWARDS_TABLES: list[str] = ["fact_player_award"]
SALARIES_TABLES: list[str] = ["dim_salary_cap", "fact_salary"]
ROSTERS_TABLES: list[str] = ["fact_roster"]
GAME_LOGS_TABLES: list[str] = ["fact_game", "player_game_log", "team_game_log"]
PBP_TABLES: list[str] = ["fact_play_by_play"]
