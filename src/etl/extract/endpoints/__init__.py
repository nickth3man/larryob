"""
nba_api V3 endpoint adapters.

Public surface
--------------
fetch_play_by_play_v3        -- PlayByPlayV3 adapter
fetch_boxscore_traditional_v3 -- BoxScoreTraditionalV3 adapter
fetch_schedule_league_v2     -- ScheduleLeagueV2 season games adapter
fetch_scoreboard_v3_for_dates -- ScoreboardV3 per-date adapter
"""

from ._boxscore_v3 import fetch_boxscore_traditional_v3
from ._game_inventory_v3 import (
    fetch_schedule_league_v2,
    fetch_scoreboard_v3_for_dates,
)
from ._play_by_play_v3 import fetch_play_by_play_v3

__all__ = [
    "fetch_boxscore_traditional_v3",
    "fetch_play_by_play_v3",
    "fetch_schedule_league_v2",
    "fetch_scoreboard_v3_for_dates",
]
