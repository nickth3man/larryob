from __future__ import annotations

NBA_LINEAGE_FIRST_START_YEAR = 1946
REQUIRED_GAME_TYPES = ("Preseason", "Regular Season", "Play-In", "Playoffs")


def _season_id(start_year: int) -> str:
    return f"{start_year}-{str(start_year + 1)[-2:]}"


def full_history_seasons(up_to_start_year: int) -> tuple[str, ...]:
    return tuple(_season_id(y) for y in range(NBA_LINEAGE_FIRST_START_YEAR, up_to_start_year + 1))
