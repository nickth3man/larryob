"""
nba_api V3 game inventory endpoint adapters.

Provides two fetch functions:

1. ``fetch_schedule_league_v2`` — fetches the full season game schedule
   via ``ScheduleLeagueV2.season_games`` (named attribute, not index).

2. ``fetch_scoreboard_v3_for_dates`` — fetches per-date scoreboard data
   via ``ScoreboardV3.game_header`` and ``ScoreboardV3.line_score``.
   ``ScoreboardV3`` is date-based, so callers must iterate per game date.
   ``ScoreboardV2`` is deprecated and has known 2025-26 line-score gaps —
   never use it for canonical ingest.

Notes
-----
- ``ScheduleLeagueV2`` uses the ``season_games`` named dataset attribute.
- ``ScoreboardV3`` uses ``game_header`` and ``line_score`` named attributes.
- Neither adapter calls ``get_data_frames()[index]`` — that pattern is fragile.
"""

from __future__ import annotations

import logging

import pandas as pd
from nba_api.stats.endpoints import scheduleleaguev2, scoreboardv3

from ...extract.api_client import APICaller

logger = logging.getLogger(__name__)


def fetch_schedule_league_v2(
    season: str,
    api_caller: APICaller | None = None,
) -> list[dict]:
    """Fetch the full season schedule via ScheduleLeagueV2.season_games.

    Parameters
    ----------
    season:
        Season ID in ``YYYY-YY`` format (e.g. ``"2023-24"``).
    api_caller:
        Optional :class:`APICaller` for rate-limiting and retry.

    Returns
    -------
    list[dict]
        One dict per game with at minimum ``game_id`` and ``game_date`` keys.
        Returns an empty list if the API returns no games.
    """
    if api_caller is None:
        api_caller = APICaller()

    # ScheduleLeagueV2 accepts the start year only (e.g. "2023" for "2023-24")
    api_season = season.split("-")[0]

    def _call() -> pd.DataFrame:
        ep = scheduleleaguev2.ScheduleLeagueV2(season=api_season)
        # Use named dataset attribute — never get_data_frames()[index]
        return ep.season_games.get_data_frame()

    df: pd.DataFrame = api_caller.call_with_backoff(
        _call,
        label=f"ScheduleLeagueV2({season})",
    )

    if df.empty:
        return []

    # Normalise column names to snake_case
    rename: dict[str, str] = {
        "gameId": "game_id",
        "gameDate": "game_date",
        "gameStatus": "game_status",
        "gameStatusText": "game_status_text",
        "weekNumber": "week_number",
        "homeTeam_teamId": "home_team_id",
        "awayTeam_teamId": "away_team_id",
        "homeTeam_score": "home_score",
        "awayTeam_score": "away_score",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    # Ensure game_id is a string (API may return int-like IDs)
    if "game_id" in df.columns:
        df["game_id"] = df["game_id"].astype(str)

    # Normalise game_date: drop time component if present (e.g. "2023-10-24T00:00:00")
    if "game_date" in df.columns:
        df["game_date"] = df["game_date"].astype(str).str[:10]

    return df.where(pd.notna(df), None).to_dict(orient="records")


def fetch_scoreboard_v3_for_dates(
    game_dates: list[str],
    api_caller: APICaller | None = None,
) -> list[dict]:
    """Fetch ScoreboardV3 data for each date in *game_dates*.

    ScoreboardV3 is date-based, so one API call is made per game date.
    ScoreboardV2 is deprecated and has known 2025-26 line-score gaps —
    this function never uses ScoreboardV2.

    Parameters
    ----------
    game_dates:
        List of date strings in ``YYYY-MM-DD`` format.
    api_caller:
        Optional :class:`APICaller` for rate-limiting and retry.

    Returns
    -------
    list[dict]
        One dict per game with ``game_id``, ``game_status``,
        ``game_status_text``, and ``period`` from the game_header.
        Returns an empty list when *game_dates* is empty.
    """
    if not game_dates:
        return []

    if api_caller is None:
        api_caller = APICaller()

    all_rows: list[dict] = []
    for game_date in game_dates:

        def _call(date: str = game_date) -> tuple[pd.DataFrame, pd.DataFrame]:
            ep = scoreboardv3.ScoreboardV3(game_date=date)
            # Use named dataset attributes — never get_data_frames()[index]
            header_df = ep.game_header.get_data_frame()
            line_score_df = ep.line_score.get_data_frame()
            return header_df, line_score_df

        try:
            header_df, _line_score_df = api_caller.call_with_backoff(
                _call,
                label=f"ScoreboardV3({game_date})",
            )
        except Exception as exc:
            logger.warning("ScoreboardV3 failed for date %s: %s", game_date, exc)
            continue

        if header_df.empty:
            continue

        # Normalise column names
        rename: dict[str, str] = {
            "gameId": "game_id",
            "gameStatus": "game_status",
            "gameStatusText": "game_status_text",
            "period": "period",
            "gameClock": "game_clock",
        }
        header_df = header_df.rename(
            columns={k: v for k, v in rename.items() if k in header_df.columns}
        )
        if "game_id" in header_df.columns:
            header_df["game_id"] = header_df["game_id"].astype(str)

        all_rows.extend(header_df.where(pd.notna(header_df), None).to_dict(orient="records"))

    return all_rows
