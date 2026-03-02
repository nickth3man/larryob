"""
nba_api PlayByPlayV3 endpoint adapter.

Fetches play-by-play data using the V3 endpoint (PlayByPlayV2 is deprecated
and returns empty JSON for 2025-26+ seasons).

Normalises the raw API payload into a flat list of dicts with stable column names.
Uses named dataset attribute ``ep.play_by_play.get_data_frame()`` as required by
the swar/nba_api API contract (not ``get_data_frames()[index]``).
"""

from __future__ import annotations

import logging

import pandas as pd
from nba_api.stats.endpoints import playbyplayv3

from ...extract.api_client import APICaller

logger = logging.getLogger(__name__)

# Column rename map: API camelCase → internal snake_case
_PBP_V3_RENAME: dict[str, str] = {
    "gameId": "game_id",
    "actionNumber": "action_number",
    "clock": "clock",
    "period": "period",
    "teamId": "team_id",
    "teamTricode": "team_tricode",
    "personId": "person_id",
    "playerName": "player_name",
    "playerNameI": "player_name_i",
    "xLegacy": "x_legacy",
    "yLegacy": "y_legacy",
    "shotDistance": "shot_distance",
    "shotResult": "shot_result",
    "isFieldGoal": "is_field_goal",
    "scoreHome": "score_home",
    "scoreAway": "score_away",
    "pointsTotal": "points_total",
    "location": "location",
    "description": "description",
    "actionType": "action_type",
    "subType": "sub_type",
    "videoAvailable": "video_available",
    "actionId": "action_id",
}


def fetch_play_by_play_v3(
    game_id: str,
    api_caller: APICaller | None = None,
) -> list[dict]:
    """Fetch and normalise play-by-play for *game_id* via PlayByPlayV3.

    Parameters
    ----------
    game_id:
        NBA game ID string (e.g. ``"0022300001"``).
    api_caller:
        Optional :class:`APICaller` for rate-limiting and retry.
        Falls back to a default instance when *None*.

    Returns
    -------
    list[dict]
        One dict per play event, with snake_case column names and
        ``game_id`` guaranteed on every row.
    """
    if api_caller is None:
        api_caller = APICaller()

    def _call() -> pd.DataFrame:
        ep = playbyplayv3.PlayByPlayV3(game_id=game_id)
        # Use named dataset attribute — never get_data_frames()[index]
        return ep.play_by_play.get_data_frame()

    df: pd.DataFrame = api_caller.call_with_backoff(
        _call,
        label=f"PlayByPlayV3({game_id})",
    )

    if df.empty:
        return []

    df = df.rename(columns=_PBP_V3_RENAME)

    # Ensure game_id is always present (API returns it, but guard anyway)
    if "game_id" not in df.columns:
        df["game_id"] = game_id
    else:
        df["game_id"] = df["game_id"].astype(str).where(df["game_id"].notna(), game_id)

    return df.where(pd.notna(df), None).to_dict(orient="records")
