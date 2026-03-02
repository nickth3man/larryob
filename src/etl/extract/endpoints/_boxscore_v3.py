"""
nba_api BoxScoreTraditionalV3 endpoint adapter.

Fetches box score data using the V3 endpoint with named dataset attributes
(``ep.player_stats.get_data_frame()`` and ``ep.team_stats.get_data_frame()``).
Never uses ``get_data_frames()[index]`` — that is the legacy pattern and is
fragile when dataset order changes.
"""

from __future__ import annotations

import logging

import pandas as pd
from nba_api.stats.endpoints import boxscoretraditionalv3

from ...extract.api_client import APICaller

logger = logging.getLogger(__name__)


def fetch_boxscore_traditional_v3(
    game_id: str,
    api_caller: APICaller | None = None,
) -> dict[str, pd.DataFrame]:
    """Fetch box score for *game_id* via BoxScoreTraditionalV3.

    Uses named dataset attributes as required by the nba_api V3 contract:
    ``ep.player_stats.get_data_frame()`` and ``ep.team_stats.get_data_frame()``.

    Parameters
    ----------
    game_id:
        NBA game ID string (e.g. ``"0022300001"``).
    api_caller:
        Optional :class:`APICaller` for rate-limiting and retry.
        Falls back to a default instance when *None*.

    Returns
    -------
    dict with keys ``"player_stats"`` and ``"team_stats"``, each a
    :class:`pandas.DataFrame` from the endpoint response.
    """
    caller = api_caller if api_caller is not None else APICaller()

    def _call() -> dict[str, pd.DataFrame]:
        ep = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id)
        # Use named dataset attributes — never get_data_frames()[index]
        # Assert is not None: nba_api sets these to None at class level but
        # load_response() always populates them after a successful API call.
        assert ep.player_stats is not None, "BoxScoreTraditionalV3: player_stats not loaded"
        assert ep.team_stats is not None, "BoxScoreTraditionalV3: team_stats not loaded"
        player_df = ep.player_stats.get_data_frame()
        team_df = ep.team_stats.get_data_frame()
        return {"player_stats": player_df, "team_stats": team_df}

    payload: dict[str, pd.DataFrame] = caller.call_with_backoff(
        _call,
        label=f"BoxScoreTraditionalV3({game_id})",
    )
    return payload
