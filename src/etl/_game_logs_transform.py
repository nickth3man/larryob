"""
Transform functions for game logs ETL.

This module contains pure transformation functions and column mappings
extracted from game_logs.py for maintainability.
"""

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------ #
# Column mappings                                                     #
# ------------------------------------------------------------------ #

# nba_api PlayerGameLogs column → our schema column
PGL_RENAME: dict[str, str] = {
    "GAME_ID": "game_id",
    "PLAYER_ID": "player_id",
    "TEAM_ID": "team_id",
    "GAME_DATE": "game_date",
    "MATCHUP": "matchup",
    "WL": "wl",
    "MIN": "minutes_played",
    "FGM": "fgm",
    "FGA": "fga",
    "FG3M": "fg3m",
    "FG3A": "fg3a",
    "FTM": "ftm",
    "FTA": "fta",
    "OREB": "oreb",
    "DREB": "dreb",
    "REB": "reb",
    "AST": "ast",
    "STL": "stl",
    "BLK": "blk",
    "TOV": "tov",
    "PF": "pf",
    "PTS": "pts",
    "PLUS_MINUS": "plus_minus",
}

# Columns required for player_game_log insert
PGL_COLS: list[str] = [
    "game_id",
    "player_id",
    "team_id",
    "minutes_played",
    "fgm",
    "fga",
    "fg3m",
    "fg3a",
    "ftm",
    "fta",
    "oreb",
    "dreb",
    "reb",
    "ast",
    "stl",
    "blk",
    "tov",
    "pf",
    "pts",
    "plus_minus",
]

# Team aggregate columns (sum across players in the same game/team)
TEAM_SUM_COLS: list[str] = [
    "fgm",
    "fga",
    "fg3m",
    "fg3a",
    "ftm",
    "fta",
    "oreb",
    "dreb",
    "reb",
    "ast",
    "stl",
    "blk",
    "tov",
    "pf",
    "pts",
]


# ------------------------------------------------------------------ #
# Transform functions                                                 #
# ------------------------------------------------------------------ #


def parse_matchup(matchup: str) -> tuple[str | None, str | None, bool]:
    """
    Parse a matchup string into team abbreviations and a home/away flag.

    The first returned abbreviation is always the team listed first in the matchup
    string. For "vs." games that team is the home team; for "@" games it is the
    away (visiting) team.

    Parameters:
        matchup (str): Matchup text in the form "HOME vs. AWAY" or "AWAY @ HOME".

    Returns:
        tuple[str | None, str | None, bool]: (first_abbr, second_abbr, is_home).
            - first_abbr: Abbreviation of the team listed first, or `None` if malformed.
            - second_abbr: Abbreviation of the team listed second, or `None` if malformed.
            - is_home: `True` if the string used " vs. " (first team is home),
              `False` if it used " @ " (first team is away) or the input was malformed.
    """
    if " vs. " in matchup:
        parts = matchup.split(" vs. ")
        return parts[0].strip(), parts[1].strip(), True
    if " @ " in matchup:
        parts = matchup.split(" @ ")
        return parts[0].strip(), parts[1].strip(), False
    return None, None, False


def build_game_rows(df: pd.DataFrame, season_id: str, season_type: str) -> list[dict[str, Any]]:
    """
    Builds game-level rows (fact_game) from a player-game-log DataFrame.

    Parameters:
        df (pd.DataFrame): Player-game-log table containing at least the columns "GAME_ID", "TEAM_ID", and "MATCHUP". "GAME_DATE" is used to populate game_date when present.
        season_id (str): Season identifier to assign to each game row.
        season_type (str): Season type to assign to each game row (e.g., "Regular Season", "Playoffs").

    Returns:
        list[dict[str, Any]]: A list of dictionaries, one per resolved game, with keys:
            - game_id: game identifier (string)
            - season_id: provided season_id
            - game_date: date string in "YYYY-MM-DD" format (empty string if unavailable)
            - home_team_id: home team id (string)
            - away_team_id: away team id (string)
            - home_score: None (placeholder)
            - away_score: None (placeholder)
            - season_type: provided season_type
            - status: game status (set to "Final")
            - arena: None (placeholder)
            - attendance: None (placeholder)
    """
    game_rows: dict[str, dict[str, Any]] = {}
    dropped = 0

    # Ensure columns exist and fill na to avoid string matching errors
    if "MATCHUP" not in df.columns or "TEAM_ID" not in df.columns:
        return []

    df_clean = df.copy()
    df_clean["MATCHUP"] = df_clean["MATCHUP"].fillna("")

    # Vectorized boolean masks
    is_home = df_clean["MATCHUP"].str.contains(" vs. ")
    is_away = df_clean["MATCHUP"].str.contains(" @ ")

    for game_id, grp in df_clean.groupby("GAME_ID", sort=False):
        gid = str(game_id)

        # Get team IDs where conditions are met for this game group
        home_teams = grp.loc[is_home, "TEAM_ID"].unique()
        away_teams = grp.loc[is_away, "TEAM_ID"].unique()
        all_teams = grp["TEAM_ID"].unique()

        home_team_id = str(home_teams[0]) if len(home_teams) > 0 else None
        away_team_id = str(away_teams[0]) if len(away_teams) > 0 else None

        # Fallback resolution if string parsing failed but exactly 2 teams exist
        if len(all_teams) == 2:
            all_teams_str = [str(t) for t in all_teams]
            if home_team_id is None and away_team_id is not None:
                home_team_id = next(t for t in all_teams_str if t != away_team_id)
            elif away_team_id is None and home_team_id is not None:
                away_team_id = next(t for t in all_teams_str if t != home_team_id)

        if home_team_id is None or away_team_id is None:
            dropped += 1
            logger.warning(
                "build_game_rows: dropping game_id=%s unresolved teams (home=%s away=%s)",
                gid,
                home_team_id,
                away_team_id,
            )
            continue

        first = grp.iloc[0]
        game_rows[gid] = {
            "game_id": gid,
            "season_id": season_id,
            "game_date": str(first.get("GAME_DATE", ""))[:10],
            "home_team_id": home_team_id,
            "away_team_id": away_team_id,
            "home_score": None,
            "away_score": None,
            "season_type": season_type,
            "status": "Final",
            "arena": None,
            "attendance": None,
        }

    if dropped > 0:
        logger.warning(
            "build_game_rows: dropped %d/%d games due to unresolved team mapping",
            dropped,
            len(df_clean["GAME_ID"].unique()),
        )

    return list(game_rows.values())


def build_player_rows(df: pd.DataFrame) -> list[dict[str, Any]]:
    """
    Produce normalized player-level rows conforming to the target schema from a player-game-log DataFrame.

    Parameters:
        df (pd.DataFrame): Input DataFrame of player game logs (expected nba_api-style column names or already-renamed columns).

    Returns:
        list[dict[str, Any]]: A list of player row dictionaries containing all required player columns (PGL_COLS) plus a `starter` column. Missing columns are added with value `None`, player/game/team IDs are coerced to strings, and any NaN values are converted to `None`.
    """
    df = df.rename(columns=PGL_RENAME)
    available = [c for c in PGL_COLS if c in df.columns]
    df_clean = df[available].copy()
    # Add missing columns as None
    for c in PGL_COLS:
        if c not in df_clean.columns:
            df_clean[c] = None
    df_clean["starter"] = None
    df_clean["game_id"] = df_clean["game_id"].astype(str)
    df_clean["player_id"] = df_clean["player_id"].astype(str)
    df_clean["team_id"] = df_clean["team_id"].astype(str)
    # Replace NaN → None for SQLite compatibility
    return df_clean.where(pd.notna(df_clean), None).to_dict(orient="records")


def build_team_rows(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Aggregate player stats per (game_id, team_id) to form team box scores."""
    df2 = df.rename(columns=PGL_RENAME)
    df2["game_id"] = df2["game_id"].astype(str)
    df2["team_id"] = df2["team_id"].astype(str)
    agg = (
        df2.groupby(["game_id", "team_id"])[TEAM_SUM_COLS]
        .agg(lambda s: None if s.isna().all() else s.sum())
        .reset_index()
    )
    return agg.where(pd.notna(agg), None).to_dict(orient="records")
