"""
ETL: fact_game, player_game_log, team_game_log.

Strategy
--------
1. For a given season, fetch every player's game log via nba_api
   `PlayerGameLogs` (one call per season = efficient, respects rate limits).
2. Derive fact_game rows from the game logs (avoids a separate games endpoint).
3. Aggregate player logs per game/team to build team_game_log rows.
4. All inserts are INSERT OR IGNORE — safe to re-run.

Rate-limit notes
----------------
* nba_api hits stats.nba.com which bans aggressive scrapers.
* call_with_backoff() wraps every API call with exponential back-off.
* A base_sleep of 3 s between calls is the community-recommended minimum.
"""

import logging
import sqlite3

import pandas as pd
from nba_api.stats.endpoints import playergamelogs

from .utils import call_with_backoff, load_cache, save_cache, upsert_rows

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------ #
# Column mappings                                                     #
# ------------------------------------------------------------------ #

# nba_api PlayerGameLogs column → our schema column
_PGL_RENAME = {
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
_PGL_COLS = [
    "game_id", "player_id", "team_id",
    "minutes_played",
    "fgm", "fga", "fg3m", "fg3a",
    "ftm", "fta",
    "oreb", "dreb", "reb",
    "ast", "stl", "blk", "tov", "pf", "pts",
    "plus_minus",
]

# Team aggregate columns (sum across players in the same game/team)
_TEAM_SUM_COLS = [
    "fgm", "fga", "fg3m", "fg3a", "ftm", "fta",
    "oreb", "dreb", "reb", "ast", "stl", "blk", "tov", "pf", "pts",
]


# ------------------------------------------------------------------ #
# Fetch raw data                                                      #
# ------------------------------------------------------------------ #

def _fetch_player_game_logs(season: str, season_type: str = "Regular Season") -> pd.DataFrame:
    """
    Pull all player game logs for *season* (e.g. '2023-24').
    Returns a raw DataFrame with original nba_api column names.
    """
    cache_key = f"pgl_{season}_{season_type.replace(' ', '_')}"
    cached = load_cache(cache_key)
    if cached:
        logger.info("player_game_logs: loaded from cache for %s.", season)
        return pd.DataFrame(cached)

    def _call():
        ep = playergamelogs.PlayerGameLogs(
            season_nullable=season,
            season_type_nullable=season_type,
            league_id_nullable="00",
        )
        return ep.get_data_frames()[0]

    df = call_with_backoff(_call, label=f"PlayerGameLogs({season})")
    save_cache(cache_key, df.to_dict(orient="records"))
    logger.info("player_game_logs: fetched %d rows for %s.", len(df), season)
    return df


# ------------------------------------------------------------------ #
# Transform                                                           #
# ------------------------------------------------------------------ #

def _parse_matchup(matchup: str) -> tuple[str | None, str | None, bool]:
    """
    Parse 'LAL vs. BOS' or 'LAL @ BOS' into (home_abbr, away_abbr, is_home).
    Returns (None, None, False) if the string is malformed.
    """
    if " vs. " in matchup:
        parts = matchup.split(" vs. ")
        return parts[0].strip(), parts[1].strip(), True
    if " @ " in matchup:
        parts = matchup.split(" @ ")
        return parts[0].strip(), parts[1].strip(), False
    return None, None, False


def _build_game_rows(df: pd.DataFrame, season_id: str, season_type: str) -> list[dict]:
    """
    Derive fact_game rows from the flat player-game-log DataFrame.
    Uses the first player log per game to establish the game record.
    """
    game_rows: dict[str, dict] = {}
    for _, row in df.drop_duplicates("GAME_ID").iterrows():
        gid = str(row["GAME_ID"])
        matchup = str(row.get("MATCHUP", ""))
        _my_abbr, opp_abbr, am_home = _parse_matchup(matchup)

        if am_home:
            home_team_id = str(row["TEAM_ID"])
            away_team_id = None  # resolved later from team table if needed
        else:
            home_team_id = None
            away_team_id = str(row["TEAM_ID"])

        game_rows[gid] = {
            "game_id": gid,
            "season_id": season_id,
            "game_date": str(row.get("GAME_DATE", ""))[:10],
            "home_team_id": home_team_id,
            "away_team_id": away_team_id,
            "home_score": None,
            "away_score": None,
            "season_type": season_type,
            "status": "Final",
            "arena": None,
            "attendance": None,
        }
    return list(game_rows.values())


def _build_player_rows(df: pd.DataFrame) -> list[dict]:
    df = df.rename(columns=_PGL_RENAME)
    available = [c for c in _PGL_COLS if c in df.columns]
    df_clean = df[available].copy()
    # Add missing columns as None
    for c in _PGL_COLS:
        if c not in df_clean.columns:
            df_clean[c] = None
    df_clean["starter"] = None
    df_clean["game_id"] = df_clean["game_id"].astype(str)
    df_clean["player_id"] = df_clean["player_id"].astype(str)
    df_clean["team_id"] = df_clean["team_id"].astype(str)
    # Replace NaN → None for SQLite compatibility
    return df_clean.where(pd.notna(df_clean), None).to_dict(orient="records")


def _build_team_rows(df: pd.DataFrame) -> list[dict]:
    """Aggregate player stats per (game_id, team_id) to form team box scores."""
    df2 = df.rename(columns=_PGL_RENAME)
    df2["game_id"] = df2["game_id"].astype(str)
    df2["team_id"] = df2["team_id"].astype(str)
    agg = (
        df2.groupby(["game_id", "team_id"])[_TEAM_SUM_COLS]
        .sum()
        .reset_index()
    )
    return agg.where(pd.notna(agg), None).to_dict(orient="records")


# ------------------------------------------------------------------ #
# Load                                                                #
# ------------------------------------------------------------------ #

def load_season(
    con: sqlite3.Connection,
    season: str,
    season_type: str = "Regular Season",
) -> dict[str, int]:
    """
    Fetch and load all game-log data for *season* (e.g. '2023-24').
    Returns a dict with counts of rows inserted per table.
    """
    df = _fetch_player_game_logs(season, season_type)
    if df.empty:
        logger.warning("No data returned for %s %s.", season, season_type)
        return {}

    game_rows = _build_game_rows(df, season, season_type)
    player_rows = _build_player_rows(df)
    team_rows = _build_team_rows(df)

    n_games = upsert_rows(con, "fact_game", game_rows)
    n_players = upsert_rows(con, "player_game_log", player_rows)
    n_teams = upsert_rows(con, "team_game_log", team_rows)

    logger.info(
        "Season %s %s → games: %d, player_logs: %d, team_logs: %d",
        season, season_type, n_games, n_players, n_teams,
    )
    return {"fact_game": n_games, "player_game_log": n_players, "team_game_log": n_teams}


def load_multiple_seasons(
    con: sqlite3.Connection,
    seasons: list[str],
    season_types: list[str] | None = None,
    inter_call_sleep: float = 3.0,
) -> None:
    """
    Convenience wrapper to load several seasons sequentially with sleep
    between calls to stay under nba_api rate limits.
    """
    import time
    if season_types is None:
        season_types = ["Regular Season", "Playoffs"]

    for season in seasons:
        for s_type in season_types:
            try:
                load_season(con, season, s_type)
            except Exception as exc:
                logger.error("Failed %s %s: %s", season, s_type, exc)
            time.sleep(inter_call_sleep)


if __name__ == "__main__":
    from src.db.schema import init_db
    logging.basicConfig(level=logging.INFO)
    con = init_db()
    load_multiple_seasons(con, ["2023-24", "2024-25"])
    con.close()
