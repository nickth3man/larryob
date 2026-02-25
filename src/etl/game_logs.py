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
* APICaller wraps every API call with exponential back-off and configurable delays.
* Delays are configurable via APIConfig or environment variables.
"""

import logging
import sqlite3
import time
from datetime import UTC, datetime

import pandas as pd
from nba_api.stats.endpoints import playergamelogs

from ..db.cache import load_cache, save_cache
from ..db.operations import transaction, upsert_rows
from ..db.tracking import already_loaded, log_load_summary, record_run
from ._game_logs_transform import (
    PGL_COLS,
    PGL_RENAME,
    TEAM_SUM_COLS,
    build_game_rows,
    build_player_rows,
    build_team_rows,
    parse_matchup,
)
from .api_client import APICaller
from .metrics import ETLTimer, record_etl_rows
from .validate import validate_rows

logger = logging.getLogger(__name__)

# Re-export for backward compatibility with tests
_PGL_RENAME = PGL_RENAME
_PGL_COLS = PGL_COLS
_TEAM_SUM_COLS = TEAM_SUM_COLS
_parse_matchup = parse_matchup
_build_game_rows = build_game_rows
_build_player_rows = build_player_rows
_build_team_rows = build_team_rows


# ------------------------------------------------------------------ #
# Fetch raw data                                                      #
# ------------------------------------------------------------------ #


def _fetch_player_game_logs(
    season: str,
    season_type: str = "Regular Season",
    api_caller: APICaller | None = None,
) -> pd.DataFrame:
    """
    Pull all player game logs for *season* (e.g. '2023-24').
    Returns a raw DataFrame with original nba_api column names.
    """
    cache_key = f"pgl_{season}_{season_type.replace(' ', '_')}"
    cached = load_cache(cache_key)
    if cached is not None:
        logger.info(
            "player_game_logs: loaded from cache for %s %s (rows=%d).",
            season,
            season_type,
            len(cached),
        )
        return pd.DataFrame(cached)

    if api_caller is None:
        api_caller = APICaller()

    def _call():
        ep = playergamelogs.PlayerGameLogs(
            season_nullable=season,
            season_type_nullable=season_type,
            league_id_nullable="00",
        )
        return ep.get_data_frames()[0]

    df = api_caller.call_with_backoff(_call, label=f"PlayerGameLogs({season})")
    save_cache(cache_key, df.to_dict(orient="records"))
    logger.info("player_game_logs: fetched %d rows for %s.", len(df), season)
    return df


# ------------------------------------------------------------------ #
# Load                                                                #
# ------------------------------------------------------------------ #


def load_season(
    con: sqlite3.Connection,
    season: str,
    season_type: str = "Regular Season",
    api_caller: APICaller | None = None,
) -> dict[str, int]:
    """
    Fetch and load all game-log data for *season* (e.g. '2023-24').
    Returns a dict with counts of rows inserted per table.
    """
    loader_id = f"game_logs.load_season.{season_type}"
    if already_loaded(con, "player_game_log", season, loader_id):
        logger.info("Skipping game logs for %s %s (already loaded)", season, season_type)
        return {}

    started_at = datetime.now(UTC).isoformat()
    started_perf = time.perf_counter()

    with ETLTimer("player_game_log", season):
        df = _fetch_player_game_logs(season, season_type, api_caller)
    logger.info(
        "Season %s %s fetch summary: fetched_player_game_rows=%d",
        season,
        season_type,
        len(df),
    )

    if df.empty:
        logger.warning("No data returned for %s %s.", season, season_type)
        record_run(con, "fact_game", season, loader_id, 0, "empty", started_at)
        record_run(con, "player_game_log", season, loader_id, 0, "empty", started_at)
        record_run(con, "team_game_log", season, loader_id, 0, "empty", started_at)
        return {}

    game_rows = build_game_rows(df, season, season_type)
    player_rows = build_player_rows(df)
    team_rows = build_team_rows(df)
    raw_counts = {
        "fact_game": len(game_rows),
        "player_game_log": len(player_rows),
        "team_game_log": len(team_rows),
    }

    game_rows = validate_rows("fact_game", game_rows)
    player_rows = validate_rows("player_game_log", player_rows)
    team_rows = validate_rows("team_game_log", team_rows)
    validated_counts = {
        "fact_game": len(game_rows),
        "player_game_log": len(player_rows),
        "team_game_log": len(team_rows),
    }
    dropped_counts = {table: raw_counts[table] - validated_counts[table] for table in raw_counts}
    logger.info(
        "Season %s %s transform summary: raw=%s validated=%s dropped=%s",
        season,
        season_type,
        raw_counts,
        validated_counts,
        dropped_counts,
    )

    if candidate_game_ids := {row["game_id"] for row in game_rows}:
        existing_game_ids: set[str] = set()
        candidate_list = list(candidate_game_ids)
        chunk_size = 900
        for i in range(0, len(candidate_list), chunk_size):
            chunk = candidate_list[i : i + chunk_size]
            placeholders = ", ".join("?" for _ in chunk)
            sql = f"SELECT game_id FROM fact_game WHERE game_id IN ({placeholders})"
            existing_game_ids.update(r[0] for r in con.execute(sql, chunk).fetchall())
        valid_game_ids = candidate_game_ids | existing_game_ids
    else:
        valid_game_ids = set()

    player_before_filter = len(player_rows)
    team_before_filter = len(team_rows)
    if valid_game_ids:
        player_rows = [r for r in player_rows if r["game_id"] in valid_game_ids]
        team_rows = [r for r in team_rows if r["game_id"] in valid_game_ids]
    else:
        player_rows = []
        team_rows = []
    logger.info(
        "Season %s %s FK prefilter: valid_games=%d player_rows_kept=%d/%d team_rows_kept=%d/%d",
        season,
        season_type,
        len(valid_game_ids),
        len(player_rows),
        player_before_filter,
        len(team_rows),
        team_before_filter,
    )

    with transaction(con):
        n_games = upsert_rows(con, "fact_game", game_rows, autocommit=False)
        n_players = upsert_rows(con, "player_game_log", player_rows, autocommit=False)
        n_teams = upsert_rows(con, "team_game_log", team_rows, autocommit=False)

    logger.info(
        "Season %s %s load summary: candidates=%s inserted={fact_game:%d,player_game_log:%d,team_game_log:%d} elapsed=%.2fs",
        season,
        season_type,
        validated_counts,
        n_games,
        n_players,
        n_teams,
        time.perf_counter() - started_perf,
    )

    record_run(con, "fact_game", season, loader_id, n_games, "ok", started_at)
    record_run(con, "player_game_log", season, loader_id, n_players, "ok", started_at)
    record_run(con, "team_game_log", season, loader_id, n_teams, "ok", started_at)

    # Record metrics
    record_etl_rows("fact_game", season, n_games)
    record_etl_rows("player_game_log", season, n_players)
    record_etl_rows("team_game_log", season, n_teams)

    log_load_summary(con, "fact_game", season)
    log_load_summary(con, "player_game_log", season)
    log_load_summary(con, "team_game_log", season)

    return {"fact_game": n_games, "player_game_log": n_players, "team_game_log": n_teams}


def load_multiple_seasons(
    con: sqlite3.Connection,
    seasons: list[str],
    season_types: list[str] | None = None,
    api_caller: APICaller | None = None,
) -> None:
    """
    Convenience wrapper to load several seasons sequentially with sleep
    between calls to stay under nba_api rate limits.
    """
    if api_caller is None:
        api_caller = APICaller()

    if season_types is None:
        season_types = ["Regular Season", "Playoffs"]

    total_runs = len(seasons) * len(season_types)
    run_idx = 0
    for season in seasons:
        for s_type in season_types:
            run_idx += 1
            logger.info(
                "Game logs [%d/%d] starting season=%s season_type=%s",
                run_idx,
                total_runs,
                season,
                s_type,
            )
            try:
                if counts := load_season(con, season, s_type, api_caller):
                    logger.info(
                        "Game logs [%d/%d] completed season=%s season_type=%s counts=%s",
                        run_idx,
                        total_runs,
                        season,
                        s_type,
                        counts,
                    )
                else:
                    logger.info(
                        "Game logs [%d/%d] completed season=%s season_type=%s with no new rows",
                        run_idx,
                        total_runs,
                        season,
                        s_type,
                    )
            except Exception as exc:
                logger.error(
                    "Game logs [%d/%d] failed season=%s season_type=%s: %s",
                    run_idx,
                    total_runs,
                    season,
                    s_type,
                    exc,
                )
            api_caller.sleep_between_calls()


if __name__ == "__main__":  # pragma: no cover
    from src.db.schema import init_db

    logging.basicConfig(level=logging.INFO)
    con = init_db()
    load_multiple_seasons(con, ["2023-24", "2024-25"])
    con.close()
