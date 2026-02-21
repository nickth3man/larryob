"""
ETL: fact_play_by_play.

Strategy
--------
* For each game_id, call nba_api `PlayByPlayV2` to get all events.
* Map official EVENTMSGTYPE / EVENTMSGACTIONTYPE codes to structured rows.
* Store player references by their numeric IDs only — never parse names
  from text descriptions (fragile and frequently wrong).
* All inserts are INSERT OR IGNORE — safe to re-run.

EVENTMSGTYPE reference
----------------------
1  Made field goal        2  Missed field goal
3  Free throw             4  Rebound
5  Turnover               6  Foul
7  Violation              8  Substitution
9  Timeout               10  Jump ball
11 Ejection              12  Start of period
13 End of period
"""

import logging
import sqlite3
from collections.abc import Iterable
from datetime import UTC

import pandas as pd
from nba_api.stats.endpoints import playbyplayv2

from .utils import (
    already_loaded,
    call_with_backoff,
    load_cache,
    log_load_summary,
    record_run,
    save_cache,
    transaction,
    upsert_rows,
)
from .validate import validate_rows

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------ #
# Column mapping                                                      #
# ------------------------------------------------------------------ #

_PBP_RENAME = {
    "GAME_ID": "game_id",
    "EVENTNUM": "eventnum",
    "PERIOD": "period",
    "PCTIMESTRING": "pc_time_string",
    "WCTIMESTRING": "wc_time_string",
    "EVENTMSGTYPE": "eventmsgtype",
    "EVENTMSGACTIONTYPE": "eventmsgactiontype",
    "PLAYER1_ID": "player1_id",
    "PLAYER2_ID": "player2_id",
    "PLAYER3_ID": "player3_id",
    "PERSON1TYPE": "person1type",
    "PERSON2TYPE": "person2type",
    "PERSON3TYPE": "person3type",
    "PLAYER1_TEAM_ID": "team1_id",
    "PLAYER2_TEAM_ID": "team2_id",
    "HOMEDESCRIPTION": "home_description",
    "VISITORDESCRIPTION": "visitor_description",
    "NEUTRALDESCRIPTION": "neutral_description",
    "SCORE": "score",
    "SCOREMARGIN": "score_margin",
}

_PBP_COLS = [
    "event_id", "game_id", "period",
    "pc_time_string", "wc_time_string",
    "eventmsgtype", "eventmsgactiontype",
    "player1_id", "player2_id", "player3_id",
    "person1type", "person2type", "person3type",
    "team1_id", "team2_id",
    "home_description", "visitor_description", "neutral_description",
    "score", "score_margin",
]


# ------------------------------------------------------------------ #
# Fetch                                                               #
# ------------------------------------------------------------------ #

def _fetch_pbp(game_id: str) -> pd.DataFrame:
    cache_key = f"pbp_{game_id}"
    cached = load_cache(cache_key)
    if cached:
        return pd.DataFrame(cached)

    def _call():
        ep = playbyplayv2.PlayByPlayV2(game_id=game_id)
        return ep.get_data_frames()[0]

    df = call_with_backoff(_call, base_sleep=1.5, label=f"PlayByPlayV2({game_id})")
    save_cache(cache_key, df.to_dict(orient="records"))
    return df


# ------------------------------------------------------------------ #
# Transform                                                           #
# ------------------------------------------------------------------ #

def _transform_pbp(df: pd.DataFrame) -> list[dict]:
    df = df.rename(columns=_PBP_RENAME)

    # Synthesise a stable event_id: zero-pad eventnum to 6 digits for correct text sort
    df["event_id"] = df["game_id"].astype(str) + "_" + df["eventnum"].astype(int).map(
        lambda x: f"{x:06d}"
    )

    # Cast IDs to str; treat "0" player IDs as None (team-level events)
    for col in ("player1_id", "player2_id", "player3_id", "team1_id", "team2_id"):
        if col in df.columns:
            df[col] = df[col].apply(
                lambda v: str(int(v)) if pd.notna(v) and str(v) not in ("0", "0.0", "") else None
            )

    # score_margin: preserve raw string ('+5', '-3', 'TIE') for TEXT column; cast at query time
    if "score_margin" in df.columns:
        df["score_margin"] = df["score_margin"].apply(
            lambda v: str(v) if pd.notna(v) and str(v) not in ("", "nan") else None
        )

    available = [c for c in _PBP_COLS if c in df.columns]
    df_clean = df[available].copy()
    for c in _PBP_COLS:
        if c not in df_clean.columns:
            df_clean[c] = None

    return df_clean.where(pd.notna(df_clean), None).to_dict(orient="records")


# ------------------------------------------------------------------ #
# Load                                                                #
# ------------------------------------------------------------------ #

def load_game(con: sqlite3.Connection, game_id: str) -> int:
    """Fetch and load play-by-play for a single *game_id*."""
    df = _fetch_pbp(game_id)
    if df.empty:
        logger.warning("No PBP data for game %s.", game_id)
        return 0
    rows = _transform_pbp(df)
    rows = validate_rows("fact_play_by_play", rows)
    with transaction(con):
        n = upsert_rows(con, "fact_play_by_play", rows, autocommit=False)
    logger.info("fact_play_by_play: %d events loaded for game %s.", n, game_id)
    return n


def load_games(
    con: sqlite3.Connection,
    game_ids: Iterable[str],
    inter_call_sleep: float = 1.5,
) -> int:
    """
    Fetch play-by-play for multiple games with rate-limit sleep between calls.
    Games already cached on disk skip the HTTP call entirely.
    """
    import time
    total = 0
    for gid in game_ids:
        try:
            total += load_game(con, gid)
        except Exception as exc:
            logger.error("PBP failed for game %s: %s", gid, exc)
        time.sleep(inter_call_sleep)
    return total


def load_season_pbp(
    con: sqlite3.Connection,
    season: str,
    limit: int | None = None,
) -> int:
    """
    Load play-by-play for all games in *season* that are already in fact_game.

    Parameters
    ----------
    limit : int | None
        If set, only process the first *limit* games (useful for testing).
    """
    loader_id = "play_by_play.load_season"
    if already_loaded(con, "fact_play_by_play", season, loader_id):
        logger.info("Skipping play by play for %s (already loaded)", season)
        return 0

    from datetime import datetime
    started_at = datetime.now(UTC).isoformat()

    cursor = con.execute(
        "SELECT game_id FROM fact_game WHERE season_id = ? ORDER BY game_date",
        (season,),
    )
    game_ids = [row[0] for row in cursor.fetchall()]
    if limit:
        game_ids = game_ids[:limit]
    logger.info("Loading PBP for %d games in season %s.", len(game_ids), season)

    total = load_games(con, game_ids)

    status = "partial" if limit else "ok"
    record_run(con, "fact_play_by_play", season, loader_id, total, status, started_at)
    log_load_summary(con, "fact_play_by_play", season)

    return total


if __name__ == "__main__":  # pragma: no cover
    from src.db.schema import init_db
    logging.basicConfig(level=logging.INFO)
    con = init_db()
    # Demo: load PBP for the first 5 games already in the db
    load_season_pbp(con, "2024-25", limit=5)
    con.close()
