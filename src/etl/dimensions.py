"""
ETL: Dimension tables — dim_team, dim_player, dim_season.

Strategy
--------
* dim_team  : nba_api static teams dataset (all 30 current + historical).
              balldontlie is used as a lightweight fallback if the API is down.
* dim_player: nba_api CommonAllPlayers endpoint — covers every player ever
              to appear in an NBA game.
* dim_season: Generated programmatically for 1946-47 → current season.

All inserts use INSERT OR IGNORE so the module is safe to re-run.
"""

import logging
import sqlite3

from nba_api.stats.endpoints import commonallplayers
from nba_api.stats.static import players as nba_players_static
from nba_api.stats.static import teams as nba_teams_static

from .utils import call_with_backoff, load_cache, save_cache, upsert_rows

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------ #
# Seasons                                                             #
# ------------------------------------------------------------------ #

NBA_FIRST_SEASON_START = 1946  # 1946-47 inaugural season


def _season_id(start_year: int) -> str:
    """Convert 1946 → '1946-47', 2023 → '2023-24'."""
    return f"{start_year}-{str(start_year + 1)[-2:]}"


def load_seasons(con: sqlite3.Connection, up_to_start_year: int = 2024) -> int:
    """Seed dim_season from the inaugural 1946-47 season through *up_to_start_year*."""
    rows = []
    for y in range(NBA_FIRST_SEASON_START, up_to_start_year + 1):
        rows.append({
            "season_id": _season_id(y),
            "start_year": y,
            "end_year": y + 1,
        })
    inserted = upsert_rows(con, "dim_season", rows)
    logger.info("dim_season: %d rows upserted.", inserted)
    return inserted


# ------------------------------------------------------------------ #
# Teams                                                               #
# ------------------------------------------------------------------ #

def _map_nba_team(t: dict) -> dict:
    """Map nba_api static team dict → dim_team row."""
    return {
        "team_id": str(t["id"]),
        "abbreviation": t["abbreviation"],
        "full_name": t["full_name"],
        "city": t["city"],
        "nickname": t["nickname"],
        "conference": None,
        "division": None,
        "color_primary": None,
        "color_secondary": None,
        "arena_name": None,
        "founded_year": None,
    }


def load_teams(con: sqlite3.Connection) -> int:
    """Seed dim_team from nba_api static data (all 30 franchises)."""
    cache_key = "teams_static"
    cached = load_cache(cache_key)
    if cached:
        logger.info("dim_team: loading from cache.")
        raw_teams = cached
    else:
        raw_teams = nba_teams_static.get_teams()
        save_cache(cache_key, raw_teams)

    rows = [_map_nba_team(t) for t in raw_teams]
    inserted = upsert_rows(con, "dim_team", rows)
    logger.info("dim_team: %d rows upserted from nba_api static data.", inserted)
    return inserted


# ------------------------------------------------------------------ #
# Players                                                             #
# ------------------------------------------------------------------ #

def _map_nba_player_static(p: dict) -> dict:
    """Map nba_api static player dict → partial dim_player row."""
    full = p.get("full_name", "")
    parts = full.split(" ", 1)
    return {
        "player_id": str(p["id"]),
        "first_name": parts[0] if parts else "",
        "last_name": parts[1] if len(parts) > 1 else "",
        "full_name": full,
        "birth_date": None,
        "birth_city": None,
        "birth_country": None,
        "height_cm": None,
        "weight_kg": None,
        "position": None,
        "draft_year": None,
        "draft_round": None,
        "draft_number": None,
        "is_active": 1 if p.get("is_active") else 0,
    }


def _map_common_all_player(row: dict) -> dict:
    """
    Map a row from CommonAllPlayers endpoint → dim_player row.
    Column names come from the nba_api DataFrame columns (lowercased).
    """
    full = row.get("display_first_last") or row.get("player_slug", "")
    parts = full.split(" ", 1)
    return {
        "player_id": str(row.get("person_id", "")),
        "first_name": parts[0] if parts else "",
        "last_name": parts[1] if len(parts) > 1 else "",
        "full_name": full,
        "birth_date": None,
        "birth_city": None,
        "birth_country": None,
        "height_cm": None,
        "weight_kg": None,
        "position": None,
        "draft_year": None,
        "draft_round": None,
        "draft_number": None,
        "is_active": 1 if str(row.get("rosterstatus", "0")) == "1" else 0,
    }


def load_players_static(con: sqlite3.Connection) -> int:
    """
    Fast seed of dim_player using nba_api static data.
    Covers all historical + active players without any HTTP calls.
    """
    raw = nba_players_static.get_players()
    rows = [_map_nba_player_static(p) for p in raw]
    inserted = upsert_rows(con, "dim_player", rows)
    logger.info("dim_player: %d rows upserted from nba_api static data.", inserted)
    return inserted


def load_players_full(con: sqlite3.Connection, season_id: str = "2024-25") -> int:
    """
    Deeper player load via CommonAllPlayers endpoint for a given season.
    Fetches richer metadata and fills gaps left by the static dataset.
    Falls back to cached data if the API call fails.
    """
    cache_key = f"common_all_players_{season_id}"
    cached = load_cache(cache_key)

    if cached:
        logger.info("dim_player full: loading from cache for %s.", season_id)
        records = cached
    else:
        def _fetch():
            ep = commonallplayers.CommonAllPlayers(
                is_only_current_season=0,
                league_id="00",
                season=season_id,
            )
            return ep.get_data_frames()[0].to_dict(orient="records")

        records = call_with_backoff(_fetch, label=f"CommonAllPlayers({season_id})")
        save_cache(cache_key, records)

    # Normalise column names to lower-case
    records = [{k.lower(): v for k, v in r.items()} for r in records]
    rows = [_map_common_all_player(r) for r in records]
    inserted = upsert_rows(con, "dim_player", rows, conflict="REPLACE")
    logger.info(
        "dim_player full: %d rows upserted from CommonAllPlayers(%s).",
        inserted, season_id,
    )
    return inserted


# ------------------------------------------------------------------ #
# Convenience: run all dimension loaders                             #
# ------------------------------------------------------------------ #

def run_all(con: sqlite3.Connection, full_players: bool = False) -> None:
    """
    Seed all dimension tables.

    Parameters
    ----------
    full_players : bool
        If True, also hit the CommonAllPlayers endpoint for richer metadata.
        Set False during tests / quick runs to avoid live HTTP calls.
    """
    load_seasons(con)
    load_teams(con)
    load_players_static(con)
    if full_players:
        load_players_full(con)


if __name__ == "__main__":
    from src.db.schema import init_db
    logging.basicConfig(level=logging.INFO)
    con = init_db()
    run_all(con, full_players=True)
    con.close()
