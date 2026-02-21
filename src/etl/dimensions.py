"""
ETL: Dimension tables — dim_team, dim_player, dim_season.

Strategy
--------
* dim_team  : nba_api static teams dataset (all 30 current + historical).
* dim_player: nba_api CommonAllPlayers endpoint — covers every player ever
              to appear in an NBA game.
* dim_season: Generated programmatically for 1946-47 → current season.

All inserts use INSERT OR IGNORE so the module is safe to re-run.
"""

import logging
import sqlite3
from datetime import UTC

from nba_api.stats.endpoints import commonallplayers, commonplayerinfo
from nba_api.stats.static import players as nba_players_static
from nba_api.stats.static import teams as nba_teams_static

from .api_client import APICaller
from .config import get_team_metadata
from .metrics import record_etl_rows
from .utils import (
    already_loaded,
    load_cache,
    record_run,
    save_cache,
    upsert_rows,
)

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
    loader_id = f"dimensions.load_seasons.{up_to_start_year}"
    if already_loaded(con, "dim_season", None, loader_id):
        logger.info("Skipping dim_season (already loaded)")
        return 0

    from datetime import datetime
    started_at = datetime.now(UTC).isoformat()

    rows = []
    for y in range(NBA_FIRST_SEASON_START, up_to_start_year + 1):
        rows.append({
            "season_id": _season_id(y),
            "start_year": y,
            "end_year": y + 1,
        })
    inserted = upsert_rows(con, "dim_season", rows)
    logger.info("dim_season: %d rows upserted.", inserted)

    record_run(con, "dim_season", None, loader_id, inserted, "ok", started_at)
    return inserted


# ------------------------------------------------------------------ #
# Teams                                                               #
# ------------------------------------------------------------------ #

# Static metadata for all 30 NBA franchises (conference, division, colors, arena).
# Keyed by team_id (NBA numeric ID as string).
_TEAM_METADATA: dict[str, dict] = {
    "1610612737": {"conference": "East", "division": "Southeast", "arena_name": "State Farm Arena",
                   "color_primary": "#E03A3E", "color_secondary": "#C1D32F", "founded_year": 1949},
    "1610612738": {"conference": "East", "division": "Atlantic", "arena_name": "TD Garden",
                   "color_primary": "#007A33", "color_secondary": "#BA9653", "founded_year": 1946},
    "1610612739": {"conference": "East", "division": "Central", "arena_name": "Rocket Mortgage FieldHouse",
                   "color_primary": "#860038", "color_secondary": "#FDBB30", "founded_year": 1970},
    "1610612740": {"conference": "West", "division": "Southwest", "arena_name": "Smoothie King Center",
                   "color_primary": "#0C2C56", "color_secondary": "#B4975A", "founded_year": 2002},
    "1610612741": {"conference": "East", "division": "Central", "arena_name": "United Center",
                   "color_primary": "#CE1141", "color_secondary": "#000000", "founded_year": 1966},
    "1610612742": {"conference": "West", "division": "Southwest", "arena_name": "American Airlines Center",
                   "color_primary": "#002B5C", "color_secondary": "#00471B", "founded_year": 1980},
    "1610612743": {"conference": "West", "division": "Northwest", "arena_name": "Ball Arena",
                   "color_primary": "#0E2240", "color_secondary": "#FEC524", "founded_year": 1976},
    "1610612744": {"conference": "West", "division": "Pacific", "arena_name": "Chase Center",
                   "color_primary": "#1D428A", "color_secondary": "#FFC52F", "founded_year": 1946},
    "1610612745": {"conference": "West", "division": "Southwest", "arena_name": "Toyota Center",
                   "color_primary": "#CE1141", "color_secondary": "#C4CED4", "founded_year": 1967},
    "1610612746": {"conference": "West", "division": "Pacific", "arena_name": "Crypto.com Arena",
                   "color_primary": "#C60C30", "color_secondary": "#EF3B24", "founded_year": 1970},
    "1610612747": {"conference": "West", "division": "Pacific", "arena_name": "Crypto.com Arena",
                   "color_primary": "#552582", "color_secondary": "#FDB927", "founded_year": 1948},
    "1610612748": {"conference": "East", "division": "Southeast", "arena_name": "Kaseya Center",
                   "color_primary": "#98002E", "color_secondary": "#000000", "founded_year": 1988},
    "1610612749": {"conference": "East", "division": "Central", "arena_name": "Fiserv Forum",
                   "color_primary": "#00471B", "color_secondary": "#EEE1C6", "founded_year": 1968},
    "1610612750": {"conference": "West", "division": "Northwest", "arena_name": "Target Center",
                   "color_primary": "#0C2340", "color_secondary": "#9EA2A2", "founded_year": 1989},
    "1610612751": {"conference": "East", "division": "Atlantic", "arena_name": "Barclays Center",
                   "color_primary": "#000000", "color_secondary": "#FFFFFF", "founded_year": 1976},
    "1610612752": {"conference": "East", "division": "Atlantic", "arena_name": "Madison Square Garden",
                   "color_primary": "#006BB6", "color_secondary": "#F58426", "founded_year": 1946},
    "1610612753": {"conference": "East", "division": "Southeast", "arena_name": "Kia Center",
                   "color_primary": "#0077C0", "color_secondary": "#000000", "founded_year": 1989},
    "1610612754": {"conference": "East", "division": "Central", "arena_name": "Gainbridge Fieldhouse",
                   "color_primary": "#002D62", "color_secondary": "#FDBB30", "founded_year": 1976},
    "1610612755": {"conference": "East", "division": "Atlantic", "arena_name": "Wells Fargo Center",
                   "color_primary": "#006BB6", "color_secondary": "#ED174C", "founded_year": 1949},
    "1610612756": {"conference": "West", "division": "Pacific", "arena_name": "Footprint Center",
                   "color_primary": "#1D1160", "color_secondary": "#E56020", "founded_year": 1968},
    "1610612757": {"conference": "West", "division": "Northwest", "arena_name": "Moda Center",
                   "color_primary": "#E03A3E", "color_secondary": "#000000", "founded_year": 1970},
    "1610612758": {"conference": "West", "division": "Pacific", "arena_name": "Golden 1 Center",
                   "color_primary": "#5A2D81", "color_secondary": "#888888", "founded_year": 1948},
    "1610612759": {"conference": "West", "division": "Southwest", "arena_name": "Frost Bank Center",
                   "color_primary": "#000000", "color_secondary": "#C4CED4", "founded_year": 1976},
    "1610612760": {"conference": "West", "division": "Northwest", "arena_name": "Paycom Center",
                   "color_primary": "#007AC1", "color_secondary": "#EF3B24", "founded_year": 2008},
    "1610612761": {"conference": "East", "division": "Atlantic", "arena_name": "Scotiabank Arena",
                   "color_primary": "#CE1141", "color_secondary": "#000000", "founded_year": 1995},
    "1610612762": {"conference": "West", "division": "Northwest", "arena_name": "Delta Center",
                   "color_primary": "#002B5C", "color_secondary": "#00471B", "founded_year": 1974},
    "1610612763": {"conference": "West", "division": "Southwest", "arena_name": "FedExForum",
                   "color_primary": "#12173F", "color_secondary": "#6ECEB2", "founded_year": 1995},
    "1610612764": {"conference": "East", "division": "Southeast", "arena_name": "Capital One Arena",
                   "color_primary": "#002B5C", "color_secondary": "#E31837", "founded_year": 1961},
    "1610612765": {"conference": "East", "division": "Central", "arena_name": "Little Caesars Arena",
                   "color_primary": "#C8102E", "color_secondary": "#1D42BA", "founded_year": 1948},
    "1610612766": {"conference": "East", "division": "Southeast", "arena_name": "Spectrum Center",
                   "color_primary": "#1D1160", "color_secondary": "#00788C", "founded_year": 1988},
}


def _map_nba_team(t: dict) -> dict:
    """Map nba_api static team dict → dim_team row."""
    team_id = str(t["id"])
    base = {
        "team_id": team_id,
        "abbreviation": t["abbreviation"],
        "full_name": t["full_name"],
        "city": t["city"],
        "nickname": t["nickname"],
    }
    meta = _TEAM_METADATA.get(team_id, {})
    base["conference"] = meta.get("conference")
    base["division"] = meta.get("division")
    base["color_primary"] = meta.get("color_primary")
    base["color_secondary"] = meta.get("color_secondary")
    base["arena_name"] = meta.get("arena_name")
    base["founded_year"] = meta.get("founded_year")
    return base


def load_teams(con: sqlite3.Connection) -> int:
    """Seed dim_team from nba_api static data (all 30 franchises)."""
    loader_id = "dimensions.load_teams"
    if already_loaded(con, "dim_team", None, loader_id):
        logger.info("Skipping dim_team (already loaded)")
        return 0

    from datetime import datetime
    started_at = datetime.now(UTC).isoformat()

    cache_key = "teams_static"
    cached = load_cache(cache_key)
    if cached:
        logger.info("dim_team: loading from cache.")
        raw_teams = cached
    else:
        raw_teams = nba_teams_static.get_teams()
        save_cache(cache_key, raw_teams)

    rows = [_map_nba_team(t) for t in raw_teams]
    try:
        inserted = upsert_rows(con, "dim_team", rows)
        record_run(con, "dim_team", None, loader_id, inserted, "ok", started_at)
        logger.info("dim_team: %d rows upserted from nba_api static data.", inserted)
        return inserted
    except Exception:
        record_run(con, "dim_team", None, loader_id, 0, "error", started_at)
        raise


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
    CommonAllPlayers does not return bio fields; use load_players_bio_enrichment for those.
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


def _height_to_cm(height_str: str | None) -> float | None:
    """Convert NBA height string '6-8' (feet-inches) to cm."""
    if not height_str or not isinstance(height_str, str):
        return None
    parts = height_str.strip().split("-")
    if len(parts) != 2:
        return None
    try:
        feet, inches = int(parts[0]), int(parts[1])
        total_inches = feet * 12 + inches
        return round(total_inches * 2.54, 1)
    except (ValueError, IndexError):
        return None


def _weight_to_kg(weight_str: str | int | None) -> float | None:
    """Convert weight in lbs to kg."""
    if weight_str is None:
        return None
    try:
        lbs = float(weight_str) if isinstance(weight_str, str) else weight_str
        return round(lbs * 0.453592, 1)
    except (ValueError, TypeError):
        return None


def _parse_birth_date(date_str: str | None) -> str | None:
    """Extract YYYY-MM-DD from date string like '1989-12-09T00:00:00'."""
    if not date_str or not isinstance(date_str, str):
        return None
    return date_str[:10] if len(date_str) >= 10 else None


def _normalize_position(pos: str | None) -> str | None:
    """Map API position strings to schema values."""
    if not pos or not isinstance(pos, str):
        return None
    p = pos.strip().upper()
    schema_positions = ("PG", "SG", "SF", "PF", "C", "G", "F", "G-F", "F-G", "F-C", "C-F")
    if p in schema_positions:
        return p
    if p in ("GUARD",):
        return "G"
    if p in ("FORWARD",):
        return "F"
    if p in ("CENTER",):
        return "C"
    return None


def _map_common_player_info(row: dict) -> dict:
    """Map CommonPlayerInfo row → dim_player row (with bio fields)."""
    row = {k.lower(): v for k, v in row.items()}
    full = row.get("display_first_last") or ""
    parts = full.split(" ", 1)
    birth = _parse_birth_date(row.get("birthdate"))
    height = _height_to_cm(row.get("height"))
    weight = _weight_to_kg(row.get("weight"))
    draft_year = row.get("draft_year")
    if draft_year is not None and str(draft_year).strip().lower() in ("", "undrafted"):
        draft_year = None
    try:
        draft_year = int(draft_year) if draft_year is not None else None
    except (ValueError, TypeError):
        draft_year = None
    draft_round = row.get("draft_round")
    try:
        draft_round = int(draft_round) if draft_round not in (None, "") else None
    except (ValueError, TypeError):
        draft_round = None
    draft_number = row.get("draft_number")
    try:
        draft_number = int(draft_number) if draft_number not in (None, "") else None
    except (ValueError, TypeError):
        draft_number = None
    if draft_year is None:
        draft_round = None
        draft_number = None
    return {
        "player_id": str(row.get("person_id", "")),
        "first_name": parts[0] if parts else "",
        "last_name": parts[1] if len(parts) > 1 else "",
        "full_name": full,
        "birth_date": birth,
        "birth_city": None,
        "birth_country": row.get("country") or None,
        "height_cm": height,
        "weight_kg": weight,
        "position": _normalize_position(row.get("position")),
        "draft_year": draft_year,
        "draft_round": draft_round,
        "draft_number": draft_number,
        "is_active": 1 if str(row.get("rosterstatus", "0")).lower() == "active" else 0,
    }


def load_players_static(con: sqlite3.Connection) -> int:
    """
    Fast seed of dim_player using nba_api static data.
    Covers all historical + active players without any HTTP calls.
    """
    loader_id = "dimensions.load_players_static"
    if already_loaded(con, "dim_player", None, loader_id):
        logger.info("Skipping dim_player static (already loaded)")
        return 0

    from datetime import datetime
    started_at = datetime.now(UTC).isoformat()

    raw = nba_players_static.get_players()
    rows = [_map_nba_player_static(p) for p in raw]
    try:
        inserted = upsert_rows(con, "dim_player", rows)
        record_run(con, "dim_player", None, loader_id, inserted, "ok", started_at)
        logger.info("dim_player: %d rows upserted from nba_api static data.", inserted)
        return inserted
    except Exception:
        record_run(con, "dim_player", None, loader_id, 0, "error", started_at)
        raise


def load_players_full(
    con: sqlite3.Connection,
    season_id: str = "2024-25",
    api_caller: APICaller | None = None,
) -> int:
    """
    Deeper player load via CommonAllPlayers endpoint for a given season.
    Fetches richer metadata and fills gaps left by the static dataset.
    Falls back to cached data if the API call fails.
    """
    if api_caller is None:
        api_caller = APICaller()

    loader_id = f"dimensions.load_players_full.{season_id}"
    if already_loaded(con, "dim_player", None, loader_id):
        logger.info("Skipping dim_player full (already loaded)")
        return 0

    from datetime import datetime
    started_at = datetime.now(UTC).isoformat()

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

        records = api_caller.call_with_backoff(_fetch, label=f"CommonAllPlayers({season_id})")
        save_cache(cache_key, records)

    # Normalise column names to lower-case
    records = [{k.lower(): v for k, v in r.items()} for r in records]
    rows = [_map_common_all_player(r) for r in records]

    try:
        inserted = upsert_rows(con, "dim_player", rows, conflict="REPLACE")
        record_run(con, "dim_player", None, loader_id, inserted, "ok", started_at)
        logger.info(
            "dim_player full: %d rows upserted from CommonAllPlayers(%s).",
            inserted, season_id,
        )
        return inserted
    except Exception:
        record_run(con, "dim_player", None, loader_id, 0, "error", started_at)
        raise


def load_players_bio_enrichment(
    con: sqlite3.Connection,
    player_ids: list[str] | None = None,
    active_only: bool = True,
    api_caller: APICaller | None = None,
) -> int:
    """
    Enrich dim_player with bio data (height, weight, birth_date, draft info, etc.)
    via CommonPlayerInfo. One API call per player; use active_only=True to limit.
    """
    if api_caller is None:
        api_caller = APICaller()
    loader_id = f"dimensions.load_players_bio_enrichment.active_{active_only}"
    selected_from_db = player_ids is None
    if selected_from_db and already_loaded(con, "dim_player", None, loader_id):
        logger.info("Skipping dim_player bio enrichment (already loaded)")
        return 0

    import time
    from datetime import datetime
    started_at = datetime.now(UTC).isoformat()

    if selected_from_db:
        if active_only:
            cur = con.execute("SELECT player_id FROM dim_player WHERE is_active = 1")
        else:
            cur = con.execute("SELECT player_id FROM dim_player")
        player_ids = [r[0] for r in cur.fetchall()]
    if player_ids is None:
        player_ids = []

    rows: list[dict] = []
    for i, pid in enumerate(player_ids):
        cache_key = f"common_player_info_{pid}"
        cached = load_cache(cache_key)
        if cached:
            rows.append(_map_common_player_info(cached))
            continue
        try:
            def _fetch():
                ep = commonplayerinfo.CommonPlayerInfo(player_id=pid)
                df = ep.get_data_frames()[0]
                if df.empty:
                    return None
                return df.iloc[0].to_dict()

            record = api_caller.call_with_backoff(_fetch, label=f"CommonPlayerInfo({pid})")
            if record:
                save_cache(cache_key, record)
                rows.append(_map_common_player_info(record))
        except Exception as exc:
            logger.warning("CommonPlayerInfo(%s) failed: %s", pid, exc)
        if (i + 1) % 50 == 0:
            logger.info("Bio enrichment: %d/%d players processed.", i + 1, len(player_ids))
        api_caller.sleep_between_calls()

    if rows:
        try:
            inserted = upsert_rows(con, "dim_player", rows, conflict="REPLACE")
            record_run(con, "dim_player", None, loader_id, inserted, "ok", started_at)
            logger.info("dim_player bio enrichment: %d rows updated.", inserted)
            return inserted
        except Exception:
            record_run(con, "dim_player", None, loader_id, 0, "error", started_at)
            raise

    if selected_from_db:
        record_run(con, "dim_player", None, loader_id, 0, "ok", started_at)
    return 0


# ------------------------------------------------------------------ #
# Convenience: run all dimension loaders                             #
# ------------------------------------------------------------------ #

def run_all(
    con: sqlite3.Connection,
    full_players: bool = False,
    enrich_bio: bool = False,
) -> None:
    """
    Seed all dimension tables.

    Parameters
    ----------
    full_players : bool
        If True, also hit the CommonAllPlayers endpoint for richer metadata.
        Set False during tests / quick runs to avoid live HTTP calls.
    enrich_bio : bool
        If True, enrich dim_player with bio data (height, weight, birth_date, draft)
        via CommonPlayerInfo. Only active players by default. Many API calls.
    """
    load_seasons(con)
    load_teams(con)
    load_players_static(con)
    if full_players:
        load_players_full(con)
    if enrich_bio:
        load_players_bio_enrichment(con, active_only=True)


if __name__ == "__main__":  # pragma: no cover
    from src.db.schema import init_db
    logging.basicConfig(level=logging.INFO)
    con = init_db()
    run_all(con, full_players=True)
    con.close()
