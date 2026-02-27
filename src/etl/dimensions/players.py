"""Player dimension loaders."""

import logging
import sqlite3
from datetime import UTC

from nba_api.stats.endpoints import commonallplayers, commonplayerinfo
from nba_api.stats.static import players as nba_players_static

from ...db.cache import load_cache, save_cache
from ...db.operations import upsert_rows
from ...db.tracking import already_loaded, record_run
from ..extract.api_client import APICaller
from .helpers import _map_common_all_player, _map_common_player_info, _map_nba_player_static

logger = logging.getLogger(__name__)


def load_players_static(con: sqlite3.Connection) -> int:
    """Seed dim_player from nba_api static player data."""
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
    """Load richer player metadata from CommonAllPlayers for a season."""
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

    records = [{k.lower(): v for k, v in r.items()} for r in records]
    rows = [_map_common_all_player(r) for r in records]

    try:
        inserted = upsert_rows(con, "dim_player", rows, conflict="REPLACE")
        record_run(con, "dim_player", None, loader_id, inserted, "ok", started_at)
        logger.info(
            "dim_player full: %d rows upserted from CommonAllPlayers(%s).",
            inserted,
            season_id,
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
    """Enrich dim_player rows with CommonPlayerInfo bio data."""
    if api_caller is None:
        api_caller = APICaller()
    loader_id = f"dimensions.load_players_bio_enrichment.active_{active_only}"
    selected_from_db = player_ids is None
    if selected_from_db and already_loaded(con, "dim_player", None, loader_id):
        logger.info("Skipping dim_player bio enrichment (already loaded)")
        return 0

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
