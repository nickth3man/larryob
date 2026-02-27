"""Team dimension loaders."""

import logging
import sqlite3
from datetime import UTC

from nba_api.stats.static import teams as nba_teams_static

from ...db.cache import load_cache, save_cache
from ...db.operations import upsert_rows
from ...db.tracking import already_loaded, record_run
from .helpers import _map_nba_team

logger = logging.getLogger(__name__)


def load_teams(con: sqlite3.Connection) -> int:
    """Seed dim_team from nba_api static team data."""
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
