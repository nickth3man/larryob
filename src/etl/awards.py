"""
ETL: fact_player_award.

Strategy
--------
* nba_api PlayerAwards endpoint returns awards per player.
* One API call per player; uses JSON cache to avoid re-fetching.
* Maps DESCRIPTION, SEASON, TYPE, SUBTYPE1 to fact_player_award schema.
"""

import logging
import sqlite3
from datetime import UTC, datetime

from nba_api.stats.endpoints import playerawards

from .api_client import APICaller
from .utils import already_loaded, load_cache, record_run, save_cache, upsert_rows

logger = logging.getLogger(__name__)


def _build_award_name(description: str, all_nba_number: str | None) -> str:
    """Build award_name; e.g. 'All-NBA' + '1' -> 'All-NBA 1st'."""
    if not description:
        return "Unknown"
    desc = str(description).strip()
    if desc.lower() == "all-nba" and all_nba_number:
        num = str(all_nba_number).strip()
        if num == "1":
            return "All-NBA 1st"
        if num == "2":
            return "All-NBA 2nd"
        if num == "3":
            return "All-NBA 3rd"
    if desc.lower() == "all-defensive team" and all_nba_number:
        num = str(all_nba_number).strip()
        if num == "1":
            return "All-Defensive 1st"
        if num == "2":
            return "All-Defensive 2nd"
    return desc


def _map_award_type(api_type: str | None) -> str:
    """Map API TYPE to award_type schema value."""
    if not api_type:
        return "individual"
    t = str(api_type).strip().lower()
    if "month" in t:
        return "monthly"
    if "week" in t:
        return "weekly"
    if "team" in t or "inclusion" in t:
        return "team_inclusion"
    return "individual"


def _player_awards_to_rows(records: list[dict]) -> list[dict]:
    """Convert PlayerAwards API records to fact_player_award rows."""
    rows: list[dict] = []
    for r in records:
        r = {k.lower(): v for k, v in r.items()}
        season = r.get("season")
        if not season or str(season).strip() == "":
            continue
        season_id = str(season).strip()
        award_name = _build_award_name(
            r.get("description") or "",
            r.get("all_nba_team_number"),
        )
        rows.append({
            "player_id": str(r.get("person_id", "")),
            "season_id": season_id,
            "award_name": award_name,
            "award_type": _map_award_type(r.get("type")),
            "trophy_name": r.get("subtype1") or None,
            "votes_received": None,
            "votes_possible": None,
        })
    return rows


def load_player_awards(
    con: sqlite3.Connection,
    player_ids: list[str],
    api_caller: APICaller | None = None,
) -> int:
    """
    Load awards for the given player IDs into fact_player_award.
    Returns total rows inserted.
    """
    if api_caller is None:
        api_caller = APICaller()

    all_rows: list[dict] = []
    for i, pid in enumerate(player_ids):
        cache_key = f"awards_{pid}"
        cached = load_cache(cache_key)
        if cached:
            all_rows.extend(_player_awards_to_rows(cached))
            continue
        try:
            def _fetch():
                ep = playerawards.PlayerAwards(player_id=pid)
                df = ep.get_data_frames()[0]
                if df.empty:
                    return []
                return df.to_dict(orient="records")

            records = api_caller.call_with_backoff(_fetch, label=f"PlayerAwards({pid})")
            save_cache(cache_key, records if records is not None else [])
            if records:
                all_rows.extend(_player_awards_to_rows(records))
        except Exception as exc:
            logger.warning("PlayerAwards(%s) failed: %s", pid, exc)
        if (i + 1) % 50 == 0:
            logger.info("Awards: %d/%d players processed.", i + 1, len(player_ids))

    if not all_rows:
        return 0
    inserted = upsert_rows(con, "fact_player_award", all_rows, conflict="IGNORE")
    logger.info("fact_player_award: %d rows upserted.", inserted)
    return inserted


def load_all_awards(
    con: sqlite3.Connection,
    active_only: bool = True,
) -> int:
    """
    Load awards for all players in dim_player.
    Use active_only=True to limit to active players (fewer API calls).
    """
    loader_id = f"awards.load_all_awards.active_{active_only}"
    if already_loaded(con, "fact_player_award", None, loader_id):
        logger.info("Skipping fact_player_award (already loaded)")
        return 0

    started_at = datetime.now(UTC).isoformat()
    try:
        if active_only:
            cur = con.execute("SELECT player_id FROM dim_player WHERE is_active = 1")
        else:
            cur = con.execute("SELECT player_id FROM dim_player")
        player_ids = [r[0] for r in cur.fetchall()]
        if not player_ids:
            logger.warning("No players found in dim_player.")
            record_run(con, "fact_player_award", None, loader_id, 0, "ok", started_at)
            return 0

        inserted = load_player_awards(con, player_ids)
        record_run(con, "fact_player_award", None, loader_id, inserted, "ok", started_at)
        return inserted
    except Exception:
        record_run(con, "fact_player_award", None, loader_id, None, "error", started_at)
        raise


if __name__ == "__main__":  # pragma: no cover
    from src.db.schema import init_db

    logging.basicConfig(level=logging.INFO)
    con = init_db()
    load_all_awards(con, active_only=True)
    con.close()
