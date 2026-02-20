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
import time

from nba_api.stats.endpoints import playerawards

from .utils import call_with_backoff, load_cache, save_cache, upsert_rows

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
    if "week" in t or "month" in t:
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
            r.get("description"),
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
    inter_call_sleep: float = 2.5,
) -> int:
    """
    Load awards for the given player IDs into fact_player_award.
    Returns total rows inserted.
    """
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

            records = call_with_backoff(_fetch, label=f"PlayerAwards({pid})")
            if records:
                save_cache(cache_key, records)
                all_rows.extend(_player_awards_to_rows(records))
        except Exception as exc:
            logger.warning("PlayerAwards(%s) failed: %s", pid, exc)
        if (i + 1) % 50 == 0:
            logger.info("Awards: %d/%d players processed.", i + 1, len(player_ids))
        time.sleep(inter_call_sleep)

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
    where = "WHERE is_active = 1" if active_only else ""
    cur = con.execute(f"SELECT player_id FROM dim_player {where}")
    player_ids = [r[0] for r in cur.fetchall()]
    if not player_ids:
        logger.warning("No players found in dim_player.")
        return 0
    return load_player_awards(con, player_ids)


if __name__ == "__main__":
    from src.db.schema import init_db

    logging.basicConfig(level=logging.INFO)
    con = init_db()
    load_all_awards(con, active_only=True)
    con.close()
