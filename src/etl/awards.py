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

from ..db.cache import load_cache, save_cache
from ..db.operations import upsert_rows
from ..db.tracking import already_loaded, record_run
from .extract.api_client import APICaller

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
        rows.append(
            {
                "player_id": str(r.get("person_id", "")),
                "season_id": season_id,
                "award_name": award_name,
                "award_type": _map_award_type(r.get("type")),
                "trophy_name": r.get("subtype1") or None,
                "votes_received": None,
                "votes_possible": None,
            }
        )
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
    cached_players = 0
    fetched_players = 0
    failed_players = 0
    total_players = len(player_ids)
    for i, pid in enumerate(player_ids):
        source = "none"
        added_rows = 0
        cache_key = f"awards_{pid}"
        cached = load_cache(cache_key)
        if cached is not None:
            source = "cache"
            cached_players += 1
            mapped = _player_awards_to_rows(cached)
            added_rows = len(mapped)
            all_rows.extend(mapped)
        else:
            try:

                def _fetch():
                    ep = playerawards.PlayerAwards(player_id=pid)
                    df = ep.get_data_frames()[0]
                    if df.empty:
                        return []
                    return df.to_dict(orient="records")

                records = api_caller.call_with_backoff(_fetch, label=f"PlayerAwards({pid})")
                fetched_players += 1
                save_cache(cache_key, records if records is not None else [])
                if records:
                    mapped = _player_awards_to_rows(records)
                    added_rows = len(mapped)
                    all_rows.extend(mapped)
                source = "api"
            except Exception as exc:
                failed_players += 1
                source = "error"
                logger.warning("PlayerAwards(%s) failed: %s", pid, exc)

        processed = i + 1
        logger.info(
            "Awards player [%d/%d] pid=%s source=%s added_rows=%d cumulative_rows=%d "
            "cached=%d fetched=%d failed=%d",
            processed,
            total_players,
            pid,
            source,
            added_rows,
            len(all_rows),
            cached_players,
            fetched_players,
            failed_players,
        )
        if processed % 10 == 0 or processed == total_players:
            logger.info(
                "Awards progress: %d/%d players processed (cached=%d fetched=%d failed=%d rows_buffered=%d)",
                processed,
                total_players,
                cached_players,
                fetched_players,
                failed_players,
                len(all_rows),
            )

    if not all_rows:
        return 0
    valid_player_ids = {r[0] for r in con.execute("SELECT player_id FROM dim_player")}
    valid_seasons = {r[0] for r in con.execute("SELECT season_id FROM dim_season")}

    filtered_rows: list[dict] = []
    skipped_missing_player = 0
    skipped_missing_season = 0
    for row in all_rows:
        if row["player_id"] not in valid_player_ids:
            skipped_missing_player += 1
            continue
        if row["season_id"] not in valid_seasons:
            skipped_missing_season += 1
            continue
        filtered_rows.append(row)

    logger.info(
        "Awards row filter: kept=%d skipped_missing_player=%d skipped_missing_season=%d",
        len(filtered_rows),
        skipped_missing_player,
        skipped_missing_season,
    )
    if not filtered_rows:
        return 0

    inserted = upsert_rows(con, "fact_player_award", filtered_rows, conflict="IGNORE")
    logger.info(
        "fact_player_award: %d rows upserted from %d filtered rows.",
        inserted,
        len(filtered_rows),
    )
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
