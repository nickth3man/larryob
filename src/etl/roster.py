"""
ETL: fact_roster.

Strategy
--------
* nba_api CommonTeamRoster returns roster per team per season.
* One API call per (team_id, season_id); uses JSON cache.
* fact_roster: player_id, team_id, season_id, start_date, end_date.
  start_date = season start (approx); end_date = NULL for current stints.
"""

import logging
import sqlite3
import time

from nba_api.stats.endpoints import commonteamroster

from .utils import call_with_backoff, load_cache, save_cache, upsert_rows

logger = logging.getLogger(__name__)


def _season_start_date(season_id: str) -> str:
    """Approximate season start date (October 1)."""
    start_year = int(season_id.split("-")[0])
    return f"{start_year}-10-01"


def load_team_roster(
    con: sqlite3.Connection,
    team_id: str,
    season_id: str,
    *,
    valid_players: set[str] | None = None,
    valid_teams: set[str] | None = None,
) -> int:
    """
    Load fact_roster for one team in one season.
    Returns number of rows inserted.
    """
    cache_key = f"roster_{team_id}_{season_id}"
    cached = load_cache(cache_key)
    if cached is not None:
        rows = cached
    else:
        try:
            # API uses start year only, e.g. 2023 for 2023-24
            api_season = season_id.split("-")[0]

            def _fetch():
                ep = commonteamroster.CommonTeamRoster(
                    team_id=team_id,
                    season=api_season,
                    league_id_nullable="00",
                )
                df = ep.get_data_frames()[0]
                if df.empty:
                    return []
                return df.to_dict(orient="records")

            records = call_with_backoff(_fetch, label=f"CommonTeamRoster({team_id},{season_id})")
            if not records:
                return 0

            start_date = _season_start_date(season_id)
            # Filter to players/teams that exist in dim tables (FK constraint)
            if valid_players is None:
                cur = con.execute("SELECT player_id FROM dim_player")
                valid_players = {r[0] for r in cur.fetchall()}
            if valid_teams is None:
                cur = con.execute("SELECT team_id FROM dim_team")
                valid_teams = {r[0] for r in cur.fetchall()}
            rows = []
            for r in records:
                pid = str(r.get("PLAYER_ID", ""))
                tid = str(r.get("TeamID", team_id))
                if pid in valid_players and tid in valid_teams:
                    rows.append({
                        "player_id": pid,
                        "team_id": tid,
                        "season_id": season_id,
                        "start_date": start_date,
                        "end_date": None,
                    })
            save_cache(cache_key, rows)
        except Exception as exc:
            logger.warning("CommonTeamRoster(%s,%s) failed: %s", team_id, season_id, exc)
            return 0

    if not rows:
        return 0
    inserted = upsert_rows(con, "fact_roster", rows, conflict="IGNORE")
    return inserted


def load_season_rosters(
    con: sqlite3.Connection,
    season_id: str,
    inter_call_sleep: float = 2.5,
) -> int:
    """
    Load fact_roster for all teams in dim_team for the given season.
    Returns total rows inserted.
    """
    cur = con.execute("SELECT team_id FROM dim_team")
    team_ids = [r[0] for r in cur.fetchall()]
    cur = con.execute("SELECT player_id FROM dim_player")
    valid_players = {r[0] for r in cur.fetchall()}
    cur = con.execute("SELECT team_id FROM dim_team")
    valid_teams = {r[0] for r in cur.fetchall()}
    total = 0
    for i, tid in enumerate(team_ids):
        total += load_team_roster(
            con, tid, season_id,
            valid_players=valid_players,
            valid_teams=valid_teams,
        )
        if (i + 1) % 5 == 0:
            logger.info("Roster: %d/%d teams processed for %s.", i + 1, len(team_ids), season_id)
        time.sleep(inter_call_sleep)
    logger.info("fact_roster: %d total rows for %s.", total, season_id)
    return total


def load_rosters_for_seasons(
    con: sqlite3.Connection,
    season_ids: list[str],
) -> int:
    """Load rosters for all teams across given seasons."""
    total = 0
    for sid in season_ids:
        total += load_season_rosters(con, sid)
    return total


if __name__ == "__main__":  # pragma: no cover
    from src.db.schema import init_db

    logging.basicConfig(level=logging.INFO)
    con = init_db()
    load_season_rosters(con, "2023-24")
    con.close()
