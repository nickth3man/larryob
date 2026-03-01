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
from datetime import UTC, datetime

from nba_api.stats.endpoints import commonteamroster

from ..db.cache import load_cache, save_cache
from ..db.operations import upsert_rows
from ..db.tracking import already_loaded, record_run
from .extract.api_client import APICaller
from .metrics import record_etl_rows
from .validation import validate_rows

logger = logging.getLogger(__name__)


def _season_start_date(season_id: str) -> str:
    """Approximate season start date (October 1)."""
    start_year = int(season_id.split("-")[0])
    return f"{start_year}-10-01"


def fetch_common_team_roster_rows(
    con: sqlite3.Connection, season_id: str, api_caller: APICaller | None = None
) -> list[dict]:
    """Fetch roster for all teams."""
    if api_caller is None:
        api_caller = APICaller()
    cur = con.execute("SELECT team_id FROM dim_team")
    team_ids = [r[0] for r in cur.fetchall()]

    api_season = season_id.split("-")[0]
    all_data = []

    for tid in team_ids:

        def _fetch():
            ep = commonteamroster.CommonTeamRoster(
                team_id=tid,
                season=api_season,
                league_id_nullable="00",
            )
            dfs = ep.get_data_frames()
            if len(dfs) > 1:
                return dfs[1].assign(TEAM_ID=tid, SEASON_ID=season_id).to_dict(orient="records")
            return []

        coaches = api_caller.call_with_backoff(_fetch, label=f"CommonTeamRoster({tid},{season_id})")
        all_data.extend(coaches)

    return all_data


def load_team_roster(
    con: sqlite3.Connection,
    team_id: str,
    season_id: str,
    *,
    valid_players: set[str] | None = None,
    valid_teams: set[str] | None = None,
    api_caller: APICaller | None = None,
) -> int:
    """
    Load fact_roster for one team in one season.
    Returns number of rows inserted.
    """
    if api_caller is None:
        api_caller = APICaller()

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

            records = api_caller.call_with_backoff(
                _fetch, label=f"CommonTeamRoster({team_id},{season_id})"
            )
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
                    rows.append(
                        {
                            "player_id": pid,
                            "team_id": tid,
                            "season_id": season_id,
                            "start_date": start_date,
                            "end_date": None,
                        }
                    )
            save_cache(cache_key, rows)
        except Exception as exc:
            logger.warning("CommonTeamRoster(%s,%s) failed: %s", team_id, season_id, exc)
            return 0

    rows = validate_rows("fact_roster", rows)
    if not rows:
        return 0
    inserted = upsert_rows(con, "fact_roster", rows, conflict="IGNORE")
    return inserted


def load_season_rosters(
    con: sqlite3.Connection,
    season_id: str,
    api_caller: APICaller | None = None,
) -> int:
    """
    Load fact_roster for all teams in dim_team for the given season.
    Returns total rows inserted.
    """
    if api_caller is None:
        api_caller = APICaller()

    loader_id = "roster.load_season_rosters"
    if already_loaded(con, "fact_roster", season_id, loader_id):
        logger.info("Skipping fact_roster for %s (already loaded)", season_id)
        return 0

    started_at = datetime.now(UTC).isoformat()
    try:
        cur = con.execute("SELECT team_id FROM dim_team")
        team_ids = [r[0] for r in cur.fetchall()]
        valid_teams = set(team_ids)  # Reuse result — avoids a second identical query
        cur = con.execute("SELECT player_id FROM dim_player")
        valid_players = {r[0] for r in cur.fetchall()}
        total = 0
        for i, tid in enumerate(team_ids):
            total += load_team_roster(
                con,
                tid,
                season_id,
                valid_players=valid_players,
                valid_teams=valid_teams,
                api_caller=api_caller,
            )
            if (i + 1) % 5 == 0:
                logger.info(
                    "Roster: %d/%d teams processed for %s.", i + 1, len(team_ids), season_id
                )
            api_caller.sleep_between_calls()
        logger.info("fact_roster: %d total rows for %s.", total, season_id)
        record_etl_rows("fact_roster", season_id, total)
        record_run(con, "fact_roster", season_id, loader_id, total, "ok", started_at)
        return total
    except Exception:
        record_run(con, "fact_roster", season_id, loader_id, None, "error", started_at)
        raise


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
