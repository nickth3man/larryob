"""
ETL: dim_salary_cap, fact_salary.

Strategy
--------
* dim_salary_cap : Hardcoded historical cap amounts (1984-85 → present).
* fact_salary    : Scraped from Basketball-Reference team contract pages
  (non-commercial, personal project use).  One GET per team per call, cached
  as JSON so subsequent runs are instant.
"""

import logging
import sqlite3
import time
from datetime import UTC

from ..db.cache import load_cache
from ..db.operations import upsert_rows
from ..db.tracking import already_loaded, record_run
from ._salaries_fetch import fetch_team_current_contracts, fetch_team_season_salaries
from .config import get_all_salary_caps, nba_abbr_to_bref
from .helpers import _norm_name
from .rate_limit import _BREF_THROTTLE, BBRRateLimitExceeded
from .validation import validate_rows

logger = logging.getLogger(__name__)

# Use centralized config for salary cap data
_SALARY_CAP_BY_SEASON = get_all_salary_caps()


# Helper function for abbreviation conversion (uses centralized config)
def _abbr_to_bref(abbr: str) -> str:
    """Convert NBA abbreviation to Basketball-Reference abbreviation."""
    result = nba_abbr_to_bref(abbr)
    return result if result is not None else abbr


_LOADER_ID = "salaries.load_player_salaries.v2"


def load_salary_cap(con: sqlite3.Connection) -> int:
    """Seed dim_salary_cap from hardcoded historical values."""
    # Ensure dim_season has the seasons we need (FK constraint)
    from .dimensions import load_seasons

    max_start_year = max(int(sid.split("-")[0]) for sid in _SALARY_CAP_BY_SEASON)
    load_seasons(con, up_to_start_year=max_start_year)
    rows = [{"season_id": sid, "cap_amount": cap} for sid, cap in _SALARY_CAP_BY_SEASON.items()]
    inserted = upsert_rows(con, "dim_salary_cap", rows, conflict="REPLACE")
    logger.info("dim_salary_cap: %d rows upserted.", inserted)
    return inserted


def _normalize_name(name: str) -> str:
    """Compatibility wrapper around shared helper normalization."""
    return _norm_name(name, strip_non_alpha=True)


def _season_team_map(
    con: sqlite3.Connection,
    season_id: str,
) -> tuple[dict[str, str], str]:
    """
    Resolve season-specific Basketball-Reference team abbreviations to team IDs.

    Prefers fact_team_season coverage (historical abbreviations like MNL/PHW/etc.).
    Falls back to dim_team -> current abbrev mapping for seasons not present there.
    """
    start_year = int(season_id.split("-")[0])

    rows = con.execute(
        """
        SELECT DISTINCT fts.bref_abbrev, dth.team_id
        FROM fact_team_season AS fts
        JOIN dim_team_history AS dth
          ON dth.team_abbrev = fts.bref_abbrev
         AND dth.season_founded <= ?
         AND dth.season_active_till >= ?
        WHERE fts.season_id = ?
          AND fts.bref_abbrev IS NOT NULL
        """,
        (start_year, start_year, season_id),
    ).fetchall()
    if rows:
        bref_to_team: dict[str, str] = {}
        for bref_abbr, team_id in rows:
            b = str(bref_abbr).strip().upper()
            t = str(team_id).strip()
            prev = bref_to_team.get(b)
            if prev and prev != t:
                logger.warning(
                    "fact_team_season duplicate bref_abbrev mapping season=%s bref_abbrev=%s team_ids=%s/%s; using first",
                    season_id,
                    b,
                    prev,
                    t,
                )
                continue
            bref_to_team[b] = t
        return bref_to_team, "fact_team_season"

    cur = con.execute("SELECT team_id, abbreviation FROM dim_team")
    bref_to_team = {}
    for team_id, nba_abbr in cur.fetchall():
        b = _abbr_to_bref(str(nba_abbr).strip().upper())
        bref_to_team[b] = str(team_id).strip()
    return bref_to_team, "dim_team"


def load_player_salaries(
    con: sqlite3.Connection,
    season_id: str,
) -> int:
    """
    Scrape fact_salary from Basketball-Reference.

    Strategy
    --------
    * Past seasons (end_year ≤ current year – 1): team season page
      ``/teams/{ABBR}/{end_year}.html`` — salary table is in an HTML comment.
    * Current / future season: team contract page ``/contracts/{ABBR}.html``.

    Each page is fetched once and cached to JSON so subsequent runs skip HTTP.
    Players are matched to dim_player by normalized name (accent-stripped,
    lower-cased). Unmatched players are logged at DEBUG level and skipped.

    Parameters
    ----------
    season_id : str
        Season string e.g. '2023-24' or '2025-26'.

    Returns
    -------
    int
        Number of rows inserted/replaced.
    """
    loader_id = _LOADER_ID
    if already_loaded(con, "fact_salary", season_id, loader_id):
        logger.info("Skipping player salaries for %s (already loaded)", season_id)
        return 0

    from datetime import datetime

    started_at = datetime.now(UTC).isoformat()
    import datetime as dt

    current_year = dt.date.today().year
    end_year = int(season_id.split("-")[0]) + 1  # '2023-24' → 2024

    # Ensure dim_season covers this season (FK guard)
    from .dimensions import load_seasons

    start_year = end_year - 1
    load_seasons(con, up_to_start_year=start_year)

    # Build normalized-name → player_id index
    cur = con.execute("SELECT player_id, full_name FROM dim_player")
    player_index: dict[str, str] = {_normalize_name(row[1]): row[0] for row in cur.fetchall()}

    bref_to_team_id, team_map_source = _season_team_map(con, season_id)
    logger.info(
        "fact_salary (%s): team_map_source=%s teams=%d",
        season_id,
        team_map_source,
        len(bref_to_team_id),
    )

    rows_to_insert: list[dict] = []
    unmatched_names: set[str] = set()
    fetched_pages = 0
    cached_pages = 0
    rate_limit_exc: BBRRateLimitExceeded | None = None

    for bref_abbr, team_id in bref_to_team_id.items():
        if not team_id:
            logger.debug("No team_id for bref_abbr=%s season=%s; skipping.", bref_abbr, season_id)
            continue

        cache_key_season = f"bref_season_sal_{bref_abbr}_{end_year}"
        cache_key_contracts = f"bref_contracts_{bref_abbr}"
        was_cached = False

        try:
            if end_year < current_year:
                # Historical (season fully complete): team season page has a commented salary table
                logger.debug("BBref team season salary: %s %d", bref_abbr, end_year)
                was_cached = load_cache(cache_key_season) is not None
                entries = [
                    {"name": e["name"], "season_id": season_id, "salary": e["salary"]}
                    for e in fetch_team_season_salaries(bref_abbr, end_year)
                ]
            else:
                # Current or future season: team contract page
                logger.debug("BBref team contracts: %s", bref_abbr)
                was_cached = load_cache(cache_key_contracts) is not None
                entries = [
                    e
                    for e in fetch_team_current_contracts(bref_abbr)
                    if e["season_id"] == season_id
                ]
        except BBRRateLimitExceeded as exc:
            rate_limit_exc = exc
            logger.warning(
                "fact_salary (%s): stopping early due rate-limit url=%s retry_after=%ds max_allowed=%ds",
                season_id,
                exc.url,
                exc.retry_after,
                exc.max_allowed,
            )
            break

        if was_cached:
            cached_pages += 1
        else:
            fetched_pages += 1

        for entry in entries:
            norm = _normalize_name(entry["name"])
            player_id = player_index.get(norm)
            if not player_id:
                unmatched_names.add(entry["name"])
                continue
            rows_to_insert.append(
                {
                    "player_id": player_id,
                    "team_id": team_id,
                    "season_id": season_id,
                    "salary": entry["salary"],
                }
            )

    logger.info(
        "fact_salary (%s): team_pages fetched=%d cached=%d candidate_rows=%d unmatched_names=%d",
        season_id,
        fetched_pages,
        cached_pages,
        len(rows_to_insert),
        len(unmatched_names),
    )

    if unmatched_names:
        logger.debug(
            "fact_salary (%s): %d unmatched player name(s): %s",
            season_id,
            len(unmatched_names),
            sorted(unmatched_names)[:10],
        )

    if rate_limit_exc and not rows_to_insert:
        record_run(con, "fact_salary", season_id, loader_id, 0, "rate_limited", started_at)
        return 0

    if not rows_to_insert:
        logger.warning(
            "fact_salary (%s): no rows to insert.",
            season_id,
        )
        record_run(con, "fact_salary", season_id, loader_id, 0, "ok", started_at)
        return 0

    rows_to_insert = validate_rows("fact_salary", rows_to_insert)
    if not rows_to_insert:
        logger.warning(
            "fact_salary (%s): all rows dropped by validation.",
            season_id,
        )
        status = "rate_limited" if rate_limit_exc else "ok"
        record_run(con, "fact_salary", season_id, loader_id, 0, status, started_at)
        return 0

    from ..db.operations import transaction

    with transaction(con):
        inserted = upsert_rows(
            con, "fact_salary", rows_to_insert, conflict="REPLACE", autocommit=False
        )
    logger.info("fact_salary (%s): %d rows upserted.", season_id, inserted)

    status = "partial_rate_limited" if rate_limit_exc else "ok"
    record_run(con, "fact_salary", season_id, loader_id, inserted, status, started_at)
    return inserted


def load_salaries_for_seasons(
    con: sqlite3.Connection,
    season_ids: list[str],
) -> int:
    """Load salary cap + player salaries for given seasons."""
    load_salary_cap(con)
    total = 0
    for sid in season_ids:
        total += load_player_salaries(con, sid)
        pause = _BREF_THROTTLE.inter_season_pause()
        if pause > 0:
            logger.info("fact_salary (%s): adaptive inter-season pause %.2fs", sid, pause)
            time.sleep(pause)
    return total


if __name__ == "__main__":  # pragma: no cover
    from src.db.schema import init_db

    logging.basicConfig(level=logging.INFO)
    con = init_db()
    load_salary_cap(con)
    load_player_salaries(con, "2023-24")
    con.close()
