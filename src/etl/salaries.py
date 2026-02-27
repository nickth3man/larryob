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
from pathlib import Path
from typing import Literal

from ..db.cache import load_cache
from ..db.operations import upsert_rows
from ..db.tracking import already_loaded, record_run
from . import _salaries_helpers
from ._salaries_fetch import fetch_team_current_contracts, fetch_team_season_salaries
from ._salaries_helpers import _normalize_name, load_salary_cap
from .config import nba_abbr_to_bref
from .rate_limit import _BREF_THROTTLE, BBRRateLimitExceeded
from .validation import validate_rows

logger = logging.getLogger(__name__)

_SALARY_CAP_BY_SEASON = _salaries_helpers._SALARY_CAP_BY_SEASON


# Helper function for abbreviation conversion (uses centralized config)
def _abbr_to_bref(abbr: str) -> str:
    """
    Map an NBA team abbreviation to the Basketball-Reference equivalent.

    Returns:
        The Basketball-Reference abbreviation for `abbr`, or `abbr` unchanged if no mapping is available.
    """
    result = nba_abbr_to_bref(abbr)
    return result if result is not None else abbr


_LOADER_ID = "salaries.load_player_salaries.v2"


def _season_team_map(
    con: sqlite3.Connection,
    season_id: str,
) -> tuple[dict[str, str], str]:
    """
    Map Basketball-Reference team abbreviations for a given season to internal team IDs.

    Looks up season-specific mappings from fact_team_season joined to dim_team_history and, if none are found, falls back to current abbreviations from dim_team.

    Parameters:
        season_id (str): Season identifier in the form "YYYY-YY" (e.g., "2023-24"); the start year is used to resolve historical mappings.

    Returns:
        tuple[dict[str, str], str]: A pair where the first element is a dict mapping Basketball-Reference abbreviations (uppercased strings) to team_id strings, and the second element is the source label: either "fact_team_season" when historical season mappings were used or "dim_team" when the current-team fallback was used.
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
    source: Literal["bref", "open", "auto"] = "auto",
    open_file: Path | None = None,
) -> int:
    """
    Load and upsert player salary records for a given NBA season into the fact_salary table.

    Builds salary rows by fetching Basketball-Reference team salary or contracts pages for the season, matching scraped
    player names to dim_player by a normalized name, validating the resulting rows, and upserting them into fact_salary.
    Records run metadata (status and counts) and may short-circuit returning 0 when the season is already loaded, when
    no valid rows are found, or when a rate limit prevents fetching data.

    Parameters:
        con (sqlite3.Connection): Database connection used for lookups, validation, and upsert.
        season_id (str): Season identifier like "2023-24" or "2025-26".
        source (Literal["bref", "open", "auto"]): Data source strategy.
            - "bref": scrape Basketball-Reference only (existing behaviour).
            - "open": load from open-source CSV only (via load_salary_history).
            - "auto": try open-source CSV first; fall back to bref if 0 rows returned for
              this season.
        open_file (Path | None): Explicit CSV path forwarded to load_salary_history when
            source is "open" or "auto". Defaults to raw/open_salaries.csv when None.

    Returns:
        int: Number of rows inserted or replaced into fact_salary; returns 0 if nothing was inserted.
    """
    loader_id = _LOADER_ID
    if already_loaded(con, "fact_salary", season_id, loader_id):
        logger.info("Skipping player salaries for %s (already loaded)", season_id)
        return 0

    from datetime import datetime

    started_at = datetime.now(UTC).isoformat()

    # ------------------------------------------------------------------ #
    # "open" source: delegate entirely to load_salary_history             #
    # ------------------------------------------------------------------ #
    if source == "open":
        # Local import keeps module dependency direction explicit.
        from src.etl.backfill._salary_history import load_salary_history

        load_salary_history(con, open_file=open_file, raw_dir=Path("raw"))
        # Return season-scoped count; load_salary_history processes the whole CSV
        # across all seasons, so we query only the requested season for accuracy.
        inserted: int = con.execute(
            "SELECT COUNT(*) FROM fact_salary WHERE season_id = ?", (season_id,)
        ).fetchone()[0]
        record_run(con, "fact_salary", season_id, loader_id, inserted, "ok", started_at)
        return inserted

    # ------------------------------------------------------------------ #
    # "auto" source: try open first; fall back to bref when 0 new rows    #
    # ------------------------------------------------------------------ #
    if source == "auto":
        from src.etl.backfill._salary_history import load_salary_history  # local import

        # Snapshot row count BEFORE open-source load so stale/partial rows from a
        # prior bref run don't falsely indicate the season is covered.
        pre_count: int = con.execute(
            "SELECT COUNT(*) FROM fact_salary WHERE season_id = ?", (season_id,)
        ).fetchone()[0]
        load_salary_history(con, open_file=open_file, raw_dir=Path("raw"))

        post_count: int = con.execute(
            "SELECT COUNT(*) FROM fact_salary WHERE season_id = ?", (season_id,)
        ).fetchone()[0]
        delta = post_count - pre_count

        if delta > 0:
            logger.info(
                "fact_salary (%s): open-source load supplied %d new rows — skipping bref scrape",
                season_id,
                delta,
            )
            record_run(con, "fact_salary", season_id, loader_id, post_count, "ok", started_at)
            return post_count

        logger.info(
            "fact_salary (%s): open-source data has 0 new rows for this season — falling back to bref",
            season_id,
        )
        # Fall through to bref scraping below.

    # ------------------------------------------------------------------ #
    # "bref" (and fallback from "auto"): scrape Basketball-Reference      #
    # ------------------------------------------------------------------ #
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
    source: Literal["bref", "open", "auto"] = "auto",
    open_file: Path | None = None,
) -> int:
    """Load salary cap + player salaries for given seasons.

    Parameters:
        con: Database connection.
        season_ids: List of season identifiers to process (e.g. ["2022-23", "2023-24"]).
        source: Data source strategy forwarded to load_player_salaries for each season.
        open_file: Explicit CSV path forwarded to load_player_salaries when source is
            "open" or "auto".
    """
    load_salary_cap(con)
    total = 0
    for sid in season_ids:
        total += load_player_salaries(con, sid, source=source, open_file=open_file)
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
