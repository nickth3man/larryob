"""Salary loading pipeline."""

import logging
import sqlite3
import time
from datetime import UTC
from pathlib import Path
from typing import Literal

from ...db.cache import load_cache
from ...db.operations import upsert_rows
from ...db.tracking import already_loaded, record_run
from .._salaries_fetch import fetch_team_current_contracts, fetch_team_season_salaries
from ..extract.rate_limit import _BREF_THROTTLE, BBRRateLimitExceeded
from ..validation import validate_rows
from .extractor import _season_team_map
from .transformer import _SALARY_CAP_BY_SEASON, _normalize_name

logger = logging.getLogger(__name__)

_LOADER_ID = "salaries.load_player_salaries.v2"


def load_salary_cap(con: sqlite3.Connection) -> int:
    """Seed dim_salary_cap using centralized cap data."""
    from ..dimensions import load_seasons

    max_start_year = max(int(sid.split("-")[0]) for sid in _SALARY_CAP_BY_SEASON)
    load_seasons(con, up_to_start_year=max_start_year)
    rows = [{"season_id": sid, "cap_amount": cap} for sid, cap in _SALARY_CAP_BY_SEASON.items()]
    inserted = upsert_rows(con, "dim_salary_cap", rows, conflict="REPLACE")
    logger.info("dim_salary_cap: %d rows upserted.", inserted)
    return inserted


def load_player_salaries(
    con: sqlite3.Connection,
    season_id: str,
    source: Literal["bref", "open", "auto"] = "auto",
    open_file: Path | None = None,
) -> int:
    """Load and upsert salary rows for a season."""
    loader_id = _LOADER_ID
    if already_loaded(con, "fact_salary", season_id, loader_id):
        logger.info("Skipping player salaries for %s (already loaded)", season_id)
        return 0

    from datetime import datetime

    started_at = datetime.now(UTC).isoformat()

    if source == "open":
        from src.etl.backfill._salary_history import load_salary_history

        load_salary_history(con, open_file=open_file, raw_dir=Path("raw"))
        inserted: int = con.execute(
            "SELECT COUNT(*) FROM fact_salary WHERE season_id = ?", (season_id,)
        ).fetchone()[0]
        record_run(con, "fact_salary", season_id, loader_id, inserted, "ok", started_at)
        return inserted

    if source == "auto":
        from src.etl.backfill._salary_history import load_salary_history

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

    import datetime as dt

    current_year = dt.date.today().year
    end_year = int(season_id.split("-")[0]) + 1

    from ..dimensions import load_seasons

    start_year = end_year - 1
    load_seasons(con, up_to_start_year=start_year)

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
                logger.debug("BBref team season salary: %s %d", bref_abbr, end_year)
                was_cached = load_cache(cache_key_season) is not None
                entries = [
                    {"name": e["name"], "season_id": season_id, "salary": e["salary"]}
                    for e in fetch_team_season_salaries(bref_abbr, end_year)
                ]
            else:
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
        logger.warning("fact_salary (%s): no rows to insert.", season_id)
        record_run(con, "fact_salary", season_id, loader_id, 0, "ok", started_at)
        return 0

    rows_to_insert = validate_rows("fact_salary", rows_to_insert)
    if not rows_to_insert:
        logger.warning("fact_salary (%s): all rows dropped by validation.", season_id)
        status = "rate_limited" if rate_limit_exc else "ok"
        record_run(con, "fact_salary", season_id, loader_id, 0, status, started_at)
        return 0

    from ...db.operations import transaction

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
    """Load salary cap + player salaries for given seasons."""
    load_salary_cap(con)
    total = 0
    for sid in season_ids:
        total += load_player_salaries(con, sid, source=source, open_file=open_file)
        pause = _BREF_THROTTLE.inter_season_pause()
        if pause > 0:
            logger.info("fact_salary (%s): adaptive inter-season pause %.2fs", sid, pause)
            time.sleep(pause)
    return total
