"""
ETL: dim_salary_cap, fact_salary.

Strategy
--------
* dim_salary_cap : Hardcoded historical cap amounts (1984-85 → present).
* fact_salary    : Scraped from Basketball-Reference team contract pages
  (non-commercial, personal project use).  One GET per team per call, cached
  as JSON so subsequent runs are instant.
"""

import io
import logging
import os
import re
import sqlite3
import time
from datetime import UTC
from typing import cast

import pandas as pd
import requests

from .config import get_all_salary_caps, nba_abbr_to_bref
from .helpers import _norm_name
from .utils import already_loaded, load_cache, record_run, save_cache, upsert_rows
from .validate import validate_rows

logger = logging.getLogger(__name__)

# Use centralized config for salary cap data
_SALARY_CAP_BY_SEASON = get_all_salary_caps()

# Helper function for abbreviation conversion (uses centralized config)
def _abbr_to_bref(abbr: str) -> str:
    """Convert NBA abbreviation to Basketball-Reference abbreviation."""
    result = nba_abbr_to_bref(abbr)
    return result if result is not None else abbr

_BREF_BASE = "https://www.basketball-reference.com"
_HEADERS = {"User-Agent": "Mozilla/5.0 (personal research project, non-commercial)"}
_LOADER_ID = "salaries.load_player_salaries.v2"


class BBRRateLimitExceeded(RuntimeError):
    """Raised when Basketball-Reference asks for an excessive Retry-After delay."""

    def __init__(self, url: str, retry_after: int, max_allowed: int) -> None:
        self.url = url
        self.retry_after = retry_after
        self.max_allowed = max_allowed
        super().__init__(
            f"BBref rate limit exceeded: url={url} retry_after={retry_after}s max_allowed={max_allowed}s"
        )


def _bref_delay_seconds() -> float:
    return float(os.getenv("LARRYOB_BREF_DELAY_SECONDS", "1.5"))


def _bref_max_retries() -> int:
    return int(os.getenv("LARRYOB_BREF_MAX_RETRIES", "3"))


def _bref_max_retry_after_seconds() -> int:
    return int(os.getenv("LARRYOB_BREF_MAX_RETRY_AFTER_SECONDS", "300"))


class _AdaptiveBRefThrottle:
    """
    Adaptive, process-wide request throttle for Basketball-Reference.

    - Starts cautiously.
    - Backs off aggressively on 429 / transient failures.
    - Slowly ramps up after sustained success.
    """

    def __init__(self) -> None:
        self.min_delay = 0.4
        self.max_delay = 30.0
        self.delay = max(self.min_delay, _bref_delay_seconds())
        self.next_allowed_at = 0.0
        self.success_streak = 0
        self.rate_limit_streak = 0

    def _sleep_until_allowed(self) -> None:
        now = time.monotonic()
        if now < self.next_allowed_at:
            time.sleep(self.next_allowed_at - now)

    def before_request(self) -> None:
        self._sleep_until_allowed()

    def on_success(self) -> None:
        self.success_streak += 1
        self.rate_limit_streak = 0
        if self.success_streak >= 3:
            self.delay = max(self.min_delay, self.delay * 0.9)
        self.next_allowed_at = time.monotonic() + self.delay

    def on_transient_error(self) -> None:
        self.success_streak = 0
        self.delay = min(self.max_delay, max(self.delay * 1.4, self.delay + 0.5))
        self.next_allowed_at = time.monotonic() + self.delay

    def on_rate_limit(self, retry_after: int | None) -> int:
        self.success_streak = 0
        self.rate_limit_streak += 1
        requested_wait = retry_after if retry_after is not None and retry_after > 0 else int(self.delay * 2)
        wait = int(max(self.min_delay, min(self.max_delay, float(requested_wait))))
        self.delay = min(self.max_delay, max(self.delay * 1.8, float(wait)))
        self.next_allowed_at = time.monotonic() + wait
        return wait

    def inter_season_pause(self) -> float:
        if self.rate_limit_streak == 0:
            return 0.0
        return min(5.0, self.delay)


_BREF_THROTTLE = _AdaptiveBRefThrottle()


def load_salary_cap(con: sqlite3.Connection) -> int:
    """Seed dim_salary_cap from hardcoded historical values."""
    # Ensure dim_season has the seasons we need (FK constraint)
    from .dimensions import load_seasons
    max_start_year = max(int(sid.split("-")[0]) for sid in _SALARY_CAP_BY_SEASON)
    load_seasons(con, up_to_start_year=max_start_year)
    rows = [
        {"season_id": sid, "cap_amount": cap}
        for sid, cap in _SALARY_CAP_BY_SEASON.items()
    ]
    inserted = upsert_rows(con, "dim_salary_cap", rows, conflict="REPLACE")
    logger.info("dim_salary_cap: %d rows upserted.", inserted)
    return inserted


def _normalize_name(name: str) -> str:
    """Compatibility wrapper around shared helper normalization."""
    return _norm_name(name, strip_non_alpha=True)


def _parse_salary(value: object) -> int | None:
    """Convert '$12,345,678' → 12345678, or None if unparseable."""
    if not isinstance(value, str):
        return None
    cleaned = re.sub(r"[^\d]", "", value)
    return int(cleaned) if cleaned else None


def _get_html(url: str, max_retries: int | None = None) -> str | None:
    """
    Fetch URL with exponential backoff on 429 Too Many Requests.
    Returns response text or None on persistent error.
    """
    retries = max_retries if max_retries is not None else _bref_max_retries()
    max_retry_after = _bref_max_retry_after_seconds()
    for attempt in range(retries):
        try:
            _BREF_THROTTLE.before_request()
            resp = requests.get(url, headers=_HEADERS, timeout=20)
            if resp.status_code == 429:
                try:
                    retry_after = int(resp.headers.get("Retry-After", 0))
                except (ValueError, TypeError):
                    retry_after = None
                if retry_after is not None and retry_after > max_retry_after:
                    raise BBRRateLimitExceeded(url, retry_after, max_retry_after)
                wait = _BREF_THROTTLE.on_rate_limit(retry_after)
                logger.warning(
                    "BBref rate-limited (%s): attempt=%d/%d retry_after=%s adaptive_wait=%ds next_delay=%.2fs",
                    url,
                    attempt + 1,
                    retries,
                    retry_after,
                    wait,
                    _BREF_THROTTLE.delay,
                )
                continue
            # Historical team abbreviations often 404; treat as terminal miss.
            if resp.status_code == 404:
                _BREF_THROTTLE.on_success()
                logger.debug("BBref page not found (404): %s", url)
                return None
            # Do not retry other non-rate-limited client errors.
            if 400 <= resp.status_code < 500:
                _BREF_THROTTLE.on_transient_error()
                logger.debug("BBref client error %s for %s; skipping.", resp.status_code, url)
                return None
            resp.raise_for_status()
            resp.encoding = "utf-8"
            _BREF_THROTTLE.on_success()
            return resp.text
        except requests.RequestException as exc:
            if attempt < retries - 1:
                _BREF_THROTTLE.on_transient_error()
                logger.debug("BBref fetch error (%s), retry %d: %s", url, attempt + 1, exc)
            else:
                _BREF_THROTTLE.on_transient_error()
                logger.warning("BBref fetch failed (%s): %s", url, exc)
    return None


def _fetch_team_season_salaries(bref_abbr: str, end_year: int) -> list[dict]:
    """
    Scrape per-player salaries for a historical season from the BBref team
    season page.  BBref embeds the salary table inside an HTML comment.

    Returns list of dicts: {name, salary}.
    """
    cache_key = f"bref_season_sal_{bref_abbr}_{end_year}"
    cached = load_cache(cache_key)
    if cached is not None:
        return cached

    url = f"{_BREF_BASE}/teams/{bref_abbr}/{end_year}.html"
    html = _get_html(url)
    if not html:
        return []

    # Salary table is in an HTML comment block
    comment_blocks = re.findall(r"<!--(.*?)-->", html, re.DOTALL)
    rows: list[dict] = []
    for block in comment_blocks:
        if "salary" not in block.lower() or "<table" not in block:
            continue
        try:
            tables = pd.read_html(io.StringIO(block), flavor="lxml")
        except Exception as exc:
            logger.debug("BBref HTML table parse failed (block): %s", exc)
            continue
        if not tables:
            continue
        df = pd.DataFrame(tables[0])
        # Expected columns: Rk | player_name | Salary
        name_col = next((c for c in df.columns if str(c).lower() in ("player", "unnamed: 1")), None)
        sal_col = next((c for c in df.columns if str(c).lower() == "salary"), None)
        if name_col is None or sal_col is None:
            continue
        for _, row in df.iterrows():
            sal = _parse_salary(row.get(sal_col))
            if sal is not None:
                rows.append({"name": str(row[name_col]).strip(), "salary": sal})
        break  # only need the first matching table

    save_cache(cache_key, rows)
    return rows


def _fetch_team_current_contracts(bref_abbr: str) -> list[dict]:
    """
    Scrape multi-year contracts from the BBref team contract page.
    Returns list of dicts: {name, season_id, salary}.
    Used for current/future seasons (the page only shows active contracts).
    """
    cache_key = f"bref_contracts_{bref_abbr}"
    cached = load_cache(cache_key)
    if cached is not None:
        return cached

    url = f"{_BREF_BASE}/contracts/{bref_abbr}.html"
    html = _get_html(url)
    if not html:
        return []

    try:
        tables = pd.read_html(io.StringIO(html), flavor="lxml")
    except Exception as exc:
        logger.warning("BBref HTML parse failed (%s): %s", bref_abbr, exc)
        return []

    if not tables:
        return []

    df = pd.DataFrame(tables[0])
    df.columns = pd.Index([
        b if str(a).startswith("Unnamed") else f"{a}__{b}"
        for a, b in df.columns
    ])
    df = cast(pd.DataFrame, df[df["Player"] != "Player"]).copy()
    df = cast(pd.DataFrame, df[df["Player"].notna()]).copy()

    salary_cols = [c for c in df.columns if c.startswith("Salary__")]
    rows: list[dict] = []
    for _, row in df.iterrows():
        player_name = str(row["Player"]).strip()
        for col in salary_cols:
            season_id = col.split("__", 1)[1]
            sal = _parse_salary(row.get(col))
            if sal is not None:
                rows.append({"name": player_name, "season_id": season_id, "salary": sal})

    save_cache(cache_key, rows)
    return rows


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
    player_index: dict[str, str] = {
        _normalize_name(row[1]): row[0] for row in cur.fetchall()
    }

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
                    for e in _fetch_team_season_salaries(bref_abbr, end_year)
                ]
            else:
                # Current or future season: team contract page
                logger.debug("BBref team contracts: %s", bref_abbr)
                was_cached = load_cache(cache_key_contracts) is not None
                entries = [
                    e for e in _fetch_team_current_contracts(bref_abbr)
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
            rows_to_insert.append({
                "player_id": player_id,
                "team_id": team_id,
                "season_id": season_id,
                "salary": entry["salary"],
            })

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

    from .utils import transaction
    with transaction(con):
        inserted = upsert_rows(con, "fact_salary", rows_to_insert, conflict="REPLACE", autocommit=False)
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
