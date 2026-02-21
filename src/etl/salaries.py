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
import re
import sqlite3
import time
import unicodedata
from datetime import UTC
from typing import cast

import pandas as pd
import requests

from .utils import already_loaded, load_cache, record_run, save_cache, upsert_rows
from .validate import validate_rows

logger = logging.getLogger(__name__)

# NBA team abbreviation → Basketball-Reference abbreviation
_ABBR_TO_BREF: dict[str, str] = {
    "ATL": "ATL",
    "BKN": "BRK",
    "BOS": "BOS",
    "CHA": "CHO",
    "CHI": "CHI",
    "CLE": "CLE",
    "DAL": "DAL",
    "DEN": "DEN",
    "DET": "DET",
    "GSW": "GSW",
    "HOU": "HOU",
    "IND": "IND",
    "LAC": "LAC",
    "LAL": "LAL",
    "MEM": "MEM",
    "MIA": "MIA",
    "MIL": "MIL",
    "MIN": "MIN",
    "NOP": "NOP",
    "NYK": "NYK",
    "OKC": "OKC",
    "ORL": "ORL",
    "PHI": "PHI",
    "PHX": "PHO",
    "POR": "POR",
    "SAC": "SAC",
    "SAS": "SAS",
    "TOR": "TOR",
    "UTA": "UTA",
    "WAS": "WAS",
}

_BREF_BASE = "https://www.basketball-reference.com"
_HEADERS = {"User-Agent": "Mozilla/5.0 (personal research project, non-commercial)"}
_REQUEST_DELAY = 4.0  # seconds between requests (be polite)

# Historical NBA salary cap by season (USD). Source: Basketball-Reference, RealGM, CBA FAQ.
_SALARY_CAP_BY_SEASON: dict[str, int] = {
    "1984-85": 3_600_000,
    "1985-86": 4_233_000,
    "1986-87": 4_945_000,
    "1987-88": 6_164_000,
    "1988-89": 7_232_000,
    "1989-90": 9_802_000,
    "1990-91": 11_871_000,
    "1991-92": 12_500_000,
    "1992-93": 14_000_000,
    "1993-94": 15_175_000,
    "1994-95": 15_964_000,
    "1995-96": 23_000_000,
    "1996-97": 24_363_000,
    "1997-98": 26_900_000,
    "1998-99": 30_000_000,
    "1999-00": 34_000_000,
    "2000-01": 35_500_000,
    "2001-02": 42_500_000,
    "2002-03": 40_271_000,
    "2003-04": 43_840_000,
    "2004-05": 43_870_000,
    "2005-06": 49_500_000,
    "2006-07": 53_135_000,
    "2007-08": 55_630_000,
    "2008-09": 58_680_000,
    "2009-10": 57_700_000,
    "2010-11": 58_044_000,
    "2011-12": 58_044_000,
    "2012-13": 58_044_000,
    "2013-14": 58_679_000,
    "2014-15": 63_065_000,
    "2015-16": 70_000_000,
    "2016-17": 94_143_000,
    "2017-18": 99_093_000,
    "2018-19": 101_869_000,
    "2019-20": 109_140_000,
    "2020-21": 109_140_000,
    "2021-22": 112_414_000,
    "2022-23": 123_655_000,
    "2023-24": 136_021_000,
    "2024-25": 140_588_000,
    "2025-26": 154_647_000,
}


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
    """Lowercase, strip accents, strip non-alpha characters for fuzzy matching."""
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_str = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"[^a-z ]", "", ascii_str.lower()).strip()


def _parse_salary(value: object) -> int | None:
    """Convert '$12,345,678' → 12345678, or None if unparseable."""
    if not isinstance(value, str):
        return None
    cleaned = re.sub(r"[^\d]", "", value)
    return int(cleaned) if cleaned else None


def _get_html(url: str, max_retries: int = 3) -> str | None:
    """
    Fetch URL with exponential backoff on 429 Too Many Requests.
    Returns response text or None on persistent error.
    """
    delay = 15.0
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=20)
            if resp.status_code == 429:
                try:
                    retry_after = int(resp.headers.get("Retry-After", delay))
                except (ValueError, TypeError):
                    retry_after = int(delay)
                logger.info("BBref rate-limited (%s); waiting %ds…", url, retry_after)
                time.sleep(retry_after)
                delay *= 2
                continue
            resp.raise_for_status()
            resp.encoding = "utf-8"
            return resp.text
        except requests.RequestException as exc:
            if attempt < max_retries - 1:
                logger.debug("BBref fetch error (%s), retry %d: %s", url, attempt + 1, exc)
                time.sleep(delay)
                delay *= 2
            else:
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
    loader_id = "salaries.load_player_salaries"
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

    cur = con.execute("SELECT team_id, abbreviation FROM dim_team")
    team_abbr_to_id: dict[str, str] = {row[1]: row[0] for row in cur.fetchall()}

    rows_to_insert: list[dict] = []
    unmatched_names: set[str] = set()

    for nba_abbr, bref_abbr in _ABBR_TO_BREF.items():
        team_id = team_abbr_to_id.get(nba_abbr)
        if not team_id:
            logger.debug("Team %s not found in dim_team; skipping.", nba_abbr)
            continue

        cache_key_season = f"bref_season_sal_{bref_abbr}_{end_year}"
        cache_key_contracts = f"bref_contracts_{bref_abbr}"
        was_cached = False

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

        if not was_cached:
            time.sleep(_REQUEST_DELAY)

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

    if unmatched_names:
        logger.debug(
            "fact_salary (%s): %d unmatched player name(s): %s",
            season_id,
            len(unmatched_names),
            sorted(unmatched_names)[:10],
        )

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
        record_run(con, "fact_salary", season_id, loader_id, 0, "ok", started_at)
        return 0

    from .utils import transaction
    with transaction(con):
        inserted = upsert_rows(con, "fact_salary", rows_to_insert, conflict="REPLACE", autocommit=False)
    logger.info("fact_salary (%s): %d rows upserted.", season_id, inserted)

    record_run(con, "fact_salary", season_id, loader_id, inserted, "ok", started_at)
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
        time.sleep(2.0)
    return total


if __name__ == "__main__":  # pragma: no cover
    from src.db.schema import init_db

    logging.basicConfig(level=logging.INFO)
    con = init_db()
    load_salary_cap(con)
    load_player_salaries(con, "2023-24")
    con.close()
