"""
Internal salary fetching functions for Basketball-Reference scraping.

These functions handle the actual HTML parsing and data extraction
from Basketball-Reference team pages.
"""

import io
import logging
import re
from typing import cast

import pandas as pd

from ..db.cache import load_cache, save_cache
from .rate_limit import BREF_BASE, fetch_html

logger = logging.getLogger(__name__)


def _parse_salary(value: object) -> int | None:
    """
    Parse a salary string like "$12,345,678" into an integer.

    Parameters:
        value (object): Raw salary value, typically a string such as "$1,234,567".

    Returns:
        int | None: Parsed salary as an integer (e.g., 1234567), or None if the input is not a string or contains no digits.
    """
    if not isinstance(value, str):
        return None
    cleaned = re.sub(r"[^\d]", "", value)
    return int(cleaned) if cleaned else None


def fetch_team_season_salaries(bref_abbr: str, end_year: int) -> list[dict]:
    """
    Fetch per-player salaries for a historical team season from Basketball-Reference.

    Parameters:
        bref_abbr (str): Team abbreviation used in BBref URLs (e.g., "LAL").
        end_year (int): Season end year (e.g., 2023 for the 2022-23 season).

    Returns:
        list[dict]: A list of records where each record has keys:
            - name (str): Player name trimmed of surrounding whitespace.
            - salary (int): Parsed salary in whole dollars.
    """
    cache_key = f"bref_season_sal_{bref_abbr}_{end_year}"
    cached = load_cache(cache_key)
    if cached is not None:
        return cached

    url = f"{BREF_BASE}/teams/{bref_abbr}/{end_year}.html"
    html = fetch_html(url)
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


def fetch_team_current_contracts(bref_abbr: str) -> list[dict]:
    """
    Fetches active multi-year player contracts for a Basketball-Reference team.

    Parses the team's contracts page and returns one entry per non-empty salary cell found, covering current and future seasons.

    Parameters:
        bref_abbr (str): Basketball-Reference team abbreviation used to build the contracts page URL.

    Returns:
        list[dict]: List of records where each record contains:
            - name (str): Player name.
            - season_id (str): Season identifier extracted from the salary column header.
            - salary (int): Parsed salary for that season.
    """
    cache_key = f"bref_contracts_{bref_abbr}"
    cached = load_cache(cache_key)
    if cached is not None:
        return cached

    url = f"{BREF_BASE}/contracts/{bref_abbr}.html"
    html = fetch_html(url)
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
    df.columns = pd.Index(
        [b if str(a).startswith("Unnamed") else f"{a}__{b}" for a, b in df.columns]
    )
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
