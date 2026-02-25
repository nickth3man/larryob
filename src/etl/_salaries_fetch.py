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
    """Convert '$12,345,678' → 12345678, or None if unparseable."""
    if not isinstance(value, str):
        return None
    cleaned = re.sub(r"[^\d]", "", value)
    return int(cleaned) if cleaned else None


def fetch_team_season_salaries(bref_abbr: str, end_year: int) -> list[dict]:
    """
    Scrape per-player salaries for a historical season from the BBref team
    season page.  BBref embeds the salary table inside an HTML comment.

    Returns list of dicts: {name, salary}.
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
    Scrape multi-year contracts from the BBref team contract page.
    Returns list of dicts: {name, season_id, salary}.
    Used for current/future seasons (the page only shows active contracts).
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
