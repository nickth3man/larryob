"""Dimension loaders package."""

import sqlite3

from nba_api.stats.endpoints import commonplayerinfo
from nba_api.stats.static import players as nba_players_static
from nba_api.stats.static import teams as nba_teams_static

from .players import load_players_bio_enrichment, load_players_full, load_players_static
from .seasons import _season_id, load_seasons
from .teams import load_teams


def run_all(
    con: sqlite3.Connection,
    full_players: bool = False,
    enrich_bio: bool = False,
) -> None:
    """Seed all dimension tables.

    Intended call order when raw data is available:
        1. raw_seed.infer_season_start_range()  — infer historical season range
        2. load_seasons(con)                    — populate seasons dimension
        3. load_teams(con)                      — populate teams dimension
        4. load_players_static(con)             — populate players dimension
    """
    load_seasons(con)
    load_teams(con)
    load_players_static(con)
    if full_players:
        load_players_full(con)
    if enrich_bio:
        load_players_bio_enrichment(con, active_only=True)


__all__ = [
    "_season_id",
    "load_seasons",
    "load_teams",
    "load_players_static",
    "load_players_full",
    "load_players_bio_enrichment",
    "run_all",
    "commonplayerinfo",
    "nba_players_static",
    "nba_teams_static",
]
