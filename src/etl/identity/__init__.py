"""ETL identity package for crosswalk resolution."""

from src.etl.identity.resolver import resolve_or_create_player, resolve_or_create_team

__all__ = ["resolve_or_create_player", "resolve_or_create_team"]
