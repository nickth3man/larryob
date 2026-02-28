"""Compatibility wrapper for bulk play-by-play loading.

The implementation lives in ``src.etl.load.bulk``.
"""

from src.etl.load.bulk import load_bulk_pbp, load_bulk_pbp_season

__all__ = ["load_bulk_pbp", "load_bulk_pbp_season"]
