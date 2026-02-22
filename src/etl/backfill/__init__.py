"""
Backfill loaders for raw CSV data.

This package provides loaders for ingesting historical NBA data from
Basketball-Reference and NBA API CSV exports into the SQLite database.

Usage:
    from src.etl.backfill import run_raw_backfill

    run_raw_backfill(con, Path("raw"))

All loaders follow a consistent pattern:
- Check if source CSV exists, skip if not
- Validate against dimension tables (seasons, teams, players)
- Transform rows using helper functions
- Upsert with validation via validate_rows()
"""

from src.etl.backfill._orchestrator import run_raw_backfill

__all__ = ["run_raw_backfill"]
