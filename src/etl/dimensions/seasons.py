"""Season dimension loaders."""

import logging
import sqlite3
from datetime import UTC

from ...db.operations import upsert_rows
from ...db.tracking import already_loaded, record_run

logger = logging.getLogger(__name__)

NBA_FIRST_SEASON_START = 1946  # 1946-47 inaugural season


def _season_id(start_year: int) -> str:
    """Convert 1946 -> '1946-47', 2023 -> '2023-24'."""
    return f"{start_year}-{str(start_year + 1)[-2:]}"


def load_seasons(con: sqlite3.Connection, up_to_start_year: int = 2024) -> int:
    """Seed dim_season from 1946-47 through *up_to_start_year*."""
    loader_id = f"dimensions.load_seasons.{up_to_start_year}"
    if already_loaded(con, "dim_season", None, loader_id):
        logger.info("Skipping dim_season (already loaded)")
        return 0

    from datetime import datetime

    started_at = datetime.now(UTC).isoformat()

    rows = []
    for y in range(NBA_FIRST_SEASON_START, up_to_start_year + 1):
        rows.append(
            {
                "season_id": _season_id(y),
                "start_year": y,
                "end_year": y + 1,
            }
        )
    inserted = upsert_rows(con, "dim_season", rows)
    logger.info("dim_season: %d rows upserted.", inserted)

    record_run(con, "dim_season", None, loader_id, inserted, "ok", started_at)
    return inserted
