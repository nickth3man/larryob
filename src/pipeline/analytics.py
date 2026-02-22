"""
Analytics view execution and DataFrame export.

Handles querying DuckDB views after ingest and exporting results to CSV,
Parquet, or JSON. DuckDB connection cleanup is also centralised here.
"""

from __future__ import annotations

import logging
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING

from src.db.analytics import get_duck_con
from src.pipeline.exceptions import AnalyticsError
from src.pipeline.validation import validate_view_name

if TYPE_CHECKING:
    import duckdb

logger = logging.getLogger(__name__)


def run_analytics_view(
    view_name: str,
    limit: int,
    output_path: Path | None,
) -> None:
    """Execute an analytics view and output results.

    Args:
        view_name: Name of the DuckDB view to query.
        limit: Maximum rows to return.
        output_path: Optional path to export results.

    Raises:
        AnalyticsError: If the view name is invalid or export format unsupported.
    """
    if limit <= 0:
        raise AnalyticsError(f"analytics limit must be > 0, got {limit}")

    safe_view = validate_view_name(view_name)
    duck = get_duck_con(force_refresh=True)

    try:
        df = duck.execute(f"SELECT * FROM {safe_view} LIMIT {limit}").df()
    except Exception as exc:
        raise AnalyticsError(
            f"Failed analytics query for view={safe_view!r} limit={limit}: {exc}"
        ) from exc
    finally:
        _cleanup_duck_connection(duck)

    if output_path:
        export_dataframe(df, output_path, safe_view, limit)
        return

    logger.info("Analytics view %s returned %d rows (limit=%d)", safe_view, len(df), limit)
    if not df.empty:
        print(df.to_string(index=False))


def _cleanup_duck_connection(duck: duckdb.DuckDBPyConnection) -> None:
    """Clean up DuckDB connection and cached state.

    Args:
        duck: The DuckDB connection to close.
    """
    with suppress(Exception):
        duck.close()

    # Lazy import to avoid a module-level cycle with src.db.analytics
    from src.db import analytics as analytics_mod

    if hasattr(analytics_mod, "_local"):
        analytics_mod._local.cached_con = None
        analytics_mod._local.cached_sqlite_path = None
        analytics_mod._local.cached_duck_db_path = None


def export_dataframe(
    df,
    output_path: Path,
    view_name: str,
    limit: int,
) -> None:
    """Export DataFrame to file based on extension.

    Args:
        df: DataFrame to export.
        output_path: Target file path.
        view_name: View name for logging.
        limit: Query limit for logging.

    Raises:
        AnalyticsError: If the export format is unsupported.
    """
    output_path = output_path.expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    suffix = output_path.suffix.lower()

    exporters = {
        ".csv": lambda: df.to_csv(output_path, index=False),
        ".parquet": lambda: df.to_parquet(output_path, index=False),
        ".json": lambda: df.to_json(output_path, orient="records"),
    }

    if suffix not in exporters:
        raise AnalyticsError(
            f"Unsupported analytics output format: {output_path} "
            "(expected .csv, .parquet, or .json)"
        )

    try:
        exporters[suffix]()
    except Exception as exc:
        raise AnalyticsError(
            f"Failed exporting analytics view {view_name} to {output_path}: {exc}"
        ) from exc

    logger.info("Analytics view %s exported (%d rows) to %s", view_name, len(df), output_path)
