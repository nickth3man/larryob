"""
Analytics view execution and DataFrame export.

Handles querying DuckDB views after ingest and exporting results to CSV,
Parquet, or JSON. DuckDB connection cleanup is also centralised here.

Design Decisions
----------------
- Uses parameterized queries for the LIMIT clause (DuckDB doesn't support
  parameterized table names, so view name validation is critical)
- Connection cleanup clears thread-local cache to prevent stale state
- Export formats are extensible via the EXPORTERS registry
- Output paths are resolved (expanduser + resolve) for reliable logging

Usage
-----
    # Print to stdout
    run_analytics_view("vw_player_totals", limit=20, output_path=None)

    # Export to file
    run_analytics_view("vw_player_totals", limit=1000, output_path=Path("out.csv"))
"""

from __future__ import annotations

import logging
import sys
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING

from src.db.analytics import get_duck_con
from src.pipeline.exceptions import AnalyticsError
from src.pipeline.validation import SUPPORTED_ANALYTICS_EXTENSIONS, validate_view_name

if TYPE_CHECKING:
    import duckdb
    import pandas as pd

logger = logging.getLogger(__name__)

#: Registry of export functions keyed by file extension
ExportFn = Callable[["pd.DataFrame", Path], None]
EXPORTERS: dict[str, ExportFn] = {}


def _coerce_stdout_text(text: str, *, encoding: str | None) -> str:
    """Return text that can be encoded by the current stdout encoding."""
    target_encoding = encoding or "utf-8"
    try:
        text.encode(target_encoding)
    except UnicodeEncodeError:
        return text.encode(target_encoding, errors="replace").decode(target_encoding)
    return text


def _register_exporter(extension: str) -> Callable[[ExportFn], ExportFn]:
    """Decorator to register an export function for a file extension.

    Args:
        extension: File extension (including dot, e.g., ".csv").

    Returns:
        Decorator function.
    """

    def decorator(fn: ExportFn) -> ExportFn:
        EXPORTERS[extension.lower()] = fn
        return fn

    return decorator


@_register_exporter(".csv")
def _export_csv(df: pd.DataFrame, path: Path) -> None:
    """Export DataFrame to CSV format."""
    df.to_csv(path, index=False)


@_register_exporter(".parquet")
def _export_parquet(df: pd.DataFrame, path: Path) -> None:
    """Export DataFrame to Parquet format."""
    df.to_parquet(path, index=False)


@_register_exporter(".json")
def _export_json(df: pd.DataFrame, path: Path) -> None:
    """Export DataFrame to JSON (records orient) format."""
    df.to_json(path, orient="records")


def run_analytics_view(
    view_name: str,
    limit: int,
    output_path: Path | None,
) -> None:
    """Execute an analytics view and output results.

    Args:
        view_name: Name of the DuckDB view to query.
        limit: Maximum rows to return (must be > 0).
        output_path: Optional path to export results. If None, prints to stdout.

    Raises:
        AnalyticsError: If the view name is invalid, limit is invalid,
            query fails, or export format is unsupported.

    Examples:
        >>> run_analytics_view("vw_player_totals", 20, None)  # Print to stdout
        >>> run_analytics_view("vw_player_totals", 1000, Path("out.csv"))
    """
    if limit <= 0:
        raise AnalyticsError(
            f"analytics limit must be > 0, got {limit}",
            view_name=view_name,
        )

    safe_view = validate_view_name(view_name)
    duck = get_duck_con(force_refresh=True)

    try:
        # Use parameterized query for LIMIT; view name is validated via regex
        df = duck.execute(
            f"SELECT * FROM {safe_view} LIMIT ?",
            [limit],
        ).df()
    except Exception as exc:
        raise AnalyticsError(
            f"Failed analytics query for view={safe_view!r} limit={limit}: {exc}",
            view_name=safe_view,
        ) from exc
    finally:
        _cleanup_duck_connection(duck)

    if output_path:
        export_dataframe(df, output_path, safe_view, limit)
        return

    logger.info("Analytics view %s returned %d rows (limit=%d)", safe_view, len(df), limit)
    if not df.empty:
        rendered = _coerce_stdout_text(df.to_string(index=False), encoding=sys.stdout.encoding)
        print(rendered)


def _cleanup_duck_connection(duck: duckdb.DuckDBPyConnection) -> None:
    """Clean up DuckDB connection and cached state.

    This function clears the thread-local connection cache to prevent
    stale connections from being reused across pipeline runs.

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
    df: pd.DataFrame,
    output_path: Path,
    view_name: str,
    limit: int,
) -> None:
    """Export DataFrame to file based on extension.

    Args:
        df: DataFrame to export.
        output_path: Target file path (supports .csv, .parquet, .json).
        view_name: View name for logging context.
        limit: Query limit for logging context.

    Raises:
        AnalyticsError: If the export format is unsupported or export fails.

    Examples:
        >>> export_dataframe(df, Path("output.csv"), "vw_totals", 100)
    """
    output_path = output_path.expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    suffix = output_path.suffix.lower()

    if suffix not in EXPORTERS:
        raise AnalyticsError(
            f"Unsupported analytics output format: {output_path} "
            f"(expected one of {', '.join(sorted(SUPPORTED_ANALYTICS_EXTENSIONS))})",
            view_name=view_name,
            output_path=str(output_path),
        )

    try:
        EXPORTERS[suffix](df, output_path)
    except Exception as exc:
        raise AnalyticsError(
            f"Failed exporting analytics view {view_name} to {output_path}: {exc}",
            view_name=view_name,
            output_path=str(output_path),
        ) from exc

    logger.info("Analytics view %s exported (%d rows) to %s", view_name, len(df), output_path)
