"""
DuckDB analytics layer.

Provides a single `get_duck_con()` factory that:
  1. Connects to an in-memory (or persistent) DuckDB instance.
  2. Installs & loads the native sqlite_scanner extension.
  3. Attaches the SQLite database as the 'nba' schema.
  4. Creates all analytical VIEWs on top of the attached tables.

Usage
-----
    from src.db.olap import get_duck_con

    con = get_duck_con()
    df = con.execute("SELECT * FROM vw_player_season_totals LIMIT 10").df()

SQL Views
---------
View definitions are stored in separate SQL files under src/db/views/:
  - player_views.sql: Player-related analytics (shooting, per-36, usage, etc.)
  - team_views.sql: Team-related analytics (standings, pace, ratings, etc.)
  - other_views.sql: Play-by-play, salary, and draft views
"""

import logging
import re
import threading
from pathlib import Path

import duckdb

from .schema import DB_PATH

logger = logging.getLogger(__name__)

# Directory containing SQL view definitions
VIEWS_DIR = Path(__file__).parent / "views"


def _load_views_from_sql_file(sql_path: Path) -> list[tuple[str, str]]:
    """
    Extracts CREATE OR REPLACE VIEW definitions from a SQL file and returns them as (view_name, view_sql) pairs.

    Parses the file at sql_path for statements of the form
        CREATE OR REPLACE VIEW <name> AS <select statement>;
    and for each match returns the view name and the SQL that follows `AS` with surrounding whitespace trimmed and trailing semicolons or trailing comments removed.

    Parameters:
        sql_path (Path): Path to the SQL file to parse.

    Returns:
        list[tuple[str, str]]: A list of (view_name, view_sql) tuples where `view_sql` is the body of the view definition (without a trailing semicolon).
    """
    views: list[tuple[str, str]] = []

    with open(sql_path, encoding="utf-8") as f:
        content = f.read()

    # Split content by CREATE OR REPLACE VIEW statements
    # This pattern captures the view name and everything after AS
    pattern = r"CREATE\s+OR\s+REPLACE\s+VIEW\s+(\w+)\s+AS\s+"

    # Find all view declarations
    matches = list(re.finditer(pattern, content, re.IGNORECASE))

    for i, match in enumerate(matches):
        view_name = match.group(1)
        start_pos = match.end()  # Position after "AS "

        # Find where this view's SQL ends (at next CREATE or end of file)
        if i + 1 < len(matches):
            end_pos = matches[i + 1].start()
        else:
            end_pos = len(content)

        # Extract the SQL and clean it up
        view_sql = content[start_pos:end_pos].strip()

        # Remove trailing semicolons and whitespace but don't split on semicolons inside the SQL body
        view_sql = view_sql.rstrip().rstrip(";").strip()

        views.append((view_name, view_sql))

    return views


def _load_all_views() -> list[tuple[str, str]]:
    """
    Load all SQL view definitions from the views directory.

    Files are processed in sorted filename order; each SQL file may contain one or more CREATE OR REPLACE VIEW statements.

    Returns:
        list[tuple[str, str]]: Combined list of (view_name, sql_statement) pairs extracted from all SQL files in VIEWS_DIR.
    """
    all_views: list[tuple[str, str]] = []

    # Load views in a consistent order
    sql_files = sorted(VIEWS_DIR.glob("*.sql"))

    for sql_file in sql_files:
        logger.debug("Loading views from %s", sql_file.name)
        views = _load_views_from_sql_file(sql_file)
        all_views.extend(views)
        logger.debug("Loaded %d views from %s", len(views), sql_file.name)

    return all_views


# Thread-local storage for cached connections
_local = threading.local()


def get_duck_con(
    sqlite_path: Path = DB_PATH,
    duck_db_path: str = ":memory:",
    *,
    force_refresh: bool = False,
) -> duckdb.DuckDBPyConnection:
    """
    Create an open DuckDB connection with the SQLite database attached as schema 'nba' and with analytical views installed.

    This function uses a per-thread cache to reuse an existing connection when sqlite_path and duck_db_path match the cached values; set force_refresh to True to recreate the connection and reinstall views.

    Parameters:
        sqlite_path (Path): Path to the SQLite `nba_raw_data.db` file to attach as schema `nba`.
        duck_db_path (str): DuckDB store location, `':memory:'` for in-memory or a file path for persistent storage.
        force_refresh (bool): If True, close any cached connection and recreate it along with all views.

    Returns:
        duckdb.DuckDBPyConnection: An open DuckDB connection with the specified DuckDB store, the SQLite database attached as schema `nba`, and all analytical views created.
    """
    if not hasattr(_local, "cached_con"):
        _local.cached_con = None
        _local.cached_sqlite_path = None
        _local.cached_duck_db_path = None

    cache_match = (
        _local.cached_sqlite_path == str(sqlite_path) and _local.cached_duck_db_path == duck_db_path
    )
    if _local.cached_con is not None and not force_refresh and cache_match:
        try:
            _local.cached_con.execute("SELECT 1")
            return _local.cached_con
        except Exception:
            _local.cached_con = None

    if _local.cached_con is not None:
        try:
            _local.cached_con.close()
        except Exception:
            pass

    con = duckdb.connect(duck_db_path)

    # Install & load the sqlite extension (bundled with DuckDB >= 0.8)
    con.execute("INSTALL sqlite;")
    con.execute("LOAD sqlite;")

    # Attach the SQLite database — its tables become accessible via 'nba.'
    con.execute(f"ATTACH '{sqlite_path}' AS nba (TYPE sqlite, READ_ONLY);")
    logger.info("Attached SQLite db: %s", sqlite_path)

    # Load and create all analytical views from SQL files
    views = _load_all_views()
    for name, sql in views:
        con.execute(f"CREATE OR REPLACE VIEW {name} AS {sql}")
        logger.debug("View created: %s", name)

    logger.info("DuckDB analytics layer ready (%d views).", len(views))
    _local.cached_con = con
    _local.cached_sqlite_path = str(sqlite_path)
    _local.cached_duck_db_path = duck_db_path
    return con


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    duck = get_duck_con()
    logger.info("Available views:")
    views = duck.execute("SHOW TABLES").fetchall()
    for v in views:
        logger.info(" - %s", v[0])
    duck.close()
    _local.cached_con = None
    _local.cached_sqlite_path = None
    _local.cached_duck_db_path = None
