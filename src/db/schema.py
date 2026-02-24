"""
SQLite schema definitions.

All fact tables use NULL (not 0) for stats that were not officially tracked
in early NBA eras (e.g., blocks/steals pre-1973-74, 3-pointers pre-1979-80).
Running this module is idempotent — it is safe to call on an existing db.

DDL statements are loaded from separate SQL files in the schema/ subdirectory:
- tables.sql: CREATE TABLE statements
- indexes.sql: CREATE INDEX statements
- migrations.sql: ALTER TABLE statements for schema evolution
- rollback.sql: Statements to reverse migrations
"""

import logging
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent.parent / "nba_raw_data.db"
SCHEMA_DIR = Path(__file__).parent / "schema"


def _load_sql_file(filename: str) -> list[str]:
    """Load SQL statements from a file, splitting on semicolons.

    Returns a list of individual SQL statements, with empty ones and
    comment-only statements filtered out.
    """
    sql_path = SCHEMA_DIR / filename
    if not sql_path.exists():
        logger.warning("SQL file not found: %s", sql_path)
        return []

    content = sql_path.read_text(encoding="utf-8")

    # Process line by line to handle comments correctly
    lines = content.splitlines()
    cleaned_lines = []
    for line in lines:
        # Remove inline comments but keep the line
        if "--" in line:
            line = line[: line.index("--")]
        cleaned_lines.append(line)

    # Join back and split on semicolons
    cleaned_content = "\n".join(cleaned_lines)
    statements = [stmt.strip() for stmt in cleaned_content.split(";")]

    # Filter out empty statements
    return [stmt for stmt in statements if stmt.strip()]


# Load DDL statements at module level for backward compatibility with tests
# DDL_STATEMENTS combines tables and indexes for the test fixture
DDL_STATEMENTS = _load_sql_file("tables.sql") + _load_sql_file("indexes.sql")
ALTER_STATEMENTS = _load_sql_file("migrations.sql")
ROLLBACK_STATEMENTS = _load_sql_file("rollback.sql")


def get_db_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Return a configured SQLite connection (WAL, FKs enabled)."""
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA foreign_keys=ON;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


def init_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Create all tables and indexes; returns an open connection.

    Loads DDL from SQL files in the schema/ subdirectory:
    1. tables.sql - Creates all tables
    2. indexes.sql - Creates all indexes
    3. migrations.sql - Applies ALTER TABLE statements (with error handling)
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = get_db_connection(db_path)

    # One-time cleanup: remove misnamed index from earlier schema versions
    con.execute("DROP INDEX IF EXISTS idx_pgl_player_season;")

    # Load and execute table definitions
    for stmt in _load_sql_file("tables.sql"):
        con.execute(stmt)

    # Load and execute index definitions
    for stmt in _load_sql_file("indexes.sql"):
        con.execute(stmt)

    # ALTER TABLE statements are not idempotent in SQLite; swallow the
    # OperationalError that fires when the column already exists.
    for stmt in ALTER_STATEMENTS:
        try:
            con.execute(stmt)
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e).lower():
                logger.debug("ALTER skipped (already applied?): %s", stmt[:50])
            else:
                raise

    con.commit()
    return con


def rollback_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Execute rollback statements to reverse migrations."""
    con = sqlite3.connect(db_path)
    for stmt in ROLLBACK_STATEMENTS:
        try:
            con.execute(stmt)
        except sqlite3.OperationalError as e:
            logger.debug("Rollback statement failed: %s", e)
    con.commit()
    return con


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    con = init_db()
    tables = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
    ).fetchall()
    logger.info("Initialized database at: %s", DB_PATH)
    logger.info("Tables: %s", [t[0] for t in tables])
    con.close()
