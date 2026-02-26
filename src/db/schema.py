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
    """
    Load SQL statements from a file in the schema directory.

    Reads the file located at SCHEMA_DIR / filename, removes inline SQL comments that start with `--`, splits the content on semicolons into individual statements, strips surrounding whitespace, and filters out empty or comment-only statements.

    Parameters:
        filename (str): Name of the SQL file inside the module's SCHEMA_DIR.

    Returns:
        list[str]: A list of individual, trimmed SQL statements from the file. If the file does not exist, returns an empty list.
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
    """
    Open and configure a SQLite connection for the NBA raw data database.

    Configures the connection to use WAL journal mode, enable foreign key enforcement, and set synchronous to NORMAL.

    Parameters:
        db_path (Path): Filesystem path to the SQLite database file. Defaults to module-level DB_PATH.

    Returns:
        sqlite3.Connection: Configured SQLite connection.
    """
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA foreign_keys=ON;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


def init_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """
    Create the database schema (tables, indexes, and migrations) and return a configured connection.

    Ensures the database parent directory exists, opens a configured SQLite connection, performs a one-time cleanup of a legacy index, executes SQL statements loaded from schema/tables.sql and schema/indexes.sql, and applies ALTER statements from migrations with idempotent handling of "duplicate column name" errors. Commits the changes before returning.

    Parameters:
        db_path (Path): Path to the SQLite database file (defaults to module DB_PATH).

    Returns:
        sqlite3.Connection: An open, configured connection to the initialized database.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = get_db_connection(db_path)

    # One-time cleanup: remove misnamed index from earlier schema versions
    con.execute("DROP INDEX IF EXISTS idx_pgl_player_season;")

    # Execute tables.sql using executescript to avoid fragile statement splitting
    tables_sql = (SCHEMA_DIR / "tables.sql").read_text(encoding="utf-8")
    con.executescript(tables_sql)

    # Execute indexes.sql; if index creation fails due to existing duplicate data, raise a helpful error
    indexes_sql = (SCHEMA_DIR / "indexes.sql").read_text(encoding="utf-8")
    try:
        con.executescript(indexes_sql)
    except sqlite3.OperationalError as e:
        msg = str(e).lower()
        if "unique" in msg or "index" in msg:
            logger.error("Index creation failed during init_db: %s", e)
            raise RuntimeError(
                "Index creation failed during init_db. This usually indicates duplicate or incompatible existing data. "
                "Please inspect the database, deduplicate conflicting rows (e.g., duplicate bref_id), and re-run init_db."
            ) from e
        else:
            raise

    # ALTER TABLE statements are not idempotent in SQLite; swallow the
    # OperationalError that fires when the column already exists. For index creation
    # related errors, raise a helpful error advising deduplication.
    for stmt in ALTER_STATEMENTS:
        try:
            con.execute(stmt)
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            if "duplicate column name" in msg:
                logger.debug("ALTER skipped (already applied?): %s", stmt[:50])
            elif "index" in msg or "unique" in msg:
                logger.error("Migration index creation failed: %s", e)
                raise RuntimeError(
                    "Migration failed during index creation. Please deduplicate conflicting data before re-running init_db."
                ) from e
            else:
                raise

    con.commit()
    return con


def rollback_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """
    Execute configured rollback SQL statements against the database to reverse applied migrations.

    Each statement from the module's rollback statements is executed in sequence; statement-level sqlite3.OperationalError exceptions are logged at debug level and do not abort the overall rollback. The transaction is committed before returning.

    Parameters:
        db_path (Path): Path to the SQLite database file to operate on.

    Returns:
        sqlite3.Connection: An open SQLite connection to the database after rollback.
    """
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
