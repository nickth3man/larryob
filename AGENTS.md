# PROJECT KNOWLEDGE BASE

**Generated:** 2026-02-20
**Branch:** dev
**Commit:** 7e2a71c

## OVERVIEW

NBA analytics pipeline: SQLite OLTP ingestion (via `nba_api` + Basketball-Reference scraping) feeding a DuckDB OLAP analytics layer. Python 3.13, `uv` for package management.

## STRUCTURE

```
larryob/
├── ingest.py          # CLI entrypoint — only file that runs directly
├── src/
│   ├── db/            # Schema (SQLite DDL) + DuckDB analytics views
│   └── etl/           # All loaders; backfill/ is a separate sub-pipeline
├── tests/             # pytest suite — never touches production nba_raw_data.db
├── raw/               # Static CSV/Parquet from Basketball-Reference (gitignored)
├── research/          # Numbered markdown notes (1.md–5.md, resources.md)
├── nba_raw_data.db    # SQLite store (gitignored, built by ingest.py)
└── .cache/            # nba_api JSON cache (gitignored, auto-rebuilt)
```

## WHERE TO LOOK

| Task | Location |
|------|----------|
| Add a new ETL loader | `src/etl/` — copy pattern from `game_logs.py` |
| Add a backfill loader (from raw/ CSV) | `src/etl/backfill/` — add `_yourmodule.py`, wire into `_orchestrator.py` |
| Add a DuckDB analytics view | `src/db/analytics.py` — append to `_VIEWS` list |
| Modify SQLite schema | `src/db/schema.py` — idempotent DDL; add ALTERs to `ALTER_STATEMENTS` |
| Add Pydantic row validation | `src/etl/models.py` |
| Add business-rule row filters | `src/etl/validate.py` — add to `RULES` dict |
| Shared ETL utilities | `src/etl/utils.py` (`upsert_rows`, `call_with_backoff`, cache, `transaction`) |
| Pure transformation helpers | `src/etl/helpers.py` (`_int`, `_flt`, `pad_game_id`, season ID conversions) |
| Test fixtures | `tests/conftest.py` |

## CODE MAP

| Symbol | Location | Role |
|--------|----------|------|
| `init_db()` | `src/db/schema.py` | Create all SQLite tables + indexes; idempotent |
| `get_duck_con()` | `src/db/analytics.py` | DuckDB factory: installs sqlite_scanner, attaches `nba_raw_data.db` as `nba.` schema, creates all views |
| `_VIEWS` | `src/db/analytics.py` | List of `(view_name, SQL)` pairs — all analytics views live here |
| `upsert_rows()` | `src/etl/utils.py` | Bulk INSERT OR IGNORE/REPLACE with SQL-injection protection |
| `call_with_backoff()` | `src/etl/utils.py` | nba_api rate-limit retry with exponential backoff |
| `already_loaded()` / `record_run()` | `src/etl/utils.py` | ETL idempotency via `etl_run_log` table |
| `validate_rows()` | `src/etl/validate.py` | Drop invalid rows by table-specific business rules |
| `run_raw_backfill()` | `src/etl/backfill/_orchestrator.py` | Ordered loader chain for raw/ CSV pipeline |
| `PlayerGameLogRow` / `TeamGameLogRow` | `src/etl/models.py` | Pydantic models for box-score row validation |

## CONVENTIONS

- **season_id format**: `"YYYY-YY"` e.g. `"2023-24"` — use `int_season_to_id()` to convert from bref integers
- **game_id format**: 10-char zero-padded string e.g. `"0022301001"` — use `pad_game_id()`
- **player_id / team_id**: stored as TEXT (NBA numeric IDs), not integers
- **NULL vs 0**: stats not tracked in early NBA eras are NULL, never 0 (blocks/steals pre-1973-74, 3PT pre-1979-80)
- **SQLite STRICT**: all tables use `STRICT` mode — types are enforced
- **ETL idempotency**: all loaders check `already_loaded()` before running; use `INSERT OR IGNORE`
- **bref_ prefix**: columns/IDs from Basketball-Reference (different from NBA API IDs)
- **Private backfill modules**: `src/etl/backfill/_*.py` — underscore prefix, import via `__init__.py`
- **DuckDB views query SQLite via `nba.` prefix**: e.g. `FROM nba.player_game_log`

## ANTI-PATTERNS (THIS PROJECT)

- Never use `nba_raw_data.db` in tests — fixtures use `:memory:` or `tmp_path`
- Never raw string-format SQL identifiers — use `_validate_identifier()` from `utils.py`
- Never store stats as 0 when they should be NULL (era-specific nulls)
- Never add ALTER TABLE statements to `DDL_STATEMENTS` — they go in `ALTER_STATEMENTS` (SQLite doesn't support `IF NOT EXISTS` for columns)
- Never commit `.cache/`, `raw/`, `*.db`, `coverage.json`, `.coverage`

## COMMANDS

```bash
# Run full ingest (dims + box scores, 2023-24 and 2024-25)
uv run python ingest.py

# Dims only (no network-heavy box score calls)
uv run python ingest.py --dims-only

# Backfill from raw/ CSVs
uv run python ingest.py --raw-backfill

# Run tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=src --cov-report=term-missing

# Lint + format
uv run ruff check .
uv run ruff format .

# Type check
uv run ty check
```

## NOTES

- `nba_api` calls sleep 3s by default (rate limiting); backoff doubles on failure up to 5 retries
- `CACHE_VERSION = 2` in `utils.py` — bump when ETL output shape changes to invalidate stale cache
- DuckDB analytics layer is read-only; all writes go through SQLite
- `fact_play_by_play.event_id` = `game_id + '_' + zero-padded eventnum (6 digits)`
- `score_margin` in PBP is TEXT (`'+5'`, `'-3'`, `'TIE'`) — cast to INTEGER at query time
- bref tables (`fact_draft`, `fact_player_season_stats`, `fact_player_advanced_season`, etc.) use `bref_player_id` with no FK to `dim_player` — covers ABA/BAA eras
- `fact_team_season.bref_abbrev` ≠ `dim_team.abbreviation` (BRK vs BKN, CHO vs CHA, etc.)
