# PROJECT KNOWLEDGE BASE

**Generated:** 2026-02-21
**Branch:** dev
**Commit:** 583e364

## OVERVIEW

NBA analytics pipeline: SQLite OLTP ingestion (via `nba_api` + Basketball-Reference scraping) feeding a DuckDB OLAP analytics layer. Python 3.13, `uv` for package management.

## STRUCTURE

```
larryob/
├── src/
│   ├── db/            # Schema (SQLite DDL) + DuckDB analytics views
│   ├── etl/           # All loaders; backfill/ is a separate sub-pipeline
│   └── pipeline/      # CLI entrypoint + orchestration (run via `uv run ingest` or `python -m src.pipeline`)
├── tests/             # pytest suite — never touches production nba_raw_data.db
├── raw/               # Static CSV/Parquet from Basketball-Reference (gitignored)
├── research/          # Numbered markdown notes (1.md–5.md, resources.md)
├── logs/              # Ingest run logs (gitignored)
├── nba_raw_data.db    # SQLite store (gitignored, built by the ingest pipeline)
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
| Shared ETL utilities | `src/etl/utils.py` (`upsert_rows`, cache, `transaction`) |
| Pure transformation helpers | `src/etl/helpers.py` (`_int`, `_flt`, `pad_game_id`, season ID conversions) |
| API rate limiting / retries | `src/etl/api_client.py` — `APICaller` class with adaptive backoff |
| Centralized config / reference data | `src/etl/config.py` — API, cache, metrics config + team metadata + salary cap data |
| Observability / metrics | `src/etl/metrics.py` — in-memory ETL metrics (rows, durations, API calls) |
| Post-ingest data reconciliation | `src/etl/validate.py` — `run_consistency_checks()` |
| CLI flags / pipeline orchestration | `src/pipeline/` — `IngestConfig`, `Stage`, `_build_stage_plan()` |
| Test fixtures | `tests/conftest.py` |

## CODE MAP

| Symbol | Location | Role |
|--------|----------|------|
| `init_db()` | `src/db/schema.py` | Create all SQLite tables + indexes; idempotent |
| `get_duck_con()` | `src/db/analytics.py` | DuckDB factory: installs sqlite_scanner, attaches `nba_raw_data.db` as `nba.` schema, creates all views |
| `_VIEWS` | `src/db/analytics.py` | List of `(view_name, SQL)` pairs — all analytics views live here |
| `upsert_rows()` | `src/etl/utils.py` | Bulk INSERT OR IGNORE/REPLACE with SQL-injection protection |
| `already_loaded()` / `record_run()` | `src/etl/utils.py` | ETL idempotency via `etl_run_log` table |
| `APICaller` | `src/etl/api_client.py` | Unified API client: exponential backoff, adaptive rate limiting, metrics integration |
| `get_api_caller()` | `src/etl/api_client.py` | Singleton factory for the default `APICaller` instance |
| `APIConfig` / `CacheConfig` / `MetricsConfig` | `src/etl/config.py` | Env-var-driven config classes (`LARRYOB_*` prefixed) |
| `_TEAM_METADATA` / `_SALARY_CAP_BY_SEASON` | `src/etl/config.py` | Static reference data for team enrichment and salary cap lookups |
| `record_etl_rows()` / `record_api_call()` | `src/etl/metrics.py` | Thread-safe in-memory metric recorders |
| `ETLTimer` | `src/etl/metrics.py` | Context manager for timing ETL stages |
| `validate_rows()` | `src/etl/validate.py` | Drop invalid rows by table-specific business rules + Pydantic models |
| `run_consistency_checks()` | `src/etl/validate.py` | Post-ingest reconciliation: player vs team box-score totals |
| `PlayerGameLogRow` / `TeamGameLogRow` | `src/etl/models.py` | Pydantic models for box-score row validation |
| `run_raw_backfill()` | `src/etl/backfill/_orchestrator.py` | Ordered loader chain for raw/ CSV pipeline |
| `IngestConfig` | `src/pipeline/models.py` | Dataclass holding all CLI flags; built via `from_args()` |
| `Stage` | `src/pipeline/models.py` | StrEnum of pipeline stages (DIMENSIONS, RAW_BACKFILL, AWARDS, etc.) |
| `CheckpointState` | `src/pipeline/models.py` | Tracks per-stage row counts and etl_run_log deltas |

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
- **API calls via APICaller**: all nba_api calls go through `get_api_caller().call_with_backoff()` — never raw `time.sleep()`
- **Config via env vars**: `LARRYOB_API_DELAY_SECONDS`, `LARRYOB_API_MAX_RETRIES`, `LARRYOB_CACHE_DIR`, `LARRYOB_METRICS_ENABLED`
- **Line length**: 100 chars (ruff configured)

## ANTI-PATTERNS (THIS PROJECT)

- Never use `nba_raw_data.db` in tests — fixtures use `:memory:` or `tmp_path`
- Never raw string-format SQL identifiers — use `_validate_identifier()` from `utils.py`
- Never store stats as 0 when they should be NULL (era-specific nulls)
- Never add ALTER TABLE statements to `DDL_STATEMENTS` — they go in `ALTER_STATEMENTS` (SQLite doesn't support `IF NOT EXISTS` for columns)
- Never commit `.cache/`, `raw/`, `*.db`, `coverage.json`, `.coverage`, `logs/`
- Never call `time.sleep()` directly in loaders — use `APICaller` methods
- Never use `pd.isna()` directly on scalars — use `_isna()` from `helpers.py`

## COMMANDS

```bash
# Run full ingest (dims + box scores, 2023-24 and 2024-25)
uv run ingest

# Dims only (no network-heavy box score calls)
uv run ingest --dims-only

# Backfill from raw/ CSVs
uv run ingest --raw-backfill

# Awards, salaries, rosters (optional stages)
uv run ingest --awards --salaries --rosters

# Analytics-only mode with export
uv run ingest --analytics-only --analytics-view vw_player_season_totals --analytics-output out.csv

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

- `nba_api` calls sleep 3s by default (rate limiting); `APICaller` adapts pacing from success/failure streaks
- `CACHE_VERSION = 2` in `config.py` — bump when ETL output shape changes to invalidate stale cache
- DuckDB analytics layer is read-only; all writes go through SQLite
- `fact_play_by_play.event_id` = `game_id + '_' + zero-padded eventnum (6 digits)`
- `score_margin` in PBP is TEXT (`'+5'`, `'-3'`, `'TIE'`) — cast to INTEGER at query time
- bref tables (`fact_draft`, `fact_player_season_stats`, `fact_player_advanced_season`, etc.) use `bref_player_id` with no FK to `dim_player` — covers ABA/BAA eras
- `fact_team_season.bref_abbrev` ≠ `dim_team.abbreviation` (BRK vs BKN, CHO vs CHA, etc.) — mapping in `config.py._ABBR_TO_BREF`
- Reconciliation checks (`run_consistency_checks`) compare player-sum vs team-total for PTS/REB/AST per game
- Metrics are opt-in via `--metrics` flag or `LARRYOB_METRICS_ENABLED=true`; exportable to HTTP endpoint
- Pipeline entry point is `src/pipeline/cli.py:main` — run via `uv run ingest` (console script) or `python -m src.pipeline`; orchestration split across `executor.py`, `stages.py`, `checkpoint.py`
