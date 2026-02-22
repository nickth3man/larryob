# src/etl — ETL Loaders

## OVERVIEW

All data ingestion logic. Two pipelines: live API (nba_api + scraping) and raw/ CSV backfill (`backfill/`). Each loader is idempotent via `etl_run_log`.

## FILES

| File | What it loads / does | Target table(s) |
|------|---------------------|-----------------|
| `dimensions.py` | Teams, players, seasons from nba_api | `dim_season`, `dim_team`, `dim_player` |
| `game_logs.py` | Player + team box scores via nba_api | `fact_game`, `player_game_log`, `team_game_log` |
| `play_by_play.py` | PBP events via nba_api | `fact_play_by_play` |
| `roster.py` | Roster stints via CommonTeamRoster | `fact_roster` |
| `salaries.py` | Salary data scraped from Basketball-Reference | `dim_salary_cap`, `fact_salary` |
| `awards.py` | Player awards via PlayerAwards endpoint | `fact_player_award` |
| `raw_backfill.py` | Thin wrapper that delegates to `backfill/` | (all backfill tables) |
| `api_client.py` | `APICaller` — unified API client with adaptive rate limiting | — |
| `config.py` | Centralized config: API/cache/metrics settings, team metadata, salary cap data, bref abbreviation mapping | — |
| `metrics.py` | In-memory metrics: ETL rows, API calls, latency, durations; `ETLTimer` context manager | — |
| `models.py` | Pydantic row validators (`BaseGameLogRow`, `PlayerGameLogRow`, `TeamGameLogRow`, `FactGameRow`, etc.) | — |
| `validate.py` | Business-rule row filters + post-ingest reconciliation checks; `_ROW_MODELS` registry | — |
| `helpers.py` | Pure transformation functions | — |
| `utils.py` | Shared utilities: cache, upsert, logging, idempotency guards | — |

## LOADER PATTERN

Every loader follows this template:
```python
def load_something(con: sqlite3.Connection, season_id: str) -> None:
    if already_loaded(con, "target_table", season_id, "module.load_something"):
        logger.info("Already loaded — skipping")
        return
    started_at = datetime.now(UTC).isoformat()
    # ... fetch via get_api_caller().call_with_backoff() ...
    # ... transform rows ...
    # ... validate_rows("target_table", rows) ...
    # ... upsert_rows(con, "target_table", rows) within transaction(con) ...
    record_run(con, "target_table", season_id, "module.load_something", row_count, "ok", started_at)
```

## API CLIENT (`api_client.py`)

- `APICaller` — all external API calls go through this class
- Adaptive pacing: speeds up after 3+ consecutive successes, slows down on failures
- `call_with_backoff(fn, label=...)` — standard retry with exponential backoff
- `call_with_backoff_custom_delay(fn, base_sleep=..., max_retries=...)` — override defaults per call
- `sleep_between_calls()` — throttle between iterative API calls in a loop
- `get_api_caller()` — singleton factory; uses `APIConfig` from `config.py`

## CONFIGURATION (`config.py`)

- `APIConfig` — `base_sleep()`, `max_retries()`, `inter_call_sleep()` — env-var overridable (`LARRYOB_*`)
- `CacheConfig` — `CACHE_VERSION = 2`, `cache_dir()` path
- `MetricsConfig` — `enabled()`, `export_endpoint()`
- `_TEAM_METADATA` — dict of 30 NBA teams with conference, division, arena, colors, founded year
- `_SALARY_CAP_BY_SEASON` — historical salary cap amounts (1984–2025)
- `_ABBR_TO_BREF` — NBA abbreviation → Basketball-Reference abbreviation mapping (BKN→BRK, CHA→CHO, PHX→PHO)

## KEY UTILITIES (`utils.py`)

- `upsert_rows(con, table, rows, conflict="IGNORE")` — bulk insert; validates identifiers against SQLi
- `load_cache(key)` / `save_cache(key, data)` — JSON cache in `.cache/`; `CACHE_VERSION` from `config.py`
- `already_loaded()` / `record_run()` — idempotency guard via `etl_run_log`
- `transaction(con)` — context manager: commit on success, rollback on exception
- `log_load_summary(con, table, season_id)` — row count + warning if below expected minimum
- `setup_logging(level, log_file)` — configure root logger with console + optional file handler

## KEY HELPERS (`helpers.py`)

- `int_season_to_id(s)` — bref end-year int → `"YYYY-YY"` e.g. `2026 → "2025-26"`
- `season_id_from_game_id(padded)` — derive season from 10-char game ID
- `season_id_from_date(date_str)` — derive season from ISO date (July cutoff)
- `season_type_from_game_id(padded)` — game type from digits [2:4] of padded ID
- `pad_game_id(game_id)` — zero-pad to 10-char TEXT
- `_int(v)` / `_flt(v)` — NA-safe scalar coercions (pandas-safe)
- `_norm_name(name)` — lowercase + strip accents for fuzzy player name matching

## VALIDATION (`validate.py`)

- `validate_rows(table, rows)` — runs Pydantic validation per table using `_ROW_MODELS` registry
- `_ROW_MODELS` — maps table name → Pydantic model class (covers game logs, fact_game, salaries, season stats, advanced, shooting)
- `run_consistency_checks(con, season_id)` — reconciles player-sum vs team-total for PTS/REB/AST
- Tables not in `_ROW_MODELS` skip Pydantic validation (structural insert only)

## CONVENTIONS

- All `_int()` / `_flt()` calls in transformers — never raw cast (pandas NA leaks)
- Cache keys: descriptive strings e.g. `f"game_logs_{season_id}_{season_type}"`
- Loader functions named `load_<thing>` or `load_<thing>_for_seasons`
- `run_all()` pattern in `dimensions.py` for orchestrating sub-loaders
- All API calls through `APICaller` — never standalone `time.sleep()` or `call_with_backoff()` from `utils.py` (legacy)
- Metrics are opt-in; recorded automatically by `APICaller` when enabled

## ANTI-PATTERNS

- Never skip `already_loaded()` check in a loader — always guard re-runs
- Never use `pd.isna()` directly on scalars — use `_isna()` from `helpers.py`
- Never raw-format table/column names into SQL strings — use `upsert_rows()` which calls `_validate_identifier()`
- Never sleep manually in loaders — use `APICaller` methods
- Never instantiate `APICaller` directly in loaders — use `get_api_caller()` singleton
