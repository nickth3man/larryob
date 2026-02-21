# src/etl — ETL Loaders

## OVERVIEW

All data ingestion logic. Two pipelines: live API (nba_api + scraping) and raw/ CSV backfill (`backfill/`). Each loader is idempotent via `etl_run_log`.

## FILES

| File | What it loads | Target table(s) |
|------|--------------|-----------------|
| `dimensions.py` | Teams, players, seasons from nba_api | `dim_season`, `dim_team`, `dim_player` |
| `game_logs.py` | Player + team box scores via nba_api | `fact_game`, `player_game_log`, `team_game_log` |
| `play_by_play.py` | PBP events via nba_api | `fact_play_by_play` |
| `roster.py` | Roster stints via CommonTeamRoster | `fact_roster` |
| `salaries.py` | Salary data scraped from Basketball-Reference | `dim_salary_cap`, `fact_salary` |
| `awards.py` | Player awards via PlayerAwards endpoint | `fact_player_award` |
| `raw_backfill.py` | Thin wrapper that delegates to `backfill/` | (all backfill tables) |
| `models.py` | Pydantic row validators | — |
| `validate.py` | Business-rule row filters | — |
| `helpers.py` | Pure transformation functions | — |
| `utils.py` | Shared utilities: cache, backoff, upsert, logging | — |

## LOADER PATTERN

Every loader follows this template:
```python
def load_something(con: sqlite3.Connection, season_id: str) -> None:
    if already_loaded(con, "target_table", season_id, "module.load_something"):
        logger.info("Already loaded — skipping")
        return
    started_at = datetime.now(UTC).isoformat()
    # ... fetch, transform, validate_rows(), upsert_rows() ...
    record_run(con, "target_table", season_id, "module.load_something", row_count, "ok", started_at)
```

## KEY UTILITIES (utils.py)

- `upsert_rows(con, table, rows, conflict="IGNORE")` — bulk insert; validates identifiers against SQLi
- `call_with_backoff(fn, base_sleep=3.0, max_retries=5)` — nba_api rate-limit retry
- `load_cache(key)` / `save_cache(key, data)` — JSON cache in `.cache/`; `CACHE_VERSION=2`
- `already_loaded()` / `record_run()` — idempotency guard via `etl_run_log`
- `transaction(con)` — context manager: commit on success, rollback on exception
- `log_load_summary(con, table, season_id)` — row count + warning if below expected minimum

## KEY HELPERS (helpers.py)

- `int_season_to_id(s)` — bref end-year int → `"YYYY-YY"` e.g. `2026 → "2025-26"`
- `season_id_from_game_id(padded)` — derive season from 10-char game ID
- `season_id_from_date(date_str)` — derive season from ISO date (July cutoff)
- `season_type_from_game_id(padded)` — game type from digits [2:4] of padded ID
- `pad_game_id(game_id)` — zero-pad to 10-char TEXT
- `_int(v)` / `_flt(v)` — NA-safe scalar coercions (pandas-safe)
- `_norm_name(name)` — lowercase + strip accents for fuzzy player name matching

## CONVENTIONS

- All `_int()` / `_flt()` calls in transformers — never raw cast (pandas NA leaks)
- Cache keys: descriptive strings e.g. `f"game_logs_{season_id}_{season_type}"`
- Loader functions named `load_<thing>` or `load_<thing>_for_seasons`
- `run_all()` pattern in `dimensions.py` for orchestrating sub-loaders

## ANTI-PATTERNS

- Never skip `already_loaded()` check in a loader — always guard re-runs
- Never use `pd.isna()` directly on scalars — use `_isna()` from `helpers.py`
- Never raw-format table/column names into SQL strings — use `upsert_rows()` which calls `_validate_identifier()`
- Never sleep manually in loaders — use `call_with_backoff()` which handles sleep
