# src/etl/backfill — Raw CSV Backfill Pipeline

## OVERVIEW

Separate sub-pipeline that loads historical data from `raw/` CSV/Parquet files (Basketball-Reference exports). No nba_api calls. Entry point: `run_raw_backfill()`.

## FILES

| File | Loads |
|------|-------|
| `_orchestrator.py` | Runs all loaders in dependency order; entry point |
| `_dims.py` | `enrich_dim_player`, `enrich_dim_team`, `load_team_history` |
| `_games.py` | `load_games`, `load_schedule` → `fact_game` |
| `_game_logs.py` | `load_player_game_logs`, `load_team_game_logs` |
| `_season_stats.py` | `load_player_season_stats`, `load_team_season`, `load_league_season` |
| `_advanced_stats.py` | `load_player_advanced`, `load_player_shooting`, `load_player_pbp_season` |
| `_awards.py` | `load_awards` → `fact_player_award` |
| `_draft.py` | `load_draft` → `fact_draft` |
| `__init__.py` | Re-exports only `run_raw_backfill` |

## LOAD ORDER (dependency-driven, in `_orchestrator.py`)

```
team_history → dim_team_enrich → dim_player_enrich
→ games → schedule
→ player_game_logs → team_game_logs
→ team_season → league_season
→ draft → player_season_stats → player_advanced → player_shooting → player_pbp_season
→ awards
```

## CONVENTIONS

- All modules are **private** (`_` prefix) — import only via `__init__.py`
- All loaders accept `(con: sqlite3.Connection, raw_dir: Path)` signature
- `raw_dir` defaults to `Path("raw")` — override via `--raw-dir` CLI flag
- All inserts use `INSERT OR IGNORE` or `INSERT OR REPLACE` — safe to re-run
- bref CSV files use `bref_player_id` and `bref_abbrev` — these do NOT FK to dim tables
- `_orchestrator.py` wraps each loader in try/except — one failure doesn't abort the pipeline

## ANTI-PATTERNS

- Never import `_dims.py`, `_games.py` etc. directly from outside `backfill/` — only `run_raw_backfill`
- Never add nba_api calls here — backfill is CSV-only, offline-capable
- Never change load order without checking FK dependencies (games before game_logs, dims before facts)
