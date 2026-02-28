# LarryOB

NBA data ingestion and analytics pipeline using:
- SQLite for transactional ETL storage (`data/databases/nba_raw_data.db`)
- DuckDB for analytical SQL views built on top of SQLite data

## Current State

- Python package: `larryob` (requires Python `>=3.13`)
- CLI entrypoint: `ingest`
- Database schema: 23 SQLite tables (`src/db/schema/tables.sql`)
- Analytics layer: 18 DuckDB views (`src/db/views/*.sql`)
- Automated tests: 57 test files under `tests/`
- Latest local test run in this workspace: `996 passed, 2 skipped`

## Quick Start

```bash
# 1) Install dependencies
uv sync

# 2) Inspect available CLI options
uv run ingest --help

# 3) Fast bootstrap: dimensions only
uv run ingest --dims-only

# 4) Full-history ingest (now default - covers 1946-47 to present)
uv run ingest

# 5) Minimal ingest (disable optional stages)
uv run ingest --no-awards --no-salaries --no-rosters --no-playoffs

# 6) Run analytics and print results
uv run ingest --analytics-view vw_player_season_totals --analytics-limit 25

## Ingest Pipeline (High Level)

Stage order in `src/pipeline`:
1. Dimension seed (`dim_season`, `dim_team`, `dim_player`)
2. Optional raw CSV backfill (`--raw-backfill`)
3. Optional awards / salary / roster loaders
4. Game log ingest
5. Reconciliation checks (unless `--skip-reconciliation`)
6. Optional play-by-play load (`--pbp-limit > 0`)

## Common CLI Flags

- Scope and seasons:
  - `--seasons 1946-47 1947-48 ...` (default: full history 1946-47 to present)
  - `--dims-only`
  - Data domains (now enabled by default, use --no-* to disable):
  - `--awards` / `--no-awards`
  - `--salaries` / `--no-salaries` with `--salary-source {bref,open,auto}`
  - `--rosters` / `--no-rosters`
  - `--include-playoffs` / `--no-playoffs`
- Raw backfill:
  - `--raw-backfill`
  - `--raw-dir raw`
  - `--raw-backfill-fail-fast`
- Play-by-play:
  - `--pbp-limit 200`
  - `--pbp-source {api,bulk,auto}`
  - `--pbp-bulk-dir raw/pbp`
- Analytics:
  - `--analytics-view <view_name>`
  - `--analytics-limit 100`
  - `--analytics-output out.csv|out.parquet|out.json`
  - `--analytics-only`
- Observability:
  - `--metrics`
  - `--metrics-summary`
  - `--metrics-export-endpoint <url>`
  - `--log-level DEBUG|INFO|WARNING|ERROR`
  - `--log-file logs/ingest.log`
  - `--runlog-tail 12`

## Available Analytics Views

Examples from `src/db/views/`:
- Player views: `vw_player_shooting`, `vw_player_season_totals`, `vw_player_per36`, `vw_player_usage`, `vw_player_advanced_full`, `vw_player_awards`
- Team views: `vw_team_standings`, `vw_team_pace`, `vw_team_ratings`, `vw_team_four_factors`
- Other views: `vw_pbp_shot_summary`, `vw_salary_cap_pct`, `vw_draft_class`

## Project Layout

- `src/config/` - runtime configuration and settings
- `src/core/` - shared base abstractions
- `src/db/` - SQLite schema + DuckDB view loader
- `src/etl/` - extraction, transform, load, backfill modules
- `src/pipeline/` - CLI parsing, validation, stage orchestration
- `tests/` - unit/integration tests
- `raw/` - source CSV/Parquet files used for raw backfill workflows
- `data/cache/` - API/cache artifacts
- `data/databases/` - SQLite database files
- `docs/` - agent docs and planning notes
- `scripts/` - maintenance utilities

## Environment Variables

Loaded from `.env` (via `python-dotenv`), with defaults in code:
- `LARRYOB_API_DELAY_SECONDS`
- `LARRYOB_API_MAX_RETRIES`
- `LARRYOB_INTER_CALL_SLEEP`
- `LARRYOB_CACHE_DIR`
- `LARRYOB_METRICS_ENABLED`
- `LARRYOB_METRICS_ENDPOINT`

## Development

```bash
# Run tests
uv run pytest

# Lint
uv run ruff check .

# Format
uv run ruff format .

# Type check
uv run ty src/
```

## Docs

- `ARCHITECTURE.md`
- `CONTRIBUTING.md`
