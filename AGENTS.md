# PROJECT KNOWLEDGE BASE

**Generated:** 2026-02-22
**Branch:** dev
**Commit:** 10e21cf

## OVERVIEW

NBA analytics pipeline with SQLite ingestion (OLTP) and DuckDB analytics (OLAP). Python 3.13+, `uv` workflow, and stage-based CLI orchestration.

## STRUCTURE

```
larryob/
├── src/
│   ├── db/                  # Schema + DuckDB analytics views (see src/db/AGENTS.md)
│   ├── etl/                 # API and scrape loaders (see src/etl/AGENTS.md)
│   │   └── backfill/        # Raw CSV backfill pipeline (see src/etl/backfill/AGENTS.md)
│   └── pipeline/            # CLI and stage orchestration (see src/pipeline/AGENTS.md)
├── tests/                   # Pytest suite and DB fixtures (see tests/AGENTS.md)
├── research/                # Analysis notes and references
├── scripts/                 # Repo utility scripts (sync_agents.py mirrors AGENTS→CLAUDE/GEMINI)
├── raw/                     # Source CSV/Parquet inputs (gitignored)
├── logs/                    # Runtime logs (gitignored)
├── .cache/                  # API response cache (gitignored)
└── nba_raw_data.db          # Local SQLite DB (gitignored)
```

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| SQLite DDL and migrations | `src/db/AGENTS.md` | STRICT tables, ALTER_STATEMENTS pattern |
| DuckDB view additions/changes | `src/db/AGENTS.md` | View lifecycle, `_VIEWS` list in analytics.py |
| Loader implementation rules | `src/etl/AGENTS.md` | Idempotency guard, APICaller, validation flow |
| Historical raw CSV backfill work | `src/etl/backfill/AGENTS.md` | Private modules, load ordering, fail-fast |
| CLI flags and stage planning | `src/pipeline/AGENTS.md` | Parser, validation, `_build_stage_plan()` |
| Exception hierarchy | `src/pipeline/AGENTS.md` | IngestError, ReconciliationError, AnalyticsError |
| Test fixtures and isolation rules | `tests/AGENTS.md` | In-memory/temp DB constraints, fixture selection |

## CODE MAP

| Symbol | Location | Role |
|--------|----------|------|
| `main()` | `src/pipeline/cli.py` | Top-level CLI entrypoint; manages connection lifecycle |
| `run_ingest_pipeline()` | `src/pipeline/executor.py` | Executes stage plan with timing and checkpoints |
| `_build_stage_plan()` | `src/pipeline/executor.py` | Builds dynamic ordered list of `(Stage, tables, fn)` |
| `init_db()` | `src/db/schema.py` | Initializes SQLite schema (idempotent) |
| `get_duck_con()` | `src/db/analytics.py` | Thread-local DuckDB factory; drops/recreates all views |
| `load_game_logs_for_seasons()` | `src/etl/game_logs.py` | Core box-score ingest path |
| `run_raw_backfill()` | `src/etl/backfill/_orchestrator.py` | Runs all CSV loaders in dependency order |
| `IngestConfig` | `src/pipeline/models.py` | Immutable (`__slots__` dataclass) config from CLI args |
| `CheckpointState` | `src/pipeline/models.py` | Mutable stage-to-stage progress tracker |
| `APICaller` / `get_api_caller()` | `src/etl/api_client.py` | Singleton API client with adaptive pacing |

## CONVENTIONS

- `season_id` format: `YYYY-YY` (e.g. `2023-24`)
- `game_id`: 10-char zero-padded text (e.g. `0022301001`)
- `player_id` / `team_id`: TEXT, not INTEGER
- Early-era unavailable stats: NULL (never 0)
- Stage checkpoint names use `"post-"` prefix (e.g. `"post-dimensions"`)
- Python style: Ruff line length 100, `E/F/W/I/UP` rules, `E501` ignored
- `LARRYOB_*` env-var prefix for all runtime knobs
- Test discovery pinned to `tests/` via `[tool.pytest.ini_options]`
- `ty` for type-checking (not mypy)

## ANTI-PATTERNS (THIS PROJECT)

- Never commit: `.cache/`, `raw/`, `logs/`, `*.db`, `.coverage`, `coverage.json`
- Never bypass directory-local AGENTS guidance for `src/db`, `src/etl`, `src/pipeline`, `tests`
- Never write to SQLite from `analytics.py` — DuckDB is read-only OLAP layer
- Never use `as any`, `@ts-ignore` type suppressions (Python equivalent: avoid `type: ignore`)
- Never raise generic `ValueError` for pipeline errors — use typed exceptions from `exceptions.py`

## COMMANDS

```bash
# Full pipeline
uv run ingest

# Common focused runs
uv run ingest --dims-only
uv run ingest --raw-backfill
uv run ingest --awards --salaries --rosters
uv run ingest --analytics-only --analytics-view vw_player_season_totals --analytics-output out.csv
uv run ingest --pbp-limit 50 --include-playoffs

# Validation
uv run pytest
uv run pytest --cov=src --cov-report=term-missing
uv run ruff check .
uv run ruff format .
uv run ty check
```

## NOTES

- Entry command: `ingest = "src.pipeline.cli:main"` (via `pyproject.toml`)
- Pre-commit hook: `scripts/sync_agents.py` mirrors `AGENTS.md` → `GEMINI.md` / `CLAUDE.md`
- `mutmut` is available for mutation testing (`uv run mutmut run`)
- `src/` and `tests/` are highest-complexity; keep deep guidance in local AGENTS files
