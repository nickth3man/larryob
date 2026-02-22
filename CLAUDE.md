# PROJECT KNOWLEDGE BASE

**Generated:** 2026-02-21
**Branch:** dev
**Commit:** c86d04f

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
├── scripts/                 # Repo utility scripts
├── raw/                     # Source CSV/Parquet inputs (gitignored)
├── logs/                    # Runtime logs (gitignored)
├── .cache/                  # API cache (gitignored)
└── nba_raw_data.db          # Local SQLite DB (gitignored)
```

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| SQLite DDL and migrations | `src/db/AGENTS.md` | Includes STRICT tables and ALTER guidance |
| DuckDB view additions/changes | `src/db/AGENTS.md` | View lifecycle and naming live there |
| Loader implementation rules | `src/etl/AGENTS.md` | Idempotency, API caller, validation flow |
| Historical raw CSV backfill work | `src/etl/backfill/AGENTS.md` | Private modules + load ordering |
| CLI flags and stage planning | `src/pipeline/AGENTS.md` | Parser, validation, `_build_stage_plan()` |
| Test fixtures and isolation rules | `tests/AGENTS.md` | In-memory/temp DB constraints |

## CODE MAP

| Symbol | Location | Role |
|--------|----------|------|
| `main()` | `src/pipeline/cli.py` | Top-level CLI entrypoint for ingest runs |
| `run_ingest_pipeline()` | `src/pipeline/executor.py` | Executes planned stages and checkpoints |
| `init_db()` | `src/db/schema.py` | Initializes SQLite schema and indexes |
| `load_game_logs_for_seasons()` | `src/etl/game_logs.py` | Core box-score ingest path |
| `load_all_backfill()` | `src/etl/raw_backfill.py` | Delegates to backfill orchestrator |

## CONVENTIONS

- `season_id` format is `YYYY-YY` (e.g. `2023-24`)
- `game_id` stored as 10-char padded text (e.g. `0022301001`)
- `player_id` / `team_id` are TEXT, not INTEGER
- Early-era unavailable stats stay NULL (not 0)
- Python style: Ruff line length 100, `E/F/W/I/UP` rules enabled
- Test discovery is pinned to `tests/` via `pytest` config
- Env-driven runtime knobs use `LARRYOB_*` prefix

## ANTI-PATTERNS (THIS PROJECT)

- Never commit generated/state artifacts: `.cache/`, `raw/`, `logs/`, `*.db`, `.coverage`, `coverage.json`
- Never bypass directory-local AGENTS guidance when touching `src/db`, `src/etl`, `src/pipeline`, or `tests`
- Never treat DuckDB analytics as a write path; all persistent writes belong to SQLite loaders

## COMMANDS

```bash
# Full pipeline
uv run ingest

# Common focused runs
uv run ingest --dims-only
uv run ingest --raw-backfill
uv run ingest --awards --salaries --rosters
uv run ingest --analytics-only --analytics-view vw_player_season_totals --analytics-output out.csv

# Validation
uv run pytest
uv run pytest --cov=src --cov-report=term-missing
uv run ruff check .
uv run ruff format .
uv run ty check
```

## NOTES

- Entry command is `ingest = "src.pipeline.cli:main"` (via `pyproject.toml`)
- Pre-commit hook runs `scripts/sync_agents.py` to mirror `AGENTS.md` into `GEMINI.md`/`CLAUDE.md`
- `src/` and `tests/` are the highest-complexity trees; keep deep guidance in local AGENTS files
