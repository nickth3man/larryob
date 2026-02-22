# src/pipeline — CLI Orchestration Layer

## OVERVIEW

CLI entry point and stage orchestration for the NBA ingest pipeline. Manages argument parsing, config building, stage plan construction, checkpoint logging, analytics export, and the full exception hierarchy.

## FILES

| File | Role |
|------|------|
| `cli.py` | `main()` entry; argument parser factory, connection lifecycle, exit codes |
| `executor.py` | `run_ingest_pipeline()`, `_build_stage_plan()`, `_execute_stage()` |
| `stages.py` | Thin stage runner functions — adapters over `src/etl/` loaders |
| `models.py` | `Stage` (StrEnum), `IngestConfig` (`__slots__` dataclass), `CheckpointState` |
| `constants.py` | `DEFAULT_SEASONS`, `*_TABLES` tuples, `StageFn` type alias, compiled regex patterns |
| `validation.py` | Pure input validators; raises typed exceptions; no I/O, no side effects |
| `exceptions.py` | `IngestError` → `ReconciliationError`, `AnalyticsError`, `ValidationError` |
| `checkpoint.py` | `log_checkpoint()` — row-count snapshots + etl_run_log tail between stages |
| `analytics.py` | `run_analytics_view()` — queries DuckDB views; `EXPORTERS` registry (.csv/.parquet/.json) |

## WHERE TO LOOK

| Task | Location |
|------|----------|
| Add a new CLI argument | `cli.py` (parser) **and** `models.py` (`IngestConfig` fields) |
| Wire a new ETL loader into the run plan | `executor.py` (`_build_stage_plan`) |
| Define the actual execution step for a stage | `stages.py` |
| Add pure validation rules for CLI inputs | `validation.py` |
| Add a new export format (CSV, Parquet, JSON) | `analytics.py` (`EXPORTERS` registry) |
| Define a new pipeline error type | `exceptions.py` |
| Update shared regex or default values | `constants.py` |
| Add a new stage checkpoint | `checkpoint.py` + `constants.py` (add to `*_TABLES`) |

## STAGE PLAN (execution order)

```
DIMENSIONS (always)
  → RAW_BACKFILL (if --raw-backfill and not --dims-only)
  → AWARDS (if --awards)
  → SALARIES (if --salaries)
  → ROSTERS (if --rosters)
  → GAME_LOGS (unless --dims-only)
    → reconciliation (unless --skip-reconciliation)
    → PBP (if --pbp-limit > 0)
```

Built dynamically by `_build_stage_plan(config)` → list of `(Stage, tables, fn, args, kwargs)`.

## EXCEPTION HIERARCHY

```
IngestError(RuntimeError)      # base; catch-all for all pipeline errors
├── ReconciliationError        # PTS/REB/AST player-vs-team mismatch; has .warning_count, .seasons
├── AnalyticsError             # view query or export failure; has .view_name, .output_path
└── ValidationError            # bad CLI args; has .argument, .value
```

All exceptions use `__slots__` and include a `context` dict for programmatic access.

## EXIT CODES (`cli.py`)

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | ValidationError |
| 2 | IngestError (incl. subclasses) |
| 3 | Unexpected exception |

## CONVENTIONS

- `IngestConfig` is immutable (`@dataclass(slots=True)`); never mutate after construction
- `CheckpointState` is deliberately mutable; call `state.update()` after each stage
- `Stage` values all have `"post-"` prefix — they mark completion, not start
- Metrics finalization always runs in `finally` block — never skip
- `validation.py` functions are pure: no imports from other `pipeline` modules (prevents circular deps with `models.py`)
- View names validated against `_VALID_IDENTIFIER` regex before any DuckDB query
- Analytics output paths always resolved with `expanduser` + `resolve`

## ANTI-PATTERNS

- Don't put I/O or side effects in `validation.py`
- Don't import other pipeline modules into `validation.py` (circular dep risk with `models.py`)
- Don't leave DB connections hanging — lifecycle managed explicitly in `cli.py`; clean thread-local cache in `analytics.py`
- Don't raise generic `ValueError` — use specific subclass from `exceptions.py` with context
- Don't skip checkpointing — every stage must call `log_checkpoint()` so progress is visible
- Don't add new stage tables to `executor.py` directly — put them in `constants.py` as `*_TABLES` tuples
