# OVERVIEW

CLI entry point and orchestration logic for the NBA data ingestion pipeline.

## WHERE TO LOOK

| Task | Location |
|------|----------|
| Add a new CLI argument | `cli.py` (parser) and `models.py` (`IngestConfig` fields) |
| Wire a new ETL loader into the run plan | `executor.py` (`_build_stage_plan`) |
| Define the actual execution step for a stage | `stages.py` |
| Add pure validation rules for CLI inputs | `validation.py` |
| Add a new export format (CSV, Parquet, JSON) | `analytics.py` (`EXPORTERS` registry) |
| Define a new pipeline error type | `exceptions.py` |
| Update shared regex or default values | `constants.py` |

## CONVENTIONS

- Keep validation pure. Functions in `validation.py` have no side effects and raise typed exceptions from `exceptions.py`.
- Treat `IngestConfig` as an immutable data structure. It uses `__slots__` for memory efficiency.
- Track mutable state in `CheckpointState`. Update it after each stage completes.
- Run metrics finalization in a `finally` block. This ensures cleanup even if a stage fails.
- Resolve output paths fully. Use `expanduser` and `resolve` in `analytics.py` for reliable logging.
- Parameterize DuckDB queries where possible. Since DuckDB doesn't support parameterized table names, validate view names strictly.

## ANTI-PATTERNS

- Don't put I/O or side effects in `validation.py`.
- Avoid importing other pipeline modules into `validation.py`. This prevents circular dependencies with `models.py`.
- Don't leave database connections hanging. Manage the connection lifecycle explicitly in `cli.py` and clean up thread-local caches in `analytics.py`.
- Stop using generic `ValueError` for complex validation. Raise specific errors from `exceptions.py` with context.
- Don't skip checkpointing. Every stage must update the `CheckpointState` so the pipeline can resume or report accurately.
