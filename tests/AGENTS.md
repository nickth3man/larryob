# tests — Pytest Suite

## OVERVIEW

Pytest suite for the NBA analytics pipeline using isolated in-memory and temp-file database fixtures. 30 test files, ~7k lines. No test ever touches `nba_raw_data.db`.

## WHERE TO LOOK

| Task | Location |
|------|----------|
| Shared database fixtures | `conftest.py` |
| Core ETL loader tests | `test_etl_*.py` (e.g., `test_etl_game_logs.py`) |
| CSV backfill tests | `test_etl_backfill_*.py` |
| ETL infrastructure | `test_etl_utils.py`, `test_etl_api_client.py` |
| Pydantic model validation | `test_etl_models.py` |
| Business rule validation | `test_etl_validate.py` |
| Pipeline orchestration | `test_ingest_integration.py` |
| Schema and DDL tests | `test_schema.py` |
| DuckDB analytics views | `test_analytics.py` |

## FIXTURES (`conftest.py`)

| Fixture | What it provides | When to use |
|---------|-----------------|-------------|
| `sqlite_con` | In-memory SQLite with full schema + migrations applied | Default for any loader test |
| `sqlite_con_with_data` | Extends `sqlite_con` with minimal FK seed rows (Lakers, Warriors, LeBron, Jokic, one game) | When testing FK constraints or joins |
| `duck_con_with_sqlite` | In-memory DuckDB with the seeded SQLite attached as `nba` via `sqlite_scanner` | For all analytics view tests |

**`duck_con_with_sqlite` mechanics**: DuckDB's sqlite extension requires a file path. The fixture uses `sqlite_con_with_data.backup()` to write the in-memory DB to `tmp_path/test_nba.db`, then attaches it. The fixture cleans up automatically.

## CONVENTIONS

- **Empty database**: Use `sqlite_con` for a fresh, schema-initialized in-memory database
- **Seeded database**: Use `sqlite_con_with_data` when testing foreign key constraints
- **DuckDB testing**: Use `duck_con_with_sqlite` — never test views against empty DB (joins/aggs return nothing)
- **API mocking**: `unittest.mock.patch` on `src.etl.api_client.APICaller`; also patch `APICaller` sleep methods to avoid `time.sleep()` in tests
- **Validation testing**: Pass raw dicts directly to `validate_rows()` before DB insertion
- **Metrics testing**: Clear the in-memory metrics singleton between tests when asserting exact counts
- **Parametrize**: Use `@pytest.mark.parametrize` for testing multiple season IDs, stat variants, and edge cases

## ANTI-PATTERNS

- Never make real network requests to nba_api or Basketball-Reference — always mock
- Never let `time.sleep()` run in tests — patch `APICaller.sleep_between_calls` and `APICaller.call_with_backoff`
- Never hardcode file paths — use `tmp_path` fixture for all temporary data and cache files
- Never test DuckDB views against an empty database — use `duck_con_with_sqlite`
- Never swallow `sqlite3.OperationalError` in tests unless specifically testing migration idempotency
- Never import from `src.etl.backfill._*` modules directly in tests — use the public `run_raw_backfill` API
