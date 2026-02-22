# TESTS KNOWLEDGE BASE

## OVERVIEW
Pytest suite for the NBA analytics pipeline using isolated in-memory and temp-file database fixtures.

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

## CONVENTIONS

- **Empty database**: Use the `sqlite_con` fixture for a fresh, schema-initialized in-memory database.
- **Seeded database**: Use `sqlite_con_with_data` when testing foreign key constraints. It provides minimal reference rows (Lakers, Warriors, LeBron, Jokic, one game).
- **DuckDB testing**: Use `duck_con_with_sqlite` for analytics tests. DuckDB requires a file path for its SQLite extension, so this fixture automatically creates a temporary file copy of the seeded database.
- **API mocking**: Mock external network calls using `unittest.mock.patch` on `src.etl.api_client.APICaller`.
- **Validation testing**: Test business rules by passing raw dictionaries directly to `validate_rows()` before database insertion.
- **Metrics testing**: Clear the in-memory metrics singleton between tests if you are asserting exact call counts or row totals.

## ANTI-PATTERNS

- Never make real network requests to the NBA API or Basketball-Reference. Always mock the responses.
- Don't let `time.sleep()` slow down the test suite. Patch the `APICaller` sleep methods.
- Never hardcode file paths for temporary data or cache files. Use the built-in pytest `tmp_path` fixture.
- Avoid testing DuckDB views against an empty database. Use `duck_con_with_sqlite` to ensure joins and aggregations actually process rows.
- Don't swallow SQLite operational errors in tests unless specifically testing migration idempotency.