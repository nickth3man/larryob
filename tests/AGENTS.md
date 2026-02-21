# tests — Test Suite

## OVERVIEW

pytest suite. Never touches `nba_raw_data.db` — all tests use in-memory SQLite or `tmp_path`. DuckDB tests require a temp file (DuckDB sqlite_scanner can't attach `:memory:`).

## FILES

| File | Tests |
|------|-------|
| `conftest.py` | Shared fixtures (see below) |
| `test_schema.py` | DDL idempotency, index creation |
| `test_analytics.py` | DuckDB view queries against seeded data |
| `test_etl_dimensions.py` | `dimensions.py` loaders |
| `test_etl_game_logs.py` | `game_logs.py` loaders |
| `test_etl_play_by_play.py` | PBP loader |
| `test_etl_roster.py` | Roster loader |
| `test_etl_salaries.py` | Salary loader (unit) |
| `test_etl_salaries_integration.py` | Salary loader (integration) |
| `test_etl_awards.py` | Awards loader |
| `test_etl_helpers.py` | Pure helper functions |
| `test_etl_models.py` | Pydantic model validation |
| `test_etl_utils.py` | `upsert_rows`, `call_with_backoff`, cache |
| `test_etl_validate.py` | Business-rule validation |
| `test_ingest_integration.py` | End-to-end ingest pipeline smoke test |

## FIXTURES (conftest.py)

| Fixture | Provides |
|---------|---------|
| `sqlite_con` | In-memory SQLite with full schema (DDL + ALTERs applied) |
| `sqlite_con_with_data` | Above + seeded: 1 season, 2 teams (LAL, GSW), 2 players (LeBron, Jokic), 1 game |
| `duck_con_with_sqlite` | DuckDB `:memory:` with `sqlite_con_with_data` backed up to `tmp_path` and attached as `nba` |

## CONVENTIONS

- All tests use fixtures — never `sqlite3.connect("nba_raw_data.db")`
- DuckDB tests always use `duck_con_with_sqlite` fixture (file-backed temp copy)
- `assert` is allowed in tests (ruff S101 suppressed for `tests/*`)
- Test files named `test_etl_<module>.py` matching `src/etl/<module>.py`
- Integration tests may be slow (API mocking); unit tests should be fast

## COMMANDS

```bash
uv run pytest                          # all tests
uv run pytest tests/test_analytics.py # single file
uv run pytest -k "test_upsert"         # by name pattern
uv run pytest --cov=src --cov-report=term-missing
```

## ANTI-PATTERNS

- Never connect to the real `nba_raw_data.db` in any test
- Never skip DuckDB temp-file workaround — `sqlite_scanner` requires a real file path
- Never mock `upsert_rows` in integration tests — use fixture databases
