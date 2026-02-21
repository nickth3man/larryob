# COVERAGE_PLAN

## Phase Status
- Phase 1: Complete
- Phase 2: Complete
- Phase 3: Complete
- Phase 4: Complete (with documented residual gaps)

## 1) Phase 1 - Structural Inventory

### 1.1 Structure
- Entrypoint: `ingest.py`
- Source: `src/db/`, `src/etl/`, `src/etl/backfill/`
- Tests: `tests/` with `test_*.py` files and shared fixtures in `tests/conftest.py`
- Config: `pyproject.toml`, `uv.lock`
- Local data/artifacts present: `.cache/`, `raw/`, `nba_raw_data.db`, `.coverage`, `coverage.json`

### 1.2 Stack
- Language: Python 3.13
- Package manager: `uv`
- Test runner/assertions: `pytest` + plain `assert`
- Mocking: `unittest.mock`, `pytest.monkeypatch`
- Coverage: `pytest-cov`
- Lint/type: `ruff`, `ty`

### 1.3 Test/Tooling Config (law)
- `pyproject.toml`:
- `tool.pytest.ini_options.testpaths = ["tests"]`
- `tool.ruff.lint.select = ["E", "F", "W", "I", "UP"]`
- Dev deps include `pytest`, `pytest-cov`, `ruff`, `ty`
- Not present: `pytest.ini`, `tox.ini`, `.coveragerc`, `.github/workflows`

### 1.4 Test Convention Sample (>=5 files read)
- `tests/test_schema.py`
- `tests/test_etl_utils.py`
- `tests/test_etl_game_logs.py`
- `tests/test_analytics.py`
- `tests/test_ingest_integration.py`
- Pattern followed: file name `test_*.py`, function name `test_*`, explicit arrange/act/assert flow, absolute imports from `src.*`

## 2) Phase 1 - Baseline Metrics

- Baseline test run: `uv run pytest`
- Result: 211 collected, 210 passed, 1 failed
- Duration: 1.78s
- Pre-existing failing test at baseline: `tests/test_analytics.py::test_views_are_queryable`

- Baseline coverage run: `uv run pytest --cov=src --cov-branch --cov-report=json:coverage_phase1.json`
- Line coverage: 63.54%
- Branch coverage: 54.35%
- Function coverage (approximate AST/executed-line heuristic): 74.11% (83/112)
- Uncovered `src/` file count: 19

## 3) Phase 1 - Priority Ranking (Top 30 requested)

Formula used:
`Priority = (Inbound Imports * 3) + (Cyclomatic Complexity * 2) + (LOC * 0.5) - (Coverage * 4)`

Notes:
- Ranked universe: `src/**/*.py` plus `ingest.py`
- Files with 100% coverage and 0 complexity ignored per instruction
- Applicable ranked files: 21

| Rank | File | Inbound | Complexity | LOC | Coverage % | Priority |
|---:|---|---:|---:|---:|---:|---:|
| 1 | `src/etl/backfill/_dims.py` | 0 | 32 | 218 | 9.79 | 133.84 |
| 2 | `ingest.py` | 0 | 12 | 160 | 0.00 | 104.00 |
| 3 | `src/etl/dimensions.py` | 3 | 70 | 511 | 77.21 | 95.66 |
| 4 | `src/etl/backfill/_awards.py` | 0 | 23 | 159 | 9.30 | 88.29 |
| 5 | `src/etl/backfill/_season_stats.py` | 0 | 19 | 182 | 13.19 | 76.25 |
| 6 | `src/etl/backfill/_games.py` | 0 | 24 | 131 | 11.70 | 66.69 |
| 7 | `src/etl/backfill/_advanced_stats.py` | 0 | 19 | 172 | 14.81 | 64.74 |
| 8 | `src/etl/backfill/_game_logs.py` | 0 | 18 | 141 | 12.94 | 54.74 |
| 9 | `src/db/analytics.py` | 0 | 11 | 715 | 82.61 | 49.07 |
| 10 | `src/db/schema.py` | 2 | 8 | 543 | 72.09 | 5.13 |
| 11 | `src/etl/salaries.py` | 1 | 54 | 420 | 81.45 | -4.81 |
| 12 | `src/etl/backfill/_draft.py` | 0 | 11 | 44 | 29.03 | -72.13 |
| 13 | `src/etl/utils.py` | 8 | 38 | 278 | 90.62 | -123.50 |
| 14 | `src/etl/roster.py` | 1 | 19 | 142 | 65.91 | -151.64 |
| 15 | `src/etl/game_logs.py` | 1 | 17 | 286 | 87.69 | -170.77 |
| 16 | `src/etl/validate.py` | 3 | 35 | 204 | 90.30 | -180.19 |
| 17 | `src/etl/backfill/_orchestrator.py` | 0 | 2 | 57 | 56.52 | -193.59 |
| 18 | `src/etl/awards.py` | 1 | 33 | 147 | 92.13 | -226.00 |
| 19 | `src/etl/play_by_play.py` | 1 | 20 | 221 | 95.74 | -229.48 |
| 20 | `src/etl/helpers.py` | 0 | 7 | 103 | 95.74 | -317.48 |
| 21 | `src/etl/models.py` | 0 | 17 | 85 | 100.00 | -323.50 |

## 4) Phase 1 - Dependency / Side-Effect Map (Top 20)

Mock boundaries identified:
- `src/etl/backfill/_dims.py`: filesystem CSV (`pd.read_csv`), SQLite writes, module constant `RAW_DIR`
- `ingest.py`: environment loading (`load_dotenv`), DB init and ETL orchestration calls
- `src/etl/dimensions.py`: `nba_api` static/endpoints, `time.sleep`, `datetime.now`, SQLite writes
- `src/etl/backfill/_awards.py`: filesystem CSV, SQLite writes
- `src/etl/backfill/_season_stats.py`: filesystem CSV, SQLite writes, validation filters
- `src/etl/backfill/_games.py`: filesystem CSV, season/date conversion helpers, SQLite writes
- `src/etl/backfill/_advanced_stats.py`: filesystem CSV, validation filters, SQLite writes
- `src/etl/backfill/_game_logs.py`: filesystem CSV chunking, validation filters, SQLite writes
- `src/db/analytics.py`: DuckDB connection, sqlite extension attach, thread-local singleton cache
- `src/db/schema.py`: SQLite connect/init/rollback and migration alters
- `src/etl/salaries.py`: HTTP (`requests.get`), HTML parsing (`pd.read_html`), sleeps/retries, SQLite writes
- `src/etl/backfill/_draft.py`: filesystem CSV, SQLite writes
- `src/etl/utils.py`: module-level cache directory creation, cache file I/O, backoff timing, run-log writes
- `src/etl/roster.py`: `nba_api` team roster endpoint, sleep pacing, SQLite writes
- `src/etl/game_logs.py`: `nba_api` game log endpoint, cache, sleep pacing, SQLite writes
- `src/etl/validate.py`: row-rule filtering and date parsing
- `src/etl/backfill/_orchestrator.py`: ordered fan-out to all backfill loaders with per-loader exception handling
- `src/etl/awards.py`: `nba_api` player awards endpoint, SQLite writes
- `src/etl/play_by_play.py`: `nba_api` PBP endpoint, sleep pacing, SQLite writes
- `src/etl/helpers.py`: pure transforms plus `pandas.isna`

## 5) Phase 2 - Static Analysis & Remediation

Commands run:
- `uv run ruff check .`
- `uv run ty check`

Initial findings fixed:
- `tests/test_analytics.py`: `_view_sql` helper returned `None` and caused baseline test failure
- `tests/conftest.py`: generator fixture return annotation mismatch
- `tests/test_ingest_integration.py`: stale singleton attributes (`_cached_*`) replaced with current `_local` cache handling

Post-remediation status:
- `ruff`: pass
- `ty`: pass
- Full suite: pass

Existing test modifications (required documentation):
- `tests/test_analytics.py`: fixed provably broken helper (`_view_sql`) so test asserts real SQL text
- `tests/conftest.py`: corrected fixture return type annotation for type checker correctness
- `tests/test_ingest_integration.py`: updated cleanup to current analytics cache model

## 6) Phase 3 - Test Generation Work Completed

New test files added:
- `tests/test_etl_backfill_dims.py`
- `tests/test_etl_backfill_games.py`
- `tests/test_etl_backfill_season_stats.py`
- `tests/test_etl_backfill_advanced_stats.py`
- `tests/test_etl_backfill_game_logs.py`
- `tests/test_etl_backfill_draft.py`
- `tests/test_etl_backfill_awards.py`
- `tests/test_etl_backfill_orchestrator.py`

Additional branch-focused tests added to existing files:
- `tests/test_analytics.py` (thread-local init/rebuild/close-error branches)
- `tests/test_schema.py` (init error path + rollback paths)

Validation cadence followed repeatedly:
- Full suite run after each module batch
- Coverage re-generated after each module batch

## 7) Phase 4 - Uncovered Branch Audit

Mutation testing check:
- `mutmut` not installed (`uv run mutmut --version` failed: program not found)
- No mutation run performed

Top-20 residual uncovered coverage after final sweep:
- `ingest.py`: not in configured coverage scope (`--cov=src`)
- Reachable but still undercovered:
- `src/etl/dimensions.py`
- `src/etl/salaries.py`
- `src/etl/utils.py`
- `src/etl/roster.py`
- `src/etl/game_logs.py`
- `src/etl/validate.py`
- `src/etl/awards.py`
- `src/etl/play_by_play.py`
- `src/etl/helpers.py`

Rationale:
- Remaining gaps are concentrated in live-API/retry/sleep/network branches and deeper edge-path logic in existing non-backfill loaders.
- Backfill pipeline and DB factory/schema high-risk gaps were fully closed first; remaining modules need a second dedicated pass to fully saturate branch paths without over-mocking internals.

Dead-code exclusions added:
- None

## 8) Final Before/After Report

Coverage (`src/`, branch enabled):
- Before (Phase 1 baseline):
- Line: 63.54%
- Branch: 54.35%
- Function (approx): 74.11%
- Uncovered files: 19
- After (final):
- Line: 90.72%
- Branch: 85.57%
- Function (approx): 95.54% (107/112)
- Uncovered files: 9

Test suite:
- Before: 211 tests (210 passed, 1 failed)
- After: 252 tests (252 passed)
- Tests added: 41

Files skipped (with justification):
- No files skipped in backfill/db priority tranche.
- Remaining uncovered branches in non-backfill top-20 modules are reachable and documented for follow-up pass.

Bugs discovered:
- Baseline bug in `tests/test_analytics.py` helper (`_view_sql`) returning `None`; corrected in Phase 2.
- Resource warning signal in `tests/test_ingest_integration.py::test_ingest_dims_only` (unclosed sqlite connections). This did not fail tests; left as follow-up cleanup.

## 9) Recommended Follow-up

1. Add a dedicated branch-completion pass for `src/etl/dimensions.py`, `src/etl/salaries.py`, and `src/etl/roster.py` (largest remaining reachable branch gaps).
2. Add test harness helpers for deterministic retry/time behavior in live API loaders to reduce mock setup noise.
3. Decide whether coverage scope should include `ingest.py` (currently excluded by `--cov=src` baseline convention).
