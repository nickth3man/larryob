# Full-History Rollout Checklist

**Date:** 2026-02-28
**Goal:** Verify NBA full-history completeness implementation is ready for production use.

## Pre-Rollout Verification

### 1. Schema Verification
- [x] New tables created: `dim_coach`, `fact_team_coach_game`, `dim_player_identifier`, `dim_team_identifier`
- [x] New table: `etl_source_fingerprint`
- [x] All indexes created successfully

### 2. Pipeline Configuration
- [x] `DEFAULT_SEASONS` generates full history (1946-47 to present)
- [x] CLI defaults enable: awards, salaries, rosters, include_playoffs
- [x] `--no-*` flags available to disable optional stages

### 3. Data Quality Fixes
- [x] Team history backfill preserves historical franchises
- [x] Player/team identifier resolver creates placeholders
- [x] Advanced stats percentages normalized to 0-1 scale
- [x] Early-era rebound nullability preserved

### 4. Tracking Infrastructure
- [x] Source fingerprint tracking available
- [x] Completeness audit reports violations
- [x] CI gate includes completeness audit

## Verification Commands

Run these commands to verify the implementation:

```bash
# 1. Run all tests
uv run pytest -q

# 2. Lint and format check
uv run ruff check . && uv run ruff format --check .

# 3. Type check
uv run ty check

# 4. Run completeness audit
uv run python scripts/completion_audit.py --db-path data/databases/nba_raw_data.db

# 5. Run completeness audit with enforcement (will fail if incomplete)
uv run python scripts/completion_audit.py --db-path data/databases/nba_raw_data.db --enforce
```

## Expected Results

1. **Tests:** All 996+ tests pass
2. **Lint:** Zero ruff errors
3. **Type check:** Zero type errors
4. **Audit:** Reports current completion state
5. **Enforcement:** May fail if database is incomplete (expected for initial rollout)

## Post-Rollout Actions

- [ ] Run full-history ingest on production database
- [ ] Verify completeness audit passes
- [ ] Update documentation with final completion metrics
- [ ] Monitor CI for completeness gate failures

## Rollback Plan

If issues are discovered:
1. Revert CLI defaults to optional mode
2. Disable completeness enforcement in CI
3. Review and fix identified issues
4. Re-enable after fixes verified
