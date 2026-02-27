# Project Reorganization Design

**Date:** 2025-02-27  
**Topic:** Project-Wide Organization and Modularity  
**Status:** Approved

---

## Overview

This project requires organization to improve modularity and future scalability. The primary issues are:

1. **Cluttered root directory:** Database files, coverage reports, AI instruction files, and cache all mixed together
2. **Files exceeding 400-line limit:** `src/etl/salaries.py` (400 lines), `src/pipeline/cli.py` (391 lines), and many others approaching the threshold
3. **Monolithic module structure:** ETL modules mix extraction, transformation, and loading concerns

---

## Goals

1. Move all root-level files to appropriate directories
2. Enforce the 400-line file limit per `AGENTS.md` guidelines
3. Restructure `src/etl/` into domain-driven packages (extract/transform/load)
4. Maintain existing functionality throughout the reorganization
5. Ensure all tests pass after reorganization

---

## 1. Root Directory Cleanup

### Files to Relocate

| Current Location | New Location | Rationale |
|------------------|--------------|-----------|
| `GEMINI.md`, `CLAUDE.md`, `AGENTS.md` | `docs/agents/` | AI instruction files grouped together |
| `nba_raw_data.db` | `data/databases/` | Database files separate from source |
| `.coverage`, `coverage.json` | `reports/coverage/` | Test artifacts in dedicated reports directory |
| `.cache/` (entire directory) | `data/cache/` | Cache files moved to data folder |

### New Root Directory Structure

```
larryob/
├── data/               # NEW: Data storage
│   ├── cache/         # Moved from .cache/
│   └── databases/     # Database files
├── docs/              # Documentation
│   ├── agents/        # AI instruction files (moved)
│   └── plans/         # Design documents
├── reports/           # NEW: Generated reports
│   └── coverage/      # Coverage reports (moved)
├── src/               # Source code (restructured)
├── tests/             # Tests
├── pyproject.toml
├── README.md
├── CONTRIBUTING.md
├── ARCHITECTURE.md
├── .env.example
├── uv.lock
└── .gitignore
```

---

## 2. Source Code Domain-Driven Restructure

### Current Structure

```
src/
├── __init__.py
├── db/
│   ├── cache/
│   ├── operations/
│   ├── schema/
│   └── tracking/
├── etl/
│   ├── api_client.py
│   ├── awards.py
│   ├── backfill/
│   ├── dimensions.py
│   ├── game_logs.py
│   ├── metrics.py
│   ├── play_by_play.py
│   ├── rate_limit.py
│   ├── salaries.py
│   └── ...helpers
└── pipeline/
    ├── cli.py
    ├── executor.py
    └── __init__.py
```

### Proposed Structure

```
src/
├── config/                    # NEW: Centralized configuration
│   ├── __init__.py
│   └── settings.py           # Consolidated settings
├── core/                     # NEW: Shared utilities
│   ├── __init__.py
│   ├── base.py              # Base classes
│   └── utils.py             # Shared utilities
├── db/
│   ├── __init__.py
│   ├── schema/              # DDL files
│   ├── operations/
│   │   ├── __init__.py
│   │   ├── upsert.py
│   │   └── utils.py
│   ├── cache/
│   │   ├── __init__.py
│   │   └── file_cache.py
│   └── tracking/
│       ├── __init__.py
│       └── etl_log.py
├── etl/
│   ├── __init__.py
│   ├── extract/             # NEW: Data extraction
│   │   ├── __init__.py
│   │   ├── api_client.py   # Moved from etl/api_client.py
│   │   ├── endpoints/      # NEW: API endpoint definitions
│   │   │   ├── __init__.py
│   │   │   ├── games.py
│   │   │   ├── players.py
│   │   │   └── stats.py
│   │   └── rate_limit.py   # Moved from etl/rate_limit.py
│   ├── transform/           # NEW: Data transformation
│   │   ├── __init__.py
│   │   ├── dimensions.py   # Split/refactored from etl/dimensions.py
│   │   ├── game_logs.py    # Split/refactored from etl/game_logs.py
│   │   └── play_by_play.py # Split/refactored from etl/play_by_play.py
│   ├── load/                # NEW: Data loading
│   │   ├── __init__.py
│   │   └── bulk.py         # Bulk loading operations
│   ├── backfill/            # Existing backfill scripts
│   │   ├── __init__.py
│   │   ├── _base.py
│   │   ├── _advanced_stats/  # Split from _advanced_stats.py
│   │   │   ├── __init__.py
│   │   │   ├── base.py
│   │   │   ├── player.py
│   │   │   └── team.py
│   │   ├── _awards.py
│   │   ├── _dims.py
│   │   └── ...other scripts
│   ├── salaries/            # Split from etl/salaries.py
│   │   ├── __init__.py
│   │   ├── extractor.py
│   │   ├── transformer.py
│   │   └── loader.py
│   ├── metrics/             # Split from etl/metrics.py
│   │   ├── __init__.py
│   │   ├── calculator.py
│   │   └── reporter.py
│   └── dimensions/          # Split from etl/dimensions.py
│       ├── __init__.py
│       ├── players.py
│       ├── teams.py
│       └── seasons.py
└── pipeline/
    ├── __init__.py
    ├── cli/                  # Split from pipeline/cli.py
    │   ├── __init__.py
    │   ├── main.py          # Entry point
    │   ├── commands.py      # Command definitions
    │   ├── args.py          # Argument parsing
    │   └── runner.py        # Command execution
    └── executor/             # Split from pipeline/executor.py
        ├── __init__.py
        ├── base.py
        ├── steps.py
        └── orchestrator.py
```

---

## 3. File Splitting Details

### Files Exceeding 400 Lines

| File | Lines | Split Plan |
|------|-------|------------|
| `src/etl/salaries.py` | 400 | Split into `salaries/extractor.py` (extraction), `salaries/transformer.py` (data transformation), `salaries/loader.py` (database loading) |
| `src/pipeline/cli.py` | 391 | Split into `cli/commands.py` (command definitions), `cli/args.py` (argument parsing), `cli/runner.py` (command execution logic) |
| `src/pipeline/executor.py` | 287 | Split into `executor/steps.py` (step definitions), `executor/orchestrator.py` (execution flow) |

### Files Near Limit (Preemptive Split)

| File | Lines | Split Plan |
|------|-------|------------|
| `src/etl/backfill/_advanced_stats.py` | 373 | Split into `_advanced_stats/base.py`, `_advanced_stats/player.py`, `_advanced_stats/team.py` |
| `src/etl/backfill/_orchestrator.py` | 355 | Split orchestration logic into dedicated coordinator module |
| `src/etl/backfill/_pbp_bulk.py` | 344 | Split bulk operations into `load/bulk.py` |
| `src/etl/metrics.py` | 342 | Split into `metrics/calculator.py`, `metrics/reporter.py` |
| `src/etl/backfill/_dims.py` | 320 | Extract common dimension handling to `dimensions/` package |
| `src/etl/backfill/_awards.py` | 320 | Consider splitting award-specific logic |
| `src/etl/dimensions.py` | 318 | Split into `dimensions/players.py`, `dimensions/teams.py`, `dimensions/seasons.py` |
| `src/etl/game_logs.py` | 313 | Split transformation logic to `transform/game_logs.py` |
| `src/etl/backfill/_base.py` | 304 | Review for potential extraction of common patterns |
| `src/etl/backfill/_season_stats.py` | 299 | Consider splitting by stat type |

---

## 4. Import Path Updates

All internal imports must be updated to reflect new module locations:

### Example Changes

```python
# Before
from src.etl.api_client import NBAApiClient
from src.etl.salaries import process_salaries

# After
from src.etl.extract.api_client import NBAApiClient
from src.etl.salaries.extractor import extract_salaries
from src.etl.salaries.transformer import transform_salaries
from src.etl.salaries.loader import load_salaries
```

---

## 5. Configuration Updates

### pyproject.toml

- Update tool configurations to reference new paths
- Update coverage report directory to `reports/coverage/`
- Update cache directory references to `data/cache/`

### Environment Files

- Update `.env.example` to reference new database paths
- Ensure `.gitignore` ignores `data/` contents appropriately

---

## 6. Testing Considerations

- All test imports must be updated
- Test file structure should mirror new `src/` structure
- Database path references in tests need updating
- Coverage configuration paths need updating

---

## 7. Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Import errors | Comprehensive import path audit |
| Broken tests | Update test imports, verify all pass |
| Missing files | Use git to track all moves (preserve history) |
| Configuration drift | Update pyproject.toml, .env files |
| Cache/database path issues | Update all path references systematically |

---

## 8. Success Criteria

- [ ] All root files moved to appropriate directories
- [ ] No files exceed 400 lines
- [ ] All tests pass
- [ ] Coverage reports generate to correct location
- [ ] Database operations work correctly
- [ ] CLI functions correctly
- [ ] Cache operations work correctly

---

## Approval

**Approved by:** nickth3man  
**Date:** 2025-02-27  
**Approach:** Aggressive Restructure (Domain-Driven)
