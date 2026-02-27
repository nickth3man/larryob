# Project Reorganization Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Move root files to appropriate directories and restructure src/ into domain-driven packages while splitting files exceeding 400 lines.

**Architecture:** Implement domain-driven package structure with clear separation of concerns (extract/transform/load). Use git mv to preserve history. Update all imports systematically.

**Tech Stack:** Python, pytest, ruff, uv

---

## Prerequisites

1. Working directory is `C:\Users\nicolas\Documents\GitHub\larryob`
2. Git repository is clean (all changes committed)
3. All tests currently pass

---

## Phase 1: Root Directory Cleanup

### Task 1: Create New Directory Structure

**Files:**
- Create: `docs/agents/` (directory)
- Create: `data/cache/` (directory)
- Create: `data/databases/` (directory)
- Create: `reports/coverage/` (directory)

**Step 1: Create directories**

Run:
```bash
mkdir -p docs/agents
mkdir -p data/cache
mkdir -p data/databases
mkdir -p reports/coverage
```

**Step 2: Commit directory creation**

```bash
git add docs/ data/ reports/
git commit -m "chore: create new directory structure for reorganization"
```

---

### Task 2: Move AI Instruction Files

**Files:**
- Move: `GEMINI.md` → `docs/agents/GEMINI.md`
- Move: `CLAUDE.md` → `docs/agents/CLAUDE.md`
- Move: `AGENTS.md` → `docs/agents/AGENTS.md`
- Modify: `.gitignore` (if needed)

**Step 1: Move files with git mv**

Run:
```bash
git mv GEMINI.md docs/agents/
git mv CLAUDE.md docs/agents/
git mv AGENTS.md docs/agents/
```

**Step 2: Verify moves**

Run:
```bash
ls -la docs/agents/
```
Expected: Shows GEMINI.md, CLAUDE.md, AGENTS.md

**Step 3: Commit**

```bash
git commit -m "chore: move AI instruction files to docs/agents/"
```

---

### Task 3: Move Database File

**Files:**
- Move: `nba_raw_data.db` → `data/databases/nba_raw_data.db`
- Modify: `.env` (update DB_PATH)
- Modify: `.env.example` (update example path)

**Step 1: Move database with git mv**

Run:
```bash
git mv nba_raw_data.db data/databases/
```

**Step 2: Update .env file**

Read `.env` and update any database path references from `nba_raw_data.db` to `data/databases/nba_raw_data.db`

**Step 3: Update .env.example**

Read `.env.example` and update example path

**Step 4: Verify and commit**

Run:
```bash
ls -la data/databases/
git add .env .env.example
git commit -m "chore: move database file to data/databases/"
```

---

### Task 4: Move Cache Directory

**Files:**
- Move: `.cache/` → `data/cache/`
- Modify: `.gitignore` (update cache path)

**Step 1: Move cache directory**

Run:
```bash
# First unstage the .cache if it's gitignored
git mv .cache data/
```

**Step 2: Update .gitignore**

Find `.cache` in `.gitignore` and update to `data/cache/`

**Step 3: Verify and commit**

Run:
```bash
ls -la data/
git add .gitignore
git commit -m "chore: move cache directory to data/cache/"
```

---

### Task 5: Move Coverage Reports

**Files:**
- Move: `.coverage` → `reports/coverage/.coverage`
- Move: `coverage.json` → `reports/coverage/coverage.json`
- Modify: `pyproject.toml` (update coverage paths)

**Step 1: Move coverage files**

Run:
```bash
git mv .coverage reports/coverage/
git mv coverage.json reports/coverage/
```

**Step 2: Update pyproject.toml**

Find coverage configuration and update:
- Report output directory to `reports/coverage/`
- Data file path to `reports/coverage/.coverage`

**Step 3: Verify and commit**

Run:
```bash
ls -la reports/coverage/
git add pyproject.toml
git commit -m "chore: move coverage files to reports/coverage/"
```

---

### Task 6: Update .gitignore for New Structure

**Files:**
- Modify: `.gitignore`

**Step 1: Read current .gitignore**

**Step 2: Update paths**

Replace:
```
.cache/
```
With:
```
data/cache/
```

Add:
```
data/databases/*.db
reports/coverage/
```

**Step 3: Commit**

```bash
git add .gitignore
git commit -m "chore: update .gitignore for new directory structure"
```

---

## Phase 2: Source Code Restructure - Create New Directories

### Task 7: Create src/config/

**Files:**
- Create: `src/config/__init__.py`
- Create: `src/config/settings.py`

**Step 1: Create directory and init**

```python
# src/config/__init__.py
"""Configuration module."""

from .settings import get_settings, Settings

__all__ = ["get_settings", "Settings"]
```

**Step 2: Create settings.py**

```python
# src/config/settings.py
"""Application settings and configuration."""

import os
from pathlib import Path


class Settings:
    """Application settings."""
    
    def __init__(self):
        self.project_root = Path(__file__).parent.parent.parent
        self.data_dir = self.project_root / "data"
        self.db_path = self.data_dir / "databases" / "nba_raw_data.db"
        self.cache_dir = self.data_dir / "cache"


def get_settings() -> Settings:
    """Get application settings."""
    return Settings()
```

**Step 3: Commit**

```bash
git add src/config/
git commit -m "feat: create config module for centralized settings"
```

---

### Task 8: Create src/core/

**Files:**
- Create: `src/core/__init__.py`
- Create: `src/core/base.py`

**Step 1: Create directory and init**

```python
# src/core/__init__.py
"""Core shared utilities and base classes."""

from .base import BaseETL, BaseExtractor, BaseTransformer, BaseLoader

__all__ = ["BaseETL", "BaseExtractor", "BaseTransformer", "BaseLoader"]
```

**Step 2: Create base.py**

```python
# src/core/base.py
"""Base classes for ETL operations."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List


class BaseETL(ABC):
    """Base class for ETL operations."""
    
    @abstractmethod
    def run(self) -> Any:
        """Execute the ETL operation."""
        pass


class BaseExtractor(ABC):
    """Base class for data extraction."""
    
    @abstractmethod
    def extract(self, **kwargs) -> Any:
        """Extract data from source."""
        pass


class BaseTransformer(ABC):
    """Base class for data transformation."""
    
    @abstractmethod
    def transform(self, data: Any) -> Any:
        """Transform extracted data."""
        pass


class BaseLoader(ABC):
    """Base class for data loading."""
    
    @abstractmethod
    def load(self, data: Any) -> None:
        """Load transformed data to destination."""
        pass
```

**Step 3: Commit**

```bash
git add src/core/
git commit -m "feat: create core module with base ETL classes"
```

---

### Task 9: Create src/etl/extract/

**Files:**
- Create: `src/etl/extract/__init__.py`
- Move: `src/etl/api_client.py` → `src/etl/extract/api_client.py`
- Move: `src/etl/rate_limit.py` → `src/etl/extract/rate_limit.py`
- Create: `src/etl/extract/endpoints/__init__.py`

**Step 1: Create directory structure**

Run:
```bash
mkdir -p src/etl/extract/endpoints
touch src/etl/extract/endpoints/__init__.py
```

**Step 2: Move existing files**

Run:
```bash
git mv src/etl/api_client.py src/etl/extract/
git mv src/etl/rate_limit.py src/etl/extract/
```

**Step 3: Create extract __init__.py**

```python
# src/etl/extract/__init__.py
"""Data extraction module."""

from .api_client import NBAApiClient
from .rate_limit import RateLimiter

__all__ = ["NBAApiClient", "RateLimiter"]
```

**Step 4: Update imports in moved files**

Open `src/etl/extract/api_client.py` and check if it imports from `rate_limit.py`. If yes, update:
```python
# From
from .rate_limit import RateLimiter
# Or
from ..rate_limit import RateLimiter

# To
from .rate_limit import RateLimiter
```

**Step 5: Run tests to verify**

Run:
```bash
uv run pytest tests/test_etl_api_client.py -v
```
Expected: All tests pass

**Step 6: Commit**

```bash
git add src/etl/extract/
git commit -m "refactor: move extraction modules to etl/extract/"
```

---

### Task 10: Create src/etl/transform/

**Files:**
- Create: `src/etl/transform/__init__.py`
- Move: `src/etl/game_logs.py` → `src/etl/transform/game_logs.py`
- Move: `src/etl/play_by_play.py` → `src/etl/transform/play_by_play.py`
- Move: `src/etl/_game_logs_transform.py` → `src/etl/transform/_game_logs.py`

**Step 1: Create directory**

Run:
```bash
mkdir -p src/etl/transform
```

**Step 2: Move files**

Run:
```bash
git mv src/etl/game_logs.py src/etl/transform/
git mv src/etl/play_by_play.py src/etl/transform/
git mv src/etl/_game_logs_transform.py src/etl/transform/_game_logs.py
```

**Step 3: Update imports in moved files**

Check each file for relative imports and update them:
- `game_logs.py`: Check for imports from other etl modules
- `play_by_play.py`: Check for imports from other etl modules

**Step 4: Create __init__.py**

```python
# src/etl/transform/__init__.py
"""Data transformation module."""

from .game_logs import transform_game_logs
from .play_by_play import transform_play_by_play

__all__ = ["transform_game_logs", "transform_play_by_play"]
```

**Step 5: Update files that import from old locations**

Find all files that import from old paths:
```bash
grep -r "from src.etl.game_logs import" src/
grep -r "from src.etl.play_by_play import" src/
grep -r "from src.etl._game_logs_transform import" src/
```

Update each import to new location.

**Step 6: Run tests**

Run:
```bash
uv run pytest tests/test_etl_game_logs.py -v
```
Expected: Tests pass

**Step 7: Commit**

```bash
git add src/etl/transform/
git commit -m "refactor: move transformation modules to etl/transform/"
```

---

### Task 11: Create src/etl/load/

**Files:**
- Create: `src/etl/load/__init__.py`
- Create: `src/etl/load/bulk.py` (from _pbp_bulk.py logic)

**Step 1: Create directory and __init__.py**

```python
# src/etl/load/__init__.py
"""Data loading module."""

from .bulk import bulk_load

__all__ = ["bulk_load"]
```

**Step 2: Extract bulk loading logic**

Read `src/etl/backfill/_pbp_bulk.py` and extract bulk loading functions to `src/etl/load/bulk.py`

**Step 3: Update imports in backfill files**

Update `src/etl/backfill/_pbp_bulk.py` to import from new location.

**Step 4: Run tests**

Run:
```bash
uv run pytest tests/test_etl_backfill_pbp_bulk.py -v
```
Expected: Tests pass

**Step 5: Commit**

```bash
git add src/etl/load/
git commit -m "feat: create load module with bulk loading functions"
```

---

### Task 12: Split src/etl/salaries.py

**Files:**
- Read: `src/etl/salaries.py`
- Create: `src/etl/salaries/__init__.py`
- Create: `src/etl/salaries/extractor.py`
- Create: `src/etl/salaries/transformer.py`
- Create: `src/etl/salaries/loader.py`
- Delete: `src/etl/salaries.py`

**Step 1: Read and analyze src/etl/salaries.py**

Understand the structure and identify:
- Extraction functions (API calls)
- Transformation functions (data cleaning/structuring)
- Loading functions (database operations)

**Step 2: Create directory and __init__.py**

```python
# src/etl/salaries/__init__.py
"""Salary data ETL module."""

from .extractor import extract_salaries, extract_player_salaries
from .transformer import transform_salaries, clean_salary_data
from .loader import load_salaries, upsert_salary_records

__all__ = [
    "extract_salaries",
    "extract_player_salaries",
    "transform_salaries",
    "clean_salary_data",
    "load_salaries",
    "upsert_salary_records",
]
```

**Step 3: Create extractor.py**

Extract API-related functions and salary data fetching logic.

**Step 4: Create transformer.py**

Extract data cleaning, validation, and structuring functions.

**Step 5: Create loader.py**

Extract database insertion and upsert functions.

**Step 6: Update imports throughout codebase**

Find and update all imports from `src.etl.salaries` to new submodule imports.

**Step 7: Verify file sizes**

Each file should be under 400 lines. If not, further split required.

**Step 8: Run tests**

Run:
```bash
uv run pytest tests/ -k salary -v
```
Expected: All salary-related tests pass

**Step 9: Commit**

```bash
git add src/etl/salaries/
git rm src/etl/salaries.py
git commit -m "refactor: split salaries.py into domain-driven modules

Split 400-line file into:
- salaries/extractor.py (API extraction)
- salaries/transformer.py (data transformation)
- salaries/loader.py (database loading)"
```

---

### Task 13: Split src/pipeline/cli.py

**Files:**
- Read: `src/pipeline/cli.py`
- Create: `src/pipeline/cli/__init__.py`
- Create: `src/pipeline/cli/main.py`
- Create: `src/pipeline/cli/commands.py`
- Create: `src/pipeline/cli/args.py`
- Create: `src/pipeline/cli/runner.py`
- Delete: `src/pipeline/cli.py`

**Step 1: Read and analyze src/pipeline/cli.py**

Understand the structure:
- Entry point/main function
- Command definitions
- Argument parsing
- Command execution logic

**Step 2: Create cli directory and __init__.py**

```python
# src/pipeline/cli/__init__.py
"""Pipeline CLI module."""

from .main import main
from .commands import COMMANDS
from .args import parse_args

__all__ = ["main", "COMMANDS", "parse_args"]
```

**Step 3: Create args.py**

Extract argument parsing logic.

**Step 4: Create commands.py**

Extract command definitions and mappings.

**Step 5: Create runner.py**

Extract command execution logic.

**Step 6: Create main.py**

Entry point that orchestrates arg parsing and execution.

```python
# src/pipeline/cli/main.py
"""CLI entry point."""

from .args import parse_args
from .runner import run_command


def main():
    """Main CLI entry point."""
    args = parse_args()
    run_command(args)


if __name__ == "__main__":
    main()
```

**Step 7: Update pyproject.toml entry point**

Update console_scripts to point to new location:
```toml
[project.scripts]
nba-etl = "src.pipeline.cli.main:main"
```

**Step 8: Update imports throughout codebase**

Find and update all imports from `src.pipeline.cli`.

**Step 9: Run tests**

Run:
```bash
uv run pytest tests/ -k cli -v
```
Expected: All CLI tests pass

**Step 10: Commit**

```bash
git add src/pipeline/cli/
git rm src/pipeline/cli.py
git add pyproject.toml
git commit -m "refactor: split cli.py into domain-driven modules

Split 391-line file into:
- cli/args.py (argument parsing)
- cli/commands.py (command definitions)
- cli/runner.py (command execution)
- cli/main.py (entry point)"
```

---

### Task 14: Split src/pipeline/executor.py

**Files:**
- Read: `src/pipeline/executor.py`
- Create: `src/pipeline/executor/__init__.py`
- Create: `src/pipeline/executor/steps.py`
- Create: `src/pipeline/executor/orchestrator.py`
- Delete: `src/pipeline/executor.py`

**Step 1: Read and analyze src/pipeline/executor.py**

Identify:
- Step definitions
- Orchestration logic
- Execution flow

**Step 2: Create executor directory and __init__.py**

```python
# src/pipeline/executor/__init__.py
"""Pipeline executor module."""

from .steps import Step, PipelineStep
from .orchestrator import Orchestrator, run_pipeline

__all__ = ["Step", "PipelineStep", "Orchestrator", "run_pipeline"]
```

**Step 3: Create steps.py**

Extract step-related classes and definitions.

**Step 4: Create orchestrator.py**

Extract execution flow and orchestration logic.

**Step 5: Update imports**

Find and update all imports from `src.pipeline.executor`.

**Step 6: Run tests**

Run:
```bash
uv run pytest tests/ -k executor -v
```
Expected: Tests pass

**Step 7: Commit**

```bash
git add src/pipeline/executor/
git rm src/pipeline/executor.py
git commit -m "refactor: split executor.py into domain-driven modules

Split 287-line file into:
- executor/steps.py (step definitions)
- executor/orchestrator.py (execution orchestration)"
```

---

### Task 15: Split src/etl/dimensions.py

**Files:**
- Read: `src/etl/dimensions.py`
- Create: `src/etl/dimensions/__init__.py`
- Create: `src/etl/dimensions/players.py`
- Create: `src/etl/dimensions/teams.py`
- Create: `src/etl/dimensions/seasons.py`
- Move: `src/etl/_dimensions_helpers.py` → `src/etl/dimensions/helpers.py`
- Delete: `src/etl/dimensions.py`

**Step 1: Read and analyze dimensions files**

Understand structure of `src/etl/dimensions.py` and `src/etl/_dimensions_helpers.py`.

**Step 2: Create dimensions directory and __init__.py**

```python
# src/etl/dimensions/__init__.py
"""Dimension data ETL module."""

from .players import process_players, load_players
from .teams import process_teams, load_teams
from .seasons import process_seasons, load_seasons
from .helpers import dimension_helper_functions

__all__ = [
    "process_players",
    "load_players",
    "process_teams",
    "load_teams",
    "process_seasons",
    "load_seasons",
]
```

**Step 3: Create players.py**

Extract player dimension logic.

**Step 4: Create teams.py**

Extract team dimension logic.

**Step 5: Create seasons.py**

Extract season dimension logic.

**Step 6: Move and rename helpers**

```bash
git mv src/etl/_dimensions_helpers.py src/etl/dimensions/helpers.py
```

**Step 7: Update imports**

Find and update all imports from old paths.

**Step 8: Run tests**

Run:
```bash
uv run pytest tests/ -k dimension -v
```
Expected: Tests pass

**Step 9: Commit**

```bash
git add src/etl/dimensions/
git rm src/etl/dimensions.py
git commit -m "refactor: split dimensions.py into domain-driven modules

Split 318-line file into:
- dimensions/players.py
- dimensions/teams.py
- dimensions/seasons.py
- dimensions/helpers.py"
```

---

### Task 16: Split src/etl/metrics.py

**Files:**
- Read: `src/etl/metrics.py`
- Create: `src/etl/metrics/__init__.py`
- Create: `src/etl/metrics/calculator.py`
- Create: `src/etl/metrics/reporter.py`
- Delete: `src/etl/metrics.py`

**Step 1: Read and analyze src/etl/metrics.py**

**Step 2: Create metrics directory and __init__.py**

```python
# src/etl/metrics/__init__.py
"""Metrics calculation and reporting module."""

from .calculator import calculate_metrics, calculate_player_metrics
from .reporter import generate_report, format_metrics

__all__ = ["calculate_metrics", "calculate_player_metrics", "generate_report", "format_metrics"]
```

**Step 3: Create calculator.py**

Extract metrics calculation functions.

**Step 4: Create reporter.py**

Extract reporting and formatting functions.

**Step 5: Update imports**

**Step 6: Run tests**

Run:
```bash
uv run pytest tests/test_etl_metrics.py -v
```
Expected: Tests pass

**Step 7: Commit**

```bash
git add src/etl/metrics/
git rm src/etl/metrics.py
git commit -m "refactor: split metrics.py into calculator and reporter modules"
```

---

### Task 17: Split src/etl/backfill/_advanced_stats.py

**Files:**
- Read: `src/etl/backfill/_advanced_stats.py`
- Create: `src/etl/backfill/_advanced_stats/__init__.py`
- Create: `src/etl/backfill/_advanced_stats/base.py`
- Create: `src/etl/backfill/_advanced_stats/player.py`
- Create: `src/etl/backfill/_advanced_stats/team.py`
- Delete: `src/etl/backfill/_advanced_stats.py`

**Step 1: Read and analyze _advanced_stats.py**

**Step 2: Create _advanced_stats directory and __init__.py**

```python
# src/etl/backfill/_advanced_stats/__init__.py
"""Advanced stats backfill module."""

from .base import BaseAdvancedStatsBackfill
from .player import PlayerAdvancedStatsBackfill
from .team import TeamAdvancedStatsBackfill

__all__ = ["BaseAdvancedStatsBackfill", "PlayerAdvancedStatsBackfill", "TeamAdvancedStatsBackfill"]
```

**Step 3: Create base.py**

Extract base class and shared logic.

**Step 4: Create player.py**

Extract player-specific advanced stats logic.

**Step 5: Create team.py**

Extract team-specific advanced stats logic.

**Step 6: Update imports**

**Step 7: Run tests**

Run:
```bash
uv run pytest tests/test_etl_backfill_advanced_stats.py -v
```
Expected: Tests pass

**Step 8: Commit**

```bash
git add src/etl/backfill/_advanced_stats/
git rm src/etl/backfill/_advanced_stats.py
git commit -m "refactor: split _advanced_stats.py into domain-driven modules

Split 373-line file into:
- _advanced_stats/base.py
- _advanced_stats/player.py
- _advanced_stats/team.py"
```

---

## Phase 3: Update Test Structure

### Task 18: Update Test Imports

**Files:**
- All test files in `tests/`

**Step 1: Find all files importing from old paths**

Run:
```bash
grep -r "from src.etl.salaries import" tests/
grep -r "from src.etl.game_logs import" tests/
grep -r "from src.etl.play_by_play import" tests/
grep -r "from src.etl.dimensions import" tests/
grep -r "from src.etl.metrics import" tests/
grep -r "from src.pipeline.cli import" tests/
grep -r "from src.pipeline.executor import" tests/
```

**Step 2: Update each test file**

For each file with old imports, update to new paths:

```python
# Before
from src.etl.salaries import process_salaries

# After
from src.etl.salaries.extractor import extract_salaries
from src.etl.salaries.transformer import transform_salaries
from src.etl.salaries.loader import load_salaries
```

**Step 3: Run all tests**

Run:
```bash
uv run pytest tests/ -v
```
Expected: All tests pass

**Step 4: Commit**

```bash
git add tests/
git commit -m "test: update test imports for new module structure"
```

---

### Task 19: Verify File Line Counts

**Step 1: Check all Python files**

Run:
```bash
find src -name "*.py" | xargs wc -l | sort -rn | head -30
```

**Step 2: Verify no files exceed 400 lines**

All files should be ≤ 400 lines (per AGENTS.md rule).

If any files still exceed 400 lines, create additional tasks to split them.

**Step 3: Document any remaining large files**

If files cannot be split further, add justification comment at top of file.

---

### Task 20: Final Verification

**Step 1: Run full test suite**

Run:
```bash
uv run pytest tests/ -v
```
Expected: All tests pass

**Step 2: Run linter**

Run:
```bash
ruff check .
ruff format .
```
Expected: No errors, formatting applied

**Step 3: Verify directory structure**

Run:
```bash
tree -L 3 -I '__pycache__|*.pyc'
```
Expected: Clean directory structure matching design

**Step 4: Verify imports work**

Run:
```bash
uv run python -c "from src.etl.salaries import extract_salaries; print('OK')"
uv run python -c "from src.pipeline.cli import main; print('OK')"
```
Expected: Both print "OK"

**Step 5: Final commit**

```bash
git commit -m "chore: complete project reorganization

- Moved root files to organized directories
- Restructured src/ into domain-driven packages
- Split files exceeding 400 lines
- Updated all imports
- All tests passing"
```

---

## Rollback Plan

If issues arise:

1. **Keep git history** - All moves use `git mv`, history preserved
2. **Revert by commit** - Can revert individual commits if needed
3. **Test at each step** - Each task has verification step

---

## Success Criteria

- [ ] All root files moved to appropriate directories
- [ ] No files in src/ exceed 400 lines
- [ ] All tests pass (`uv run pytest tests/`)
- [ ] Linter passes (`ruff check .`)
- [ ] Code formatted (`ruff format .`)
- [ ] Imports work correctly
- [ ] CLI functions correctly
- [ ] Database/cache paths work correctly

---

## Notes

- Use `git mv` for all file moves to preserve history
- Update imports incrementally after each move
- Run tests after each task to catch issues early
- If a file is difficult to split, consider if the logic can be simplified first
