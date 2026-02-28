# NBA Full-History Completeness Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a mandatory, complete NBA-lineage database (1946-47 to present) covering players, seasons, games, franchises, coaches, and salaries for preseason, regular season, play-in, and playoffs.

**Architecture:** Use a raw-first ingestion model with canonical dimensions/facts and identity crosswalks so no historical records are dropped due to unresolved IDs. Make the default pipeline a full-history profile with mandatory domains (awards, rosters, salaries, PBP), and enforce completeness with automated audits and CI gates.

**Tech Stack:** Python 3.13, SQLite, DuckDB, pandas, nba_api, pytest, ruff

---

### Task 1: Add Full-History Completeness Contract Constants

**Files:**
- Create: `src/pipeline/completeness.py`
- Modify: `src/pipeline/constants.py`
- Test: `tests/test_pipeline_completeness.py`

**Step 1: Write the failing test**

```python
# tests/test_pipeline_completeness.py
from src.pipeline.completeness import (
    NBA_LINEAGE_FIRST_START_YEAR,
    REQUIRED_GAME_TYPES,
    full_history_seasons,
)


def test_full_history_contract_defaults():
    assert NBA_LINEAGE_FIRST_START_YEAR == 1946
    assert REQUIRED_GAME_TYPES == ("Preseason", "Regular Season", "Play-In", "Playoffs")
    seasons = full_history_seasons(up_to_start_year=2025)
    assert seasons[0] == "1946-47"
    assert seasons[-1] == "2025-26"
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_pipeline_completeness.py -v`
Expected: FAIL with `ModuleNotFoundError` for `src.pipeline.completeness`

**Step 3: Write minimal implementation**

```python
# src/pipeline/completeness.py
from __future__ import annotations

NBA_LINEAGE_FIRST_START_YEAR = 1946
REQUIRED_GAME_TYPES = ("Preseason", "Regular Season", "Play-In", "Playoffs")


def _season_id(start_year: int) -> str:
    return f"{start_year}-{str(start_year + 1)[-2:]}"


def full_history_seasons(up_to_start_year: int) -> tuple[str, ...]:
    return tuple(_season_id(y) for y in range(NBA_LINEAGE_FIRST_START_YEAR, up_to_start_year + 1))
```

```python
# src/pipeline/constants.py
from src.pipeline.completeness import full_history_seasons

DEFAULT_SEASONS: tuple[str, ...] = full_history_seasons(2025)
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_pipeline_completeness.py tests/test_pipeline_cli.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/pipeline/completeness.py src/pipeline/constants.py tests/test_pipeline_completeness.py tests/test_pipeline_cli.py
git commit -m "feat: add full-history completeness contract constants"
```

### Task 2: Add Coach and Identity Crosswalk Schema

**Files:**
- Modify: `src/db/schema/tables.sql`
- Modify: `src/db/schema/indexes.sql`
- Test: `tests/test_db_schema_full_history.py`

**Step 1: Write the failing test**

```python
# tests/test_db_schema_full_history.py
from src.db.schema import init_db


def test_full_history_tables_exist(tmp_path):
    db_path = tmp_path / "test.db"
    con = init_db(db_path)
    try:
        names = {
            r[0]
            for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        assert "dim_coach" in names
        assert "fact_team_coach_game" in names
        assert "dim_player_identifier" in names
        assert "dim_team_identifier" in names
    finally:
        con.close()
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_db_schema_full_history.py -v`
Expected: FAIL on missing table assertions

**Step 3: Write minimal implementation**

```sql
-- src/db/schema/tables.sql
CREATE TABLE IF NOT EXISTS dim_coach (
    coach_id TEXT PRIMARY KEY,
    full_name TEXT NOT NULL,
    first_name TEXT,
    last_name TEXT,
    first_seen_season_id TEXT REFERENCES dim_season(season_id),
    last_seen_season_id TEXT REFERENCES dim_season(season_id)
) STRICT;

CREATE TABLE IF NOT EXISTS fact_team_coach_game (
    game_id TEXT NOT NULL REFERENCES fact_game(game_id),
    team_id TEXT NOT NULL REFERENCES dim_team(team_id),
    coach_id TEXT NOT NULL REFERENCES dim_coach(coach_id),
    PRIMARY KEY (game_id, team_id)
) STRICT;

CREATE TABLE IF NOT EXISTS dim_player_identifier (
    source_system TEXT NOT NULL,
    source_id TEXT NOT NULL,
    player_id TEXT NOT NULL REFERENCES dim_player(player_id),
    match_confidence REAL,
    PRIMARY KEY (source_system, source_id)
) STRICT;

CREATE TABLE IF NOT EXISTS dim_team_identifier (
    source_system TEXT NOT NULL,
    source_id TEXT NOT NULL,
    team_id TEXT NOT NULL REFERENCES dim_team(team_id),
    PRIMARY KEY (source_system, source_id)
) STRICT;
```

```sql
-- src/db/schema/indexes.sql
CREATE INDEX IF NOT EXISTS idx_coach_name ON dim_coach(full_name);
CREATE INDEX IF NOT EXISTS idx_team_coach_coach ON fact_team_coach_game(coach_id);
CREATE INDEX IF NOT EXISTS idx_player_identifier_player ON dim_player_identifier(player_id);
CREATE INDEX IF NOT EXISTS idx_team_identifier_team ON dim_team_identifier(team_id);
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_db_schema_full_history.py tests/test_db_schema.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/db/schema/tables.sql src/db/schema/indexes.sql tests/test_db_schema_full_history.py
git commit -m "feat: add coach and identity crosswalk schema tables"
```

### Task 3: Seed Historical Seasons/Teams/Players from Raw Data

**Files:**
- Create: `src/etl/dimensions/raw_seed.py`
- Modify: `src/etl/dimensions/seasons.py`
- Modify: `src/etl/dimensions/teams.py`
- Modify: `src/etl/dimensions/players.py`
- Modify: `src/etl/dimensions/__init__.py`
- Test: `tests/test_etl_dimensions_full_history.py`

**Step 1: Write the failing test**

```python
# tests/test_etl_dimensions_full_history.py
from src.etl.dimensions.raw_seed import infer_season_start_range


def test_infer_season_start_range_from_raw(tmp_path):
    min_y, max_y = infer_season_start_range(raw_dir="raw")
    assert min_y == 1946
    assert max_y >= 2025
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_etl_dimensions_full_history.py -v`
Expected: FAIL because `raw_seed` module/function does not exist

**Step 3: Write minimal implementation**

```python
# src/etl/dimensions/raw_seed.py
from __future__ import annotations

import pandas as pd
from pathlib import Path


def infer_season_start_range(raw_dir: str | Path = "raw") -> tuple[int, int]:
    path = Path(raw_dir) / "Games.csv"
    df = pd.read_csv(path, usecols=["gameDateTimeEst"])
    dates = pd.to_datetime(df["gameDateTimeEst"], errors="coerce")
    min_year = int(dates.min().year) - 1
    max_year = int(dates.max().year) if int(dates.max().month) >= 7 else int(dates.max().year) - 1
    return (min_year, max_year)
```

```python
# src/etl/dimensions/__init__.py (concept)
# call raw-based seeders before nba_api-only enrichments
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_etl_dimensions_full_history.py tests/test_etl_dimensions.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/etl/dimensions/raw_seed.py src/etl/dimensions/seasons.py src/etl/dimensions/teams.py src/etl/dimensions/players.py src/etl/dimensions/__init__.py tests/test_etl_dimensions_full_history.py
git commit -m "feat: seed historical seasons teams and players from raw datasets"
```

### Task 4: Preserve Historical Team History Rows (No Current-Team Filter)

**Files:**
- Modify: `src/etl/backfill/_dims.py`
- Test: `tests/test_etl_backfill_dims.py`

**Step 1: Write the failing test**

```python
# tests/test_etl_backfill_dims.py (new case)
def test_load_team_history_keeps_historical_franchises(sqlite_con, tmp_path):
    # team history with historical IDs not present in current 30-team static set
    ...
    load_team_history(sqlite_con, raw_dir=tmp_path)
    count = sqlite_con.execute("SELECT COUNT(*) FROM dim_team_history").fetchone()[0]
    assert count > 0
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_etl_backfill_dims.py -v`
Expected: FAIL because loader skips rows via `valid_team_ids` filtering

**Step 3: Write minimal implementation**

```python
# src/etl/backfill/_dims.py (concept)
# remove hard filter:
# if team_id not in valid_team_ids: skip

# insert all league-lineage rows instead:
# include league in {"NBA", "BAA"}
# include non-NBA preseason opponents under explicit external policy if required
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_etl_backfill_dims.py tests/etl/backfill/test_orchestrator.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/etl/backfill/_dims.py tests/test_etl_backfill_dims.py tests/etl/backfill/test_orchestrator.py
git commit -m "fix: retain historical franchise rows in team history backfill"
```

### Task 5: Implement Deterministic Player/Team Identifier Resolution

**Files:**
- Create: `src/etl/identity/resolver.py`
- Modify: `src/etl/backfill/_player_career.py`
- Modify: `src/etl/backfill/_all_star.py`
- Modify: `src/etl/backfill/_all_nba.py`
- Modify: `src/etl/backfill/_awards.py`
- Test: `tests/test_etl_identity_resolver.py`

**Step 1: Write the failing test**

```python
# tests/test_etl_identity_resolver.py
from src.etl.identity.resolver import resolve_or_create_player


def test_resolve_or_create_player_creates_placeholder(sqlite_con):
    pid = resolve_or_create_player(sqlite_con, source_system="bref", source_id="ackerdo01", full_name="Don Ackerman")
    assert pid
    row = sqlite_con.execute("SELECT full_name FROM dim_player WHERE player_id = ?", (pid,)).fetchone()
    assert row[0] == "Don Ackerman"
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_etl_identity_resolver.py -v`
Expected: FAIL on missing module/function

**Step 3: Write minimal implementation**

```python
# src/etl/identity/resolver.py
def resolve_or_create_player(con, source_system: str, source_id: str, full_name: str) -> str:
    # 1) check dim_player_identifier
    # 2) if missing, create placeholder dim_player row with synthetic ID
    # 3) insert into dim_player_identifier
    # 4) return player_id
    ...
```

```python
# backfill loaders
# replace "skip when player_id unresolved" with resolver call and continue insert
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_etl_identity_resolver.py tests/test_etl_backfill_all_star.py tests/test_etl_backfill_all_nba.py tests/test_etl_backfill_awards.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/etl/identity/resolver.py src/etl/backfill/_player_career.py src/etl/backfill/_all_star.py src/etl/backfill/_all_nba.py src/etl/backfill/_awards.py tests/test_etl_identity_resolver.py tests/test_etl_backfill_all_star.py tests/test_etl_backfill_all_nba.py tests/test_etl_backfill_awards.py
git commit -m "feat: add no-drop identity resolver for player and team mapping"
```

### Task 6: Normalize Advanced Stats Percentages to Canonical Scale

**Files:**
- Modify: `src/etl/backfill/_advanced_stats/player.py`
- Modify: `src/etl/schemas.py`
- Test: `tests/test_etl_backfill_advanced_stats.py`

**Step 1: Write the failing test**

```python
# tests/test_etl_backfill_advanced_stats.py (new case)
def test_advanced_stats_percent_fields_accept_100_scale(sqlite_con, tmp_path):
    # row with usg_percent=28.3 should be ingested as 0.283 or accepted equivalent
    ...
    assert inserted == 1
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_etl_backfill_advanced_stats.py -v`
Expected: FAIL due validation dropping 100-scale pct values

**Step 3: Write minimal implementation**

```python
# src/etl/backfill/_advanced_stats/player.py

def _pct_01(value):
    v = _flt(value)
    if v is None:
        return None
    return v / 100.0 if v > 1.0 else v
```

```python
# use _pct_01 for orb_pct, drb_pct, trb_pct, ast_pct, stl_pct, blk_pct, tov_pct, usg_pct
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_etl_backfill_advanced_stats.py tests/test_etl_schemas.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/etl/backfill/_advanced_stats/player.py src/etl/schemas.py tests/test_etl_backfill_advanced_stats.py tests/test_etl_schemas.py
git commit -m "fix: normalize advanced stats percent fields to canonical scale"
```

### Task 7: Preserve Early-Era Nullability in Game Logs

**Files:**
- Modify: `src/etl/backfill/_game_logs.py`
- Modify: `src/etl/transform/_game_logs.py`
- Test: `tests/test_etl_backfill_game_logs.py`
- Test: `tests/test_etl_game_logs.py`

**Step 1: Write the failing test**

```python
# tests/test_etl_backfill_game_logs.py (new case)
def test_early_era_oreb_dreb_zero_pattern_not_dropped(sqlite_con, tmp_path):
    # historical row where reb_total > 0 and oreb/dreb unavailable
    ...
    assert inserted > 0
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_etl_backfill_game_logs.py tests/test_etl_game_logs.py -v`
Expected: FAIL due strict OREB+DREB=REB validation drops

**Step 3: Write minimal implementation**

```python
# src/etl/backfill/_game_logs.py
# if oreb==0 and dreb==0 and reb>0 in pre-tracked eras, map oreb/dreb -> None
```

```python
# src/etl/transform/_game_logs.py
# apply same normalization for API path to keep behavior consistent
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_etl_backfill_game_logs.py tests/test_etl_game_logs.py tests/test_validate_rules.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/etl/backfill/_game_logs.py src/etl/transform/_game_logs.py tests/test_etl_backfill_game_logs.py tests/test_etl_game_logs.py tests/test_validate_rules.py
git commit -m "fix: preserve early-era nullability in game log ingestion"
```

### Task 8: Make Full-History Pipeline Mandatory by Default

**Files:**
- Modify: `src/pipeline/cli/args.py`
- Modify: `src/pipeline/models.py`
- Modify: `src/pipeline/executor/steps.py`
- Modify: `src/pipeline/stages.py`
- Modify: `README.md`
- Test: `tests/test_pipeline_cli.py`
- Test: `tests/test_pipeline_executor.py`
- Test: `tests/test_pipeline_stages.py`

**Step 1: Write the failing test**

```python
# tests/test_pipeline_cli.py (new/updated defaults)
def test_default_pipeline_enables_full_history_domains():
    ns = _parse([])
    assert ns.awards is True
    assert ns.salaries is True
    assert ns.rosters is True
    assert ns.include_playoffs is True
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_pipeline_cli.py tests/test_pipeline_executor.py tests/test_pipeline_stages.py -v`
Expected: FAIL because defaults are currently optional/disabled

**Step 3: Write minimal implementation**

```python
# src/pipeline/cli/args.py
# set defaults for mandatory completeness profile:
# awards=True, salaries=True, rosters=True, include_playoffs=True
# set pbp_limit default to None/all-season mode
```

```python
# src/pipeline/executor/steps.py
plan.append((Stage.AWARDS, AWARDS_TABLES, load_all_awards, (), {"active_only": False}))
```

```python
# src/pipeline/stages.py
load_season_pbp(con, season, limit=config.pbp_limit, source=config.pbp_source, bulk_dir=config.pbp_bulk_dir or Path("raw/pbp"))
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_pipeline_cli.py tests/test_pipeline_executor.py tests/test_pipeline_stages.py tests/test_pipeline_cli_source_flags.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/pipeline/cli/args.py src/pipeline/models.py src/pipeline/executor/steps.py src/pipeline/stages.py README.md tests/test_pipeline_cli.py tests/test_pipeline_executor.py tests/test_pipeline_stages.py tests/test_pipeline_cli_source_flags.py
git commit -m "feat: make full-history completeness pipeline the default profile"
```

### Task 9: Replace Coarse Skip Logic with Source Fingerprints

**Files:**
- Create: `src/db/tracking/fingerprint.py`
- Modify: `src/db/tracking/etl_log.py`
- Modify: `src/db/schema/tables.sql`
- Test: `tests/test_db_tracking_fingerprint.py`

**Step 1: Write the failing test**

```python
# tests/test_db_tracking_fingerprint.py
from src.db.tracking.fingerprint import should_run_loader


def test_should_run_loader_when_source_hash_changes(sqlite_con):
    assert should_run_loader(sqlite_con, "player_game_log", "2023-24", "loader", "hash-a") is True
    # persist hash-a, then hash-b should force rerun
    ...
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_db_tracking_fingerprint.py -v`
Expected: FAIL (module/function not found)

**Step 3: Write minimal implementation**

```python
# src/db/tracking/fingerprint.py
# table: etl_source_fingerprint(table_name, season_id, loader, source_hash, updated_at)
# should_run_loader -> True when missing hash or hash changed
```

```python
# replace already_loaded-only calls in critical loaders with fingerprint-aware guard
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_db_tracking_fingerprint.py tests/test_db_tracking_etl_log.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/db/tracking/fingerprint.py src/db/tracking/etl_log.py src/db/schema/tables.sql tests/test_db_tracking_fingerprint.py tests/test_db_tracking_etl_log.py
git commit -m "feat: add source fingerprint tracking to avoid frozen incomplete loads"
```

### Task 10: Add Completeness Audit + CI Enforcement

**Files:**
- Modify: `scripts/completion_audit.py`
- Create: `research/completeness_contract.yaml`
- Modify: `.github/workflows/commit-gate.yml`
- Test: `tests/test_completion_audit.py`

**Step 1: Write the failing test**

```python
# tests/test_completion_audit.py
from scripts.completion_audit import evaluate_completion


def test_completion_audit_enforces_required_game_types(sqlite_con):
    data = evaluate_completion(sqlite_con)
    assert "missing_required_game_types" in data
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_completion_audit.py -v`
Expected: FAIL because audit does not expose required game-type checks

**Step 3: Write minimal implementation**

```python
# scripts/completion_audit.py
# add contract-driven checks:
# - season range starts at 1946-47
# - required game types: preseason/regular/play-in/playoffs
# - salaries required coverage by season where source exists
# - unresolved entity counts
# add --enforce flag: exits non-zero when violations exist
```

```yaml
# research/completeness_contract.yaml
season_start: "1946-47"
required_game_types:
  - Preseason
  - Regular Season
  - Play-In
  - Playoffs
required_domains:
  - players
  - seasons
  - games
  - franchises
  - coaches
  - salaries
```

```yaml
# .github/workflows/commit-gate.yml
# add step after pytest:
# uv run python scripts/completion_audit.py --db-path data/databases/nba_raw_data.db --enforce
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_completion_audit.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add scripts/completion_audit.py research/completeness_contract.yaml .github/workflows/commit-gate.yml tests/test_completion_audit.py
git commit -m "feat: enforce completeness contract via audit and CI gate"
```

### Task 11: End-to-End Verification and Rollout Safety

**Files:**
- Modify: `README.md`
- Create: `docs/plans/2026-02-27-full-history-rollout-checklist.md`

**Step 1: Write failing verification command matrix (doc-first)**

```markdown
# docs/plans/2026-02-27-full-history-rollout-checklist.md
- [ ] full-history ingest dry run
- [ ] raw-backfill reconciliation by season
- [ ] awards/all-star/all-nba no-drop counts
- [ ] salaries completeness by season
- [ ] PBP coverage by required game type
```

**Step 2: Run verification commands and capture expected checks**

Run: `uv run pytest -q`
Expected: PASS

Run: `uv run ruff check . && uv run ruff format --check .`
Expected: PASS

Run: `uv run python scripts/completion_audit.py --db-path data/databases/nba_raw_data.db --enforce`
Expected: exit code 0 on complete DB, non-zero otherwise

**Step 3: Update README operational section**

```markdown
# README.md
# add mandatory full-history command
uv run ingest --full-history
```

**Step 4: Re-run critical test subsets**

Run: `uv run pytest tests/test_pipeline_cli.py tests/test_etl_dimensions_full_history.py tests/test_completion_audit.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add README.md docs/plans/2026-02-27-full-history-rollout-checklist.md
git commit -m "docs: add full-history rollout and verification checklist"
```

---

## Execution Notes

- Apply @superpowers/test-driven-development before each code task.
- Apply @superpowers/verification-before-completion before claiming milestone completion.
- Keep each task independently shippable; do not batch-merge unverified changes.
- Preserve historical nullability semantics (missing stat != zero) throughout transformations.

## Final Validation Gate

Run in order:

1. `uv run ruff check .`
2. `uv run ruff format --check .`
3. `uv run ty check`
4. `uv run pytest -q`
5. `uv run python scripts/completion_audit.py --db-path data/databases/nba_raw_data.db --enforce`

Expected: all commands succeed with zero completeness violations.
