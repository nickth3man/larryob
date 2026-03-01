# NBA API Canonical Graph Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace mixed raw/legacy ingest paths with a canonical, game-centric `nba_api` V3 ingestion graph that enforces strict parity gates and deterministic replay.

**Architecture:** Introduce explicit endpoint adapter modules (inventory, boxscore, PBP) that normalize API payloads into stable internal row contracts. Build canonical loaders on top of those adapters, then wire them into the existing pipeline stage runner and validation gates. Keep orchestration incremental-safe via source fingerprint checks, with blocking validation before publish.

**Tech Stack:** Python 3.13, `nba-api>=1.11.4`, pandas, SQLite (`STRICT` tables), pytest, ruff, existing pipeline/executor modules.

**Source-Verified Constraints (from `swar/nba_api` current `master`):**
- Use named dataset attributes (for example `endpoint.player_stats.get_data_frame()`) instead of `get_data_frames()[index]`.
- `PlayByPlayV2` is deprecated and NBA API now returns empty JSON; `PlayByPlayV3` is required.
- `ScoreboardV2` is deprecated due known 2025-26 line-score gaps; `ScoreboardV3` is required.
- `ScoreboardV3` is date-based (`game_date`), so season inventory logic must call it per game date.
- `LeagueGameFinder.game_id_nullable` is documented as ignored by API; always filter by `GAME_ID` client-side.
- `CommonTeamRoster` may omit `Coaches`; roster/coach loaders must tolerate missing coach dataset.

---

### Task 1: Add V3 Endpoint Adapter Layer

**Files:**
- Create: `src/etl/extract/endpoints/_game_inventory_v3.py`
- Create: `src/etl/extract/endpoints/_boxscore_v3.py`
- Create: `src/etl/extract/endpoints/_play_by_play_v3.py`
- Modify: `src/etl/extract/endpoints/__init__.py`
- Test: `tests/etl/test_endpoint_adapters_v3.py`

**Step 1: Write the failing tests**

```python
def test_fetch_play_by_play_v3_normalizes_action_number():
    rows = fetch_play_by_play_v3(game_id="0022300001", api_caller=FakeCaller())
    assert rows[0]["game_id"] == "0022300001"
    assert rows[0]["action_number"] == 1

def test_fetch_boxscore_v3_uses_named_dataset_attributes_not_indexes():
    payload = fetch_boxscore_traditional_v3("0022300001", api_caller=FakeCaller())
    assert "player_stats" in payload and "team_stats" in payload
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/etl/test_endpoint_adapters_v3.py::test_fetch_play_by_play_v3_normalizes_action_number -v`  
Expected: FAIL with `ImportError`/missing adapter functions.

**Step 3: Write minimal implementation**

```python
def fetch_play_by_play_v3(game_id: str, api_caller: APICaller | None = None) -> list[dict]:
    ep = playbyplayv3.PlayByPlayV3(game_id=game_id)
    df = ep.play_by_play.get_data_frame().rename(columns={"actionNumber": "action_number"})
    df["game_id"] = game_id
    return df.to_dict(orient="records")
```

**Step 4: Run tests and quality checks**

Run: `uv run pytest tests/etl/test_endpoint_adapters_v3.py -v`  
Expected: PASS

Run: `uv run ruff check . && uv run ruff format .`  
Expected: PASS

**Step 5: Commit**

```bash
git add src/etl/extract/endpoints/_game_inventory_v3.py src/etl/extract/endpoints/_boxscore_v3.py src/etl/extract/endpoints/_play_by_play_v3.py src/etl/extract/endpoints/__init__.py tests/etl/test_endpoint_adapters_v3.py
git commit -m "feat(etl): add nba_api v3 endpoint adapters"
```

### Task 2: Implement Canonical Game Inventory Loader (`fact_game`)

**Files:**
- Create: `src/etl/canonical/_game_inventory.py`
- Create: `src/etl/canonical/__init__.py`
- Modify: `src/pipeline/stages.py`
- Modify: `src/pipeline/constants.py`
- Test: `tests/test_etl_canonical_game_inventory.py`
- Test: `tests/test_pipeline_stages.py`

**Step 1: Write the failing tests**

```python
def test_load_canonical_game_inventory_applies_scoreboard_corrections(sqlite_con_with_data):
    counts = load_canonical_game_inventory(sqlite_con_with_data, "2023-24", api_caller=FakeCaller())
    assert counts["fact_game"] > 0
    row = sqlite_con_with_data.execute("SELECT status FROM fact_game WHERE game_id='0022300001'").fetchone()
    assert row[0] == "Final"

def test_load_canonical_game_inventory_handles_empty_season_weeks():
    rows = fetch_schedule_league_v2("1947-48", api_caller=FakeCallerEmptyWeeks())
    assert isinstance(rows, list)
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_etl_canonical_game_inventory.py::test_load_canonical_game_inventory_applies_scoreboard_corrections -v`  
Expected: FAIL with missing loader implementation.

**Step 3: Write minimal implementation**

```python
def load_canonical_game_inventory(con: sqlite3.Connection, season: str, api_caller: APICaller | None = None) -> dict[str, int]:
    schedule_rows = fetch_schedule_league_v2(season, api_caller)
    game_dates = sorted({row["game_date"] for row in schedule_rows})
    scoreboard_rows = fetch_scoreboard_v3_for_dates(game_dates, api_caller)
    merged_rows = merge_schedule_with_scoreboard(schedule_rows, scoreboard_rows)
    rows = validate_rows("fact_game", merged_rows)
    inserted = upsert_rows(con, "fact_game", rows)
    return {"fact_game": inserted}
```

**Step 4: Run tests and quality checks**

Run: `uv run pytest tests/test_etl_canonical_game_inventory.py tests/test_pipeline_stages.py -v`  
Expected: PASS

Run: `uv run ruff check . && uv run ruff format .`  
Expected: PASS

**Step 5: Commit**

```bash
git add src/etl/canonical/_game_inventory.py src/etl/canonical/__init__.py src/pipeline/stages.py src/pipeline/constants.py tests/test_etl_canonical_game_inventory.py tests/test_pipeline_stages.py
git commit -m "feat(etl): add canonical fact_game inventory loader"
```

### Task 3: Implement Canonical V3 Box Score Loaders (`player_game_log`, `team_game_log`)

**Files:**
- Create: `src/etl/canonical/_boxscore.py`
- Modify: `src/etl/transform/game_logs.py`
- Modify: `src/pipeline/stages.py`
- Test: `tests/test_etl_canonical_boxscore.py`
- Test: `tests/test_etl_game_logs.py`

**Step 1: Write the failing tests**

```python
def test_load_boxscores_v3_writes_two_team_rows_per_final_game(sqlite_con_with_data):
    counts = load_canonical_boxscores_for_game(sqlite_con_with_data, "0022300001", api_caller=FakeCaller())
    assert counts["team_game_log"] == 2
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_etl_canonical_boxscore.py::test_load_boxscores_v3_writes_two_team_rows_per_final_game -v`  
Expected: FAIL with missing canonical box score loader.

**Step 3: Write minimal implementation**

```python
def load_canonical_boxscores_for_game(con: sqlite3.Connection, game_id: str, api_caller: APICaller | None = None) -> dict[str, int]:
    endpoint = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id)
    player_rows = transform_boxscore_player_rows(endpoint.player_stats.get_data_frame())
    team_rows = transform_boxscore_team_rows(endpoint.team_stats.get_data_frame())
    player_rows = validate_rows("player_game_log", player_rows)
    team_rows = validate_rows("team_game_log", team_rows)
    with transaction(con):
        n_players = upsert_rows(con, "player_game_log", player_rows, autocommit=False)
        n_teams = upsert_rows(con, "team_game_log", team_rows, autocommit=False)
    return {"player_game_log": n_players, "team_game_log": n_teams}
```

**Step 4: Run tests and quality checks**

Run: `uv run pytest tests/test_etl_canonical_boxscore.py tests/test_etl_game_logs.py tests/test_pipeline_stages.py -v`  
Expected: PASS

Run: `uv run ruff check . && uv run ruff format .`  
Expected: PASS

**Step 5: Commit**

```bash
git add src/etl/canonical/_boxscore.py src/etl/transform/game_logs.py src/pipeline/stages.py tests/test_etl_canonical_boxscore.py tests/test_etl_game_logs.py tests/test_pipeline_stages.py
git commit -m "feat(etl): load canonical v3 boxscores into game log facts"
```

### Task 4: Migrate PBP Ingest to `PlayByPlayV3` Only

**Files:**
- Modify: `src/etl/transform/play_by_play.py`
- Test: `tests/test_etl_play_by_play.py`
- Test: `tests/test_pipeline_stages.py`

**Step 1: Write the failing tests**

```python
def test_fetch_pbp_uses_playbyplay_v3_endpoint():
    with patch("src.etl.transform.play_by_play.playbyplayv3.PlayByPlayV3") as mock_v3:
        _fetch_pbp("0022300001")
    mock_v3.assert_called_once()
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_etl_play_by_play.py::test_fetch_pbp_uses_playbyplay_v3_endpoint -v`  
Expected: FAIL because implementation still imports/calls `PlayByPlayV2`.

**Step 3: Write minimal implementation**

```python
from nba_api.stats.endpoints import playbyplayv3

def _build_event_id(game_id: str, action_number: int) -> str:
    return f"{game_id}_{action_number:06d}"
```

**Step 4: Run tests and quality checks**

Run: `uv run pytest tests/test_etl_play_by_play.py tests/test_pipeline_stages.py -v`  
Expected: PASS

Run: `uv run ruff check . && uv run ruff format .`  
Expected: PASS

**Step 5: Commit**

```bash
git add src/etl/transform/play_by_play.py tests/test_etl_play_by_play.py tests/test_pipeline_stages.py
git commit -m "refactor(etl): migrate play-by-play ingest to v3 endpoint"
```

### Task 5: Add Canonical Roster + Coach Load Path

**Files:**
- Modify: `src/etl/roster.py`
- Create: `src/etl/canonical/_coach_assignments.py`
- Modify: `src/pipeline/constants.py`
- Test: `tests/etl/test_roster_fetch.py`
- Test: `tests/test_etl_coach_assignments.py`

**Step 1: Write the failing tests**

```python
def test_load_roster_upserts_dim_coach_and_fact_team_coach_game(sqlite_con_with_data):
    counts = load_coach_assignments(sqlite_con_with_data, season_id="2023-24", api_caller=FakeCaller())
    assert counts["dim_coach"] >= 1
    assert counts["fact_team_coach_game"] >= 1

def test_load_roster_handles_missing_coaches_dataset(sqlite_con_with_data):
    counts = load_coach_assignments(sqlite_con_with_data, season_id="1950-51", api_caller=FakeCallerNoCoaches())
    assert counts["dim_coach"] == 0
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_etl_coach_assignments.py::test_load_roster_upserts_dim_coach_and_fact_team_coach_game -v`  
Expected: FAIL because coach assignments are not loaded today.

**Step 3: Write minimal implementation**

```python
def load_coach_assignments(con: sqlite3.Connection, season_id: str, api_caller: APICaller | None = None) -> dict[str, int]:
    coach_rows, bridge_rows = transform_roster_coach_rows(fetch_common_team_roster_rows(season_id, api_caller))
    n_coaches = upsert_rows(con, "dim_coach", coach_rows)
    n_bridge = upsert_rows(con, "fact_team_coach_game", bridge_rows)
    return {"dim_coach": n_coaches, "fact_team_coach_game": n_bridge}
```

**Step 4: Run tests and quality checks**

Run: `uv run pytest tests/etl/test_roster_fetch.py tests/test_etl_coach_assignments.py -v`  
Expected: PASS

Run: `uv run ruff check . && uv run ruff format .`  
Expected: PASS

**Step 5: Commit**

```bash
git add src/etl/roster.py src/etl/canonical/_coach_assignments.py src/pipeline/constants.py tests/etl/test_roster_fetch.py tests/test_etl_coach_assignments.py
git commit -m "feat(etl): add canonical coach assignment ingest from roster payloads"
```

### Task 6: Add Strict Blocking Validation Gates for Canonical Parity

**Files:**
- Create: `src/pipeline/parity.py`
- Modify: `src/pipeline/stages.py`
- Modify: `src/pipeline/executor/steps.py`
- Modify: `src/etl/validation.py`
- Test: `tests/test_pipeline_validation.py`
- Test: `tests/test_pipeline_validation_coverage.py`

**Step 1: Write the failing tests**

```python
def test_blocking_parity_gate_raises_on_score_mismatch(sqlite_con_with_data):
    with pytest.raises(ReconciliationError):
        run_blocking_parity_gates(sqlite_con_with_data, seasons=("2023-24",))
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_pipeline_validation.py::test_blocking_parity_gate_raises_on_score_mismatch -v`  
Expected: FAIL with missing parity gate function.

**Step 3: Write minimal implementation**

```python
def run_blocking_parity_gates(con: sqlite3.Connection, seasons: tuple[str, ...]) -> None:
    score_mismatches = query_score_mismatches(con, seasons)
    if score_mismatches:
        raise ReconciliationError(len(score_mismatches), seasons=list(seasons))
```

**Step 4: Run tests and quality checks**

Run: `uv run pytest tests/test_pipeline_validation.py tests/test_pipeline_validation_coverage.py tests/test_pipeline_executor.py -v`  
Expected: PASS

Run: `uv run ruff check . && uv run ruff format .`  
Expected: PASS

**Step 5: Commit**

```bash
git add src/pipeline/parity.py src/pipeline/stages.py src/pipeline/executor/steps.py src/etl/validation.py tests/test_pipeline_validation.py tests/test_pipeline_validation_coverage.py tests/test_pipeline_executor.py
git commit -m "feat(pipeline): enforce blocking canonical parity gates"
```

### Task 7: Wire Source Fingerprints + Incremental Correction Window

**Files:**
- Modify: `src/db/tracking/fingerprint.py`
- Create: `src/pipeline/correction.py`
- Modify: `src/pipeline/models.py`
- Modify: `src/pipeline/cli/args.py`
- Modify: `src/pipeline/cli/runner.py`
- Test: `tests/test_db_tracking_fingerprint.py`
- Test: `tests/test_pipeline_cli_source_flags.py`
- Test: `tests/test_pipeline_executor.py`

**Step 1: Write the failing tests**

```python
def test_should_run_loader_when_source_hash_changes(sqlite_con):
    save_loader_fingerprint(sqlite_con, "fact_game", "2023-24", "canonical.inventory", "abc")
    assert should_run_loader(sqlite_con, "fact_game", "2023-24", "canonical.inventory", "xyz")

def test_league_game_finder_filters_game_id_client_side():
    df = fetch_league_game_finder_rows(season="2023-24")
    filtered = df[df["GAME_ID"] == "0022301181"]
    assert len(filtered) <= len(df)
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_db_tracking_fingerprint.py::test_should_run_loader_when_source_hash_changes -v`  
Expected: FAIL due missing test module/helper coverage.

**Step 3: Write minimal implementation**

```python
@dataclass(slots=True)
class IngestConfig:
    correction_window_days: int = 14
```

**Step 4: Run tests and quality checks**

Run: `uv run pytest tests/test_db_tracking_fingerprint.py tests/test_pipeline_cli_source_flags.py tests/test_pipeline_executor.py -v`  
Expected: PASS

Run: `uv run ruff check . && uv run ruff format .`  
Expected: PASS

**Step 5: Commit**

```bash
git add src/db/tracking/fingerprint.py src/pipeline/correction.py src/pipeline/models.py src/pipeline/cli/args.py src/pipeline/cli/runner.py tests/test_db_tracking_fingerprint.py tests/test_pipeline_cli_source_flags.py tests/test_pipeline_executor.py
git commit -m "feat(pipeline): add fingerprint-driven reprocessing and correction window"
```

### Task 8: Final Integration, Regression Pass, and Docs

**Files:**
- Modify: `docs/plans/2026-03-01-nba-api-canonical-graph-design.md`
- Modify: `CLAUDE.md`
- Modify: `AGENTS.md`
- Test: `tests/` (full suite)

**Step 1: Write final failing integration check (if absent)**

```python
def test_ingest_pipeline_canonical_mode_smoke(sqlite_con_with_data):
    config = IngestConfig(seasons=("2023-24",), pbp_limit=1, skip_reconciliation=False)
    run_ingest_pipeline(sqlite_con_with_data, config)
```

**Step 2: Run test to verify it fails before final wiring**

Run: `uv run pytest tests/test_pipeline_executor.py::test_ingest_pipeline_canonical_mode_smoke -v`  
Expected: FAIL until all stage wiring and flags are complete.

**Step 3: Complete final wiring and docs**

```python
# keep these legacy aliases in _ROW_MODELS (do not remove):
# "player_game_log", "team_game_log"
```

Update docs:
- Add implementation status section to design doc.
- Add one-line guardrail: `Do not use PlayByPlayV2 or ScoreboardV2 for canonical ingest; always use PlayByPlayV3 and ScoreboardV3.`

**Step 4: Run full verification**

Run: `uv run ruff check . && uv run ruff format .`  
Expected: PASS

Run: `uv run pytest tests/`  
Expected: PASS

**Step 5: Commit**

```bash
git add docs/plans/2026-03-01-nba-api-canonical-graph-design.md CLAUDE.md AGENTS.md
git commit -m "docs: finalize canonical graph implementation rollout and guardrails"
```

### Task 9: Size and Module Boundary Guard (400-line rule)

**Files:**
- Modify as needed: any newly created module exceeding 400 lines
- Test: impacted tests from prior tasks

**Step 1: Write the failing check**

```python
def test_new_canonical_modules_are_split_and_readable():
    assert Path("src/etl/canonical/_boxscore.py").read_text().count("\n") < 400
```

**Step 2: Run test to verify it fails where needed**

Run: `uv run pytest tests/test_etl_canonical_boxscore.py -v`  
Expected: FAIL if any new file crosses the limit.

**Step 3: Refactor oversized modules**

```python
# split helpers into:
# src/etl/canonical/_boxscore_transform.py
# src/etl/canonical/_boxscore_load.py
```

**Step 4: Run targeted and full checks**

Run: `uv run pytest tests/test_etl_canonical_boxscore.py tests/test_pipeline_executor.py -v`  
Expected: PASS

Run: `uv run ruff check . && uv run ruff format .`  
Expected: PASS

Run: `uv run pytest tests/`  
Expected: PASS

**Step 5: Commit**

```bash
git add src/etl/canonical tests/
git commit -m "refactor(etl): split canonical modules to enforce 400-line cap"
```

## Execution Notes

- Use `@superpowers:test-driven-development` for each task.
- Use `@superpowers:verification-before-completion` before claiming each task complete.
- Keep `_ROW_MODELS` legacy keys (`player_game_log`, `team_game_log`) intact while adding any new canonical aliases.
- Do not introduce `PlayByPlayV2`/`ScoreboardV2` in new code paths.

## Open questions

- Should canonical inventory load always run for every season in `config.seasons`, or only for seasons with no successful fingerprint match?
- Should historical endpoint gaps be encoded as explicit allowlist policy in code (`non_blocking_windows.json`) during this implementation, or deferred to a follow-up?
