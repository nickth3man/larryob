# Validation Coverage for 6 Unvalidated Fact Tables — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Extend `validate_rows()` to cover `fact_player_award`, `fact_all_star`, `fact_all_nba`, `fact_all_nba_vote`, `fact_draft`, and `fact_roster`, dropping and warning on invalid rows.

**Architecture:** Add 6 Pydantic models to `src/etl/schemas.py`, register them in `_ROW_MODELS` in `src/etl/validation.py`, and call `validate_rows()` immediately before `upsert_rows()` in each loader. No new files beyond a test file; no schema migrations; no pipeline changes.

**Tech Stack:** Python 3.13, Pydantic v2 (`BaseModel`, `ConfigDict`, `Field`, `model_validator`, `field_validator`), pytest, uv.

---

## Task 1: Write failing tests for all 6 new Pydantic models

**Files:**
- Create: `tests/test_etl_schemas_extended.py`

**Step 1: Create the test file**

```python
# tests/test_etl_schemas_extended.py
"""Tests for the 6 new Pydantic row models added to src.etl.schemas."""

import pytest
from pydantic import ValidationError

from src.etl.schemas import (
    FactAllNbaRow,
    FactAllNbaVoteRow,
    FactAllStarRow,
    FactDraftRow,
    FactPlayerAwardRow,
    FactRosterRow,
)


# ------------------------------------------------------------------ #
# FactPlayerAwardRow                                                  #
# ------------------------------------------------------------------ #


def test_fact_player_award_valid():
    row = FactPlayerAwardRow(
        player_id="2544",
        season_id="2023-24",
        award_name="MVP",
        award_type="individual",
    )
    assert row.award_name == "MVP"


def test_fact_player_award_missing_required_field_fails():
    with pytest.raises(ValidationError):
        FactPlayerAwardRow(player_id="2544", season_id="2023-24", award_name="MVP")
        # missing award_type


def test_fact_player_award_invalid_award_type_fails():
    with pytest.raises(ValidationError):
        FactPlayerAwardRow(
            player_id="2544",
            season_id="2023-24",
            award_name="MVP",
            award_type="bogus",
        )


def test_fact_player_award_all_valid_award_types():
    for t in ("individual", "weekly", "monthly", "team_inclusion"):
        row = FactPlayerAwardRow(
            player_id="P1", season_id="2023-24", award_name="X", award_type=t
        )
        assert row.award_type == t


def test_fact_player_award_votes_valid():
    row = FactPlayerAwardRow(
        player_id="P1",
        season_id="2023-24",
        award_name="MVP",
        award_type="individual",
        votes_received=80,
        votes_possible=100,
    )
    assert row.votes_received == 80


def test_fact_player_award_votes_received_exceeds_possible_fails():
    with pytest.raises(ValidationError, match="votes_received cannot exceed votes_possible"):
        FactPlayerAwardRow(
            player_id="P1",
            season_id="2023-24",
            award_name="MVP",
            award_type="individual",
            votes_received=101,
            votes_possible=100,
        )


def test_fact_player_award_votes_received_equals_possible_passes():
    row = FactPlayerAwardRow(
        player_id="P1",
        season_id="2023-24",
        award_name="MVP",
        award_type="individual",
        votes_received=100,
        votes_possible=100,
    )
    assert row.votes_received == 100


def test_fact_player_award_negative_votes_fails():
    with pytest.raises(ValidationError):
        FactPlayerAwardRow(
            player_id="P1",
            season_id="2023-24",
            award_name="MVP",
            award_type="individual",
            votes_received=-1,
        )


def test_fact_player_award_extra_fields_ignored():
    row = FactPlayerAwardRow(
        player_id="P1",
        season_id="2023-24",
        award_name="MVP",
        award_type="individual",
        unknown_field="ignored",
    )
    assert not hasattr(row, "unknown_field")


# ------------------------------------------------------------------ #
# FactAllStarRow                                                      #
# ------------------------------------------------------------------ #


def test_fact_all_star_valid_minimal():
    row = FactAllStarRow(player_id="2544", season_id="2023-24")
    assert row.player_id == "2544"
    assert row.is_replacement == 0


def test_fact_all_star_missing_player_id_fails():
    with pytest.raises(ValidationError):
        FactAllStarRow(season_id="2023-24")


def test_fact_all_star_is_starter_valid_values():
    for v in (0, 1, None):
        row = FactAllStarRow(player_id="P1", season_id="2023-24", is_starter=v)
        assert row.is_starter == v


def test_fact_all_star_is_starter_invalid_value_fails():
    with pytest.raises(ValidationError, match="is_starter must be 0, 1, or None"):
        FactAllStarRow(player_id="P1", season_id="2023-24", is_starter=2)


def test_fact_all_star_is_replacement_invalid_value_fails():
    with pytest.raises(ValidationError, match="is_replacement must be 0 or 1"):
        FactAllStarRow(player_id="P1", season_id="2023-24", is_replacement=5)


def test_fact_all_star_with_team_fields():
    row = FactAllStarRow(
        player_id="P1",
        season_id="2023-24",
        team_id="1610612747",
        selection_team="Team LeBron",
        is_starter=1,
        is_replacement=0,
    )
    assert row.selection_team == "Team LeBron"


# ------------------------------------------------------------------ #
# FactAllNbaRow                                                       #
# ------------------------------------------------------------------ #


def test_fact_all_nba_valid():
    row = FactAllNbaRow(
        player_id="2544", season_id="2023-24", team_type="All-NBA", team_number=1
    )
    assert row.team_number == 1


def test_fact_all_nba_missing_team_type_fails():
    with pytest.raises(ValidationError):
        FactAllNbaRow(player_id="P1", season_id="2023-24")


def test_fact_all_nba_empty_team_type_fails():
    with pytest.raises(ValidationError, match="team_type must not be empty"):
        FactAllNbaRow(player_id="P1", season_id="2023-24", team_type="   ")


def test_fact_all_nba_valid_team_numbers():
    for n in (1, 2, 3, None):
        row = FactAllNbaRow(player_id="P1", season_id="2023-24", team_type="All-NBA", team_number=n)
        assert row.team_number == n


def test_fact_all_nba_invalid_team_number_fails():
    with pytest.raises(ValidationError, match="team_number must be 1, 2, or 3"):
        FactAllNbaRow(player_id="P1", season_id="2023-24", team_type="All-NBA", team_number=4)


def test_fact_all_nba_position_optional():
    row = FactAllNbaRow(
        player_id="P1", season_id="2023-24", team_type="All-Defense", position="SF"
    )
    assert row.position == "SF"


# ------------------------------------------------------------------ #
# FactAllNbaVoteRow                                                   #
# ------------------------------------------------------------------ #


def test_fact_all_nba_vote_valid():
    row = FactAllNbaVoteRow(
        player_id="2544",
        season_id="2023-24",
        team_type="All-NBA",
        pts_won=900,
        pts_max=1000,
        share=0.9,
    )
    assert row.share == pytest.approx(0.9)


def test_fact_all_nba_vote_pts_won_exceeds_max_fails():
    with pytest.raises(ValidationError, match="pts_won cannot exceed pts_max"):
        FactAllNbaVoteRow(
            player_id="P1",
            season_id="2023-24",
            team_type="All-NBA",
            pts_won=1001,
            pts_max=1000,
        )


def test_fact_all_nba_vote_pts_won_equals_max_passes():
    row = FactAllNbaVoteRow(
        player_id="P1",
        season_id="2023-24",
        team_type="All-NBA",
        pts_won=1000,
        pts_max=1000,
    )
    assert row.pts_won == 1000


def test_fact_all_nba_vote_share_out_of_range_fails():
    with pytest.raises(ValidationError):
        FactAllNbaVoteRow(
            player_id="P1", season_id="2023-24", team_type="All-NBA", share=1.5
        )


def test_fact_all_nba_vote_negative_pts_won_fails():
    with pytest.raises(ValidationError):
        FactAllNbaVoteRow(
            player_id="P1", season_id="2023-24", team_type="All-NBA", pts_won=-1
        )


def test_fact_all_nba_vote_vote_counts_valid():
    row = FactAllNbaVoteRow(
        player_id="P1",
        season_id="2023-24",
        team_type="All-NBA",
        first_team_votes=50,
        second_team_votes=30,
        third_team_votes=20,
    )
    assert row.first_team_votes == 50


# ------------------------------------------------------------------ #
# FactDraftRow                                                        #
# ------------------------------------------------------------------ #


def test_fact_draft_valid_minimal():
    row = FactDraftRow(season_id="2023-24")
    assert row.season_id == "2023-24"


def test_fact_draft_missing_season_id_fails():
    with pytest.raises(ValidationError):
        FactDraftRow(draft_round=1, overall_pick=1)


def test_fact_draft_valid_round_1_and_2():
    for r in (1, 2, None):
        row = FactDraftRow(season_id="2023-24", draft_round=r)
        assert row.draft_round == r


def test_fact_draft_invalid_round_fails():
    with pytest.raises(ValidationError, match="draft_round must be 1 or 2"):
        FactDraftRow(season_id="2023-24", draft_round=3)


def test_fact_draft_overall_pick_must_be_positive():
    with pytest.raises(ValidationError):
        FactDraftRow(season_id="2023-24", overall_pick=0)


def test_fact_draft_overall_pick_one_passes():
    row = FactDraftRow(season_id="2023-24", overall_pick=1)
    assert row.overall_pick == 1


def test_fact_draft_full_row():
    row = FactDraftRow(
        season_id="2023-24",
        draft_round=1,
        overall_pick=1,
        bref_team_abbrev="SAS",
        bref_player_id="wembavi01",
        player_name="Victor Wembanyama",
        college=None,
        lg="NBA",
    )
    assert row.player_name == "Victor Wembanyama"


# ------------------------------------------------------------------ #
# FactRosterRow                                                       #
# ------------------------------------------------------------------ #


def test_fact_roster_valid():
    row = FactRosterRow(
        player_id="2544",
        team_id="1610612747",
        season_id="2023-24",
        start_date="2023-10-01",
    )
    assert row.start_date == "2023-10-01"
    assert row.end_date is None


def test_fact_roster_missing_start_date_fails():
    with pytest.raises(ValidationError):
        FactRosterRow(player_id="P1", team_id="T1", season_id="2023-24")


def test_fact_roster_end_date_after_start_passes():
    row = FactRosterRow(
        player_id="P1",
        team_id="T1",
        season_id="2023-24",
        start_date="2023-10-01",
        end_date="2024-04-14",
    )
    assert row.end_date == "2024-04-14"


def test_fact_roster_end_date_before_start_fails():
    with pytest.raises(ValidationError, match="end_date must be after start_date"):
        FactRosterRow(
            player_id="P1",
            team_id="T1",
            season_id="2023-24",
            start_date="2023-10-01",
            end_date="2023-09-30",
        )


def test_fact_roster_end_date_equal_to_start_fails():
    with pytest.raises(ValidationError, match="end_date must be after start_date"):
        FactRosterRow(
            player_id="P1",
            team_id="T1",
            season_id="2023-24",
            start_date="2023-10-01",
            end_date="2023-10-01",
        )
```

**Step 2: Run test to verify they all fail with ImportError**

```bash
uv run pytest tests/test_etl_schemas_extended.py -v 2>&1 | head -20
```

Expected: `ImportError: cannot import name 'FactAllNbaRow' from 'src.etl.schemas'`

**Step 3: Commit the failing tests**

```bash
git add tests/test_etl_schemas_extended.py
git commit -m "test: add failing tests for 6 new Pydantic row models"
```

---

## Task 2: Add 6 Pydantic models to schemas.py

**Files:**
- Modify: `src/etl/schemas.py` (currently 125 lines — stays well under 400)

**Step 1: Update the import line at the top of the file**

The current top of `src/etl/schemas.py` is:
```python
from datetime import date

from pydantic import BaseModel, ConfigDict, Field, model_validator
```

Change to:
```python
from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
```

**Step 2: Append the 6 new models to the bottom of schemas.py**

```python
class FactPlayerAwardRow(BaseModel):
    model_config = ConfigDict(extra="ignore")
    player_id: str
    season_id: str
    award_name: str
    award_type: Literal["individual", "weekly", "monthly", "team_inclusion"]
    trophy_name: str | None = None
    votes_received: int | None = Field(default=None, ge=0)
    votes_possible: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def votes_received_le_possible(self) -> "FactPlayerAwardRow":
        if self.votes_received is not None and self.votes_possible is not None:
            if self.votes_received > self.votes_possible:
                raise ValueError("votes_received cannot exceed votes_possible")
        return self


class FactAllStarRow(BaseModel):
    model_config = ConfigDict(extra="ignore")
    player_id: str
    season_id: str
    team_id: str | None = None
    selection_team: str | None = None
    is_starter: int | None = Field(default=None)
    is_replacement: int = Field(default=0)

    @model_validator(mode="after")
    def validate_flags(self) -> "FactAllStarRow":
        if self.is_starter is not None and self.is_starter not in (0, 1):
            raise ValueError("is_starter must be 0, 1, or None")
        if self.is_replacement not in (0, 1):
            raise ValueError("is_replacement must be 0 or 1")
        return self


class FactAllNbaRow(BaseModel):
    model_config = ConfigDict(extra="ignore")
    player_id: str
    season_id: str
    team_type: str
    team_number: int | None = Field(default=None)
    position: str | None = None

    @field_validator("team_type")
    @classmethod
    def team_type_nonempty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("team_type must not be empty")
        return v

    @field_validator("team_number")
    @classmethod
    def team_number_valid(cls, v: int | None) -> int | None:
        if v is not None and v not in (1, 2, 3):
            raise ValueError("team_number must be 1, 2, or 3")
        return v


class FactAllNbaVoteRow(BaseModel):
    model_config = ConfigDict(extra="ignore")
    player_id: str
    season_id: str
    team_type: str
    team_number: int | None = None
    position: str | None = None
    pts_won: int | None = Field(default=None, ge=0)
    pts_max: int | None = Field(default=None, ge=0)
    share: float | None = Field(default=None, ge=0.0, le=1.0)
    first_team_votes: int | None = Field(default=None, ge=0)
    second_team_votes: int | None = Field(default=None, ge=0)
    third_team_votes: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def pts_won_le_max(self) -> "FactAllNbaVoteRow":
        if self.pts_won is not None and self.pts_max is not None:
            if self.pts_won > self.pts_max:
                raise ValueError("pts_won cannot exceed pts_max")
        return self


class FactDraftRow(BaseModel):
    model_config = ConfigDict(extra="ignore")
    season_id: str
    draft_round: int | None = Field(default=None)
    overall_pick: int | None = Field(default=None, ge=1)
    bref_team_abbrev: str | None = None
    bref_player_id: str | None = None
    player_name: str | None = None
    college: str | None = None
    lg: str | None = None

    @field_validator("draft_round")
    @classmethod
    def draft_round_valid(cls, v: int | None) -> int | None:
        if v is not None and v not in (1, 2):
            raise ValueError("draft_round must be 1 or 2")
        return v


class FactRosterRow(BaseModel):
    model_config = ConfigDict(extra="ignore")
    player_id: str
    team_id: str
    season_id: str
    start_date: str
    end_date: str | None = None

    @model_validator(mode="after")
    def end_after_start(self) -> "FactRosterRow":
        if self.end_date is not None and self.end_date <= self.start_date:
            raise ValueError("end_date must be after start_date")
        return self
```

**Step 3: Run the new tests to verify they pass**

```bash
uv run pytest tests/test_etl_schemas_extended.py -v
```

Expected: all ~35 tests PASS

**Step 4: Run ruff**

```bash
uv run ruff check src/etl/schemas.py && uv run ruff format src/etl/schemas.py
```

Expected: no errors

**Step 5: Commit**

```bash
git add src/etl/schemas.py
git commit -m "feat: add Pydantic row models for 6 unvalidated fact tables"
```

---

## Task 3: Register the 6 models in _ROW_MODELS and add a coverage test

**Files:**
- Modify: `src/etl/validation.py` (lines 1–30, import block and `_ROW_MODELS` dict)
- Modify: `tests/test_etl_schemas.py` (add one new test at the bottom)

**Step 1: Update the import block in validation.py**

Current imports (lines 10–18):
```python
from .schemas import (
    FactGameRow,
    FactPlayerAdvancedSeasonRow,
    FactPlayerSeasonStatsRow,
    FactPlayerShootingSeasonRow,
    FactSalaryRow,
    PlayerGameLogRow,
    TeamGameLogRow,
)
```

Replace with:
```python
from .schemas import (
    FactAllNbaRow,
    FactAllNbaVoteRow,
    FactAllStarRow,
    FactDraftRow,
    FactGameRow,
    FactPlayerAdvancedSeasonRow,
    FactPlayerAwardRow,
    FactPlayerSeasonStatsRow,
    FactPlayerShootingSeasonRow,
    FactRosterRow,
    FactSalaryRow,
    PlayerGameLogRow,
    TeamGameLogRow,
)
```

**Step 2: Update `_ROW_MODELS` dict in validation.py**

Current dict (lines 22–30):
```python
_ROW_MODELS = {
    "player_game_log": PlayerGameLogRow,
    "team_game_log": TeamGameLogRow,
    "fact_game": FactGameRow,
    "fact_salary": FactSalaryRow,
    "fact_player_season_stats": FactPlayerSeasonStatsRow,
    "fact_player_advanced_season": FactPlayerAdvancedSeasonRow,
    "fact_player_shooting_season": FactPlayerShootingSeasonRow,
}
```

Replace with:
```python
_ROW_MODELS = {
    "player_game_log": PlayerGameLogRow,
    "team_game_log": TeamGameLogRow,
    "fact_game": FactGameRow,
    "fact_salary": FactSalaryRow,
    "fact_player_season_stats": FactPlayerSeasonStatsRow,
    "fact_player_advanced_season": FactPlayerAdvancedSeasonRow,
    "fact_player_shooting_season": FactPlayerShootingSeasonRow,
    "fact_player_award": FactPlayerAwardRow,
    "fact_all_star": FactAllStarRow,
    "fact_all_nba": FactAllNbaRow,
    "fact_all_nba_vote": FactAllNbaVoteRow,
    "fact_draft": FactDraftRow,
    "fact_roster": FactRosterRow,
}
```

**Step 3: Add a coverage test to tests/test_etl_schemas.py**

Append to the bottom of `tests/test_etl_schemas.py`:

```python
# ------------------------------------------------------------------ #
# _ROW_MODELS registry coverage                                       #
# ------------------------------------------------------------------ #


def test_row_models_covers_all_validated_tables():
    """Ensure every expected table has a registered Pydantic model."""
    from src.etl.validation import _ROW_MODELS

    expected_tables = {
        "player_game_log",
        "team_game_log",
        "fact_game",
        "fact_salary",
        "fact_player_season_stats",
        "fact_player_advanced_season",
        "fact_player_shooting_season",
        "fact_player_award",
        "fact_all_star",
        "fact_all_nba",
        "fact_all_nba_vote",
        "fact_draft",
        "fact_roster",
    }
    assert set(_ROW_MODELS.keys()) == expected_tables
```

**Step 4: Run ruff and the relevant tests**

```bash
uv run ruff check src/etl/validation.py && uv run ruff format src/etl/validation.py
uv run pytest tests/test_etl_schemas.py tests/test_etl_schemas_extended.py -v
```

Expected: all tests PASS

**Step 5: Commit**

```bash
git add src/etl/validation.py tests/test_etl_schemas.py
git commit -m "feat: register 6 new row models in _ROW_MODELS validation registry"
```

---

## Task 4: Wire validate_rows() in awards.py

**Files:**
- Modify: `src/etl/awards.py`

**Step 1: Add the import**

In `src/etl/awards.py`, the existing imports end around line 7. Add one import:

```python
from .validation import validate_rows
```

Add it directly after the existing `from ..db.operations import upsert_rows` line.

**Step 2: Add validate_rows() call before upsert**

In `load_player_awards()`, find the existing code around line 190:

```python
    if not filtered_rows:
        return 0

    inserted = upsert_rows(con, "fact_player_award", filtered_rows, conflict="IGNORE")
```

Add the validation call:

```python
    if not filtered_rows:
        return 0

    filtered_rows = validate_rows("fact_player_award", filtered_rows)
    if not filtered_rows:
        return 0

    inserted = upsert_rows(con, "fact_player_award", filtered_rows, conflict="IGNORE")
```

**Step 3: Run ruff and the awards tests**

```bash
uv run ruff check src/etl/awards.py && uv run ruff format src/etl/awards.py
uv run pytest tests/test_etl_awards.py -v
```

Expected: all existing tests PASS

**Step 4: Commit**

```bash
git add src/etl/awards.py
git commit -m "feat: validate fact_player_award rows before upsert in awards.py"
```

---

## Task 5: Wire validate_rows() in roster.py

**Files:**
- Modify: `src/etl/roster.py`

**Step 1: Add the import**

In `src/etl/roster.py`, after the existing `from ..db.operations import upsert_rows` line, add:

```python
from .validation import validate_rows
```

**Step 2: Add validate_rows() call before upsert in load_team_roster()**

In `load_team_roster()`, find the code around line 102-104:

```python
    if not rows:
        return 0
    inserted = upsert_rows(con, "fact_roster", rows, conflict="IGNORE")
    return inserted
```

Add the validation call:

```python
    if not rows:
        return 0
    rows = validate_rows("fact_roster", rows)
    if not rows:
        return 0
    inserted = upsert_rows(con, "fact_roster", rows, conflict="IGNORE")
    return inserted
```

**Step 3: Run ruff and roster tests**

```bash
uv run ruff check src/etl/roster.py && uv run ruff format src/etl/roster.py
uv run pytest tests/test_etl_salaries.py -v  # no dedicated roster test file; check related
```

> **Note:** There is no `tests/test_etl_roster.py`. The roster path is exercised through integration tests. Run `uv run pytest tests/test_ingest_integration.py -v -k roster` if available, otherwise run full suite.

**Step 4: Commit**

```bash
git add src/etl/roster.py
git commit -m "feat: validate fact_roster rows before upsert in roster.py"
```

---

## Task 6: Wire validate_rows() in backfill/_all_star.py

**Files:**
- Modify: `src/etl/backfill/_all_star.py`

**Step 1: Add the import**

After the existing `from src.db.operations import upsert_rows` line:

```python
from src.etl.validation import validate_rows
```

**Step 2: Add validate_rows() call before upsert**

In `load_all_star_selections()`, find the code near the end (around line 127):

```python
    inserted = upsert_rows(con, "fact_all_star", rows)
    logger.info("fact_all_star: %d inserted/ignored, %d skipped", inserted, skipped)
    return inserted
```

Change to:

```python
    rows = validate_rows("fact_all_star", rows)
    inserted = upsert_rows(con, "fact_all_star", rows)
    logger.info("fact_all_star: %d inserted/ignored, %d skipped", inserted, skipped)
    return inserted
```

**Step 3: Run ruff and all_star tests**

```bash
uv run ruff check src/etl/backfill/_all_star.py && uv run ruff format src/etl/backfill/_all_star.py
uv run pytest tests/test_etl_backfill_all_star.py -v
```

Expected: all existing tests PASS

**Step 4: Commit**

```bash
git add src/etl/backfill/_all_star.py
git commit -m "feat: validate fact_all_star rows before upsert in _all_star.py"
```

---

## Task 7: Wire validate_rows() in backfill/_all_nba.py (two loaders)

**Files:**
- Modify: `src/etl/backfill/_all_nba.py`

**Step 1: Add the import**

After the existing `from src.db.operations import upsert_rows` line:

```python
from src.etl.validation import validate_rows
```

**Step 2: Add validate_rows() call in load_all_nba_teams()**

Find the code near line 125–127:

```python
    inserted = upsert_rows(con, "fact_all_nba", rows)
    logger.info("fact_all_nba: %d inserted/ignored, %d skipped", inserted, skipped)
    return inserted
```

Change to:

```python
    rows = validate_rows("fact_all_nba", rows)
    inserted = upsert_rows(con, "fact_all_nba", rows)
    logger.info("fact_all_nba: %d inserted/ignored, %d skipped", inserted, skipped)
    return inserted
```

**Step 3: Add validate_rows() call in load_all_nba_votes()**

Find the code near line 180–182:

```python
    inserted = upsert_rows(con, "fact_all_nba_vote", rows)
    logger.info("fact_all_nba_vote: %d inserted/ignored, %d skipped", inserted, skipped)
    return inserted
```

Change to:

```python
    rows = validate_rows("fact_all_nba_vote", rows)
    inserted = upsert_rows(con, "fact_all_nba_vote", rows)
    logger.info("fact_all_nba_vote: %d inserted/ignored, %d skipped", inserted, skipped)
    return inserted
```

**Step 4: Run ruff and all_nba tests**

```bash
uv run ruff check src/etl/backfill/_all_nba.py && uv run ruff format src/etl/backfill/_all_nba.py
uv run pytest tests/test_etl_backfill_all_nba.py -v
```

Expected: all existing tests PASS

**Step 5: Commit**

```bash
git add src/etl/backfill/_all_nba.py
git commit -m "feat: validate fact_all_nba and fact_all_nba_vote rows before upsert"
```

---

## Task 8: Wire validate_rows() in backfill/_draft.py

**Files:**
- Modify: `src/etl/backfill/_draft.py`

**Step 1: Add the import**

After the existing `from src.db.operations import upsert_rows` line:

```python
from src.etl.validation import validate_rows
```

**Step 2: Add validate_rows() call before upsert in load_draft()**

Find the code near line 98:

```python
    inserted = upsert_rows(con, "fact_draft", rows)
    logger.info(
        "fact_draft: %d inserted/ignored, %d skipped",
        inserted,
        skipped,
    )
```

Change to:

```python
    rows = validate_rows("fact_draft", rows)
    inserted = upsert_rows(con, "fact_draft", rows)
    logger.info(
        "fact_draft: %d inserted/ignored, %d skipped",
        inserted,
        skipped,
    )
```

**Step 3: Run ruff and draft tests**

```bash
uv run ruff check src/etl/backfill/_draft.py && uv run ruff format src/etl/backfill/_draft.py
uv run pytest tests/test_etl_backfill_draft.py -v
```

Expected: all existing tests PASS

**Step 4: Commit**

```bash
git add src/etl/backfill/_draft.py
git commit -m "feat: validate fact_draft rows before upsert in _draft.py"
```

---

## Task 9: Final verification

**Step 1: Run the full test suite**

```bash
uv run pytest tests/ -v --tb=short 2>&1 | tail -20
```

Expected: all tests PASS (1025+, 2 skipped)

**Step 2: Verify ruff is clean across all changed files**

```bash
uv run ruff check src/etl/schemas.py src/etl/validation.py src/etl/awards.py src/etl/roster.py src/etl/backfill/_all_star.py src/etl/backfill/_all_nba.py src/etl/backfill/_draft.py
```

Expected: `All checks passed!`

**Step 3: Check file sizes remain under 400 lines**

```bash
wc -l src/etl/schemas.py src/etl/validation.py
```

Expected: schemas.py ≤ 220 lines, validation.py ≤ 215 lines

---

## Summary of Commits

| Commit | Message |
|--------|---------|
| 1 | `test: add failing tests for 6 new Pydantic row models` |
| 2 | `feat: add Pydantic row models for 6 unvalidated fact tables` |
| 3 | `feat: register 6 new row models in _ROW_MODELS validation registry` |
| 4 | `feat: validate fact_player_award rows before upsert in awards.py` |
| 5 | `feat: validate fact_roster rows before upsert in roster.py` |
| 6 | `feat: validate fact_all_star rows before upsert in _all_star.py` |
| 7 | `feat: validate fact_all_nba and fact_all_nba_vote rows before upsert` |
| 8 | `feat: validate fact_draft rows before upsert in _draft.py` |
