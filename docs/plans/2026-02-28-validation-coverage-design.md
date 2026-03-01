# Design: Row-Level Validation Coverage for Unvalidated Fact Tables

**Date:** 2026-02-28
**Status:** Approved
**Scope:** Data quality — extend Pydantic validation to 6 fact tables currently loaded without schema checks

---

## Background

`validate_rows()` in `src/etl/validation.py` provides Pydantic-based validation for 7 tables (game logs, salaries, advanced stats). Six additional fact tables are loaded directly to `upsert_rows()` with only FK pre-filtering and no field-level validation:

- `fact_player_award` (awards.py)
- `fact_all_star` (backfill/_all_star.py)
- `fact_all_nba` (backfill/_all_nba.py)
- `fact_all_nba_vote` (backfill/_all_nba.py)
- `fact_draft` (backfill/_draft.py)
- `fact_roster` (roster.py)

**Failure mode:** Drop + warn (consistent with existing `validate_rows()` behavior).

---

## Architecture

Three layers of change, no new files, no schema migrations:

```
src/etl/schemas.py       ← +6 Pydantic model classes
src/etl/validation.py    ← +6 entries in _ROW_MODELS dict
5 loader files           ← +1 import, +1 validate_rows() call each
```

---

## Pydantic Schema Designs

All models use `ConfigDict(extra="ignore")` to match existing conventions.

### FactPlayerAwardRow

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
```

### FactAllStarRow

```python
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
```

### FactAllNbaRow

```python
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
```

### FactAllNbaVoteRow

```python
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
```

### FactDraftRow

```python
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
```

### FactRosterRow

```python
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

---

## Loader Wiring

Pattern (identical to existing `game_logs.py`):

```python
from src.etl.validation import validate_rows  # add to imports

# Before upsert_rows():
rows = validate_rows("fact_player_award", rows)
```

| Loader file | Function | Insert before |
|-------------|----------|---------------|
| `src/etl/awards.py` | `load_player_awards` | `upsert_rows(con, "fact_player_award", filtered_rows, ...)` |
| `src/etl/roster.py` | `load_team_roster` | `upsert_rows(con, "fact_roster", rows, ...)` |
| `src/etl/backfill/_all_star.py` | `load_all_star_selections` | `upsert_rows(con, "fact_all_star", rows)` |
| `src/etl/backfill/_all_nba.py` | `load_all_nba_teams` | `upsert_rows(con, "fact_all_nba", rows)` |
| `src/etl/backfill/_all_nba.py` | `load_all_nba_votes` | `upsert_rows(con, "fact_all_nba_vote", rows)` |
| `src/etl/backfill/_draft.py` | `load_draft` | `upsert_rows(con, "fact_draft", rows)` |

---

## Test Plan

New test file: `tests/test_etl_schemas_extended.py`

For each model:
- Valid row passes validation
- Missing required field raises `ValidationError`
- Out-of-range value raises `ValidationError`
- Cross-field violation raises `ValidationError`
- Boundary condition (at limit) passes

Also update `tests/test_etl_schemas.py` to verify all 13 tables are registered in `_ROW_MODELS`.

---

## Dependency Rules

- `db`: unchanged
- `etl`: schemas.py and validation.py updated; loaders gain one import each
- `pipeline`: unchanged
- `tests`: new test file added

## File Size Check

- `src/etl/schemas.py`: 125 lines → ~200 lines (well under 400 limit)
- `src/etl/validation.py`: 200 lines → ~210 lines (unchanged in structure)
- All loader files: each gains ≤3 lines (import + 1 call)
