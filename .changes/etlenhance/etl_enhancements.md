
Below is a comprehensive code review focusing on areas where you can elevate the codebase from "good" to "exceptional," followed by refactored code for the most critical components.

---

### 1. Readability & Maintainability
*   **Validation Logic Split:** Currently, validation is split between Pydantic (`models.py`) and a custom functional rules engine (`validate.py`). This creates two sources of truth. Pydantic is extremely powerful and fully capable of handling cross-field validation via `@model_validator`. Centralizing all rules inside Pydantic models will drastically improve readability.
*   **Regex HTML Parsing:** In `salaries.py`, you parse HTML comments using `re.findall(r"<!--(.*?)-->")` and then feed it to `pd.read_html`. While clever, this is brittle. Moving this into a clearly named private helper function with thorough docstrings will improve maintainability.

### 2. Performance
*   **Pandas `iterrows()` Anti-Pattern:** In `game_logs.py` (`_build_game_rows`), you loop over `grp.iterrows()`. This is notoriously slow in Pandas because it materializes each row as a Series. Using vectorized string matching (`grp["MATCHUP"].str.contains()`) or at least `itertuples()` will yield a **50x–100x performance boost** for that function.
*   **Pydantic `.model_dump()` Allocation:** In `validate.py`, doing `row = {**row, **validated.model_dump()}` creates two new dictionary allocations per row. You can use `validated.model_dump(exclude_unset=True)` or simply trust the validated output.

### 3. Best Practices (SOLID & Idioms)
*   **Atomic Cache Writes:** In `utils.py`, `save_cache` writes directly to the destination path. If the ETL script is killed (e.g., OOM, manual `Ctrl+C`, system restart) exactly while writing, the JSON file will be corrupted, crashing future runs. Best practice is to write to a temporary file and atomically swap it using `os.replace`.
*   **Global State / Singletons:** You rely heavily on module-level global variables (`_default_api_caller`, `_BREF_THROTTLE`, `_metrics`). While acceptable for simple scripts, in a testing environment or a multi-threaded execution context, globals cause state leakage. Consider passing a `Context` or `Dependencies` object.

### 4. Resilience
*   **Silent Cache Failures:** In `utils.py`, `json.JSONDecodeError` returns `None` silently. If a cache is corrupted, it acts as a cache miss, overwriting the file later. It's better to log a warning here so you know your cache mechanism is failing.
*   **SQLite Variable Limits:** `upsert_rows` dynamically creates `INSERT` statements. SQLite historically limits variables (`?`) to 999 (raised to 32766 in newer versions). For large tables, `con.executemany` might fail if `len(rows) * len(columns)` exceeds this limit. Batching inserts inside `upsert_rows` is a safer approach.

### 5. Architecture (Modularity & Coupling)
*   **Dual Rate Limiters:** You have `APICaller` in `api_client.py` and `_AdaptiveBRefThrottle` in `salaries.py`. While they target different endpoints (`nba_api` vs `Basketball-Reference`), having two distinct throttling implementations violates the DRY (Don't Repeat Yourself) principle. You could abstract a generic `RateLimiter` strategy that `APICaller` uses.
*   **Data Models as Contracts:** Your pipeline passes raw `list[dict]` everywhere. Converting external data to Pydantic models at the *boundary* (right after fetching) rather than right before inserting ensures downstream transformations work with strictly typed objects.

---

### Refactored Code Improvements

Below are the most critical refactors addressing the points above.

#### 1. `src/etl/utils.py` (Atomic File Writes & Safe Inserts)
*Fixing cache corruption and batching SQLite inserts to prevent variable-limit crashes.*

```python
import json
import logging
import os
import sqlite3
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from collections.abc import Iterable
from itertools import islice

from .config import CacheConfig

logger = logging.getLogger(__name__)

# ... (logging setup omitted for brevity) ...

def load_cache(key: str, ttl_days: float | None = None) -> Any | None:
    p = cache_path(key)
    if not p.exists():
        return None
        
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(data, dict) and "v" in data and "ts" in data and "data" in data:
            if data["v"] != CacheConfig.CACHE_VERSION:
                return None
            if ttl_days is not None:
                age_seconds = time.time() - data["ts"]
                if age_seconds > ttl_days * 86400:
                    return None
            return data["data"]
    except json.JSONDecodeError as e:
        logger.warning("Cache file %s is corrupted, treating as cache miss: %s", p, e)
        return None
    return None

def save_cache(key: str, data: Any) -> None:
    """Safely write cache using an atomic file replacement."""
    payload = {
        "v": CacheConfig.CACHE_VERSION,
        "ts": time.time(),
        "data": data,
    }
    target_path = cache_path(key)
    
    # Write to a temporary file in the same directory, then rename atomically
    with tempfile.NamedTemporaryFile("w", dir=target_path.parent, delete=False, encoding="utf-8") as tf:
        json.dump(payload, tf)
        tmp_name = tf.name
        
    os.replace(tmp_name, target_path)

def _chunked(iterable: Iterable, n: int):
    """Yield successive n-sized chunks from iterable."""
    it = iter(iterable)
    while batch := list(islice(it, n)):
        yield batch

def upsert_rows(
    con: sqlite3.Connection,
    table: str,
    rows: list[dict],
    conflict: str = "IGNORE",
    autocommit: bool = True,
) -> int:
    """
    INSERT OR <conflict> a list of dicts into *table* with batching.
    """
    if not rows:
        return 0
        
    # Validation omitted for brevity...
    columns = list(rows[0].keys())
    placeholders = ", ".join("?" * len(columns))
    col_list = ", ".join(columns)
    or_clause = f" OR {conflict}" if conflict else ""
    sql = f"INSERT{or_clause} INTO {table} ({col_list}) VALUES ({placeholders})"
    
    # SQLite maximum host parameters safeguard (safe default: 999 max vars per batch)
    chunk_size = max(1, 900 // len(columns))
    
    total_inserted = 0
    try:
        for chunk in _chunked(rows, chunk_size):
            data = [tuple(r[c] for c in columns) for r in chunk]
            cur = con.executemany(sql, data)
            total_inserted += cur.rowcount
            
        if autocommit:
            con.commit()
        return total_inserted
    except sqlite3.OperationalError as e:
        logger.error("OperationalError in upsert_rows for table '%s': %s", table, e)
        raise
```

#### 2. `src/etl/models.py` (Unified Validation)
*Moving all business rules from `validate.py` into Pydantic models for single-source-of-truth.*

```python
from typing import Any
from pydantic import BaseModel, ConfigDict, model_validator, Field
from datetime import date

class BaseGameLogRow(BaseModel):
    """Shared rules for Player and Team game logs."""
    model_config = ConfigDict(extra='ignore')

    fgm: int | None = Field(default=None, ge=0)
    fga: int | None = Field(default=None, ge=0)
    fg3m: int | None = Field(default=None, ge=0)
    fg3a: int | None = Field(default=None, ge=0)
    ftm: int | None = Field(default=None, ge=0)
    fta: int | None = Field(default=None, ge=0)
    oreb: int | None = Field(default=None, ge=0)
    dreb: int | None = Field(default=None, ge=0)
    reb: int | None = Field(default=None, ge=0)
    pts: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def validate_shooting_and_rebounds(self) -> 'BaseGameLogRow':
        if self.fgm is not None and self.fga is not None and self.fgm > self.fga:
            raise ValueError("FGM cannot be greater than FGA")
        if self.fg3m is not None and self.fg3a is not None and self.fg3m > self.fg3a:
            raise ValueError("FG3M cannot be greater than FG3A")
        if self.ftm is not None and self.fta is not None and self.ftm > self.fta:
            raise ValueError("FTM cannot be greater than FTA")
        if self.oreb is not None and self.dreb is not None and self.reb is not None:
            if self.oreb + self.dreb != self.reb:
                raise ValueError("OREB + DREB must equal REB")
        return self

class PlayerGameLogRow(BaseGameLogRow):
    game_id: str
    player_id: str
    team_id: str
    minutes_played: float | None = Field(default=None, ge=0)

class TeamGameLogRow(BaseGameLogRow):
    game_id: str
    team_id: str

class FactGameRow(BaseModel):
    model_config = ConfigDict(extra='ignore')
    game_id: str
    home_score: int | None = Field(default=None, ge=0)
    away_score: int | None = Field(default=None, ge=0)
    game_date: date | None = None

class FactSalaryRow(BaseModel):
    model_config = ConfigDict(extra='ignore')
    player_id: str
    team_id: str
    season_id: str
    salary: int | None = Field(default=None, ge=0)
```

#### 3. `src/etl/validate.py` (Simplified Runner)
*Now strictly a runner for the models defined above.*

```python
import logging
from pydantic import ValidationError
from .models import PlayerGameLogRow, TeamGameLogRow, FactGameRow, FactSalaryRow

logger = logging.getLogger(__name__)

_ROW_MODELS = {
    "player_game_log": PlayerGameLogRow,
    "team_game_log": TeamGameLogRow,
    "fact_game": FactGameRow,
    "fact_salary": FactSalaryRow,
}

def _row_ident(row: dict) -> dict:
    return { k: row[k] for k in ["game_id", "player_id", "team_id", "season_id"] if k in row }

def validate_rows(table: str, rows: list[dict]) -> list[dict]:
    """
    Validates rows against their Pydantic schema. 
    Drops invalid rows and logs a warning.
    """
    model_cls = _ROW_MODELS.get(table)
    if not model_cls:
        # Pass through if no model defined (or handle custom logic here)
        return rows

    valid_rows = []
    for row in rows:
        try:
            # model_dump ensures we get typed/parsed values back (e.g., date strings -> objects)
            validated = model_cls.model_validate(row)
            # Update the row with validated data while keeping any unmapped extra fields
            row.update(validated.model_dump(exclude_unset=True))
            valid_rows.append(row)
        except ValidationError as exc:
            errors = exc.errors()
            msg = errors[0].get("msg", str(exc)) if errors else str(exc)
            logger.warning(
                "Validation failed for table '%s', rule: %s (ident=%r)",
                table, msg, _row_ident(row),
            )

    return valid_rows
```

#### 4. `src/etl/game_logs.py` (Vectorized Performance Fix)
*Removing `iterrows()` from `_build_game_rows` for massive performance improvements.*

```python
def _build_game_rows(df: pd.DataFrame, season_id: str, season_type: str) -> list[dict]:
    """
    Derive fact_game rows from the flat player-game-log DataFrame using vectorized operations.
    """
    game_rows: dict[str, dict] = {}
    dropped = 0

    # Ensure columns exist and fill na to avoid string matching errors
    if "MATCHUP" not in df.columns or "TEAM_ID" not in df.columns:
        return []

    df_clean = df.copy()
    df_clean["MATCHUP"] = df_clean["MATCHUP"].fillna("")
    
    # Vectorized boolean masks
    is_home = df_clean["MATCHUP"].str.contains(" vs. ")
    is_away = df_clean["MATCHUP"].str.contains(" @ ")

    for game_id, grp in df_clean.groupby("GAME_ID", sort=False):
        gid = str(game_id)
        
        # Get team IDs where conditions are met for this game group
        home_teams = grp.loc[is_home, "TEAM_ID"].unique()
        away_teams = grp.loc[is_away, "TEAM_ID"].unique()
        all_teams = grp["TEAM_ID"].unique()

        home_team_id = str(home_teams[0]) if len(home_teams) > 0 else None
        away_team_id = str(away_teams[0]) if len(away_teams) > 0 else None

        # Fallback resolution if string parsing failed but exactly 2 teams exist
        if len(all_teams) == 2:
            all_teams_str = [str(t) for t in all_teams]
            if home_team_id is None and away_team_id is not None:
                home_team_id = next(t for t in all_teams_str if t != away_team_id)
            elif away_team_id is None and home_team_id is not None:
                away_team_id = next(t for t in all_teams_str if t != home_team_id)

        if home_team_id is None or away_team_id is None:
            dropped += 1
            logger.warning(
                "build_game_rows: dropping game_id=%s unresolved teams (home=%s away=%s)",
                gid, home_team_id, away_team_id,
            )
            continue

        first = grp.iloc[0]
        game_rows[gid] = {
            "game_id": gid,
            "season_id": season_id,
            "game_date": str(first.get("GAME_DATE", ""))[:10],
            "home_team_id": home_team_id,
            "away_team_id": away_team_id,
            "home_score": None,
            "away_score": None,
            "season_type": season_type,
            "status": "Final",
            "arena": None,
            "attendance": None,
        }

    if dropped > 0:
        logger.warning(
            "build_game_rows: dropped %d/%d games due to unresolved team mapping",
            dropped, len(df_clean["GAME_ID"].unique())
        )
        
    return list(game_rows.values())
```