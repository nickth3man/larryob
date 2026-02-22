# src/db — Database Layer

## OVERVIEW

Two-database architecture: SQLite (OLTP writes) + DuckDB (OLAP reads). DuckDB attaches SQLite via `sqlite_scanner` extension — no data duplication.

## FILES

| File | Role |
|------|------|
| `schema.py` | All SQLite DDL + migration ALTERs. `init_db()` is the only write path to schema. |
| `analytics.py` | DuckDB factory + all analytical VIEWs as `(name, SQL)` pairs in `_VIEWS`. |

## ANALYTICS VIEWS (`_VIEWS` in analytics.py)

| View | Purpose |
|------|---------|
| `vw_player_shooting` | eFG%, TS%, per-season shooting efficiency |
| `vw_player_season_totals` | Season totals from `player_game_log` |
| `vw_player_last10` | Rolling last-10 games stats |
| `vw_team_standings` | W/L, PCT, GB by season |
| `vw_team_pace` | Pace and possessions |
| `vw_pbp_shot_summary` | Play-by-play shot outcomes |
| `vw_player_awards` | Awards with player names joined |
| `vw_salary_cap_pct` | Player salary as % of cap |
| `vw_player_per36` | Per-36-minute stats |
| `vw_player_usage` | Usage rate and shot distribution |
| `vw_team_ratings` | Off/Def/Net rating |
| `vw_player_clutch` | Clutch-time (PBP-derived) stats |
| `vw_player_season_advanced` | Calculated advanced stats (PER etc.) |
| `vw_player_per100` | Per-100-possessions stats |
| `vw_player_advanced_full` | Joins calculated + bref precomputed advanced |
| `vw_team_four_factors` | Dean Oliver four factors |
| `vw_draft_class` | Draft history with career stats |
| `vw_player_shooting_zones` | Zone-level shooting breakdown |

## CONVENTIONS

- All view SQL references SQLite tables as `nba.<table>` (DuckDB attachment alias)
- `get_duck_con()` is NOT thread-safe — uses `threading.local()` internally; each thread gets its own connection
- Schema is idempotent: `CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`
- New column migrations go in `ALTER_STATEMENTS` (not `DDL_STATEMENTS`) — wrapped in try/except for idempotency
- `ROLLBACK_STATEMENTS` exists for reversing column additions — keep in sync with `ALTER_STATEMENTS`
- `DB_PATH` resolves to `<repo_root>/nba_raw_data.db`; tests never touch this file (use `:memory:` or `tmp_path`)
- All tables use `STRICT` mode — SQLite enforces column types strictly

## ADDING A NEW VIEW

Append to `_VIEWS` list in `analytics.py`:
```python
(
    "vw_my_view",
    """
    SELECT ...
    FROM nba.player_game_log l
    JOIN nba.dim_player p ON p.player_id = l.player_id
    """,
),
```
No migration needed — views are dropped and recreated on each `get_duck_con()` call.

## ADDING A NEW TABLE

1. Add `CREATE TABLE IF NOT EXISTS ... STRICT` DDL to `DDL_STATEMENTS` in `schema.py`
2. Add any column indexes to `DDL_STATEMENTS` as `CREATE INDEX IF NOT EXISTS`
3. Add `ALTER_STATEMENTS` entries only for subsequent column additions (not initial DDL)
4. Update `tests/conftest.py` fixtures if the table needs seed data for FK compliance

## ANTI-PATTERNS

- Never write to SQLite from `analytics.py` — read-only DuckDB layer
- Never add executable DDL to `ALTER_STATEMENTS` for new tables — use `DDL_STATEMENTS`
- Never call `get_duck_con()` concurrently from multiple threads without separate connections
- Never hardcode `DB_PATH` in tests — always use `:memory:` or `tmp_path`
