# NBA Database Completion Report

_Generated: 2026-02-26T21:02:38.502287+00:00_

## Table Row Counts

| Table | Rows |
|-------|-----:|
| `dim_season` | 80 |
| `dim_team` | 30 |
| `dim_player` | 5,115 |
| `dim_salary_cap` | 41 |
| `dim_league_season` | 79 |
| `dim_team_history` | 68 |
| `fact_roster` | 23,428 |
| `fact_game` | 72,721 |
| `fact_play_by_play` | 0 |
| `fact_player_award` | 10,955 |
| `fact_all_star` | 1,889 |
| `fact_all_nba` | 2,107 |
| `fact_all_nba_vote` | 4,331 |
| `fact_salary` | 2,567 |
| `fact_team_season` | 1,867 |
| `fact_draft` | 8,383 |
| `fact_player_season_stats` | 32,606 |
| `fact_player_advanced_season` | 4,566 |
| `fact_player_shooting_season` | 17,513 |
| `fact_player_pbp_season` | 17,521 |
| `team_game_log` | 145,337 |
| `player_game_log` | 1,478,707 |
| `etl_run_log` | 742 |

## Column Coverage

### `dim_player.bref_id` (93.4% populated)

- Total players: 5,115
- Non-NULL (have bref_id): 4,775
- NULL (missing bref_id): 340

### `dim_team.bref_abbrev` (100.0% populated)

- Total teams: 30
- Non-NULL (have bref_abbrev): 30
- NULL (missing bref_abbrev): 0

## ETL Run Log Summary

### Overall status totals

- `empty`: 12
- `ok`: 632
- `partial`: 2
- `partial_rate_limited`: 3
- `rate_limited`: 93

### Per-table breakdown

| table_name | empty | ok | partial | partial_rate_limited | rate_limited |
|------------|------:|------:|------:|------:|------:|
| `dim_league_season` | 0 | 1 | 0 | 0 | 0 |
| `dim_player` | 0 | 5 | 0 | 0 | 0 |
| `dim_season` | 0 | 80 | 0 | 0 | 0 |
| `dim_team` | 0 | 2 | 0 | 0 | 0 |
| `dim_team_history` | 0 | 1 | 0 | 0 | 0 |
| `fact_all_nba` | 0 | 1 | 0 | 0 | 0 |
| `fact_all_nba_vote` | 0 | 1 | 0 | 0 | 0 |
| `fact_all_star` | 0 | 1 | 0 | 0 | 0 |
| `fact_draft` | 0 | 1 | 0 | 0 | 0 |
| `fact_game` | 4 | 145 | 0 | 0 | 0 |
| `fact_play_by_play` | 0 | 0 | 2 | 0 | 0 |
| `fact_player_advanced_season` | 0 | 1 | 0 | 0 | 0 |
| `fact_player_award` | 0 | 3 | 0 | 0 | 0 |
| `fact_player_pbp_season` | 0 | 1 | 0 | 0 | 0 |
| `fact_player_season_stats` | 0 | 1 | 0 | 0 | 0 |
| `fact_player_shooting_season` | 0 | 1 | 0 | 0 | 0 |
| `fact_roster` | 0 | 78 | 0 | 0 | 0 |
| `fact_salary` | 0 | 20 | 0 | 3 | 93 |
| `fact_team_season` | 0 | 1 | 0 | 0 | 0 |
| `player_game_log` | 4 | 144 | 0 | 0 | 0 |
| `team_game_log` | 4 | 144 | 0 | 0 | 0 |

## Key Gaps

- `fact_play_by_play` is **empty** — play-by-play data not yet ingested.
- 340 player(s) missing `bref_id` (6.6% uncovered). See `players_missing_bref_id.txt`.
- 1 table(s) exist but have 0 rows: `fact_play_by_play`.

## Salary Season Coverage

| Season | Rows |
|--------|-----:|
| 1985-86 | 236 |
| 1986-87 | 39 |
| 2000-01 | 98 |
| 2010-11 | 411 |
| 2022-23 | 444 |
| 2023-24 | 450 |
| 2024-25 | 432 |
| 2025-26 | 457 |
