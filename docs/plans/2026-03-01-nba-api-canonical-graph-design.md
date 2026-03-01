# NBA API Canonical Graph Design

Date: 2026-03-01  
Status: Approved design (ready for implementation planning)

## 1. Objective

Ingest *all* available NBA and BAA data from `nba_api` into this project's database using a game-centric canonical graph model, with strict parity guarantees for canonical fields.

Target scope includes:
- Players
- Teams and franchise history
- Games
- Team and player box scores for every game
- Play-by-play events
- Rosters and coaches
- Draft and awards

The primary system of record is `nba_api` (Stats + Live endpoints where applicable), not local/raw CSVs.

## 2. Design Principles

- Canonical-first: each table has a defined source of truth endpoint.
- Deterministic ingest: idempotent upserts keyed by natural IDs.
- Strict parity: no tolerance windows for canonical numeric/string fields.
- Replayable: source fingerprints enable deterministic reprocessing.
- Explicit failure: blocking mismatches fail the run (no silent drift).

## 3. Canonical Source Strategy

### 3.1 Source precedence

For each logical domain:
- Game inventory and schedule/state:
  - `ScheduleLeagueV2`
  - `ScoreboardV3` (status and score corrections)
- Game team/player box scores:
  - `BoxScoreTraditionalV3` (canonical for core box stats)
  - `BoxScore*V3` siblings for additional stat domains
- Event timeline:
  - `PlayByPlayV3` only
- Player/team identity:
  - `CommonAllPlayers`
  - `CommonPlayerInfo`
  - `stats.static.players` / `stats.static.teams` for bootstrap support
- Franchise history:
  - `FranchiseHistory`
- Draft:
  - `DraftHistory`
- Rosters and coaches:
  - `CommonTeamRoster`
- Standing-level reconciliation:
  - `LeagueStandingsV3`
  - `LeagueGameLog` / `LeagueGameFinder` for cross-checks

### 3.2 Explicit endpoint constraints

- Do not use `PlayByPlayV2` (deprecated, empty JSON behavior).
- Do not use `ScoreboardV2` for canonical ingest (known line-score issues in 2025-26 interval).
- Do not rely on `LeagueGameFinder.game_id_nullable` server-side filtering; filter client-side.

## 4. Endpoint-to-Table Mapping

## 4.1 Dimensions

- `dim_season`
  - Source: generated range + observed seasons from schedule/game/player endpoints
  - Rule: continuous coverage from `1946-47` to current season

- `dim_team`
  - Source precedence:
    - `FranchiseHistory`
    - `stats.static.teams`
    - observed game team IDs
  - Rule: `team_id` values must match API IDs exactly

- `dim_team_history`
  - Source:
    - `FranchiseHistory` dataset
    - `DefunctTeams` dataset

- `dim_player`
  - Source precedence:
    - `CommonAllPlayers`
    - `CommonPlayerInfo` hydration
  - Rule: canonical key is API `player_id`; no synthetic IDs

- `dim_player_identifier` / `dim_team_identifier`
  - Source: canonical mapping built during ingest
  - Rule: only auditable mappings with deterministic keys

## 4.2 Core facts

- `fact_game`
  - Source precedence:
    - `ScheduleLeagueV2` baseline
    - `ScoreboardV3` correction layer
  - Rule: one canonical row per `game_id`

- `team_game_log`
  - Source:
    - `BoxScoreTraditionalV3` team dataset
  - Rule: exactly 2 rows for each final game

- `player_game_log`
  - Source:
    - `BoxScoreTraditionalV3` player dataset
  - Rule: strict key `(game_id, player_id)` and valid `team_id`

- `fact_play_by_play`
  - Source:
    - `PlayByPlayV3`
  - Rule: stable event key from `(game_id, actionNumber)`

## 4.3 Context facts

- `fact_roster`
  - Source:
    - `CommonTeamRoster`
  - Grain:
    - `(player_id, team_id, season_id)` with temporal fields

- `dim_coach` and `fact_team_coach_game`
  - Source:
    - `CommonTeamRoster` coaches dataset

- `fact_draft`
  - Source:
    - `DraftHistory`

- `fact_player_award`
  - Source:
    - `PlayerAwards`

## 4.4 Aggregate/reconciliation layers

- `fact_team_season`, `fact_player_season_stats`, and advanced layers
  - Source:
    - canonical game-level rollups where possible
    - `LeagueDash*` / `PlayerCareerStats` endpoints for domain-level parity checks and additional measures
  - Rule: reconcile aggregate totals to canonical game-level facts wherever mathematically applicable

## 5. Validation Contract

## 5.1 Completeness gates (blocking)

- No missing seasons from `1946-47` to current.
- All API games appear in `fact_game`.
- Every final game has complete team box coverage (2 rows).
- Every final game has player box rows.
- Every final game has PBP rows unless endpoint explicitly indicates unavailable.

## 5.2 Exactness gates (blocking)

- Exact equality for canonical keys (`game_id`, `team_id`, `player_id`).
- Exact equality for canonical scores/stat fields.
- Controlled enum/status mapping only; unmapped values fail.

## 5.3 Cross-endpoint reconciliation (blocking)

- `fact_game` final score equals team totals from `BoxScoreTraditionalV3` and scoreboard line score.
- Season W/L derived from `fact_game` reconciles to `LeagueStandingsV3` where supported.
- Player season totals aggregated from game logs reconcile to `LeagueDashPlayerStats` for aligned filters.

## 5.4 Drift and freshness

- Persist response fingerprint (`endpoint + params + payload hash + fetched_at`).
- Re-run affected game/season loads when source hash changes.
- Schema drift (new/missing columns or parser mismatch) triggers quarantine + run failure.

## 6. Orchestration Flow

## 6.1 Run profiles

- `full_backfill`
  - Seasons: `1946-47` to current
  - Purpose: complete canonical graph construction

- `incremental_daily`
  - Scope: active season + recent correction window
  - Purpose: freshness and late correction capture

## 6.2 Execution order

1. Seed dimensions and identifier crosswalks.
2. Build canonical game inventory from schedule + scoreboard.
3. Fan out by `game_id`:
   - ingest box scores
   - ingest play-by-play
4. Ingest roster/coach context per team-season.
5. Ingest draft/awards and other context domains.
6. Compute/reconcile aggregates.
7. Run blocking validation gates.
8. Publish only if all blocking gates pass.

## 6.3 Batching and concurrency

- Unit of work: `game_id`.
- Inventory stages run sequentially.
- Game-node ingest runs in bounded parallel workers.
- Single global rate-limit controller shared by workers.
- Checkpoints at:
  - season
  - game
  - endpoint fingerprint

## 6.4 Retry and replay

- Exponential backoff with jitter for transient failures.
- Endpoint cooldown on repeated anti-bot/rate-limit responses.
- Dead-letter queue for unreconciled game nodes.
- Idempotent replay by key + hash.

## 6.5 Incremental correction loop

- Re-validate last N days (recommended 14) each run.
- If fingerprint drift detected, reprocess impacted game node and dependent aggregates.

## 7. Error Handling Policy

- Transport/transient errors:
  - retry with capped attempts
- Rate-limit/anti-bot responses:
  - throttle increase + deferred replay
- Schema drift:
  - hard-fail endpoint ingest; quarantine payload
- Semantic integrity failures:
  - hard-fail affected node and block run success
- Partial historical availability:
  - allowed only when policy explicitly marks endpoint/time-range as non-blocking

No run is marked `ok` while blocking gates remain unresolved.

## 8. Test Strategy

## 8.1 Test layers

- Unit tests:
  - transformers/parsers/mappers
- Contract tests:
  - endpoint schema and parser compatibility
- Live canaries:
  - minimal real API checks, including early-era season coverage
- Integration tests:
  - season slice through full game-centric flow
- Parity tests:
  - strict API-vs-DB equality on canonical fields
- Full-run audits:
  - completeness and exactness SQL gate suite

## 8.2 Pass/fail criteria by table

- `dim_season`: complete continuous range
- `dim_team`: all referenced team IDs resolvable
- `dim_player`: all referenced player IDs resolvable
- `fact_game`: game set equality to canonical inventory
- `team_game_log`: exactly 2 rows per final game
- `player_game_log`: valid player-team-game relationships
- `fact_play_by_play`: unique event keys and source parity
- `fact_roster`/coach tables: complete where source returns rows
- `fact_draft`/awards: key uniqueness + foreign key integrity

## 9. Deliverables for Implementation Phase

- New ingestion modules for:
  - game inventory
  - V3 boxscore canonical load
  - V3 play-by-play canonical load
  - roster/coach canonical load
  - reconciliation + parity reporting
- Migration of existing deprecated endpoint usage (`PlayByPlayV2`, `ScoreboardV2`) to V3 workflow.
- Blocking validation suite integrated with pipeline exit codes.
- Run manifest + parity report artifacts for each execution.

## 10. Open Risks and Mitigations

- API behavioral changes:
  - mitigate with schema contract tests + drift quarantine
- Historical endpoint gaps:
  - encode explicit non-blocking policy only for proven endpoint-era limits
- Throughput/time for full backfill:
  - mitigate with game-level checkpointing and bounded parallel fanout

## 11. Proposed CLAUDE.md and AGENTS.md Addition

One-line addition to reduce repeated ingest errors:

`Do not use PlayByPlayV2 or ScoreboardV2 for canonical ingest; always use PlayByPlayV3 and ScoreboardV3.`

