# Pragmatic NBA DB Completion Plan (Research-Driven)

## Summary
Apply the strongest actionable improvements from `research/4.md`, `research/5.md`, and `research/resources.md` to close high-impact data gaps: historical play-by-play, salary coverage, and player `bref_id`/HOF enrichment. Implement source-strategy ingestion (`open data + existing API/scrape`), conservative ID matching, and a repeatable completion audit that regenerates the remaining-missing player list.

## Scope
- In: Completion-focused ingestion for `fact_play_by_play`, `fact_salary`, `dim_player.bref_id`, `dim_player.hof`; run evaluation/reporting; tests and CLI/config wiring.
- Out: New advanced domains (tracking/lineups/betting), paid data vendors, major OLAP redesign.

## Public Interfaces / Types
1. Add `pbp_source: Literal["auto","api","bulk"]` to `IngestConfig` in `src/pipeline/models.py`; default `"auto"`.
2. Add `pbp_bulk_dir: str` to `IngestConfig`; default `"raw/pbp"`.
3. Add `salary_source: Literal["auto","bref","open"]` to `IngestConfig`; default `"auto"`.
4. Add `salary_open_file: str | None` to `IngestConfig`; default `None`.
5. Add `completion_report: str | None` to `IngestConfig`; used when report output is requested.
6. Add CLI flags in `src/pipeline/cli.py`: `--pbp-source`, `--pbp-bulk-dir`, `--salary-source`, `--salary-open-file`, `--completion-report [PATH]`.
7. Update `src/pipeline/validation.py` to validate flag combinations and required paths for source modes.
8. Preserve existing CLI/stage behavior as default-compatible (no breaking changes).

## Action Items
[ ] Add `scripts/completion_audit.py` to evaluate run state and write `research/completion_report.md` plus `research/players_missing_bref_id.txt` (sorted unique unresolved names).

[ ] Implement `src/etl/backfill/_pbp_bulk.py` to ingest open-data PBP files from `raw/pbp/`, normalize to `fact_play_by_play`, and enforce idempotency with key (`game_id`, `event_num`).

[ ] Extend `src/etl/play_by_play.py` with source dispatch (`api|bulk|auto`); in `auto`, run bulk first then API for missing games only.

[ ] Implement `src/etl/backfill/_salary_history.py` to ingest open salary files (CSV/parquet), normalize season format `YYYY-YY`, and insert idempotently by (`player_id`, `season_id`).

[ ] Extend `src/etl/salaries.py` with source dispatch (`bref|open|auto`); in `auto`, fill from open source then scrape BRef for missing coverage while preserving existing BRef rows as authoritative.

[ ] Harden `src/etl/backfill/_player_career.py` with deterministic normalization (punctuation/suffix/case handling), conservative linking only, and unresolved-name output for manual review.

[ ] Register new/updated loaders in `src/etl/backfill/_orchestrator.py` with dependency-safe ordering so identity enrichment and bulk fills happen before downstream checks.

[ ] Add tests in `tests/test_etl_play_by_play.py` for bulk mapping, dedupe, and `auto` fallback behavior.

[ ] Add tests in `tests/test_etl_salaries.py` for source strategy, conflict policy, and partial recovery after rate limits.

[ ] Add tests in `tests/test_etl_backfill_player_career.py` for normalization, conservative linking, and unresolved-output generation; add validation tests for new CLI flags in pipeline validation tests.

[ ] Run verification sequence after implementation: `uv run pytest`, then `uv run ingest --raw-backfill --pbp-source auto --salary-source auto`, then `uv run python scripts/completion_audit.py`.

## Test Cases and Scenarios
- Bulk PBP input includes duplicate (`game_id`, `event_num`) rows and remains idempotent.
- PBP `auto` mode on partially populated seasons fetches only missing games from API.
- Salary open-source rows with malformed season IDs are rejected with clear diagnostics.
- Salary conflict with existing BRef record keeps BRef value and logs fallback behavior.
- Player names with suffixes/apostrophes/periods normalize consistently.
- Ambiguous player matches are not auto-written and appear in unresolved output.
- Invalid CLI source/path combinations fail fast in validation.

## Assumptions and Defaults
- Open historical datasets are manually placed in `raw/`; no paid/proxy source is used.
- Default operational modes are `--pbp-source auto` and `--salary-source auto`.
- ID linking policy is conservative plus review (no aggressive fuzzy auto-linking).
- No new core tables are required for this completion phase.
- Completion is judged by explicit counts and missing lists in `research/completion_report.md`.
