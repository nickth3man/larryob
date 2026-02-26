-- NBA Data Warehouse Schema: Migration Statements
-- These ALTER statements are applied with try/except so re-running is safe.
-- SQLite does not support ALTER TABLE ... ADD COLUMN IF NOT EXISTS.

-- dim_player additions for Basketball-Reference cross-reference.
-- SQLite's ALTER TABLE ADD COLUMN does not support UNIQUE; uniqueness is
-- enforced by the index created below.
ALTER TABLE dim_player ADD COLUMN bref_id TEXT;
ALTER TABLE dim_player ADD COLUMN college TEXT;
ALTER TABLE dim_player ADD COLUMN hof INTEGER DEFAULT 0;

-- dim_team: bref uses different abbreviations (BRK vs BKN, CHO vs CHA, etc.)
ALTER TABLE dim_team ADD COLUMN bref_abbrev TEXT;

-- Indexes on the new columns — must run AFTER the ALTER TABLE statements.
CREATE UNIQUE INDEX IF NOT EXISTS idx_player_bref ON dim_player(bref_id);

-- ============================================================================
-- Rollback / Down migration
-- Run these statements to reverse the changes applied above.
-- Note: SQLite does not support DROP COLUMN; columns added by ALTER TABLE
-- cannot be removed without recreating the affected table. The index can be
-- dropped independently.
-- ============================================================================
-- DROP INDEX IF EXISTS idx_player_bref;
