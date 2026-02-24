-- NBA Data Warehouse Schema: Rollback Statements
-- Used to reverse migrations if needed.

DROP INDEX IF EXISTS idx_player_bref;
ALTER TABLE dim_player DROP COLUMN bref_id;
ALTER TABLE dim_player DROP COLUMN college;
ALTER TABLE dim_player DROP COLUMN hof;
ALTER TABLE dim_team DROP COLUMN bref_abbrev;
