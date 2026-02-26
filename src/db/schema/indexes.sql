-- NBA Data Warehouse Schema: Index Definitions
-- Indexes for common query patterns

-- Player game log indexes
CREATE INDEX IF NOT EXISTS idx_pgl_player ON player_game_log(player_id);
CREATE INDEX IF NOT EXISTS idx_pgl_team ON player_game_log(team_id);
CREATE INDEX IF NOT EXISTS idx_pgl_game ON player_game_log(game_id);
CREATE INDEX IF NOT EXISTS idx_pgl_player_game ON player_game_log(player_id, game_id);

-- Game indexes
CREATE INDEX IF NOT EXISTS idx_game_date ON fact_game(game_date);
CREATE INDEX IF NOT EXISTS idx_game_season ON fact_game(season_id);
CREATE INDEX IF NOT EXISTS idx_game_home ON fact_game(home_team_id);
CREATE INDEX IF NOT EXISTS idx_game_away ON fact_game(away_team_id);

-- Play-by-play indexes
CREATE INDEX IF NOT EXISTS idx_pbp_game ON fact_play_by_play(game_id);
CREATE INDEX IF NOT EXISTS idx_pbp_game_period ON fact_play_by_play(game_id, period);
CREATE INDEX IF NOT EXISTS idx_pbp_player1 ON fact_play_by_play(player1_id);

-- Roster indexes
CREATE INDEX IF NOT EXISTS idx_roster_player ON fact_roster(player_id);
CREATE INDEX IF NOT EXISTS idx_roster_player_dates ON fact_roster(player_id, start_date, end_date);
CREATE UNIQUE INDEX IF NOT EXISTS idx_roster_unique ON fact_roster(player_id, team_id, season_id);

-- Team game log indexes
CREATE INDEX IF NOT EXISTS idx_tgl_team ON team_game_log(team_id);

-- All-Star indexes
CREATE INDEX IF NOT EXISTS idx_allstar_player ON fact_all_star(player_id);
CREATE INDEX IF NOT EXISTS idx_allstar_season ON fact_all_star(season_id);

-- All-NBA indexes
CREATE INDEX IF NOT EXISTS idx_allnba_player ON fact_all_nba(player_id);
CREATE INDEX IF NOT EXISTS idx_allnba_season ON fact_all_nba(season_id);

-- All-NBA vote indexes
CREATE INDEX IF NOT EXISTS idx_allnba_vote_player ON fact_all_nba_vote(player_id);
CREATE INDEX IF NOT EXISTS idx_allnba_vote_season ON fact_all_nba_vote(season_id);

-- Team history index
CREATE INDEX IF NOT EXISTS idx_team_hist_team ON dim_team_history(team_id);

-- Team season indexes
CREATE INDEX IF NOT EXISTS idx_fts_season ON fact_team_season(season_id);
CREATE INDEX IF NOT EXISTS idx_fts_abbrev ON fact_team_season(bref_abbrev);
CREATE INDEX IF NOT EXISTS idx_dim_team_bref_abbrev ON dim_team(bref_abbrev);

-- Draft indexes
CREATE INDEX IF NOT EXISTS idx_draft_season ON fact_draft(season_id);
CREATE INDEX IF NOT EXISTS idx_draft_player ON fact_draft(bref_player_id);

-- Player season stats indexes
CREATE INDEX IF NOT EXISTS idx_pss_player ON fact_player_season_stats(bref_player_id);
CREATE INDEX IF NOT EXISTS idx_pss_season ON fact_player_season_stats(season_id);

-- Player advanced season indexes
CREATE INDEX IF NOT EXISTS idx_pas_player ON fact_player_advanced_season(bref_player_id);
CREATE INDEX IF NOT EXISTS idx_pas_season ON fact_player_advanced_season(season_id);

-- Player shooting index
CREATE INDEX IF NOT EXISTS idx_pshoot_player ON fact_player_shooting_season(bref_player_id);

-- Player PBP season index
CREATE INDEX IF NOT EXISTS idx_ppbp_player ON fact_player_pbp_season(bref_player_id);

-- ETL run log index
CREATE INDEX IF NOT EXISTS idx_runlog_table_season ON etl_run_log(table_name, season_id);

-- ============================================================================
-- Rollback / Down migration
-- Run these statements to remove all indexes created above.
-- ============================================================================
-- DROP INDEX IF EXISTS idx_pgl_player;
-- DROP INDEX IF EXISTS idx_pgl_team;
-- DROP INDEX IF EXISTS idx_pgl_game;
-- DROP INDEX IF EXISTS idx_pgl_player_game;
-- DROP INDEX IF EXISTS idx_game_date;
-- DROP INDEX IF EXISTS idx_game_season;
-- DROP INDEX IF EXISTS idx_game_home;
-- DROP INDEX IF EXISTS idx_game_away;
-- DROP INDEX IF EXISTS idx_pbp_game;
-- DROP INDEX IF EXISTS idx_pbp_game_period;
-- DROP INDEX IF EXISTS idx_pbp_player1;
-- DROP INDEX IF EXISTS idx_roster_player;
-- DROP INDEX IF EXISTS idx_roster_player_dates;
-- DROP INDEX IF EXISTS idx_roster_unique;
-- DROP INDEX IF EXISTS idx_tgl_team;
-- DROP INDEX IF EXISTS idx_allstar_player;
-- DROP INDEX IF EXISTS idx_allstar_season;
-- DROP INDEX IF EXISTS idx_allnba_player;
-- DROP INDEX IF EXISTS idx_allnba_season;
-- DROP INDEX IF EXISTS idx_allnba_vote_player;
-- DROP INDEX IF EXISTS idx_allnba_vote_season;
-- DROP INDEX IF EXISTS idx_team_hist_team;
-- DROP INDEX IF EXISTS idx_fts_season;
-- DROP INDEX IF EXISTS idx_fts_abbrev;
-- DROP INDEX IF EXISTS idx_draft_season;
-- DROP INDEX IF EXISTS idx_draft_player;
-- DROP INDEX IF EXISTS idx_pss_player;
-- DROP INDEX IF EXISTS idx_pss_season;
-- DROP INDEX IF EXISTS idx_pas_player;
-- DROP INDEX IF EXISTS idx_pas_season;
-- DROP INDEX IF EXISTS idx_pshoot_player;
-- DROP INDEX IF EXISTS idx_ppbp_player;
-- DROP INDEX IF EXISTS idx_runlog_table_season;
