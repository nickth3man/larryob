-- Miscellaneous analytical views for DuckDB analytics layer.
-- Includes play-by-play, salary, and draft-related views.
-- These views reference SQLite tables via the 'nba.' schema prefix.

-- ============================================================================== --
-- Play-by-play: shot distribution by event type                                  --
-- ============================================================================== --
CREATE OR REPLACE VIEW vw_pbp_shot_summary AS
SELECT
    p.full_name                        AS player_name,
    pbp.game_id,
    g.game_date,
    g.season_id,
    SUM(CASE WHEN pbp.eventmsgtype = 1 THEN 1 ELSE 0 END) AS made_shots,
    SUM(CASE WHEN pbp.eventmsgtype = 2 THEN 1 ELSE 0 END) AS missed_shots,
    SUM(CASE WHEN pbp.eventmsgtype = 3 THEN 1 ELSE 0 END) AS free_throws,
    SUM(CASE WHEN pbp.eventmsgtype = 5 THEN 1 ELSE 0 END) AS turnovers
FROM nba.fact_play_by_play pbp
JOIN nba.dim_player p ON p.player_id = pbp.player1_id
JOIN nba.fact_game g ON g.game_id = pbp.game_id
GROUP BY p.full_name, pbp.game_id, g.game_date, g.season_id;

-- ============================================================================== --
-- Salary: contract as % of cap (era-agnostic comparison)                         --
-- ============================================================================== --
CREATE OR REPLACE VIEW vw_salary_cap_pct AS
SELECT
    p.full_name,
    t.abbreviation AS team,
    s.season_id,
    s.salary,
    cap.cap_amount,
    ROUND(100.0 * s.salary / NULLIF(cap.cap_amount, 0), 2) AS cap_pct
FROM nba.fact_salary s
JOIN nba.dim_player p USING (player_id)
JOIN nba.dim_team t USING (team_id)
JOIN nba.dim_salary_cap cap USING (season_id)
ORDER BY s.season_id DESC, cap_pct DESC;

-- ============================================================================== --
-- Draft class career value                                                       --
-- ============================================================================== --
CREATE OR REPLACE VIEW vw_draft_class AS
SELECT
    d.season_id              AS draft_season_id,
    d.draft_round,
    d.overall_pick,
    d.bref_team_abbrev       AS drafted_by,
    d.player_name,
    d.college,
    a.bref_player_id,
    -- Career aggregates (sum across all seasons)
    SUM(a.ws)    AS career_ws,
    SUM(a.vorp)  AS career_vorp,
    SUM(a.g)     AS career_g,
    ROUND(AVG(a.per), 1) AS avg_per,
    ROUND(AVG(a.bpm), 1) AS avg_bpm
FROM nba.fact_draft d
LEFT JOIN nba.fact_player_advanced_season a
       ON a.bref_player_id = d.bref_player_id
GROUP BY
    d.season_id, d.draft_round, d.overall_pick,
    d.bref_team_abbrev, d.player_name, d.college, a.bref_player_id;
