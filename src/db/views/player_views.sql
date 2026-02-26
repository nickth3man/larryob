-- Player-related analytical views for DuckDB analytics layer.
-- These views reference SQLite tables via the 'nba.' schema prefix.

-- ============================================================================== --
-- Shooting efficiency                                                            --
-- ============================================================================== --
CREATE OR REPLACE VIEW vw_player_shooting AS
SELECT
    p.player_id,
    p.full_name,
    g.season_id,
    COUNT(*)                                                AS games,
    SUM(l.minutes_played)                                   AS total_minutes,
    SUM(l.fgm)  AS fgm,  SUM(l.fga)  AS fga,
    SUM(l.fg3m) AS fg3m, SUM(l.fg3a) AS fg3a,
    SUM(l.ftm)  AS ftm,  SUM(l.fta)  AS fta,
    SUM(l.pts)  AS pts,
    -- eFG% = (FGM + 0.5 * FG3M) / FGA
    ROUND(
        (SUM(l.fgm) + 0.5 * COALESCE(SUM(l.fg3m), 0))
        / NULLIF(SUM(l.fga), 0),
    3) AS efg_pct,
    -- TS% = PTS / (2 * (FGA + 0.44 * FTA))
    ROUND(
        SUM(l.pts)
        / NULLIF(2.0 * (SUM(l.fga) + 0.44 * COALESCE(SUM(l.fta), 0)), 0),
    3) AS ts_pct
FROM nba.player_game_log l
JOIN nba.dim_player p USING (player_id)
JOIN nba.fact_game g USING (game_id)
GROUP BY p.player_id, p.full_name, g.season_id;

-- ============================================================================== --
-- Per-game averages (season totals / games played)                               --
-- ============================================================================== --
CREATE OR REPLACE VIEW vw_player_season_totals AS
SELECT
    p.player_id,
    p.full_name,
    g.season_id,
    COUNT(*)                                       AS gp,
    ROUND(AVG(l.minutes_played), 1)                AS mpg,
    ROUND(AVG(l.pts),   1)                         AS ppg,
    ROUND(AVG(l.reb),   1)                         AS rpg,
    ROUND(AVG(l.ast),   1)                         AS apg,
    ROUND(AVG(l.stl),   1)                         AS spg,
    ROUND(AVG(l.blk),   1)                         AS bpg,
    ROUND(AVG(l.tov),   1)                         AS topg,
    ROUND(AVG(l.plus_minus), 1)                    AS net_rtg,
    SUM(l.pts)                                     AS total_pts,
    SUM(l.reb)                                     AS total_reb,
    SUM(l.ast)                                     AS total_ast
FROM nba.player_game_log l
JOIN nba.dim_player p USING (player_id)
JOIN nba.fact_game g USING (game_id)
GROUP BY p.player_id, p.full_name, g.season_id;

-- ============================================================================== --
-- Last N games rolling window (default 10)                                       --
-- ============================================================================== --
CREATE OR REPLACE VIEW vw_player_last10 AS
WITH ranked AS (
    SELECT
        l.*,
        g.game_date,
        ROW_NUMBER() OVER (
            PARTITION BY l.player_id
            ORDER BY g.game_date DESC
        ) AS rn
    FROM nba.player_game_log l
    JOIN nba.fact_game g USING (game_id)
)
SELECT
    r.player_id,
    p.full_name,
    COUNT(*)                       AS games,
    ROUND(AVG(r.pts),  1)          AS ppg,
    ROUND(AVG(r.reb),  1)          AS rpg,
    ROUND(AVG(r.ast),  1)          AS apg,
    ROUND(AVG(r.plus_minus), 1)    AS net_rtg
FROM ranked r
JOIN nba.dim_player p USING (player_id)
WHERE r.rn <= 10
GROUP BY r.player_id, p.full_name;

-- ============================================================================== --
-- Per-36-minute stats (normalized to 36 min)                                     --
-- ============================================================================== --
CREATE OR REPLACE VIEW vw_player_per36 AS
SELECT
    p.player_id,
    p.full_name,
    g.season_id,
    COUNT(*) AS gp,
    ROUND(36.0 * SUM(l.pts)   / NULLIF(SUM(l.minutes_played), 0), 1) AS ppg36,
    ROUND(36.0 * SUM(l.reb)   / NULLIF(SUM(l.minutes_played), 0), 1) AS rpg36,
    ROUND(36.0 * SUM(l.ast)   / NULLIF(SUM(l.minutes_played), 0), 1) AS apg36,
    ROUND(36.0 * SUM(l.stl)   / NULLIF(SUM(l.minutes_played), 0), 1) AS spg36,
    ROUND(36.0 * SUM(l.blk)   / NULLIF(SUM(l.minutes_played), 0), 1) AS bpg36,
    ROUND(36.0 * SUM(l.tov)   / NULLIF(SUM(l.minutes_played), 0), 1) AS topg36
FROM nba.player_game_log l
JOIN nba.dim_player p USING (player_id)
JOIN nba.fact_game g USING (game_id)
WHERE l.minutes_played > 0
GROUP BY p.player_id, p.full_name, g.season_id;

-- ============================================================================== --
-- Usage rate: % of team possessions used while on court                          --
-- USG% = 100 * (FGA + 0.44*FTA + TOV) * (TmMP/5) / (MP * (TmFGA + 0.44*TmFTA + TmTOV)) --
-- ============================================================================== --
CREATE OR REPLACE VIEW vw_player_usage AS
WITH team_totals AS (
    SELECT
        l.game_id,
        l.team_id,
        SUM(l.minutes_played) AS tm_mp,
        SUM(l.fga) AS tm_fga,
        SUM(COALESCE(l.fta, 0)) AS tm_fta,
        SUM(COALESCE(l.tov, 0)) AS tm_tov
    FROM nba.player_game_log l
    GROUP BY l.game_id, l.team_id
)
SELECT
    p.player_id,
    p.full_name,
    g.season_id,
    COUNT(*) AS gp,
    ROUND(
        100.0 * AVG(
            (l.fga + 0.44 * COALESCE(l.fta, 0) + COALESCE(l.tov, 0))
            * (tt.tm_mp / 5.0)
            / NULLIF(
                l.minutes_played
                * (tt.tm_fga + 0.44 * tt.tm_fta + tt.tm_tov),
                0
            )
        ),
    1
    ) AS usg_pct
FROM nba.player_game_log l
JOIN nba.dim_player p USING (player_id)
JOIN nba.fact_game g USING (game_id)
JOIN team_totals tt ON tt.game_id = l.game_id AND tt.team_id = l.team_id
WHERE l.minutes_played > 0
  AND (tt.tm_fga + 0.44 * tt.tm_fta + tt.tm_tov) > 0
GROUP BY p.player_id, p.full_name, g.season_id;

-- ============================================================================== --
-- Combined advanced metrics per player per season                                --
-- ============================================================================== --
CREATE OR REPLACE VIEW vw_player_season_advanced AS
WITH base AS (
    SELECT
        p.player_id,
        p.full_name,
        g.season_id,
        COUNT(*) AS gp,
        SUM(l.minutes_played) AS total_mp,
        SUM(l.fgm) AS fgm, SUM(l.fga) AS fga,
        SUM(l.fg3m) AS fg3m, SUM(l.ftm) AS ftm, SUM(l.fta) AS fta,
        SUM(l.pts) AS pts, SUM(l.reb) AS reb, SUM(l.ast) AS ast,
        AVG(l.plus_minus) AS net_rtg
    FROM nba.player_game_log l
    JOIN nba.dim_player p USING (player_id)
    JOIN nba.fact_game g USING (game_id)
    GROUP BY p.player_id, p.full_name, g.season_id
)
SELECT
    player_id,
    full_name,
    season_id,
    gp,
    ROUND((fgm + 0.5 * COALESCE(fg3m, 0)) / NULLIF(fga, 0), 3) AS efg_pct,
    ROUND(pts / NULLIF(2.0 * (fga + 0.44 * COALESCE(fta, 0)), 0), 3) AS ts_pct,
    ROUND(36.0 * pts / NULLIF(total_mp, 0), 1) AS ppg36,
    ROUND(36.0 * reb / NULLIF(total_mp, 0), 1) AS rpg36,
    ROUND(36.0 * ast / NULLIF(total_mp, 0), 1) AS apg36,
    ROUND(net_rtg, 1) AS net_rtg
FROM base
WHERE total_mp > 0;

-- ============================================================================== --
-- Per-100 possessions (from player_game_log + dim_league_season)                 --
-- ============================================================================== --
CREATE OR REPLACE VIEW vw_player_per100 AS
WITH base AS (
    SELECT
        p.player_id,
        p.full_name,
        g.season_id,
        COUNT(*)               AS gp,
        SUM(l.minutes_played)  AS mp,
        SUM(l.pts)   AS pts,  SUM(l.reb)  AS reb,  SUM(l.ast)  AS ast,
        SUM(l.stl)   AS stl,  SUM(l.blk)  AS blk,  SUM(l.tov)  AS tov,
        SUM(l.fgm)   AS fgm,  SUM(l.fga)  AS fga,
        SUM(l.fg3m)  AS fg3m, SUM(l.fg3a) AS fg3a,
        SUM(l.ftm)   AS ftm,  SUM(l.fta)  AS fta,
        SUM(l.oreb)  AS oreb, SUM(l.dreb) AS dreb,
        SUM(l.pf)    AS pf
    FROM nba.player_game_log l
    JOIN nba.dim_player p USING (player_id)
    JOIN nba.fact_game g USING (game_id)
    GROUP BY p.player_id, p.full_name, g.season_id
),
-- Estimate possessions: FGA - OREB + TOV + 0.44*FTA
poss_est AS (
    SELECT
        b.*,
        NULLIF(b.fga - b.oreb + b.tov + 0.44 * b.fta, 0) AS poss
    FROM base b
)
SELECT
    player_id, full_name, season_id, gp,
    ROUND(100.0 * pts  / NULLIF(poss, 0), 1) AS pts_per100,
    ROUND(100.0 * reb  / NULLIF(poss, 0), 1) AS reb_per100,
    ROUND(100.0 * ast  / NULLIF(poss, 0), 1) AS ast_per100,
    ROUND(100.0 * stl  / NULLIF(poss, 0), 1) AS stl_per100,
    ROUND(100.0 * blk  / NULLIF(poss, 0), 1) AS blk_per100,
    ROUND(100.0 * tov  / NULLIF(poss, 0), 1) AS tov_per100,
    ROUND(100.0 * fgm  / NULLIF(poss, 0), 1) AS fgm_per100,
    ROUND(100.0 * fga  / NULLIF(poss, 0), 1) AS fga_per100,
    ROUND(100.0 * fg3m / NULLIF(poss, 0), 1) AS fg3m_per100,
    ROUND(100.0 * fg3a / NULLIF(poss, 0), 1) AS fg3a_per100,
    ROUND(100.0 * ftm  / NULLIF(poss, 0), 1) AS ftm_per100,
    ROUND(100.0 * fta  / NULLIF(poss, 0), 1) AS fta_per100,
    ROUND(100.0 * pf   / NULLIF(poss, 0), 1) AS pf_per100,
    ROUND((fgm + 0.5 * COALESCE(fg3m, 0)) / NULLIF(fga, 0), 3) AS efg_pct,
    ROUND(pts / NULLIF(2.0 * (fga + 0.44 * COALESCE(fta, 0)), 0), 3) AS ts_pct
FROM poss_est
WHERE mp > 0;

-- ============================================================================== --
-- Full Basketball-Reference advanced stats (precomputed PER/WS/BPM)              --
-- Extends vw_player_season_advanced with data from fact_player_advanced_season   --
-- ============================================================================== --
CREATE OR REPLACE VIEW vw_player_advanced_full AS
SELECT
    p.player_id,
    p.full_name,
    p.bref_id,
    a.season_id,
    a.team_abbrev,
    a.pos,
    a.age,
    a.g,
    a.gs,
    a.mp,
    a.per,
    a.ts_pct,
    a.x3p_ar,
    a.f_tr,
    a.orb_pct,
    a.drb_pct,
    a.trb_pct,
    a.ast_pct,
    a.stl_pct,
    a.blk_pct,
    a.tov_pct,
    a.usg_pct,
    a.ows,
    a.dws,
    a.ws,
    a.ws_48,
    a.obpm,
    a.dbpm,
    a.bpm,
    a.vorp
FROM nba.fact_player_advanced_season a
LEFT JOIN nba.dim_player p ON p.bref_id = a.bref_player_id;

-- ============================================================================== --
-- Clutch: 4th quarter, score within 5 points (made/missed shots)                 --
-- ============================================================================== --
CREATE OR REPLACE VIEW vw_player_clutch AS
WITH clutch_events AS (
    SELECT
        pbp.player1_id AS player_id,
        pbp.game_id,
        g.season_id,
        pbp.eventmsgtype
    FROM nba.fact_play_by_play pbp
    JOIN nba.fact_game g ON g.game_id = pbp.game_id
    WHERE pbp.period = 4
      AND pbp.player1_id IS NOT NULL
      AND (
        pbp.score_margin = 'TIE'
        OR pbp.score_margin IN ('+1','+2','+3','+4','+5','-1','-2','-3','-4','-5')
      )
)
SELECT
    p.player_id,
    p.full_name,
    ce.season_id,
    COUNT(*) AS clutch_events,
    SUM(CASE WHEN ce.eventmsgtype = 1 THEN 1 ELSE 0 END) AS clutch_made,
    SUM(CASE WHEN ce.eventmsgtype = 2 THEN 1 ELSE 0 END) AS clutch_missed
FROM clutch_events ce
JOIN nba.dim_player p ON p.player_id = ce.player_id
GROUP BY p.player_id, p.full_name, ce.season_id;

-- ============================================================================== --
-- Shooting zone distribution and efficiency (1997–present)                       --
-- ============================================================================== --
CREATE OR REPLACE VIEW vw_player_shooting_zones AS
SELECT
    s.bref_player_id,
    p.full_name,
    p.player_id,
    s.season_id,
    s.team_abbrev,
    s.g,
    s.mp,
    s.avg_dist_fga,
    -- Zone shot distribution
    s.pct_fga_0_3    AS dist_0_3_pct,
    s.pct_fga_3_10   AS dist_3_10_pct,
    s.pct_fga_10_16  AS dist_10_16_pct,
    s.pct_fga_16_3p  AS dist_16_3p_pct,
    s.pct_fga_3p     AS dist_3p_pct,
    -- Zone FG%
    s.fg_pct_0_3     AS fg_pct_0_3,
    s.fg_pct_3_10    AS fg_pct_3_10,
    s.fg_pct_10_16   AS fg_pct_10_16,
    s.fg_pct_16_3p   AS fg_pct_16_3p,
    s.fg_pct_3p      AS fg_pct_3p,
    -- Assisted rates
    s.pct_ast_2p,
    s.pct_ast_3p,
    -- Dunk metrics
    s.pct_dunks_fga,
    s.num_dunks,
    -- Corner 3
    s.pct_corner3_3pa,
    s.corner3_pct
FROM nba.fact_player_shooting_season s
LEFT JOIN nba.dim_player p ON p.bref_id = s.bref_player_id;

-- ============================================================================== --
-- Awards: career trophy case per player                                          --
-- ============================================================================== --
CREATE OR REPLACE VIEW vw_player_awards AS
SELECT
    p.player_id,
    p.full_name,
    a.award_name,
    COUNT(*) AS times_won,
    MIN(a.season_id) AS first_won,
    MAX(a.season_id) AS last_won
FROM nba.fact_player_award a
JOIN nba.dim_player p USING (player_id)
GROUP BY p.player_id, p.full_name, a.award_name
ORDER BY p.full_name, times_won DESC;
