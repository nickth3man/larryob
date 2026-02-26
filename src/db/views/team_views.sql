-- Team-related analytical views for DuckDB analytics layer.
-- These views reference SQLite tables via the 'nba.' schema prefix.

-- ============================================================================== --
-- Team season standings                                                          --
-- ============================================================================== --
CREATE OR REPLACE VIEW vw_team_standings AS
SELECT
    t.abbreviation,
    t.full_name,
    g.season_id,
    COUNT(*)                                        AS gp,
    SUM(CASE WHEN l.pts > opp.pts THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN l.pts < opp.pts THEN 1 ELSE 0 END) AS losses,
    ROUND(
        SUM(CASE WHEN l.pts > opp.pts THEN 1.0 ELSE 0.0 END)
        / NULLIF(COUNT(*), 0),
    3) AS win_pct
FROM nba.team_game_log l
JOIN nba.team_game_log opp
    ON opp.game_id = l.game_id AND opp.team_id != l.team_id
JOIN nba.dim_team t ON t.team_id = l.team_id
JOIN nba.fact_game g ON g.game_id = l.game_id
GROUP BY t.abbreviation, t.full_name, g.season_id
ORDER BY g.season_id DESC, win_pct DESC;

-- ============================================================================== --
-- Pace estimate (possessions per 48 min)                                         --
-- Requires modern era data (1973-74 onwards where TOV/OReb tracked)             --
-- ============================================================================== --
CREATE OR REPLACE VIEW vw_team_pace AS
SELECT
    t.abbreviation,
    t.full_name,
    g.season_id,
    COALESCE(SUM(l.fga), 0)                         AS total_fga,
    COALESCE(SUM(l.fta), 0)                         AS total_fta,
    COALESCE(SUM(l.oreb), 0)                        AS total_oreb,
    COALESCE(SUM(l.tov), 0)                         AS total_tov,
    COALESCE(SUM(l.fgm), 0)                         AS total_fgm,
    -- Possession estimate:
    -- Poss ≈ FGA + 0.4*FTA - 1.07*(ORB/(ORB+Opp_DRB))*(FGA-FGM) + TOV
    ROUND(
        0.5 * (
            COALESCE(SUM(l.fga), 0)
            + 0.4 * COALESCE(SUM(l.fta), 0)
            - 1.07 * (
                COALESCE(SUM(l.oreb), 0)
                / NULLIF(COALESCE(SUM(l.oreb), 0) + COALESCE(SUM(opp.dreb), 0), 0)
            ) * (COALESCE(SUM(l.fga), 0) - COALESCE(SUM(l.fgm), 0))
            + COALESCE(SUM(l.tov), 0)
        ),
    1) AS poss_estimate
FROM nba.team_game_log l
JOIN nba.team_game_log opp ON opp.game_id = l.game_id AND opp.team_id != l.team_id
JOIN nba.dim_team t ON t.team_id = l.team_id
JOIN nba.fact_game g ON g.game_id = l.game_id
WHERE l.oreb IS NOT NULL AND l.tov IS NOT NULL
GROUP BY t.abbreviation, t.full_name, g.season_id;

-- ============================================================================== --
-- Team offensive/defensive ratings (pts per 100 poss)                            --
-- ============================================================================== --
CREATE OR REPLACE VIEW vw_team_ratings AS
WITH poss AS (
    SELECT
        l.game_id,
        l.team_id,
        g.season_id,
        l.pts,
        opp.pts AS opp_pts,
        (
            l.fga + 0.4 * COALESCE(l.fta, 0)
            - 1.07 * (COALESCE(l.oreb, 0) / NULLIF(COALESCE(l.oreb, 0) + COALESCE(opp.dreb, 0), 0))
              * (l.fga - COALESCE(l.fgm, 0))
            + COALESCE(l.tov, 0)
        ) AS team_poss
    FROM nba.team_game_log l
    JOIN nba.team_game_log opp ON opp.game_id = l.game_id AND opp.team_id != l.team_id
    JOIN nba.fact_game g ON g.game_id = l.game_id
    WHERE l.oreb IS NOT NULL AND l.tov IS NOT NULL
),
season_totals AS (
    SELECT
        team_id,
        season_id,
        SUM(pts) AS total_pts,
        SUM(opp_pts) AS total_opp_pts,
        SUM(team_poss) AS total_poss
    FROM poss
    GROUP BY team_id, season_id
)
SELECT
    t.abbreviation,
    t.full_name,
    st.season_id,
    ROUND(100.0 * st.total_pts / NULLIF(st.total_poss, 0), 1) AS off_rtg,
    ROUND(100.0 * st.total_opp_pts / NULLIF(st.total_poss, 0), 1) AS def_rtg,
    ROUND(100.0 * (st.total_pts - st.total_opp_pts) / NULLIF(st.total_poss, 0), 1) AS net_rtg
FROM season_totals st
JOIN nba.dim_team t ON t.team_id = st.team_id;

-- ============================================================================== --
-- Team Four Factors (Dean Oliver) per team per season                            --
-- ============================================================================== --
CREATE OR REPLACE VIEW vw_team_four_factors AS
SELECT
    t.season_id,
    t.bref_abbrev,
    d.full_name   AS team_name,
    t.w,
    t.l,
    t.pace,
    t.o_rtg,
    t.d_rtg,
    t.n_rtg,
    -- Offensive four factors
    t.e_fg_pct    AS off_efg,
    t.tov_pct     AS off_tov_pct,
    t.orb_pct     AS off_orb_pct,
    t.ft_fga      AS off_ft_fga,
    -- Defensive four factors
    t.opp_e_fg_pct AS def_efg,
    t.opp_tov_pct  AS def_tov_pct,
    t.drb_pct      AS def_drb_pct,
    t.opp_ft_fga   AS def_ft_fga
FROM nba.fact_team_season t
LEFT JOIN nba.dim_team d ON d.bref_abbrev = t.bref_abbrev
WHERE t.playoffs = 0;
