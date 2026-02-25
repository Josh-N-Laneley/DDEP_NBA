-- =============================================
-- NBA Player Stats 2024-25 - Sample Queries
-- =============================================

-- 1. Young Players Excelling (Under 25)
-- Shows promising young talent sorted by points scored
SELECT 
    p.player_name,
    p.age,
    ROUND((f.pts / NULLIF(f.gp, 0))::numeric, 2) AS pts_per_game,
    ROUND((f.ast / NULLIF(f.gp, 0))::numeric, 2) AS ast_per_game,
    ROUND((f.reb / NULLIF(f.gp, 0))::numeric, 2) AS reb_per_game,
    f.w_pct
FROM fact_player_stats f
JOIN dim_player p ON f.player_id = p.player_id
WHERE p.age < 25
ORDER BY pts_per_game DESC
LIMIT 10;

-- 2. Best Offensive Players
-- Ranks players by points, assists and field goal efficiency
SELECT 
    p.player_name,
    ROUND((f.pts / NULLIF(f.gp, 0))::numeric, 2) AS pts_per_game,
    ROUND((f.ast / NULLIF(f.gp, 0))::numeric, 2) AS ast_per_game,
    f.fg_pct,
    f.fg3_pct,
    f.ft_pct
FROM fact_player_stats f
JOIN dim_player p ON f.player_id = p.player_id
ORDER BY (f.pts / NULLIF(f.gp, 0)) DESC
LIMIT 10;

-- 3. Best Defensive Players
-- Ranks players by steals, blocks and defensive rebounds
SELECT 
    p.player_name,
    f.stl,
    f.blk,
    f.dreb,
    f.pf
FROM fact_player_stats f
JOIN dim_player p ON f.player_id = p.player_id
ORDER BY (f.stl + f.blk) DESC
LIMIT 10;

-- 4. Win Percentage Leaders
-- Shows teams ranked by win percentage
SELECT 
    t.team_abbreviation,
    ROUND(AVG(f.w_pct)::numeric, 3) AS win_pct
FROM fact_player_stats f
JOIN dim_team t ON f.team_id = t.team_id
GROUP BY t.team_abbreviation
ORDER BY win_pct DESC;

-- 5. Win Percentage vs Fantasy Points
-- Explores whether teams with higher win % have higher fantasy point players
SELECT 
    t.team_abbreviation,
    ROUND(AVG(f.w_pct)::numeric, 3) AS win_pct,
    ROUND(AVG(f.nba_fantasy_pts)::numeric, 2) AS avg_fantasy_pts
FROM fact_player_stats f
JOIN dim_team t ON f.team_id = t.team_id
GROUP BY t.team_abbreviation
ORDER BY win_pct DESC;

-- 6. Team scoring leaders
-- Ranks every player within their team by points per game
SELECT
p.player_name,
t.team_abbreviation,
ROUND((f.pts / NULLIF(f.gp, 0))::numeric, 2) AS ppg,
RANK() OVER (
    PARTITION BY t.team_abbreviation
    ORDER BY (f.pts / NULLIF(f.gp, 0)) DESC
) AS team_ppg_rank
FROM fact_player_stats f
LEFT JOIN dim_player p ON p.player_id = f.player_id
LEFT JOIN dim_team t ON t.team_id = f.team_id
ORDER BY t.team_abbreviation, team_ppg_rank
