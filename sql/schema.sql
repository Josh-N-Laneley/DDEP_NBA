-- Drop tables if they exist
DROP TABLE IF EXISTS fact_player_stats;
DROP TABLE IF EXISTS dim_player;
DROP TABLE IF EXISTS dim_team;

-- Team dimension table
CREATE TABLE dim_team (
    team_id INTEGER PRIMARY KEY,
    team_abbreviation VARCHAR(10) NOT NULL
);

-- Player dimension table
CREATE TABLE dim_player (
    player_id INTEGER PRIMARY KEY,
    player_name VARCHAR(100) NOT NULL,
    nickname VARCHAR(100),
    age FLOAT
);

-- Fact table
CREATE TABLE fact_player_stats (
    stat_id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL REFERENCES dim_player(player_id),
    team_id INTEGER NOT NULL REFERENCES dim_team(team_id),
    gp INTEGER,
    w INTEGER,
    l INTEGER,
    w_pct FLOAT,
    min FLOAT,
    fgm FLOAT,
    fga FLOAT,
    fg_pct FLOAT,
    fg3m FLOAT,
    fg3a FLOAT,
    fg3_pct FLOAT,
    ftm FLOAT,
    fta FLOAT,
    ft_pct FLOAT,
    oreb FLOAT,
    dreb FLOAT,
    reb FLOAT,
    ast FLOAT,
    tov FLOAT,
    stl FLOAT,
    blk FLOAT,
    blka FLOAT,
    pf FLOAT,
    pfd FLOAT,
    pts FLOAT,
    plus_minus FLOAT,
    dd2 INTEGER,
    td3 INTEGER,
    nba_fantasy_pts FLOAT
);
