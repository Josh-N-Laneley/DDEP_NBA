# Transform raw NBA player stats CSV into three clean DataFrames
# ready for loading into dim_player, dim_team and fact_player_stats

import pandas as pd
import os

def transform_player_stats():
    """
    Reads the raw NBA CSV, selects and renames columns to match the 
    database schema, cleans the data, and saves three processed CSVs
    ready for loading into PostgreSQL.
    """

    # Load raw CSV
    print("Loading raw CSV...")
    df = pd.read_csv('data/raw/nba_player_stats_2024_25.csv')
    print(f"{len(df)} rows loaded from raw CSV")

    # -------------------------
    # dim_player
    # -------------------------
    dim_player_df = df[['PLAYER_ID', 'PLAYER_NAME', 'NICKNAME', 'AGE']].copy()

    # Rename columns to match schema
    dim_player_df.columns = ['player_id', 'player_name', 'nickname', 'age']

    # Remove duplicate players (e.g. traded players appear more than once)
    dim_player_df = dim_player_df.drop_duplicates(subset='player_id')

    # Handle missing values
    dim_player_df['nickname'] = dim_player_df['nickname'].fillna('Unknown')
    dim_player_df['age'] = dim_player_df['age'].fillna(df['AGE'].mean())

    print(f"dim_player: {len(dim_player_df)} rows")

    # -------------------------
    # dim_team
    # -------------------------
    dim_team_df = df[['TEAM_ID', 'TEAM_ABBREVIATION']].copy()

    # Rename columns to match schema
    dim_team_df.columns = ['team_id', 'team_abbreviation']

    # Remove duplicate teams
    dim_team_df = dim_team_df.drop_duplicates(subset='team_id')

    print(f"dim_team: {len(dim_team_df)} rows")

    # -------------------------
    # fact_player_stats
    # -------------------------
    fact_cols = [
        'PLAYER_ID', 'TEAM_ID',
        'GP', 'W', 'L', 'W_PCT', 'MIN',
        'FGM', 'FGA', 'FG_PCT',
        'FG3M', 'FG3A', 'FG3_PCT',
        'FTM', 'FTA', 'FT_PCT',
        'OREB', 'DREB', 'REB',
        'AST', 'TOV', 'STL', 'BLK', 'BLKA', 'PF', 'PFD',
        'PTS', 'PLUS_MINUS', 'DD2', 'TD3', 'NBA_FANTASY_PTS'
    ]

    fact_player_stats_df = df[fact_cols].copy()

    # Rename columns to match schema
    fact_player_stats_df.columns = [col.lower() for col in fact_cols]

    # Handle missing values — fill with 0 for stats
    fact_player_stats_df = fact_player_stats_df.fillna(0)

    print(f"fact_player_stats: {len(fact_player_stats_df)} rows")

    # -------------------------
    # Save processed CSVs
    # -------------------------
    os.makedirs('data/processed', exist_ok=True)

    dim_player_df.to_csv('data/processed/dim_player.csv', index=False)
    dim_team_df.to_csv('data/processed/dim_team.csv', index=False)
    fact_player_stats_df.to_csv('data/processed/fact_player_stats.csv', index=False)

    print("Transformation complete. Processed CSVs saved to data/processed/")

    return dim_player_df, dim_team_df, fact_player_stats_df

# Only runs if this file is executed directly
# Prevents automatic execution when imported by pipeline.py
if __name__ == "__main__":
    transform_player_stats()