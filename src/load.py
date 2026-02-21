# Load cleaned DataFrames into PostgreSQL
# Inserts dimension tables first, then fact table to respect foreign key constraints

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

def get_engine():
    """
    Creates and returns a SQLAlchemy engine using credentials from .env file.
    """
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')

    connection_string = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    engine = create_engine(connection_string)
    print("Database connection established")
    return engine

def load_dim_player(df, engine):
    """
    Loads dim_player DataFrame into PostgreSQL.
    Uses INSERT ... ON CONFLICT DO NOTHING to handle duplicates safely.
    """
    rows = df.to_sql(
        'dim_player',
        engine,
        if_exists='append',
        index=False,
        method='multi'
    )
    print(f"dim_player: {rows} rows inserted")

def load_dim_team(df, engine):
    """
    Loads dim_team DataFrame into PostgreSQL.
    """
    rows = df.to_sql(
        'dim_team',
        engine,
        if_exists='append',
        index=False,
        method='multi'
    )
    print(f"dim_team: {rows} rows inserted")

def load_fact_player_stats(df, engine):
    """
    Loads fact_player_stats DataFrame into PostgreSQL.
    """
    rows = df.to_sql(
        'fact_player_stats',
        engine,
        if_exists='append',
        index=False,
        method='multi'
    )
    print(f"fact_player_stats: {rows} rows inserted")

def truncate_tables(engine):
    """
    Clears all tables before loading to ensure idempotent pipeline runs.
    Truncates fact table first to respect foreign key constraints.
    """
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE fact_player_stats RESTART IDENTITY CASCADE;"))
        conn.execute(text("TRUNCATE TABLE dim_player RESTART IDENTITY CASCADE;"))
        conn.execute(text("TRUNCATE TABLE dim_team RESTART IDENTITY CASCADE;"))
        conn.commit()
    print("Tables truncated successfully")
    
def load_all(dim_player_df, dim_team_df, fact_player_stats_df):
    """
    Orchestrates the loading of all three tables in the correct order.
    Dimension tables first, then fact table.
    """
    engine = get_engine()
    print("Loading data into PostgreSQL...")
    
    # Clear tables first to ensure idempotent runs
    truncate_tables(engine)
    
    # Load dimension tables first
    load_dim_player(dim_player_df, engine)
    load_dim_team(dim_team_df, engine)
    
    # Load fact table last
    load_fact_player_stats(fact_player_stats_df, engine)

    print("All data loaded successfully!")

# Only runs if this file is executed directly
# Prevents automatic execution when imported by pipeline.py
if __name__ == "__main__":
    dim_player_df = pd.read_csv('data/processed/dim_player.csv')
    dim_team_df = pd.read_csv('data/processed/dim_team.csv')
    fact_player_stats_df = pd.read_csv('data/processed/fact_player_stats.csv')
    
    load_all(dim_player_df, dim_team_df, fact_player_stats_df)