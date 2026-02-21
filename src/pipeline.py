# Pipeline orchestrator - runs the full ETL process end to end
# Extract → Transform → Load

import extract
import transform
import load

def run_pipeline():
    """
    Orchestrates the full ETL pipeline:
    1. Extract raw data from NBA API
    2. Transform and clean data
    3. Load into PostgreSQL
    """
    
    print("=" * 40)
    print("Starting NBA ETL Pipeline")
    print("=" * 40)

    # Step 1 - Extract
    print("\n[1/3] Extracting data from NBA API...")
    extract.extract_player_stats()

    # Step 2 - Transform
    print("\n[2/3] Transforming data...")
    dim_player_df, dim_team_df, fact_player_stats_df = transform.transform_player_stats()

    # Step 3 - Load
    print("\n[3/3] Loading data into PostgreSQL...")
    load.load_all(dim_player_df, dim_team_df, fact_player_stats_df)

    print("\n" + "=" * 40)
    print("Pipeline completed successfully!")
    print("=" * 40)

# Only runs if this file is executed directly
if __name__ == "__main__":
    run_pipeline()