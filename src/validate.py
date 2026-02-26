# Validates that data loaded into PostgreSQL matches the processed CSVs
# Checks row counts, duplicates, nulls and foreign key integrity

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

def get_engine():
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    connection_string = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    return create_engine(connection_string)

def validate_row_counts(engine):
    """
    Compares row counts between processed CSVs and PostgreSQL tables.
    Flags any mismatches.
    """
    print("\n--- Row Count Validation ---")
    
    tables = {
        'dim_player': 'data/processed/dim_player.csv',
        'dim_team': 'data/processed/dim_team.csv',
        'fact_player_stats': 'data/processed/fact_player_stats.csv'
    }
    
    all_passed = True
    
    for table, csv_path in tables.items():
        csv_count = len(pd.read_csv(csv_path))
        
        with engine.connect() as conn:
            db_count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
        
        status = "PASS" if csv_count == db_count else "FAIL"
        if csv_count != db_count:
            all_passed = False
            
        print(f"{status} | {table}: CSV={csv_count} rows, DB={db_count} rows")
    
    return all_passed

def validate_no_duplicates(engine):
    """
    Checks for duplicate primary keys in each table.
    """
    print("\n--- Duplicate Validation ---")
    
    checks = {
        'dim_player': 'player_id',
        'dim_team': 'team_id',
        'fact_player_stats': 'stat_id'
    }
    
    all_passed = True
    
    for table, pk in checks.items():
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM (
                    SELECT {pk}, COUNT(*) 
                    FROM {table} 
                    GROUP BY {pk} 
                    HAVING COUNT(*) > 1
                ) duplicates
            """)).scalar()
        
        status = "PASS" if result == 0 else "FAIL"
        if result != 0:
            all_passed = False
            
        print(f"{status} | {table}: {result} duplicate {pk}s found")
    
    return all_passed

def validate_no_nulls(engine):
    """
    Checks critical columns for null values.
    """
    print("\n--- Null Value Validation ---")
    
    checks = {
        'dim_player': 'player_id',
        'dim_team': 'team_id',
        'fact_player_stats': 'player_id'
    }
    
    all_passed = True
    
    for table, column in checks.items():
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM {table} WHERE {column} IS NULL
            """)).scalar()
        
        status = "PASS" if result == 0 else "❌ FAIL"
        if result != 0:
            all_passed = False
            
        print(f"{status} | {table}.{column}: {result} null values found")
    
    return all_passed

def validate_foreign_keys(engine):
    """
    Checks that all foreign keys in fact_player_stats 
    exist in their respective dimension tables.
    """
    print("\n--- Foreign Key Validation ---")
    
    all_passed = True
    
    with engine.connect() as conn:
        # Check player_id foreign key
        result = conn.execute(text("""
            SELECT COUNT(*) FROM fact_player_stats f
            LEFT JOIN dim_player p ON f.player_id = p.player_id
            WHERE p.player_id IS NULL
        """)).scalar()
        
        status = "PASS" if result == 0 else "❌ FAIL"
        if result != 0:
            all_passed = False
        print(f"{status} | fact_player_stats.player_id: {result} orphaned records")
        
        # Check team_id foreign key
        result = conn.execute(text("""
            SELECT COUNT(*) FROM fact_player_stats f
            LEFT JOIN dim_team t ON f.team_id = t.team_id
            WHERE t.team_id IS NULL
        """)).scalar()
        
        status = "PASS" if result == 0 else "❌ FAIL"
        if result != 0:
            all_passed = False
        print(f"{status} | fact_player_stats.team_id: {result} orphaned records")
    
    return all_passed

def run_validation():
    """
    Runs all validation checks and reports overall status.
    """
    print("=" * 40)
    print("Running Data Validation")
    print("=" * 40)
    
    engine = get_engine()
    
    results = [
        validate_row_counts(engine),
        validate_no_duplicates(engine),
        validate_no_nulls(engine),
        validate_foreign_keys(engine)
    ]
    
    print("\n" + "=" * 40)
    if all(results):
        print("All validation checks passed!")
    else:
        print("Some validation checks failed — review above")
    print("=" * 40)

# Only runs if this file is executed directly
if __name__ == "__main__":
    run_validation()