# Extract NBA player stats from the NBA API for the 2024-25 Regular Season

from nba_api.stats.endpoints import leaguedashplayerstats

def extract_player_stats():
    """
    Instantiates the LeagueDashPlayerStats class to call the NBA API
    and returns a DataFrame of player stats for the 2024-25 regular season.
    """
    
    # Instantiate the LeagueDashPlayerStats class with season parameters
    player_stats = leaguedashplayerstats.LeagueDashPlayerStats(
        season='2024-25',
        season_type_all_star='Regular Season'
    )

    # Extract the first DataFrame from the response
    player_stats_df = player_stats.get_data_frames()[0]

    # Save raw data to CSV
    player_stats_df.to_csv('data/raw/nba_player_stats_2024_25.csv', index=False)
    
    print(f"Extraction complete. {len(player_stats_df)} rows saved to data/raw/nba_player_stats_2024_25.csv")
    
    return player_stats_df

# Only runs if this file is executed directly 
# Prevents automatic execution when imported by pipeline.py
if __name__ == "__main__":
    extract_player_stats()