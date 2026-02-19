#import pands 
import pandas

#import nba_api
import nba_api
from nba_api.stats.endpoints import leaguedashplayerstats

#create object(?) to get 24/25 season stats 
player_stats = leaguedashplayerstats.LeagueDashPlayerStats(season='2024-25', season_type_all_star="Regular Season")

#convert to dataframe
player_stats_df = player_stats.get_data_frames()[0]

#save to csv
player_stats_df.to_csv('nba_player_stats_2024_25.csv', index=False)

