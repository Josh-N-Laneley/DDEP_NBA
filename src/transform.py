import pandas as pd

player_stats_df = pd.read_csv("raw/nba_player_stats_2024_25.csv", encoding="utf-8-sig")
psdf = player_stats_df  # assigned to a new variable for easier reference

#check for duplicates
print(psdf.duplicated().any())