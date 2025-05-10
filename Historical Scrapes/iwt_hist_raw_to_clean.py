############## IWT RAW TO CLEAN SCRIPT ##############
# This script cleans the pwa raw exports and preps them so they can be appended to iwt data.

# packages
import pandas as pd
import ast
from urllib.parse import urlparse, parse_qs


# data load
#heat_scores_df = pd.read_csv('Historical Scrapes/Data/Raw/PWA/pwa_aggregated_heat_scores_raw.csv')
#heat_results_df = pd.read_csv('Historical Scrapes/Data/Raw/PWA/pwa_aggregated_heat_results_raw.csv')
heat_progression_df = pd.read_csv('Historical Scrapes/Data/Raw/IWT/combined_iwt_heat_progression_format.csv')
#final_rank_df = pd.read_csv('Historical Scrapes/Data/Raw/PWA/pwa_final_ranks_raw.csv')

# -----------------------------------
# heat scores data cleaning
# -----------------------------------


# -----------------------------------
# heat results data cleaning
# -----------------------------------


# -----------------------------------
# final rank data cleaning
# -----------------------------------


# -----------------------------------
# heat progression cleaning
# -----------------------------------

# Compute total heats per (eventDivisionId, round_name)
heat_progression_df['Total_Round_Heats'] = (
    heat_progression_df
    .groupby(['eventDivisionId', 'round_name'])['round_name']
    .transform('size')
)
# Compute max heats across all rounds for each eventDivisionId
heat_progression_df['Max_Heats'] = (
    heat_progression_df
    .groupby('eventDivisionId')['Total_Round_Heats']
    .transform('max')
)

# Sort so heat_order is in ascending order within each eventDivisionId
heat_progression_df = heat_progression_df.sort_values(
    ['eventDivisionId', 'heat_order']
)

# Add an Index within each (eventDivisionId, round_order) group
heat_progression_df['actual_heat_order'] = (
    heat_progression_df
    .groupby(['eventDivisionId', 'round_order'])
    .cumcount()
    .add(1)
)

# Calculate y_pos exactly as in your M script
heat_progression_df['y_pos'] = (
    heat_progression_df['Max_Heats'] / 2 + 0.5
    - (heat_progression_df['Total_Round_Heats'] / 2 + 0.5)
    + heat_progression_df['actual_heat_order']
)






# -----------------------------------
# evetn data cleaning (DONE ELSEWHERE)
# -----------------------------------
# TBC




# Optionally, save the cleaned data back to CSV files
#heat_scores_df.to_csv('Historical Scrapes/Data/Clean/PWA/pwa_heat_scores_clean.csv', index=False)
#heat_results_df.to_csv('Historical Scrapes/Data/Clean/PWA/pwa_heat_results_clean.csv', index=False)
#final_rank_df.to_csv('Historical Scrapes/Data/Clean/PWA/pwa_final_ranks_clean.csv', index=False)
heat_progression_df.to_csv('Historical Scrapes/Data/Clean/IWT/iwt_heat_progression_clean.csv', index=False)
