############## PWA RAW TO CLEAN SCRIPT ##############
# This script cleans the pwa raw exports and preps them so they can be appended to iwt data.

# packages
import pandas as pd
import ast
from urllib.parse import urlparse, parse_qs


# -----------------------------------
# heat scores data cleaning
# -----------------------------------
heat_scores_df = pd.read_csv('Historical Scrapes/Data/Raw/PWA/pwa_aggregated_heat_scores_raw.csv')

# Remove anything before "_" in athleteid so "Browne_BRA-105" becomes "BRA-105"
heat_scores_df['athleteId'] = heat_scores_df['athleteId'].apply(lambda x: x.split('_')[-1])
# Create new column by combining heat_id and athleteid
heat_scores_df['heat_id_athleteid'] = heat_scores_df['heat_id'].astype(str) + '_' + heat_scores_df['athleteId']
# Replace all occurrence of "E-510" with "E-51" in athleteid
heat_scores_df['athleteId'] = heat_scores_df['athleteId'].str.replace("E-510", "E-51")
heat_scores_df['athleteId'] = heat_scores_df['athleteId'].str.replace("K-579", "K-90")
heat_scores_df = heat_scores_df.rename(columns={
    'athleteId': 'athlete_id', 
    'winBy': 'win_by',
    'eventDivisionId': 'division_id',
    'heat_id_athleteid' : 'heat_id_athlete_id'})

heat_scores_df.to_csv('Historical Scrapes/Data/Clean/PWA/pwa_heat_scores_clean.csv', index=False)

# -----------------------------------
# heat results data cleaning
# -----------------------------------
heat_results_df = pd.read_csv('Historical Scrapes/Data/Raw/PWA/pwa_aggregated_heat_results_raw.csv')

# Remove anything before "_" in athleteid so "Browne_BRA-105" becomes "BRA-105"
heat_results_df['athleteId'] = heat_results_df['athleteId'].apply(lambda x: x.split('_')[-1])
# Create new column by combining heat_id and athleteid
heat_results_df['heat_id_athlete_id'] = heat_results_df['heat_id'].astype(str) + '_' + heat_results_df['athleteId']
# Replace all occurrence of "E-510" with "E-51" in athleteid
heat_results_df['athleteId'] = heat_results_df['athleteId'].str.replace("E-510", "E-51")
heat_results_df['athleteId'] = heat_results_df['athleteId'].str.replace("K-579", "K-90")
heat_results_df = heat_results_df.rename(columns={
    'athleteId': 'athlete_id', 
    'winBy': 'win_by',
    'eventDivisionId': 'division_id'})


heat_results_df.to_csv('Historical Scrapes/Data/Clean/PWA/pwa_heat_results_clean.csv', index=False)


# -----------------------------------
# final rank data cleaning
# -----------------------------------

final_rank_df = pd.read_csv('Historical Scrapes/Data/Raw/PWA/pwa_final_ranks_raw.csv')

# Replace all occurrence of "E-510" with "E-51" in the new athleteid column (assumed to be from sail_no)
final_rank_df['sail_no'] = final_rank_df['sail_no'].astype(str).str.replace("E-510", "E-51")
# Drop the original athleteid column
final_rank_df = final_rank_df.drop(columns=['athlete_id', 'Points'])
# Rename columns: sail_no to athleteId and Name to name
final_rank_df = final_rank_df.rename(columns={
    'sail_no': 'athlete_id', 
    'Name': 'name',
    'eventDivisionid': 'division_id'})

# Add indicator for incomplete event divisions.
# For each group (by eventid and eventDivisionid), count how many rows have place == 1.
# If count > 1, mark 'incomplete' as True for all rows in that group.
final_rank_df['incomplete'] = final_rank_df.groupby(['event_id', 'division_id'])['place'] \
                                             .transform(lambda x: (x == 1).sum() > 1)

final_rank_df.to_csv('Historical Scrapes/Data/Clean/PWA/pwa_final_ranks_clean.csv', index=False)
# -----------------------------------
# heat progression cleaning
# -----------------------------------
heat_progression_df = pd.read_csv('Historical Scrapes/Data/Raw/PWA/pwa_aggregated_heat_progression_raw.csv')
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
# event data cleaning (DONE ELSEWHERE)
# -----------------------------------
# TBC


# Optionally, save the cleaned data back to CSV files
heat_scores_df.to_csv('Historical Scrapes/Data/Clean/PWA/pwa_heat_scores_clean.csv', index=False)
final_rank_df.to_csv('Historical Scrapes/Data/Clean/PWA/pwa_final_ranks_clean.csv', index=False)
heat_progression_df.to_csv('Historical Scrapes/Data/Clean/PWA/pwa_heat_progression_clean.csv', index=False)
