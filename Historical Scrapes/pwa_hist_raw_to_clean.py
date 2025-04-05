############## PWA RAW TO CLEAN SCRIPT ##############
# This script cleans the pwa raw exports and preps them so they can be appended to iwt data.

# packages
import pandas as pd

# data load
heat_scores_df = pd.read_csv('Historical Scrapes/Data/Raw/PWA/pwa_aggregated_heat_scores_raw.csv')
heat_results_df = pd.read_csv('Historical Scrapes/Data/Raw/PWA/pwa_aggregated_heat_results_raw.csv')
event_data_df = pd.read_csv('Historical Scrapes/Data/Raw/PWA/pwa_event_data_raw.csv')
final_rank_df = pd.read_csv('Historical Scrapes/Data/Raw/PWA/pwa_final_ranks_raw.csv')

# -----------------------------------
# heat scores data cleaning
# -----------------------------------
# Remove anything before "_" in athleteid so "Browne_BRA-105" becomes "BRA-105"
heat_scores_df['athleteId'] = heat_scores_df['athleteId'].apply(lambda x: x.split('_')[-1])
# Create new column by combining heat_id and athleteid
heat_scores_df['heat_id_athleteid'] = heat_scores_df['heat_id'].astype(str) + '_' + heat_scores_df['athleteId']
# Replace all occurrence of "E-510" with "E-51" in athleteid
heat_scores_df['athleteId'] = heat_scores_df['athleteId'].str.replace("E-510", "E-51")
heat_scores_df['athleteId'] = heat_scores_df['athleteId'].str.replace("K-579", "K-90")

# -----------------------------------
# heat results data cleaning
# -----------------------------------
# Remove anything before "_" in athleteid so "Browne_BRA-105" becomes "BRA-105"
heat_results_df['athleteId'] = heat_results_df['athleteId'].apply(lambda x: x.split('_')[-1])
# Create new column by combining heat_id and athleteid
heat_results_df['heat_id_athleteid'] = heat_results_df['heat_id'].astype(str) + '_' + heat_results_df['athleteId']
# Replace all occurrence of "E-510" with "E-51" in athleteid
heat_results_df['athleteId'] = heat_results_df['athleteId'].str.replace("E-510", "E-51")
heat_results_df['athleteId'] = heat_results_df['athleteId'].str.replace("K-579", "K-90")

# -----------------------------------
# final rank data cleaning
# -----------------------------------
# Replace all occurrence of "E-510" with "E-51" in the new athleteid column (assumed to be from sail_no)
final_rank_df['sail_no'] = final_rank_df['sail_no'].astype(str).str.replace("E-510", "E-51")
# Drop the original athleteid column
final_rank_df = final_rank_df.drop(columns=['athlete_id'])
# Rename columns: sail_no to athleteId and Name to name
final_rank_df = final_rank_df.rename(columns={'sail_no': 'athleteId', 'Name': 'name'})

# Add indicator for incomplete event divisions.
# For each group (by eventid and eventDivisionid), count how many rows have place == 1.
# If count > 1, mark 'incomplete' as True for all rows in that group.
final_rank_df['incomplete'] = final_rank_df.groupby(['event_id', 'eventDivisionid'])['place'] \
                                             .transform(lambda x: (x == 1).sum() > 1)

# -----------------------------------
# event data cleaning
# -----------------------------------
# see function for steps 

# -----------------------------------
# heat progression cleaning
# -----------------------------------
# TBC

# Optionally, save the cleaned data back to CSV files
heat_scores_df.to_csv('Historical Scrapes/Data/Clean/PWA/pwa_heat_scores_clean.csv', index=False)
heat_results_df.to_csv('Historical Scrapes/Data/Clean/PWA/pwa_heat_results_clean.csv', index=False)
final_rank_df.to_csv('Historical Scrapes/Data/Clean/PWA/pwa_final_ranks_clean.csv', index=False)
