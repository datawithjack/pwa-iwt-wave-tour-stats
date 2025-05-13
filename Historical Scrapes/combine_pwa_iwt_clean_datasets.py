########## COMBINED PWA AND IWT CLEAN DATA ###########

## Combine progression data
# load data sets
iwt_prog = pd.read_csv('Historical Scrapes/Data/Clean/IWT/iwt_heat_progression_clean.csv')
pwa_prog = pd.read_csv('Historical Scrapes/Data/Clean/PWA/pwa_heat_progression_clean.csv')

combined_prog = pd.concat([iwt_prog, pwa_prog], ignore_index=True, sort=False)
combined_prog.rename(columns={'eventDivisionId': 'division_id'}, inplace=True)  # rename eventdividsiond
combined_prog.to_csv("Historical Scrapes/Data/Clean/Combined/combined_heat_progression_data.csv")

## Combine final rank data
iwt_final_rank = pd.read_csv('Historical Scrapes/Data/Clean/IWT/iwt_final_ranks_clean.csv')
pwa_final_rank  = pd.read_csv('Historical Scrapes/Data/Clean/PWA/pwa_final_ranks_clean.csv')

combined_final_ranks = pd.concat([iwt_final_rank, pwa_final_rank], ignore_index=True, sort=False)
combined_final_ranks.to_csv("Historical Scrapes/Data/Clean/Combined/combined_final_rank_data.csv")

## combine heat results data
iwt_heat_results = pd.read_csv('Historical Scrapes/Data/Clean/IWT/iwt_heat_results_clean.csv')
pwa_heat_results= pd.read_csv('Historical Scrapes/Data/Clean/PWA/pwa_heat_results_clean.csv')

combined_heats_results = pd.concat([iwt_heat_results, pwa_heat_results], ignore_index=True, sort=False)
combined_heats_results.to_csv("Historical Scrapes/Data/Clean/Combined/combined_heat_results_data.csv")

## combine heat scores data
iwt_heat_scores = pd.read_csv('Historical Scrapes/Data/Clean/IWT/iwt_heat_scores_clean.csv')
pwa_heat_scores = pd.read_csv('Historical Scrapes/Data/Clean/PWA/pwa_heat_scores_clean.csv')

combined_heat_scores = pd.concat([iwt_heat_scores, pwa_heat_scores], ignore_index = True, sort = False)
combined_heat_scores.to_csv('Historical Scrapes/Data/Clean/Combined/combined_heat_scores_data.csv')


### USE BELOW TO CHECK DATATSET BEFORE MERGING
pwa_data = pwa_heat_results
iwt_data = iwt_heat_results 

# 1. Inspect
print("IWT columns:", iwt_data.columns.tolist())
print("PWA columns:", pwa_data.columns.tolist())

# 2. Compare
common_cols = set(iwt_data.columns).intersection(pwa_data.columns)
iwt_only    = set(iwt_data.columns) - common_cols
pwa_only    = set(pwa_data.columns) - common_cols

print("Common columns:", common_cols)
print("IWT-only columns:", iwt_only)
print("PWA-only columns:", pwa_only)

