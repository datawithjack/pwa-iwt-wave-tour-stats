########## COMBINED PWA AND IWT CLEAN DATA ###########

## Combine progression data
# load data sets
iwt_prog = pd.read_csv('Historical Scrapes/Data/Clean/IWT/iwt_heat_progression_clean.csv')
pwa_prog = pd.read_csv('Historical Scrapes/Data/Clean/PWA/pwa_heat_progression_clean.csv')

combined_prog = pd.concat([iwt_prog, pwa_prog], ignore_index=True, sort=False)
combined_prog.to_csv("Historical Scrapes/Data/Clean/Combined/combined_heat_progression_data.csv")





### USE BELOW TO CHECK DATATSET BEFORE MERGING
#  = pwa_data
#  = iwt_data
### APPEND EVENTS DATA
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

