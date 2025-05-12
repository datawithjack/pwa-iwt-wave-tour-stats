#### CREATE HISTORICAL ALL EVENT RECORD #####
import os
import requests
import json
import pandas as pd
import re


from functions_iwt import (
    fetch_wave_tour_events,
    extract_results_published_events,
    fetch_event_divisions,
    fetch_event_division_results,
    flatten_heat_progression,
    flatten_heat_results_and_scores,
    process_event_division,
    clean_heat_order
)
#### get all IWT EVENTS and division info
fetch_wave_tour_events()

# 1. Load your JSON
with open('wave_tour_events_cleaned.json', 'r') as f:
    data = json.load(f)

# 2. Take only the first three
events = data #[:3]
# 2. Enrich each event with division IDs + names
for ev in events:
    ev_id = int(ev['event_id'])
    divisions = fetch_event_divisions(ev_id)
    if divisions:
        ids, names = zip(*divisions)
        ev['division_ids']   = list(ids)
        ev['division_names'] = list(names)
    else:
        ev['division_ids']   = []
        ev['division_names'] = []

# 3. Normalize into a flat table
df = pd.json_normalize(events)

# 4. Explode so each (id, name) pair is its own row
df = df.explode(['division_ids', 'division_names'])

# 5. Optionally, rename for clarity
df = df.rename(columns={
    'division_ids':   'division_id',
    'division_names': 'division_name'
})

# 6. Final cleaning: add empty columns and rename and columsn to match pwa data 
df["sex"] = ""
df["event_link"] = ""
df["division_rank_name"] = df["division_name"]
df["division_rank_id"] = df["division_id"]
df["source"] = "live heats"
df["elimination"] = ""

# 7. Write to CSV
out_fn = 'Historical Scrapes/Data/Clean/IWT/iwt_event_data_with_division_clean.csv'
df.to_csv(out_fn, index=False)
print(f"Wrote {len(df)} rows to {out_fn}")


#### REMOVE PWA EVENTS from IWT DATA (i.e hosts and run by pwa)
iwt_event_df = pd.read_csv('Historical Scrapes/Data/Clean/IWT/iwt_event_data_with_division_clean.csv')
# list of IDs to remove
exclude_div_ids = [
    247060, 247061, 353311, 353312,
    400442, 400473, 247053, 247054,
    353299, 353300
]
# filter out rows where division_id is in exclude_div_ids
filtered_iwt = iwt_event_df[~iwt_event_df['division_id'].isin(exclude_div_ids)]
# if you want to overwrite the original:
iwt_event_df = filtered_iwt



### LOAD PWA DATA  & CLEAN
df = pd.read_csv('Historical Scrapes/Data/Raw/PWA/pwa_event_data_raw.csv')
clean_event_df(df, output_file="Historical Scrapes/Data/Clean/PWA/pwa_event_data_clean.csv")

pwa_event_df = pd.read_csv('Historical Scrapes/Data/Clean/PWA/pwa_event_data_clean.csv')


### APPEND EVENTS DATA
# 1. Inspect
print("IWT columns:", iwt_event_df.columns.tolist())
print("PWA columns:", pwa_event_df.columns.tolist())

# 2. Compare
common_cols = set(iwt_event_df.columns).intersection(pwa_event_df.columns)
iwt_only    = set(iwt_event_df.columns) - common_cols
pwa_only    = set(pwa_event_df.columns) - common_cols

print("Common columns:", common_cols)
print("IWT-only columns:", iwt_only)
print("PWA-only columns:", pwa_only)

# 3. (Optional) Re-index so both have the same columns order
#    If you want to enforce the same column order before concatenation:
all_cols = list(common_cols) + list(iwt_only) + list(pwa_only)
iwt_event_df = iwt_event_df.reindex(columns=all_cols)
pwa_event_df = pwa_event_df.reindex(columns=all_cols)

# 4. Append
combined_df = pd.concat([iwt_event_df, pwa_event_df], ignore_index=True, sort=False)

# 5. Verify
print("Combined shape:", combined_df.shape)
print("Combined columns:", combined_df.columns.tolist())

combined_df.to_csv("Historical Scrapes/Data/Clean/Combined/combined_event_data.csv")

# Combined data cleaning


