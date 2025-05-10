#### CREATE HISTORICAL ALL EVENT RECORD #####
import os
import requests
import json
import pandas as pd
import re


from functions_iwt_progression_results_scores import (
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

import json
import pandas as pd

# 1. Load your JSON
with open('wave_tour_events_cleaned.json', 'r') as f:
    data = json.load(f)

# 2. Take only the first three
events = data[:3]
# 2. Enrich each event with division IDs + names
for ev in events:
    ev_id = int(ev['id'])
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

# 6. Write to CSV
out_fn = 'wave_tour_events_exploded.csv'
df.to_csv(out_fn, index=False)
print(f"Wrote {len(df)} rows to {out_fn}")


### FORMAT data 
#### REMOVE PWA EVENTS
### APPEND PWA EVENTS
# read in csv
