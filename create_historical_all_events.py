#### CREATE HISTORICAL ALL EVENT RECORD #####
import os
import requests
import json
import pandas as pd
import re
import numpy as np

from utils.functions_iwt_scrape import fetch_wave_tour_events
from utils.functions_clean import _parse_rank, standardise_event_name, pwa_clean_events, iwt_clean_events


#### Load IWT Events and clean
fetch_wave_tour_events()
iwt_clean_events(
        'wave_tour_events_cleaned.json',
        'Historical Scrapes/Data/Clean/IWT/iwt_event_data_with_division_clean.csv'
    )
iwt_df = pd.read_csv('Historical Scrapes/Data/Clean/IWT/iwt_event_data_with_division_clean.csv') 

#### Load PWA Events and clean
pwa_df = pd.read_csv('Historical Scrapes/Data/Raw/PWA/pwa_event_data_raw.csv')
pwa_clean_events(pwa_df, output_file="Historical Scrapes/Data/Clean/PWA/pwa_event_data_clean.csv")
pwa_df = pd.read_csv('Historical Scrapes/Data/Clean/PWA/pwa_event_data_clean.csv')

#### Combined clean datasets
combined_df = pd.concat([iwt_df, pwa_df], ignore_index=True, sort=False)

### Write combined dataframe to csv
combined_df.to_csv('Historical Scrapes/Data/Clean/Combined/combined_event_data_v3.csv')




#### WORKLFOW FOR NEW COMBINED
# load raw data
# run cleaning functions
# combine and write to csv
