import pandas as pd

# ------------------------------
# Load datasets
# ------------------------------
iwt_events = pd.read_csv("iwt_events.csv")
pwa_events = pd.read_csv("Historical Scrapes/Data/Clean/PWA/pwa_event_data_clean.csv")

# ------------------------------
# Convert date columns (using day-first format)
# ------------------------------
iwt_events['start_date_dt'] = pd.to_datetime(iwt_events['start_date'], dayfirst=True, errors='coerce')
iwt_events['end_date_dt'] = pd.to_datetime(iwt_events['finish_date'], dayfirst=True, errors='coerce')
pwa_events['pwa_start_date_dt'] = pd.to_datetime(pwa_events['pwa_start_date'], dayfirst=True, errors='coerce')
pwa_events['pwa_end_date_dt'] = pd.to_datetime(pwa_events['pwa_end_date'], dayfirst=True, errors='coerce')

# ------------------------------
# Create unique PWA events for matching
# ------------------------------
pwa_unique = pwa_events[['pwa_event_id', 'pwa_event_name', 'pwa_start_date_dt', 'pwa_end_date_dt']].drop_duplicates()

# ------------------------------
# Filter IWT events: only consider 4 or 5 star events with non-blank stars for matching
# ------------------------------
iwt_4_5_star = iwt_events[iwt_events['stars'].isin([4, 5]) & iwt_events['stars'].notna()].copy()
iwt_other = iwt_events[~iwt_events.index.isin(iwt_4_5_star.index)].copy()

# ------------------------------
# Matching function: match based purely on date proximity (±2 days)
# ------------------------------
def date_match(iwt_row, pwa_df):
    # Ensure IWT event has valid start and end dates
    if pd.isna(iwt_row['start_date_dt']) or pd.isna(iwt_row['end_date_dt']):
        return None

    for _, pwa_row in pwa_df.iterrows():
        # Skip if PWA event dates are missing
        if pd.isna(pwa_row['pwa_start_date_dt']) or pd.isna(pwa_row['pwa_end_date_dt']):
            continue

        # Check if both start and end dates are within ±2 days
        if (abs((iwt_row['start_date_dt'] - pwa_row['pwa_start_date_dt']).days) <= 2 and
            abs((iwt_row['end_date_dt'] - pwa_row['pwa_end_date_dt']).days) <= 2):
            return pwa_row['pwa_event_id']
    return None

# ------------------------------
# Apply matching function to 4/5 star IWT events
# ------------------------------
iwt_4_5_star['matched_pwa_event_id'] = iwt_4_5_star.apply(lambda row: date_match(row, pwa_unique), axis=1)

# ------------------------------
# Combine matched 4/5 star events with the rest (which remain unmatched)
# ------------------------------
final_iwt_events = pd.concat([iwt_4_5_star, iwt_other], ignore_index=True)

# ------------------------------
# Save the final output
# ------------------------------
final_iwt_events.to_csv("final_iwt_events_with_pwa_ids.csv", index=False)
print("Matching complete. Final dataset saved as 'final_iwt_events_with_pwa_ids.csv'.")