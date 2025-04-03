import pandas as pd
from fuzzywuzzy import process
import hashlib

# Load data
iwt_df = pd.read_csv('Athlete Database/Clean Data/iwt_sailors_clean.csv')
pwa_df = pd.read_csv('Athlete Database/Clean Data/pwa_sailors_clean.csv')
country_info = pd.read_csv('Athlete Database/Clean Data/country_info_v2.csv')


#################################################################################
### MANUAL DATA ENTRY TO ENSURE PWA NAMES MATCH
# -- 1) Manually fix mismatched names in pwa_df
# Create a dictionary of PWA_Name -> IWT_NAME
name_map = {
    # PWA NAME  :  IWT NAME
    'Coraline Foveau': 'Coco Foveau',
    'Justyna A. Sniady': 'Justyna Snaidy',
    'Michael Friedl (M)': 'Mike Friedl (sr)'
}

# Apply the mapping to the relevant column in pwa_df
# Adjust the column name if yours is different (e.g. "PWA_Name")
pwa_df['pwa_name'] = pwa_df['pwa_name'].replace(name_map)

###################################################################################

####### COUNTRY MAPPING #####

# Clean join keys: lowercase and strip whitespace
pwa_df['pwa_nationality'] = pwa_df['pwa_nationality'].astype(str).str.strip().str.lower()
country_info['pwa_demonyms'] = country_info['pwa_demonyms'].astype(str).str.strip().str.lower()

# Perform left join to bring in country name (from 'Name' in country_info)
pwa_df = pwa_df.merge(
    country_info[['pwa_demonyms', 'Name','live_heats_nationality']],
    how='left',
    left_on='pwa_nationality',
    right_on='pwa_demonyms'
)

# Rename columns for clarity
pwa_df.rename(columns={
    'Name': 'country'      # Country name from country_info
}, inplace=True)


# Track what’s still available for matching
available_names = pwa_df['pwa_name'].dropna().unique().tolist()

# Create lookup dictionaries
pwa_birth_dict = dict(zip(pwa_df['pwa_name'], pwa_df['pwa_yob']))
pwa_nationality_dict = dict(zip(pwa_df['pwa_name'], pwa_df['live_heats_nationality']))

# Results list
results = []

# Stage 1: Exact and fuzzy match (91+)
unmatched = []

for _, row in iwt_df.iterrows():
    name = row['iwt_name']
    yob = row['iwt_yob']
    matched = False

    if name in available_names:
        results.append({'iwt_name': name, 'best_match': name, 'score': 100, 'stage': 'Exact'})
        available_names.remove(name)
        matched = True
    else:
        match, score = process.extractOne(name, available_names)
        if score >= 91:
            results.append({'iwt_name': name, 'best_match': match, 'score': score, 'stage': 'Fuzzy91'})
            available_names.remove(match)
            matched = True

    if not matched:
        unmatched.append((name, yob, row.get('country', None), row.get('nationality', None)))

# Stage 2: Match on YOB +/-1
still_unmatched = []

for name, yob, country, nationality in unmatched:
    candidates = [n for n in available_names 
                  if pwa_birth_dict.get(n) is not None and abs(pwa_birth_dict[n] - yob) <= 1]
    if candidates:
        match, score = process.extractOne(name, candidates)
        if score >= 80:
            results.append({'iwt_name': name, 'best_match': match, 'score': score, 'stage': 'YOB±1'})
            available_names.remove(match)
            continue

    still_unmatched.append((name, yob, country, nationality))

# Stage 3: Match on name + country OR nationality
for name, yob, country, nationality in still_unmatched:
    candidates = [n for n in available_names if (
        (nationality and pwa_nationality_dict.get(n) == nationality)
    )]

    if candidates:
        match, score = process.extractOne(name, candidates)
        if score >= 90:
            results.append({'iwt_name': name, 'best_match': match, 'score': score, 'stage': 'CountryMatch'})
            available_names.remove(match)
            continue

    # Final fallback
    results.append({'iwt_name': name, 'best_match': None, 'score': 0, 'stage': 'Unmatched'})

# Convert results to DataFrame
fuzzy_matches_df = pd.DataFrame(results)

# Merge back with original iwt_df
merged_df = fuzzy_matches_df.merge(iwt_df, how='left', left_on='iwt_name', right_on='iwt_name')

# Merge in selected pwa_df data using the matched name
pwa_selected = pwa_df[['pwa_name', 'pwa_sail_no', 'pwa_url', 'pwa_yob']].copy()
pwa_selected.columns = ['best_match', 'pwa_sail_no', 'pwa_url', 'pwa_yob']

# Merge to get PWA info
merged_df = merged_df.merge(pwa_selected, how='left', on='best_match')


# Rename 'best_match' to 'pwa_Name' for consistency
merged_df.rename(columns={'best_match': 'pwa_name'}, inplace=True)


## create unique sailor id based on iwt_name and iwt_yob

def generate_8_digit_id(name, year):
    input_str = f"{name}_{year}"
    hash_str = hashlib.md5(input_str.encode()).hexdigest()
    id_int = int(hash_str, 16) % 100000000  # 8-digit range: 0 to 99,999,999

    return f"{id_int:08d}"

# Applying to the DataFrame:
merged_df['id'] = merged_df.apply(lambda row: generate_8_digit_id(row['iwt_name'], row['iwt_yob']), axis=1)
print(merged_df)

# create sailor_pwa_iwt_id table

sailor_pwa_iwt_ids = merged_df[['id', 'iwt_id', 'iwt_alt_id', 'pwa_sail_no']].copy()

# Step 2: Pivot (melt) the columns 'a', 'b', 'c' into a long format
sailor_pwa_iwt_ids = pd.melt(
    sailor_pwa_iwt_ids, 
    id_vars='id',          # Keep 'id' as the identifier column
    value_vars=['iwt_id', 'iwt_alt_id', 'pwa_sail_no'],  # These columns will be pivoted into rows
    var_name='org_id_type',   # Name for the new column that will contain 'a', 'b', 'c'
    value_name='value'     # Name for the new column that will contain the corresponding values
)

# remove na's
# Filter out rows where 'value' is NaN
sailor_pwa_iwt_ids = sailor_pwa_iwt_ids.dropna(subset=['value'])

# write sailor_pwa_iwt_ids and merged_df to csvs

merged_df.to_csv('Athlete Database/Clean Data/pwa_iwt_sailor_combined_clean.csv', index=False)
sailor_pwa_iwt_ids.to_csv('Athlete Database/Clean Data/pwa_iwt_sailor_link_table.csv', index=False)


