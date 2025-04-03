import pandas as pd
from fuzzywuzzy import process

# Load data
iwt_df = pd.read_csv('unique_athletes.csv')
pwa_df = pd.read_csv('pwa_sailor_clean.csv')
country_info = pd.read_csv('country_info_v2.csv')


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
pwa_df['Name'] = pwa_df['Name'].replace(name_map)

###################################################################################

####### COUNTRY MAPPING #####

# Clean join keys: lowercase and strip whitespace
pwa_df['Nationality_clean'] = pwa_df['Nationality'].astype(str).str.strip().str.lower()
country_info['pwa_demonyms_clean'] = country_info['pwa_demonyms'].astype(str).str.strip().str.lower()

# Perform left join to bring in country name (from 'Name' in country_info)
pwa_df = pwa_df.merge(
    country_info[['pwa_demonyms_clean', 'Name','live_heats_nationality']],
    how='left',
    left_on='Nationality_clean',
    right_on='pwa_demonyms_clean'
)

# Remove any rows that were accidentally added (i.e. where 'Name_x' is NaN)
pwa_df = pwa_df[pwa_df['Name_x'].notna()].copy()

# Rename columns for clarity
pwa_df.rename(columns={
    'Name_x': 'Name',        # Sailor name
    'Name_y': 'Country'      # Country name from country_info
}, inplace=True)


# Track what’s still available for matching
available_names = pwa_df['Name'].dropna().unique().tolist()

# Create lookup dictionaries
pwa_birth_dict = dict(zip(pwa_df['Name'], pwa_df['Year of Birth']))
pwa_nationality_dict = dict(zip(pwa_df['Name'], pwa_df['live_heats_nationality']))

# Results list
results = []

# Stage 1: Exact and fuzzy match (91+)
unmatched = []

for _, row in iwt_df.iterrows():
    name = row['name']
    yob = row['year_of_birth']
    matched = False

    if name in available_names:
        results.append({'name': name, 'best_match': name, 'score': 100, 'stage': 'Exact'})
        available_names.remove(name)
        matched = True
    else:
        match, score = process.extractOne(name, available_names)
        if score >= 91:
            results.append({'name': name, 'best_match': match, 'score': score, 'stage': 'Fuzzy91'})
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
            results.append({'name': name, 'best_match': match, 'score': score, 'stage': 'YOB±1'})
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
            results.append({'name': name, 'best_match': match, 'score': score, 'stage': 'CountryMatch'})
            available_names.remove(match)
            continue

    # Final fallback
    results.append({'name': name, 'best_match': None, 'score': 0, 'stage': 'Unmatched'})

# Convert results to DataFrame
fuzzy_matches_df = pd.DataFrame(results)

# Merge back with original iwt_df
merged_df = fuzzy_matches_df.merge(iwt_df, how='left', left_on='name', right_on='name')

# Merge in selected pwa_df data using the matched name
pwa_selected = pwa_df[['Name', 'Sail No', 'Profile URL', 'Year of Birth']].copy()
pwa_selected.columns = ['best_match', 'pwa_Sail_No', 'pwa_Profile_URL', 'pwa_Year_of_Birth']

# Merge to get PWA info
merged_df = merged_df.merge(pwa_selected, how='left', on='best_match')

# Prefix iwt columns
merged_df = merged_df.rename(columns=lambda col: f"iwt_{col}" if col in iwt_df.columns else col)

# Rename 'best_match' to 'pwa_Name' for consistency
merged_df.rename(columns={'best_match': 'pwa_Name'}, inplace=True)

# Reorder columns
ordered_cols = ['iwt_name', 'pwa_Name', 'score', 'stage'] + \
               [col for col in merged_df.columns if col.startswith('iwt_') and col != 'iwt_name'] + \
               ['pwa_Sail_No', 'pwa_Profile_URL', 'pwa_Year_of_Birth']
merged_df = merged_df[ordered_cols]



# Save to CSV
merged_df.to_csv('pwa_iwt_sailor_fuzzy_matches.csv', index=False)



