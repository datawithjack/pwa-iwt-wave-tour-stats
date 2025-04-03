import pandas as pd
from fuzzywuzzy import process

# Load data
iwt_df = pd.read_csv('unique_athletes.csv')
pwa_df = pd.read_csv('pwa_sailor_clean.csv')

# Prepare available names list
pwa_names = pwa_df['Name'].dropna().unique().tolist()
available_names = pwa_names.copy()

# Results list
results = []

# Loop through each name in iwt_df
for name in iwt_df['name']:
    matched = False

    # 1. Direct match (exact string)
    if name in available_names:
        results.append({'name': name, 'best_match': name, 'score': 100})
        available_names.remove(name)
        matched = True
    else:
        # 2. Fuzzy match (if not already matched)
        if available_names:
            match, score = process.extractOne(name, available_names)
            if score >= 91:
                results.append({'name': name, 'best_match': match, 'score': score})
                available_names.remove(match)
                matched = True

    if not matched:
        results.append({'name': name, 'best_match': None, 'score': 0})

# Convert and save
fuzzy_matches_df = pd.DataFrame(results)
fuzzy_matches_df.to_csv('fuzzy_matched.csv', index=False)
print(fuzzy_matches_df.head())

