import ast
import pandas as pd


def clean_event_df(df, output_file=None):
    # -------------------------------
    # Expand final_rank column into final_rank_label and final_rank_code columns
    # -------------------------------
    new_final_rank_rows = []
    for _, row in df.iterrows():
        try:
            final_rank_dict = ast.literal_eval(row['final_rank'])
            if 'Wave Men' in final_rank_dict:
                label = 'Wave Men'
                code = final_rank_dict['Wave Men']
            else:
                label, code = list(final_rank_dict.items())[0]
            new_row = row.copy()
            new_row['final_rank_label'] = label
            new_row['final_rank_code'] = code
        except Exception:
            new_row = row.copy()
            new_row['final_rank_label'] = None
            new_row['final_rank_code'] = None
        new_final_rank_rows.append(new_row)
    df_expanded = pd.DataFrame(new_final_rank_rows)

    # -------------------------------
    # Clean the 'category_codes' column:
    # -------------------------------
    df_expanded['category_codes'] = (
        df_expanded['category_codes']
        .fillna('')
        .astype(str)
        .apply(lambda x: [item.replace("'", "").replace("[", "").replace("]", "").strip()
                          for item in x.split(',')])
    )
    # -------------------------------
    # Clean the 'elimination_names' column:
    # -------------------------------
    df_expanded['elimination_names'] = (
        df_expanded['elimination_names']
        .fillna('')
        .astype(str)
        .apply(lambda x: [item.replace("'", "").replace("[", "").replace("]", "").strip()
                          for item in x.split(',')])
    )
    # Keep only rows where codes match names
    df_matched = df_expanded[df_expanded.apply(
        lambda row: len(row['category_codes']) == len(row['elimination_names']), axis=1
    )]

    # -------------------------------
    # Explode category & elimination into separate rows
    # -------------------------------
    exploded_rows = []
    for _, row in df_matched.iterrows():
        for cat, elim in zip(row['category_codes'], row['elimination_names']):
            r = row.copy()
            r['category_codes'] = cat
            r['elimination_names'] = elim
            exploded_rows.append(r)
    new_df = pd.DataFrame(exploded_rows)

    # -------------------------------
    # Filter only wave events
    # -------------------------------
    new_df = new_df[new_df['elimination_names'].str.contains('wave', case=False, na=False)]

    # -------------------------------
    # Gender filter
    # -------------------------------
    men_cond = (~new_df['elimination_names'].str.contains(r'\bmen(s)?\b', case=False, regex=True)) | \
               (new_df['final_rank_label'].str.contains(r'\bmen(s)?\b', case=False, regex=True))
    women_cond = (~new_df['elimination_names'].str.contains(r'\bwomen(s)?\b', case=False, regex=True)) | \
                 (new_df['final_rank_label'].str.contains(r'\bwomen(s)?\b', case=False, regex=True))
    new_df = new_df[men_cond & women_cond]

    # -------------------------------
    # Extract sex from final_rank_label
    # -------------------------------
    new_df['sex'] = new_df['final_rank_label'].str.extract(r'(?i)\b(men|women)\b')[0].str.capitalize()


    # -------------------------------
    # Add empty 'location' and 'stars' columns
    # -------------------------------
    new_df['location'] = ''
    new_df['stars'] = ''

    # -------------------------------
    # Add start, end date columns
    # -------------------------------
    new_df['start_date'] = pd.to_datetime(
        new_df['event_date'].str.split(' - ').str[0] + ' ' + new_df['year'].astype(str),
        format='%b %d %Y',
        errors='coerce'
    ).dt.strftime('%Y-%m-%d')
    
    new_df['end_date'] = pd.to_datetime(
        new_df['event_date'].str.split(' - ').str[1] + ' ' + new_df['year'].astype(str),
        format='%b %d %Y',
        errors='coerce'
    ).dt.strftime('%Y-%m-%d')
    # -------------------------------
    # Rename columns as per PWA mapping
    # -------------------------------
    rename_map = {
        'event_id':            'event_id',
        'event_name':          'event_name',
        'section':             'results_status',
        'division_id':         'category_codes',
        'division_name':       'elimination_names',
        'division_rank_name':  'final_rank_label',
        'division_rank_id':    'final_rank_code',
        'sex':                 'sex',
        'event_href':          'event_link'
        }

    # -------------------------------
    # Drop original final_rank
    # -------------------------------
    new_df = new_df.drop(columns=['final_rank', 'ladder_url','id', 'year', 'event_date'])

    final_df = new_df.rename(columns=rename_map)

    # -------------------------------
    # Output to CSV if requested
    # -------------------------------
    if output_file:
        final_df.to_csv(output_file, index=False)

    return final_df
    
df = pd.read_csv('Historical Scrapes/Data/Raw/PWA/pwa_event_data_raw.csv')
    
clean_event_df(df, output_file="test_new_clean_pwa.csv")
