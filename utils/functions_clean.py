## PWA Cleaning Functions
import pandas as pd
import ast
import re
import numpy as np
import json

from utils.functions_iwt_scrape import fetch_event_divisions  

def _parse_rank(cell):
    """
    Turn a string like:
      "{'Wave Men': '960', 'Wave Women': '961'}"
    into a list of tuples:
      [('Wave Men','960'),('Wave Women','961')]
    """
    s = str(cell)
    # find all 'key': 'value' pairs
    return re.findall(r"'([^']+)'\s*:\s*'([^']+)'", s)


def standardise_event_name(row):
    # start by removing any standalone 4-digit year from the raw name
    raw = re.sub(r'\b\d{4}\b', '', str(row['event_name']))
    name = raw.lower()
    stars = row.get('stars', np.nan)  # adjust if your column is named differently
    
    # Treat blank or NaN star as “blank”
    is_blank = pd.isna(stars) or str(stars).strip()==''

    # Rule 1: high-star or blank events with key substrings
    if (stars in [4, 5] or is_blank) and 'gran canaria' in name:
        return 'Gran Canaria World Cup'
    if (stars in [4, 5] or is_blank) and 'tenerife' in name:
        return 'Tenerife World Cup'
    if (stars in [4, 5] or is_blank) and 'sylt' in name:
        return 'Sylt World Cup'
    if (stars in [4, 5] or is_blank) and 'aloha classic' in name:
        return 'Aloha Classic'
    if (stars in [4, 5] or is_blank) and 'chile' in name:
        return 'Chile World Cup'
    if (stars in [4, 5] or is_blank) and 'japan' in name:
        return 'Japan World Cup'  

    # Rule 2: if “STAR” appears, drop everything up to and including it
    parts = re.split(r'star', raw, flags=re.IGNORECASE)
    if len(parts) > 1:
        return parts[1].strip()

    # Rule 3: otherwise, if there’s a “:”, drop everything to the left of it
    if ':' in raw:
        return raw.split(':', 1)[1].strip()

    # Fallback: leave as-is (but with year already removed)
    return raw.strip()



def pwa_clean_events(df: pd.DataFrame, output_file: str = None) -> pd.DataFrame:
    """
    Cleans raw PWA event DataFrame and returns cleaned DataFrame.
    """
    
    # --- 0) DROP rows where elimination_names is blank/null ---
    df = df[
        df['elimination_names'].notna()
        & (df['elimination_names'].astype(str).str.strip() != '')
    ]
    
    # --- 1) EXPLODE final_rank into two columns -----------------
    df['rank_items'] = df['final_rank'].apply(_parse_rank)
    df = df.explode('rank_items')
    rank_expanded = df['rank_items'].apply(
        lambda x: pd.Series(x if isinstance(x, (list,tuple)) else [None, None])
    )
    rank_expanded.columns = ['division_rank_name','division_rank_id']
    df = pd.concat([df.drop(columns=['rank_items','final_rank']), rank_expanded], axis=1)

    # --- 2) PARSE category_codes & elimination_names into lists ---
    for col in ['category_codes', 'elimination_names']:
        df[col] = (
            df[col]
            .fillna('')
            .astype(str)
            .str.strip("[]")
            .str.replace("'", "")
            .str.split(',')
            .apply(lambda items: [i.strip() for i in items if i.strip()])
        )

    # --- 3) FILTER OUT any rows where elimination_names list ended up empty ---
    df = df[df['elimination_names'].str.len() > 0]

    # only keep rows where the two lists are same-length
    df = df[df['category_codes'].str.len() == df['elimination_names'].str.len()]

    # --- 4) EXPLODE both lists together into division_id/name ---
    df = (
        df
        .explode(['category_codes', 'elimination_names'])
        .rename(columns={
            'category_codes': 'division_id',
            'elimination_names': 'division_name'
        })
    )

    # --- 5) FILTER only wave events ---
    df = df[df['division_name'].str.contains('wave', case=False, na=False)]

    # --- 6) EXTRACT sex from the rank label ---
    df['sex'] = (
        df['division_rank_name']
        .str.extract(r'(?i)\b(men|women)\b')[0]
        .str.capitalize()
    )

    # --- 7) BUILD datetime cols, day_window, formatted dates ---
    df['start_dt'] = pd.to_datetime(
        df['event_date'].str.split(' - ').str[0] + ' ' + df['year'].astype(str),
        format='%b %d %Y', errors='coerce'
    )
    df['finish_dt'] = pd.to_datetime(
        df['event_date'].str.split(' - ').str[1] + ' ' + df['year'].astype(str),
        format='%b %d %Y', errors='coerce'
    )
    df['day_window']  = (df['finish_dt'] - df['start_dt']).dt.days
    df['start_date']  = df['start_dt'].dt.strftime('%Y-%m-%d')
    df['finish_date'] = df['finish_dt'].dt.strftime('%Y-%m-%d')
    
    # drop helper cols
    df = df.drop(columns=['id', 'start_dt', 'finish_dt', 'event_date','ladder_url'])

    # --- 8) RENAME to match your output schema ---
    df = df.rename(columns={
        'name': 'event_name',
        'section': 'results_status',
        'event_href': 'event_link'
    })

    # --- 9) ADD static mappings & extra fields as before ---
    # ADD placeholders & compute date fields -------------
    location_map = {
        '2024 Gran Canaria GLORIA PWA Windsurfing Grand Slam ******': 'Spain',
        '2024 Tenerife PWA World Cup ****': 'Spain',
        '2024 Citroën PWA Windsurf World Cup Sylt *******': 'Germany',
        '2023 Gran Canaria PWA Windsurfing Grand Slam ******': 'Spain',
        'schauinslandreisen Windsurf World Cup Sylt, presented by got2b *******': 'Germany',
        '2022 Gran Canaria PWA Windsurfing World Cup *****': 'Spain',
        '2022 Mercedes-Benz World Cup Sylt *******': 'Germany',
        'SOMWR 10 x Marignane PWA Grand Slam, Presented by Greentech Festival******': 'France',
        '2019 Gran Canaria PWA World Cup': 'Spain',
        '2019 Tenerife PWA World Cup': 'Spain',
        '2019 Mercedes-Benz World Cup Sylt': 'Germany',
        '2019 Mercedes-Benz Aloha Classic': 'Hawaii',
        'Gran Canaria': 'Spain',
        'Tenerife': 'Spain',
        'Mercedes-Benz World Cup Sylt': 'Germany',
        'Pozo Izquierdo, Gran Canaria': 'Spain',
        'El Medano, Tenerife': 'Spain',
        'NoveNove Maui Aloha Classic': 'Hawaii',
    }
    df['location'] = df['event_name'].map(location_map).fillna('')

    stars_map = {
        '2024 Gran Canaria GLORIA PWA Windsurfing Grand Slam ******': 5,
        '2024 Tenerife PWA World Cup ****':                  4,
        '2024 Citroën PWA Windsurf World Cup Sylt *******': 5,
        '2023 Gran Canaria PWA Windsurfing Grand Slam ******': 5,
        'schauinslandreisen Windsurf World Cup Sylt, presented by got2b *******': 5,
    }
    df['stars'] = df['event_name'].map(stars_map).fillna('')

    df['source']    = 'pwa'

    df['elimination_type'] = df['division_name'].apply(
        lambda x: 'Double'
        if 'double elimination' in x.lower()
        else 'Single' if 'elimination' in x.lower()
        else ''
    )

    # 1) Derive division_name_sex
    df['division_name_sex'] = (
        df['division_name']
        .astype(str)  # ensure we don’t error on NaNs
        .apply(lambda x: 'Women' if 'Women' in x 
                            else ('Men' if 'Men' in x else pd.NA))
    )

    # 2) Keep only rows where the new column matches sex
    df_matched = df[df['division_name_sex'] == df['sex']].copy()

    # create standard event name
    df_matched['standard_event_name'] = df_matched.apply(standardise_event_name, axis=1)

    # rename columns
    df_matched.rename(
        columns={
            'division_name': 'elimination_name',
            'division_id': 'elimination_id',
            'division_name_sex': 'division_name',
            'division_rank_id': 'division_id'

            }, inplace=True)


    # (optional) if you want to drop the helper column afterwards:
    df_matched = df_matched.drop(columns=['division_rank_name'])


    if output_file:
        df_matched.to_csv(output_file, index=False)

    return df_matched


def iwt_clean_events(input_json: str,
                 output_csv: str,
                 exclude_events=None,
                 exclude_div_ids=None):
    """
    Load events from JSON, enrich with divisions, clean and write to CSV.
    """
    exclude_events   = exclude_events or [295232, 296885]
    exclude_div_ids  = exclude_div_ids or [
        247060, 247061, 353311, 353312,
        400442, 400473, 247053, 247054,
        353299, 353300
    ]
    
    # 1) load JSON
    with open(input_json, 'r') as f:
        events = json.load(f)
    
    # 2) fetch divisions for each event
    for ev in events:
        ev_id = int(ev['event_id'])
        divs = fetch_event_divisions(ev_id) or []
        ids, names = zip(*divs) if divs else ([], [])
        ev['division_ids']   = list(ids)
        ev['division_names'] = list(names)
    
    # 3) normalize & explode
    df = pd.json_normalize(events)
    df = df.explode(['division_ids', 'division_names'])
    
    # 4) rename for clarity
    df = df.rename(columns={
        'division_ids':   'division_id',
        'division_names': 'division_name'
    })
    
    # 5) filter out unwanted events & divisions
    df = df[~df['event_id'].isin(exclude_events)]
    df = df[~df['division_id'].isin(exclude_div_ids)]
    
    # 6) derive sex
    conds = [
        df['division_name'].str.contains(r'\b(Men|Boys)\b',  case=False, na=False),
        df['division_name'].str.contains(r'\b(Women|Girls)\b', case=False, na=False),
    ]
    df['sex'] = np.select(conds, ['Men','Women'], default='')
    
    # 7) other fields
    df['event_link']     = 'https://liveheats.com/events/' + df['event_id'].astype(str)
    df['source']         = 'live heats'
    df['elimination_name'] = df['division_name']
    df['elimination_id']   = df['division_id']
    
    # 8) standardise event name via your function
    df['standard_event_name'] = df.apply(standardise_event_name, axis=1)
    
    # 9) parse & format dates
    df['start_date'] = pd.to_datetime(df['start_date'], dayfirst=True, errors='coerce')
    df['year']       = df['start_date'].dt.year
    df['start_date'] = df['start_date'].dt.strftime('%Y-%m-%d')
    
    df['finish_date'] = pd.to_datetime(df['finish_date'], dayfirst=True, errors='coerce')
    df['finish_date'] = df['finish_date'].dt.strftime('%Y-%m-%d')
    
    # 10) write out
    df.to_csv(output_csv, index=False)

