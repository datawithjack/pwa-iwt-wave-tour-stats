import requests
import json
import os
import time
import copy
import pandas as pd
import re

# Constants
GRAPHQL_URL = "https://liveheats.com/api/graphql"
OUTPUT_DIR = "event_results"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# def fetch_wave_tour_events(short_name="WaveTour", output_file="wave_tour_events.json"):
#     """
#     Fetch all Wave Tour events and save to a JSON file.
#     """
#     headers = {"Content-Type": "application/json","User-Agent":"Mozilla/5.0"}
#     query = """query getOrganisationByShortName($shortName: String) {
#       organisationByShortName(shortName: $shortName) {
#         events {
#           id
#           name
#           status
#           date
#           daysWindow
#           hideFinals
#           series { id name }
#           currentScheduleIndex
#         }
#       }
#     }"""
#     payload = {"query": query, "variables": {"shortName": short_name}}
#     resp = requests.post(GRAPHQL_URL, headers=headers, json=payload)
#     resp.raise_for_status()
#     data = resp.json()
#     with open(output_file, "w", encoding="utf-8") as f:
#         json.dump(data, f, indent=4)
#     return data


def fetch_wave_tour_events():
    """
    Fetch events from the API.
    Saves raw JSON data to 'wave_tour_events_raw.json' for reference.
    Saves cleaned data to:
      - 'wave_tour_events_cleaned.csv'
      - 'wave_tour_events_cleaned.json'
    Returns a DataFrame with the required and newly formatted columns.
    """
    url = "https://liveheats.com/api/graphql"
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0"
    }
    
    query = """
    query getOrganisationByShortName($shortName: String) {
      organisationByShortName(shortName: $shortName) {
        events {
          id
          name
          status
          date
          daysWindow
          hideFinals
          series {
            id
            name
          }
          currentScheduleIndex
        }
      }
    }
    """
    variables = {"shortName": "WaveTour"}
    payload = {"query": query, "variables": variables}
    
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code != 200:
        raise RuntimeError(f"Error fetching data: {response.status_code}\n{response.text}")
    
    # 1) Save raw JSON
    data = response.json()
    with open("wave_tour_events_raw.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)
    print("✅ Raw event data saved to 'wave_tour_events_raw.json'")
    
    # 2) Normalize and select
    events = data["data"]["organisationByShortName"]["events"]
    df = pd.json_normalize(events)
    df = df[["id", "name", "status", "date", "daysWindow"]]
    
    # 3) Transformations
    #   a) start_date
    df["start_date"] = pd.to_datetime(df["date"]).dt.strftime("%d/%m/%Y")
    df.drop(columns="date", inplace=True)
    #   b) finish_date = start_date + daysWindow
    finish = (
        pd.to_datetime(df["start_date"], format="%d/%m/%Y")
        + pd.to_timedelta(df["daysWindow"], unit="D")
    )
    df["finish_date"] = finish.dt.strftime("%d/%m/%Y")
    #   c) location = text before ':' in name
    df["location"] = (
        df["name"]
        .str.split(":", n=1)
        .str[0]
        .str.strip()
        .str.title()
    )
    #   d) stars = number before 'star'
    df["stars"] = df["name"].apply(
        lambda x: re.search(r"(\d+)\s*star", x, re.IGNORECASE).group(1)
        if re.search(r"(\d+)\s*star", x, re.IGNORECASE)
        else None
    )
    #   e) clean status
    df["status"] = df["status"].str.replace("_", " ").str.title()
    
    
    
    rename_map = {
        "id":                  "event_id",
        "name":                "event_name",
        "status":              "results_status",
        "daysWindow":          "day_window",
        "start_date":          "start_date",
        "finish_date":         "finish_date",
        "location":            "location",
        "stars":               "stars"
    }
    df = df.rename(columns=rename_map)
    
    # 5) Save cleaned outputs
    df.to_csv("wave_tour_events_cleaned.csv", index=False)
    df.to_json("wave_tour_events_cleaned.json", orient="records", indent=4)
    print("✅ Cleaned data saved to 'wave_tour_events_cleaned.csv' and 'wave_tour_events_cleaned.json'")
    
    return df



def extract_results_published_events(file_path="wave_tour_events.json"):
    """
    Load wave tour events JSON and return list of event IDs with status 'results_published'.
    """
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    events = data["data"]["organisationByShortName"]["events"]
    return [e["id"] for e in events if e.get("status") == "results_published"]

def fetch_event_divisions(event_id):
    """
    Fetch division IDs and names for a given event.
    Returns a list of (division_id, division_name) tuples.
    """
    headers = {"Content-Type": "application/json","User-Agent": "Mozilla/5.0"}
    query = """
    query getEvent($id: ID!) {
      event(id: $id) {
        eventDivisions {
          id
          division { id name }
        }
      }
    }"""
    payload = {"query": query, "variables": {"id": event_id}}
    resp = requests.post(GRAPHQL_URL, headers=headers, json=payload)
    resp.raise_for_status()
    divisions = resp.json()["data"]["event"]["eventDivisions"]
    return [(d["id"], d["division"]["name"]) for d in divisions]

def fetch_event_division_results(event_id, division_id):
    """
    Fetch JSON for a specific event division and save to OUTPUT_DIR.
    """
    query = """query getEventDivision($id: ID!) {
      eventDivision(id: $id) {
        id
        heatDurationMinutes
        defaultEventDurationMinutes
        formatDefinition { progression runProgression heatSizes seeds defaultHeatDurationMinutes numberOfRounds }
        heatConfig { hasPriority totalCountingRides athleteRidesLimit }
        division { id name }
        heats {
          id eventDivisionId round roundPosition position startTime endTime heatDurationMinutes
          config { maxRideScore heatSize }
          result { athleteId total winBy needs rides place }
        }
      }
    }"""
    payload = {"query": query, "variables": {"id": division_id}}
    resp = requests.post(GRAPHQL_URL, json=payload)
    if resp.status_code != 200:
        print(f"Error fetching division {division_id}: {resp.status_code}")
        return None
    data = resp.json()
    file_name = os.path.join(OUTPUT_DIR, f"event_{event_id}_division_{division_id}.json")
    with open(file_name, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)
    return data

def flatten_heat_progression(data, event_id, division_id):
    try:
        ed = data["data"]["eventDivision"]
        prog = ed["formatDefinition"]["progression"]
        heats = ed["heats"]
        division_name = ed["division"]["name"]
    except (KeyError, TypeError):
        print(f"Skipping progression for {event_id}, {division_id}")
        return None
    records = []
    for heat in heats:
        rec = {
            'source': 'Live Heats',
            'event_id': event_id,
            'eventDivisionId': division_id,
            'sex': division_name,
            'round_name': heat.get('round'),
            'round_order': heat.get('roundPosition'),
            'heat_id': heat.get('id'),
            'heat_order': heat.get('position')
        }
        entries = prog.get(str(heat.get('roundPosition')), []) or prog.get('default', [])
        for i in range(2):
            if i < len(entries):
                e = entries[i]
                maxv = e.get('max')
                to_round = e.get('to_round') or (maxv + 1 if maxv else None)
                rec[f'progression_{i}_max'] = maxv
                rec[f'progression_{i}_to_round'] = to_round
            else:
                rec[f'progression_{i}_max'] = None
                rec[f'progression_{i}_to_round'] = None
        records.append(rec)
    df = pd.DataFrame(records)
    df.rename(columns={
        'progression_0_max': 'total_winners_progressing',
        'progression_0_to_round': 'winners_progressing_to_round_order',
        'progression_1_max': 'total_losers_progressing',
        'progression_1_to_round': 'losers_progressing_to_round_order'
    }, inplace=True)
    cols = [
        'source', 'event_id', 'eventDivisionId', 'sex',
        'round_name', 'round_order', 'heat_id', 'heat_order',
        'total_winners_progressing', 'winners_progressing_to_round_order',
        'total_losers_progressing', 'losers_progressing_to_round_order'
    ]
    return df[cols]

def flatten_heat_results_and_scores(data, event_id, division_id):
    try:
        heats = data['data']['eventDivision']['heats']
    except (KeyError, TypeError):
        print(f"Skipping results/scores for {event_id},{division_id}")
        return None, None
    results_rows = []
    scores_rows = []
    for heat in heats:
        hid = heat.get('id')
        edid = heat.get('eventDivisionId')
        rlabel = heat.get('round')
        rpos = heat.get('roundPosition', 0)
        for res in heat.get('result', []):
            base = {
                'source': 'Live Heats',
                'event_id': event_id,
                'heat_id': hid,
                'eventDivisionId': edid,
                'athleteId': res.get('athleteId'),
                'result_total': res.get('total'),
                'winBy': res.get('winBy'),
                'needs': res.get('needs'),
                'place': res.get('place'),
                'round': rlabel,
                'roundPosition': rpos
            }
            results_rows.append(base)
            rides = res.get('rides') or {}
            for ride_list in rides.values():
                for ride in ride_list:
                    scores_rows.append({
                        'source': 'Live Heats',
                        'event_id': event_id,
                        'heat_id': hid,
                        'eventDivisionId': edid,
                        'athleteId': res.get('athleteId'),
                        'score': ride.get('total'),
                        'modified_total': ride.get('modified_total'),
                        'modifier': ride.get('modifier'),
                        'type': ride.get('category').rstrip('s'),
                        'counting': ride.get('scoring_ride')
                    })
    df_res = pd.DataFrame(results_rows)[[
        'source','event_id','heat_id','eventDivisionId','athleteId',
        'result_total','winBy','needs','place','round','roundPosition'
    ]]
    df_scr = pd.DataFrame(scores_rows)
    # Calculate total_points
    summary = (
        df_scr[df_scr['counting']]
        .groupby(['heat_id','athleteId'])['score']
        .sum()
        .reset_index()
        .rename(columns={'score':'total_points'})
    )
    df_scr = pd.merge(df_scr, summary, on=['heat_id','athleteId'], how='left').fillna(0)
    cols = [
        'source','event_id','heat_id','eventDivisionId','athleteId',
        'score','modified_total','modifier','type','counting','total_points'
    ]
    return df_res, df_scr[cols]

def create_final_rank_no_heat_info(json_data, event_id, division_id):
    heats = json_data['data']['eventDivision']['heats']
    if not heats:
        return None
    rows = []
    for res in heats[0].get('result', []):
        place = int(res.get('place', 999)) if res.get('place') is not None else 999
        rows.append({
            'source': 'Live Heats',
            'event_id': event_id,
            'eventDivisionId': division_id,
            'athleteId': res.get('athleteId'),
            'place': place
        })
    if not rows:
        return None
    return pd.DataFrame(rows)

def calculate_final_rank_heat_info(df_results, event_id, division_id):
    athlete_best = {}
    for _, row in df_results.iterrows():
        aid = row['athleteId']
        rp = row.get('roundPosition', 0)
        pl = int(row.get('place', 999))
        stored = athlete_best.get(aid)
        if stored:
            if rp > stored[0] or (rp == stored[0] and pl < stored[1]):
                athlete_best[aid] = (rp, pl)
        else:
            athlete_best[aid] = (rp, pl)
    sorted_ath = sorted(athlete_best.items(), key=lambda x: (-x[1][0], x[1][1]))
    rows = []
    prev_key = None
    for i, (aid, (rp, pl)) in enumerate(sorted_ath):
        rank = 1 if i == 0 else (i + 1 if (rp, pl) != prev_key else rank)
        rows.append({
            'source': 'Live Heats',
            'event_id': event_id,
            'eventDivisionId': division_id,
            'athleteId': aid,
            'place': rank
        })
        prev_key = (rp, pl)
    if not rows:
        return None
    return pd.DataFrame(rows)

def is_no_heat_info(json_data):
    try:
        heats = json_data['data']['eventDivision']['heats']
        if not heats:
            return False
        for res in heats[0].get('result', []):
            if int(res.get('total', 0)) != int(res.get('place', 0)):
                return False
        return True
    except Exception:
        return False

def process_event_division(json_data, event_id, division_id):
    if is_no_heat_info(json_data):
        df_final = create_final_rank_no_heat_info(json_data, event_id, division_id)
        return {'df_final_rank': df_final}
    df_prog = flatten_heat_progression(json_data, event_id, division_id)
    df_res, df_scr = flatten_heat_results_and_scores(json_data, event_id, division_id)
    df_final = calculate_final_rank_heat_info(df_res, event_id, division_id) if df_res is not None else None
    return {
        'df_progression': df_prog,
        'df_results': df_res,
        'df_scores': df_scr,
        'df_final_rank': df_final
    }

def clean_heat_order(df, column='heat_order'):
    """
    Example adhoc cleaning: remove non-digits from heat_order and convert to Int.
    """
    df[column] = df[column].astype(str).str.replace(r"\D+", "", regex=True)
    df[column] = pd.to_numeric(df[column], errors='coerce').astype('Int64')
    return df
