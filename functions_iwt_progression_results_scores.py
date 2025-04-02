import requests
import json
import os
import time
import pandas as pd
import copy

# Constants for the API and output directory
GRAPHQL_URL = "https://liveheats.com/api/graphql"
OUTPUT_DIR = "event_results"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def fetch_event_division_results(event_id, division_id):
    """
    Fetches event division results from the GraphQL API for the given event and division IDs.
    Saves the resulting JSON to a file and returns the JSON data.
    """
    query = """
    query getEventDivision($id: ID!) {
      eventDivision(id: $id) {
        id
        heatDurationMinutes
        defaultEventDurationMinutes
        formatDefinition {
          progression
          runProgression
          heatSizes
          seeds
          defaultHeatDurationMinutes
          numberOfRounds
        }
        heatConfig {
          hasPriority
          totalCountingRides
          athleteRidesLimit
        }
        division {
          id
          name
        }
        eventDivisions {
          id
          division {
            id
            name
          }
        }
        heats {
          id
          eventDivisionId
          round
          roundPosition
          position
          startTime
          endTime
          heatDurationMinutes
          config {
            maxRideScore
            heatSize
          }
          result {
            athleteId
            total
            winBy
            needs
            rides
            place
          }
        }
        template {
          id
          name
        }
        eventDivisionPointAllocations {
          id
          eventDivisionId
          seasonId
          pointAllocation {
            id
            name
          }
        }
        event {
          id
        }
      }
    }
    """
    variables = {"id": str(division_id)}
    response = requests.post(GRAPHQL_URL, json={"query": query, "variables": variables})

    if response.status_code == 200:
        data = response.json()
        file_name = os.path.join(OUTPUT_DIR, f"event_{event_id}_division_{division_id}.json")
        with open(file_name, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)
        print(f"Saved results for Event {event_id}, Division {division_id} -> {file_name}")
        return data
    else:
        print(f"Failed to fetch data for Event {event_id}, Division {division_id}. HTTP {response.status_code}")
        print(response.text)
        return None


def flatten_heat_progression(data, event_id, division_id):
    """
    Processes the JSON data to flatten and export heat progression information.
    Returns a DataFrame with a new 'sex' column (the division name).
    """
    try:
        event_division = data['data']['eventDivision']
        division_name = event_division['division']['name']
        original_heats = event_division['heats']
        progression_dict = event_division['formatDefinition']['progression']
    except (KeyError, TypeError) as e:
        print(f"Skipping flatten_heat_progression for event {event_id} division {division_id}: missing data ({e})")
        return None

    if not original_heats or not progression_dict:
        print(f"Skipping flatten_heat_progression for event {event_id} division {division_id}: incomplete progression data.")
        return None

    # Make a deep copy so we can modify freely
    heats_progression = copy.deepcopy(original_heats)

    for heat in heats_progression:
        # Add event_id and division_id to each heat record
        heat['event_id'] = event_id
        heat['division_id'] = division_id

        # Process progression details based on roundPosition
        round_position = heat.get('roundPosition')
        key = str(round_position) if str(round_position) in progression_dict else 'default'
        progression_list = progression_dict.get(key, [])

        # Process up to two progression entries per heat
        for i in range(2):
            if i < len(progression_list):
                entry = progression_list[i]
                max_val = entry.get('max')
                to_round_val = entry.get('to_round')
                if to_round_val is None and max_val is not None:
                    to_round_val = max_val + 1
                heat[f'progression_{i}_max'] = max_val
                heat[f'progression_{i}_to_round'] = to_round_val
            else:
                heat[f'progression_{i}_max'] = None
                heat[f'progression_{i}_to_round'] = None

        # Rename "id" to "heat_id" and remove "result" field
        heat['heat_id'] = heat.pop('id', None)
        heat.pop('result', None)

    # Flatten the data
    df_progression = pd.json_normalize(heats_progression, sep='_')

    # Rename columns as needed
    rename_dict = {
        'round': 'round_name',
        'roundPosition': 'round_order',
        'position': 'heat_order',
        'progression_0_max': 'total_winners_progressing',
        'progression_0_to_round': 'winners_progressing_to_round_order',
        'progression_1_max': 'total_losers_progressing',
        'progression_1_to_round': 'losers_progressing_to_round_order'
    }
    df_progression.rename(columns=rename_dict, inplace=True)

    # Add the new 'sex' column with the division name
    df_progression['sex'] = division_name

    # Reorder columns
    desired_columns = [
        'event_id',
        'eventDivisionId',
        'sex',
        'round_name',
        'round_order',
        'heat_id',
        'heat_order',
        'total_winners_progressing',
        'winners_progressing_to_round_order',
        'total_losers_progressing',
        'losers_progressing_to_round_order'
    ]
    df_progression = df_progression.reindex(columns=desired_columns)

    # Insert the new "source" column at the very start
    df_progression.insert(0, 'source', 'Live Heats')

    return df_progression


def flatten_heat_results_and_scores(data, event_id, division_id):
    """
    Processes the JSON data to create heat results and heat scores DataFrames.
    Returns a tuple (df_results, df_scores). This function now also includes the
    heat's "round" and "roundPosition" in the results.
    """
    try:
        original_heats = data['data']['eventDivision']['heats']
    except (KeyError, TypeError) as e:
        print(f"Skipping flatten_heat_results_and_scores for event {event_id} division {division_id}: missing heats data ({e})")
        return None, None

    if not original_heats:
        print(f"Skipping flatten_heat_results_and_scores for event {event_id} division {division_id}: no heats found.")
        return None, None

    results_rows = []
    scores_rows = []

    for heat in original_heats:
        heat_id = heat.get('id')
        eventDivisionId = heat.get('eventDivisionId')
        round_label = heat.get('round')
        round_position = heat.get('roundPosition', 0)

        if 'result' in heat and heat['result']:
            for res in heat['result']:
                base = {
                    'event_id': event_id,
                    'heat_id': heat_id,
                    'eventDivisionId': eventDivisionId,
                    'athleteId': res.get('athleteId'),
                    'result_total': res.get('total'),
                    'winBy': res.get('winBy'),
                    'needs': res.get('needs'),
                    'place': res.get('place'),
                    'round': round_label,
                    'roundPosition': round_position
                }
                results_rows.append(base)

                # Process ride information if available
                rides = res.get('rides')
                if rides and isinstance(rides, dict):
                    for ride_list in rides.values():
                        for ride in ride_list:
                            ride_row = {
                                'event_id': event_id,
                                'eventDivisionId': eventDivisionId,
                                'heat_id': heat_id,
                                'athleteId': res.get('athleteId'),
                                'ride_total': ride.get('total'),
                                'modified_total': ride.get('modified_total'),
                                'modifier': ride.get('modifier'),
                                'category': ride.get('category'),
                                'scoring_ride': ride.get('scoring_ride')
                            }
                            scores_rows.append(ride_row)
        else:
            continue

    # Create the heat results DataFrame
    df_results = pd.DataFrame(results_rows, columns=[
        'event_id', 'heat_id', 'eventDivisionId', 'athleteId',
        'result_total', 'winBy', 'needs', 'place', 'round', 'roundPosition'
    ])
    df_results.insert(0, 'source', 'Live Heats')

    # Create the heat scores DataFrame
    df_scores = pd.DataFrame(scores_rows, columns=[
        'event_id', 'heat_id', 'eventDivisionId', 'athleteId',
        'ride_total', 'modified_total', 'modifier', 'category', 'scoring_ride'
    ])

    # Calculate totals for Waves, Jumps, and all scoring rides
    grouped_wave = (
        df_scores[(df_scores['category'] == "Waves") & (df_scores['scoring_ride'] == True)]
        .groupby(['heat_id', 'athleteId'])['ride_total']
        .sum()
        .reset_index()
        .rename(columns={'ride_total': 'total_wave'})
    )
    grouped_jump = (
        df_scores[(df_scores['category'] == "Jumps") & (df_scores['scoring_ride'] == True)]
        .groupby(['heat_id', 'athleteId'])['ride_total']
        .sum()
        .reset_index()
        .rename(columns={'ride_total': 'total_jump'})
    )
    grouped_points = (
        df_scores[df_scores['scoring_ride'] == True]
        .groupby(['heat_id', 'athleteId'])['ride_total']
        .sum()
        .reset_index()
        .rename(columns={'ride_total': 'total_points'})
    )

    # Merge the calculated sums back into df_scores
    df_scores = pd.merge(df_scores, grouped_wave, on=['heat_id', 'athleteId'], how='left')
    df_scores = pd.merge(df_scores, grouped_jump, on=['heat_id', 'athleteId'], how='left')
    df_scores = pd.merge(df_scores, grouped_points, on=['heat_id', 'athleteId'], how='left')
    df_scores[['total_wave', 'total_jump', 'total_points']] = df_scores[
        ['total_wave', 'total_jump', 'total_points']
    ].fillna(0)

    # Insert 'source' column if not already present
    if 'source' not in df_scores.columns:
        df_scores.insert(0, 'source', 'Live Heats')

    # Rename columns as requested
    df_scores.rename(columns={
        'ride_total': 'score',
        'category': 'type',
        'scoring_ride': 'counting'
    }, inplace=True)

    # Replace 'Jumps' with 'Jump' and 'Waves' with 'Wave' in the type column
    df_scores['type'] = df_scores['type'].replace({
        'Jumps': 'Jump',
        'Waves': 'Wave'
    })

    # Reorder columns
    desired_scores_columns = [
        'source', 'event_id', 'heat_id', 'eventDivisionId', 'athleteId',
        'score', 'modified_total', 'modifier', 'type', 'counting',
        'total_wave', 'total_jump', 'total_points'
    ]
    df_scores = df_scores.reindex(columns=desired_scores_columns)

    return df_results, df_scores


def create_final_rank_no_heat_info(json_data, event_id, division_id):
    """
    Creates a final rank DataFrame (with athleteId and place)
    from a JSON that contains only one heat (no detailed heat info).
    Additional columns "source", "event_id", and "eventDivisionId" are added.
    If no rank/place results are found, returns None.
    """
    try:
        heats = json_data["data"]["eventDivision"]["heats"]
        if heats and len(heats) > 0:
            results = heats[0].get("result", [])
        else:
            results = []
    except Exception as e:
        print("Error accessing heats:", e)
        results = []

    ranking = []
    for res in results:
        athlete = res.get("athleteId")
        try:
            place = int(res.get("place", 999))
        except Exception as ex:
            place = 999
        if athlete is not None:
            ranking.append({"athleteId": athlete, "place": place})
    
    if not ranking:
        print(f"No rank/place results for event {event_id}, division {division_id}. Skipping final ranking.")
        return None

    df_final_rank = pd.DataFrame(ranking, columns=["athleteId", "place"])
    df_final_rank["source"] = "Live Heats"
    df_final_rank["event_id"] = event_id
    df_final_rank["eventDivisionId"] = division_id
    df_final_rank = df_final_rank[["source", "event_id", "eventDivisionId", "athleteId", "place"]]
    return df_final_rank


def calculate_final_rank_heat_info(df_results, event_id, division_id):
    """
    Calculates the final ranking DataFrame from the detailed heat results DataFrame.
    Uses each athlete's best performance determined by a higher roundPosition and a lower place.
    Returns a DataFrame with athleteId and overall final rank (place),
    plus extra columns "source", "event_id", and "eventDivisionId".
    If no ranking information is found, returns None.
    """
    athlete_best = {}
    for idx, row in df_results.iterrows():
        athlete = row['athleteId']
        round_position = row.get("roundPosition", 0)
        try:
            place = int(row.get("place", 999))
        except Exception:
            place = 999

        if athlete in athlete_best:
            stored = athlete_best[athlete]
            if round_position > stored["roundPosition"] or (round_position == stored["roundPosition"] and place < stored["place"]):
                athlete_best[athlete] = {"roundPosition": round_position, "place": place}
        else:
            athlete_best[athlete] = {"roundPosition": round_position, "place": place}

    sorted_athletes = sorted(athlete_best.items(), key=lambda item: (-item[1]["roundPosition"], item[1]["place"]))
    
    if not sorted_athletes:
        print(f"No ranking information for event {event_id}, division {division_id}.")
        return None

    final_ranking = []
    current_rank = 0
    prev_key = None

    for i, (athlete, info) in enumerate(sorted_athletes):
        current_key = (info["roundPosition"], info["place"])
        if i == 0:
            current_rank = 1
        else:
            if current_key != prev_key:
                current_rank = i + 1
        final_ranking.append({"athleteId": athlete, "place": current_rank})
        prev_key = current_key

    if not final_ranking:
        print(f"No final ranking generated for event {event_id}, division {division_id}.")
        return None

    df_final_rank = pd.DataFrame(final_ranking)
    df_final_rank["source"] = "Live Heats"
    df_final_rank["event_id"] = event_id
    df_final_rank["eventDivisionId"] = division_id
    df_final_rank = df_final_rank[["source", "event_id", "eventDivisionId", "athleteId", "place"]]
    return df_final_rank


def is_no_heat_info(json_data):
    """
    Determines whether the JSON is in the no-heat-info format.
    Checks if, for each result in the first heat, the following hold true:
      - Converting "total" to int equals the integer value of "place".
      - If rides are present, the first ride's "total" (converted to int) also equals that place.
    """
    try:
        heats = json_data["data"]["eventDivision"]["heats"]
        if not heats:
            return False
        first_heat = heats[0]
        results = first_heat.get("result", [])
        if not results:
            return False
        for res in results:
            total_val = res.get("total")
            place_val = res.get("place")
            if total_val is None or place_val is None:
                return False
            try:
                place_int = int(place_val)
            except Exception:
                return False
            if int(total_val) != place_int:
                return False
            rides = res.get("rides")
            if rides and isinstance(rides, dict):
                first_key = next(iter(rides))
                ride_list = rides[first_key]
                if ride_list and len(ride_list) > 0:
                    ride_total = ride_list[0].get("total")
                    if ride_total is None or int(ride_total) != place_int:
                        return False
        return True
    except Exception as e:
        print("Error in is_no_heat_info:", e)
        return False


def process_event_division(json_data, event_id, division_id):
    """
    Processes the event division JSON.
    - If the JSON is detected as no-heat-info (using is_no_heat_info()), only create the final rank DataFrame.
    - Otherwise (heat info present), run all processing functions and then compute the overall final rank.
    """
    if is_no_heat_info(json_data):
        df_final_rank = create_final_rank_no_heat_info(json_data, event_id, division_id)
        return {"df_final_rank": df_final_rank}
    else:
        df_progression = flatten_heat_progression(json_data, event_id, division_id)
        df_results, df_scores = flatten_heat_results_and_scores(json_data, event_id, division_id)
        
        if df_results is not None and "roundPosition" not in df_results.columns:
            df_results["roundPosition"] = 0

        if df_results is not None:
            df_final_rank = calculate_final_rank_heat_info(df_results, event_id, division_id)
        else:
            print(f"Skipping final rank calculation for event {event_id} division {division_id} due to missing heat results data.")
            df_final_rank = None

        return {
            "df_progression": df_progression,
            "df_results": df_results,
            "df_scores": df_scores,
            "df_final_rank": df_final_rank
        }
