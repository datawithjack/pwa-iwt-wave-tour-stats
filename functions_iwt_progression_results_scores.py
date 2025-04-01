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

    :param event_id: The event identifier as a string.
    :param division_id: The division identifier as a string.
    :return: The JSON data as a Python dict, or None if the fetch failed.
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
    The output CSV file is named 'iwt_heat_progression_format.csv'.
    Adds a new column 'sex' with the division name from the JSON.
    
    :param data: The JSON data (dict) obtained from the API.
    :param event_id: The event identifier as a string.
    :param division_id: The division identifier as a string.
    """
    import pandas as pd
    import copy

    # Extract the division name from JSON and assign it to the new 'sex' column.
    division_name = data['data']['eventDivision']['division']['name']

    # Extract relevant fields from JSON
    original_heats = data['data']['eventDivision']['heats']
    progression_dict = data['data']['eventDivision']['formatDefinition']['progression']

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

        # Rename "id" to "heat_id"
        heat['heat_id'] = heat.pop('id', None)

        # Remove the "result" field (not needed for progression export)
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

    # Export to CSV
    csv_filename = 'iwt_heat_progression_format.csv'
    df_progression.to_csv(csv_filename, index=False)
    print(f"Exported {csv_filename}")

def flatten_heat_results_and_scores(data, event_id, division_id):
    """
    Processes the JSON data to export heat results and heat scores.
    Two CSV files are created: 'iwt_heat_results.csv' and 'iwt_heat_scores.csv'.
    
    After creating the scores DataFrame, we also:
      - Use 'Jumps' instead of 'Jump' in the filter condition for total_jump.
      - Rename columns: ride_total->score, category->type, scoring_ride->counting.
      - Replace 'Jumps'->'Jump' and 'Waves'->'Wave' in the new 'type' column.
    """
    original_heats = data['data']['eventDivision']['heats']
    results_rows = []
    scores_rows = []

    for heat in original_heats:
        heat_id = heat.get('id')
        eventDivisionId = heat.get('eventDivisionId')
        
        # Process each result entry for the heat
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
                    'place': res.get('place')
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

    # -------------------------------
    # 1) Create and export heat results
    # -------------------------------
    df_results = pd.DataFrame(results_rows, columns=[
        'event_id', 'heat_id', 'eventDivisionId', 'athleteId', 
        'result_total', 'winBy', 'needs', 'place'
    ])
    df_results.insert(0, 'source', 'Live Heats')  # Insert 'source' at start
    csv_results = "iwt_heat_results.csv"
    df_results.to_csv(csv_results, index=False)
    print(f"Exported {csv_results}")

    # -------------------------------
    # 2) Create and process heat scores
    # -------------------------------
    df_scores = pd.DataFrame(scores_rows, columns=[
        'event_id', 'heat_id', 'eventDivisionId', 'athleteId', 
        'ride_total', 'modified_total', 'modifier', 'category', 'scoring_ride'
    ])

    # Calculate total_wave: sum of ride_total where category="Waves" and scoring_ride=True
    grouped_wave = (
        df_scores[(df_scores['category'] == "Waves") & (df_scores['scoring_ride'] == True)]
        .groupby(['heat_id', 'athleteId'])['ride_total']
        .sum()
        .reset_index()
        .rename(columns={'ride_total': 'total_wave'})
    )

    # Calculate total_jump: sum of ride_total where category="Jumps" and scoring_ride=True
    grouped_jump = (
        df_scores[(df_scores['category'] == "Jumps") & (df_scores['scoring_ride'] == True)]
        .groupby(['heat_id', 'athleteId'])['ride_total']
        .sum()
        .reset_index()
        .rename(columns={'ride_total': 'total_jump'})
    )

    # Calculate total_points: sum of ride_total where scoring_ride=True (all categories)
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

    # Replace NaN with 0 for total_wave, total_jump, total_points
    df_scores[['total_wave', 'total_jump', 'total_points']] = df_scores[
        ['total_wave', 'total_jump', 'total_points']
    ].fillna(0)

    # Insert 'source' column at the beginning
    df_scores.insert(0, 'source', 'Live Heats')

    # -------------------------------
    # 3) Rename columns per your request
    #    ride_total -> score
    #    category   -> type
    #    scoring_ride -> counting
    # -------------------------------
    df_scores.rename(columns={
        'ride_total': 'score',
        'category': 'type',
        'scoring_ride': 'counting'
    }, inplace=True)

    # -------------------------------
    # 4) Replace 'Jumps' -> 'Jump' and 'Waves' -> 'Wave' in the new 'type' column
    # -------------------------------
    df_scores['type'] = df_scores['type'].replace({
        'Jumps': 'Jump',
        'Waves': 'Wave'
    })

    # -------------------------------
    # 5) Reorder columns before exporting
    # -------------------------------
    desired_scores_columns = [
        'source', 'event_id', 'heat_id', 'eventDivisionId', 'athleteId',
        'score', 'modified_total', 'modifier', 'type', 'counting',
        'total_wave', 'total_jump', 'total_points'
    ]
    df_scores = df_scores.reindex(columns=desired_scores_columns)

    # Export iwt_heat_scores.csv
    csv_scores = "iwt_heat_scores.csv"
    df_scores.to_csv(csv_scores, index=False)
    print(f"Exported {csv_scores}")

def process_event_division(event_id, division_id):
    """
    Main pipeline function that fetches event division data and then processes
    the data to generate three CSV exports: iwt_heat_progression_format.csv,
    iwt_heat_results.csv, and iwt_heat_scores.csv.
    """
    data = fetch_event_division_results(event_id, division_id)
    if data:
        flatten_heat_progression(data, event_id, division_id)
        flatten_heat_results_and_scores(data, event_id, division_id)
    else:
        print("No data fetched, process terminated.")

# Example usage:
if __name__ == "__main__":
    event_id = "321863"
    division_id = "584936"
    process_event_division(event_id, division_id)
