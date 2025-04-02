########## GET ALL EVENT DATA FROM 'WORLD WAVE TOUR' ON LIVE HEATS ############

import requests
import json

def fetch_wave_tour_events():
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
    
    if response.status_code == 200:
        data = response.json()
        with open("wave_tour_events.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        print("Data successfully saved to wave_tour_events.json")
    else:
        print(f"Error: {response.status_code}, {response.text}")

if __name__ == "__main__":
    fetch_wave_tour_events()

################# Extract event IDs and get division information #################

import pandas as pd

def extract_results_published_events(file_path):
    # Load the JSON file
    with open(file_path, "r", encoding="utf-8") as file:
        data = json.load(file)
    
    # Extract events with status "results_published"
    events = data["data"]["organisationByShortName"]["events"]
    filtered_events = [event for event in events if event["status"] == "results_published"]
    
    # Extract event IDs
    event_ids = [event["id"] for event in filtered_events]
    
    # Convert results to a DataFrame for review (optional)
    df = pd.DataFrame(filtered_events)
    print(df)
    
    return event_ids

def fetch_event_divisions(event_id):
    url = "https://liveheats.com/api/graphql"
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0"
    }
    
    query = """
    query getEvent($id: ID!) {
      event(id: $id) {
        eventDivisions {
          id
        }
      }
    }
    """
    
    payload = {"query": query, "variables": {"id": event_id}}
    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 200:
        data = response.json()
        event_division_ids = [div["id"] for div in data["data"]["event"]["eventDivisions"]]
        return event_division_ids
    else:
        print(f"Error fetching event {event_id}: {response.status_code}, {response.text}")
        return []

################# Process Event Divisions and Combine CSVs #################

import os
import time
import pandas as pd
import copy

# Import functions from your IWT module
from functions_iwt_progression_results_scores import (
    fetch_event_division_results,
    flatten_heat_progression,
    flatten_heat_results_and_scores,
    process_event_division
)

# Lists to collect DataFrames for each CSV type across all event divisions
progression_dfs = []
results_dfs = []
scores_dfs = []
final_rank_dfs = []

# Extract event IDs from the wave tour events JSON
event_ids = extract_results_published_events("wave_tour_events.json")  # returns a list of event IDs

# Loop through each event and its divisions
for event_id in event_ids:
    division_ids = fetch_event_divisions(event_id)
    for division_id in division_ids:
        print(f"Processing Event {event_id}, Division {division_id}...")
        # Fetch the event division data (this also saves a JSON file for reference)
        data = fetch_event_division_results(event_id, division_id)
        if data:
            # Process heat progression and capture the DataFrame
            df_progression = flatten_heat_progression(data, event_id, division_id)
            if df_progression is not None:
                progression_dfs.append(df_progression)
            
            # Process heat results and scores and capture both DataFrames
            df_results, df_scores = flatten_heat_results_and_scores(data, event_id, division_id)
            if df_results is not None:
                results_dfs.append(df_results)
            if df_scores is not None:
                scores_dfs.append(df_scores)
            
            # Process final ranking using the complete pipeline.
            # This will internally decide (using is_no_heat_info) which method to use.
            event_data = process_event_division(data, event_id, division_id)
            df_final_rank = event_data.get("df_final_rank")
            if df_final_rank is not None:
                final_rank_dfs.append(df_final_rank)
        else:
            print(f"Skipping Event {event_id}, Division {division_id} due to missing data.")

# Combine and export CSVs for progression, results, and scores
if progression_dfs:
    combined_progression = pd.concat(progression_dfs, ignore_index=True)
    combined_progression.to_csv("combined_iwt_heat_progression_format.csv", index=False)
    print("Exported combined_iwt_heat_progression_format.csv")

if results_dfs:
    combined_results = pd.concat(results_dfs, ignore_index=True)
    combined_results.to_csv("combined_iwt_heat_results.csv", index=False)
    print("Exported combined_iwt_heat_results.csv")

if scores_dfs:
    combined_scores = pd.concat(scores_dfs, ignore_index=True)
    combined_scores.to_csv("combined_iwt_heat_scores.csv", index=False)
    print("Exported combined_iwt_heat_scores.csv")

# Finally, combine all the final ranking DataFrames and export to CSV.
if final_rank_dfs:
    combined_final_rank = pd.concat(final_rank_dfs, ignore_index=True)
    combined_final_rank.to_csv("combined_iwt_final_ranks.csv", index=False)
    print("Exported combined_iwt_final_ranks.csv")
else:
    print("No final rank data available to export.")
