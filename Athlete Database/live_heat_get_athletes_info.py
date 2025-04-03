import requests
import json
import os
import time

########## GET ALL EVENT DATA FROM 'WORLD WAVE TOUR' ON LIVE HEATS ############
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


########## Extract event IDs and go and get division information ############

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
    
    # Convert results to a DataFrame
    df = pd.DataFrame(filtered_events)
    print(df)  # Display results in the console
    
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


# Define the base URL for the GraphQL API
GRAPHQL_URL = "https://liveheats.com/api/graphql"

# Define the directory where results will be saved
OUTPUT_DIR = "iwt_athletes"

# Ensure the output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def fetch_event_division_results(event_id, division_id):
    """
    Fetches athlete details for competitors in a specific event division,
    extracts a unique list of athletes, and saves the data as a JSON file.
    
    :param event_id: The ID of the event.
    :param division_id: The ID of the event division.
    """
    query = """
    query getAthleteInfo($id: ID!) {
      eventDivision(id: $id) {
        heats {
          competitors {
            athlete {
              id
              name
              image
              dob
              nationality
            }
          }
        }
      }
    }
    """

    variables = {"id": str(division_id)}

    response = requests.post(GRAPHQL_URL, json={"query": query, "variables": variables})

    if response.status_code == 200:
        data = response.json()
        
        # Extract unique athletes by iterating through each heat and competitor.
        unique_athletes = {}
        for heat in data["data"]["eventDivision"]["heats"]:
            for competitor in heat["competitors"]:
                athlete = competitor["athlete"]
                unique_athletes[athlete["id"]] = athlete  # Uses athlete id as key to avoid duplicates
        
        # Convert the unique athletes dictionary into a list.
        unique_athlete_list = list(unique_athletes.values())

        file_name = f"{OUTPUT_DIR}/event_{event_id}_division_{division_id}_unique_athletes.json"
        with open(file_name, "w", encoding="utf-8") as file:
            json.dump(unique_athlete_list, file, indent=4, ensure_ascii=False)
        
        print(f"Saved unique athlete details for Event {event_id}, Division {division_id} -> {file_name}")
    else:
        print(f"Failed to fetch data for Event {event_id}, Division {division_id}. HTTP {response.status_code}")
        print(response.text)




# Instead of using hard-coded pairs, extract event IDs from the saved events file
event_ids = extract_results_published_events("wave_tour_events.json")

# Loop through each event, then fetch and process all divisions for that event
for event_id in event_ids:
    division_ids = fetch_event_divisions(event_id)
    for division_id in division_ids:
        fetch_event_division_results(event_id, division_id)
        time.sleep(1)  # Adding a delay to prevent rate limits


def create_unique_athletes_from_directory(directory):
    """
    Loops through all JSON files in the specified directory, extracts athlete records,
    and returns a list of unique athletes based on their 'id'.

    :param directory: Directory path containing athlete JSON files.
    :return: A list of unique athlete dictionaries.
    """
    unique_athletes = {}

    # Loop over all files in the directory
    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            file_path = os.path.join(directory, filename)
            with open(file_path, "r", encoding="utf-8") as file:
                try:
                    data = json.load(file)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON from {file_path}: {e}")
                    continue

                # Assuming each file contains a list of athlete dictionaries
                for athlete in data:
                    # Use athlete id as the key for uniqueness.
                    unique_athletes[athlete["id"]] = athlete

    return list(unique_athletes.values())

if __name__ == "__main__":
    directory = "iwt_athletes"
    unique_athletes = create_unique_athletes_from_directory(directory)
    output_file = "unique_athletes.json"
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(unique_athletes, f, indent=4, ensure_ascii=False)
    
    print(f"Saved {len(unique_athletes)} unique athletes to {output_file}")
