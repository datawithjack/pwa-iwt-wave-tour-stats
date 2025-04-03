import requests
import json
import os
import time
import pandas as pd

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
    output_file = "Athlete Database/Raw Data/iwt_sailors_raw.json"
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(unique_athletes, f, indent=4, ensure_ascii=False)
    
    print(f"Saved {len(unique_athletes)} unique athletes to {output_file}")


#### CREATE RAW CSV

import csv

def json_to_csv(json_file, csv_file):
    # Load JSON data using UTF-8 encoding
    with open(json_file, 'r', encoding='utf-8') as jf:
        data = json.load(jf)

    # Open CSV file for writing with utf-8-sig encoding to handle special characters properly
    with open(csv_file, 'w', newline='', encoding='utf-8-sig') as cf:
        writer = csv.writer(cf)
        # Write header row with "iwt_" prefix for each column
        writer.writerow(["iwt_id", "iwt_name", "iwt_image", "iwt_dob", "iwt_nationality", "iwt_yob"])
        
        # Process each record in the JSON data
        for item in data:
            dob = item.get("dob")
            # Extract the year from dob if available, otherwise leave empty
            yob = dob.split('-')[0] if dob else ""
            writer.writerow([
                item.get("id"),
                item.get("name"),
                item.get("image"),
                dob,
                item.get("nationality"),
                yob
            ])

if __name__ == "__main__":
    json_to_csv("Athlete Database/Raw Data/iwt_sailors_raw.json", "Athlete Database/Raw Data/iwt_sailors_raw.csv")


##### PERFORM SOME ADDITONAL DATA CLEANING AND CREATE CLEAN CSV
## ADDITIONAL DATA CLEANING FOR DUPE RECORDS

# Read the CSV file
df = pd.read_csv('Athlete Database/Raw Data/iwt_sailors_raw.csv')

# 1. Convert the 'name' column to proper (title) case.
df['iwt_name'] = df['iwt_name'].str.title()

# (Optional) Count occurrences of each name
name_counts = df['iwt_name'].value_counts().reset_index()
name_counts.columns = ['iwt_name', 'count']
print("Name Occurrence Counts:")
print(name_counts)

# 2 & 3. Define a function to merge duplicate records based on name and country.
def merge_records(group):
    # For groups with only one record, just return the record with iwt_alt_id left as NaN.
    if len(group) == 1:
        group = group.copy()
        group['iwt_alt_id'] = pd.NA
        return group.iloc[0]
    else:
        # Calculate completeness: count of non-null fields in each row.
        completeness = group.notnull().sum(axis=1)
        # Identify the row with the minimum completeness (if tie, the first encountered is chosen)
        idx_min = completeness.idxmin()
        alt_id = group.loc[idx_min, 'iwt_id']
        
        # Create a merged record by taking the first non-null value for each column.
        merged = {}
        for col in group.columns:
            # For each column, choose the first non-null value from the group.
            non_nulls = group[col].dropna()
            merged[col] = non_nulls.iloc[0] if not non_nulls.empty else None
        # Add the new column iwt_alt_id with the chosen alt id.
        merged['iwt_alt_id'] = alt_id
        
        return pd.Series(merged)

# Group by both 'name' and 'country' then apply the merge_records function.
merged_df = df.groupby(['iwt_name', 'iwt_nationality'], as_index=False, dropna=False).apply(merge_records)

# Reset the index (if needed) and inspect the cleaned DataFrame.
merged_df.reset_index(drop=True, inplace=True)
print("Cleaned and Merged Data:")
print(merged_df.head())

# (Optional) Save the cleaned DataFrame to a new CSV file.
merged_df.to_csv('Athlete Database/Clean Data/iwt_sailors_clean.csv', index=False)
