import os
import requests
import json
import pandas as pd

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
        # Save raw data for reference
        with open("wave_tour_events_raw.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        print("Raw event data successfully saved to wave_tour_events_raw.json")
        
        # Extract list of events and normalize the JSON structure
        events = data["data"]["organisationByShortName"]["events"]
        df = pd.json_normalize(events)
        
        # Select only the required columns
        columns_needed = ["id", "name", "status", "date", "daysWindow"]
        df = df[columns_needed]
        
        return df
    else:
        print(f"Error fetching data: {response.status_code}, {response.text}")
        return pd.DataFrame()

def compare_and_update_events(new_df, current_csv="wave_tour_events.csv"):
    # Check if the current CSV exists; if not, treat all events as new.
    if os.path.exists(current_csv):
        old_df = pd.read_csv(current_csv)
    else:
        print("Current events file not found. All events will be treated as new.")
        old_df = pd.DataFrame()

    # Ensure event IDs are treated as strings for consistent comparison.
    new_df["id"] = new_df["id"].astype(str)
    if not old_df.empty:
        old_df["id"] = old_df["id"].astype(str)
    
    # Create a lookup dictionary for existing events using 'id' as key.
    old_events = old_df.set_index("id").to_dict("index") if not old_df.empty else {}

    # Add a new column 'Updates' to track changes.
    new_df["Updates"] = ""

    changes_found = False

    # Iterate through the new events to check for updates.
    for idx, new_row in new_df.iterrows():
        event_id = new_row["id"]
        if event_id not in old_events:
            # Flag new events.
            new_df.at[idx, "Updates"] = "New Event Added"
            print(f"New event found: {new_row['name']} (ID: {event_id}) -> New Event Added")
            changes_found = True
        else:
            # Compare the status field.
            old_status = old_events[event_id].get("status")
            new_status = new_row["status"]
            if old_status != new_status:
                update_text = f"{old_status} -> {new_status}"
                new_df.at[idx, "Updates"] = update_text
                print(f"Status changed for event '{new_row['name']}' (ID: {event_id}): {update_text}")
                changes_found = True

    # Optionally: Check for events that are in the old data but missing in the new data.
    if not old_df.empty:
        new_ids = set(new_df["id"])
        for event_id, details in old_events.items():
            if event_id not in new_ids:
                print(f"Event missing in latest data (might have been removed): {details.get('name')} (ID: {event_id})")
                # You can decide to handle missing events as needed.

    # If any changes were detected or the file did not exist, update the CSV.
    if changes_found or old_df.empty:
        new_df.to_csv(current_csv, index=False)
        print(f"Changes detected. '{current_csv}' has been updated.")
    else:
        print("No changes detected.")

def main():
    # Fetch the latest events from the API.
    new_events_df = fetch_wave_tour_events()
    
    # If new data was successfully fetched, compare it with the existing CSV.
    if not new_events_df.empty:
        compare_and_update_events(new_events_df)
    else:
        print("No new data fetched. Exiting.")

if __name__ == "__main__":
    main()
