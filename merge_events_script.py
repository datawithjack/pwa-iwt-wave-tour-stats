import os
import requests
import json
import pandas as pd
import re
from sqlalchemy import create_engine, MetaData, Table, Column, BigInteger, Text
from sqlalchemy.dialects.mysql import insert

# ------------------------------
# Database Configuration & Setup
# ------------------------------

db_config = {
    'user': os.getenv('DB_USER', 'avnadmin').strip(),
    'password': os.getenv('DB_PASSWORD', 'AVNS_ND3WBdcIqIQvtuWr2ka').strip(),
    'host': os.getenv('DB_HOST', 'mysql-world-wave-tour-database-pwa-iwt-windsurf-stats.l.aivencloud.com').strip(),
    'port': os.getenv('DB_PORT', '28343').strip(),
    'database': os.getenv('DB_DATABASE', 'defaultdb').strip()
}

# Build the connection string; if SSL is required, include the ssl_ca parameter.
ssl_ca = os.getenv('MYSQL_SSL_CA')
ssl_args = f"?ssl_ca={ssl_ca}" if ssl_ca else ""

engine = create_engine(
    f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}{ssl_args}"
)

# Create a metadata instance and define the table with the new columns
metadata = MetaData()
all_events = Table(
    'ALL_EVENTS',
    metadata,
    Column('id', BigInteger, primary_key=True),
    Column('name', Text),
    Column('status', Text),
    Column('start_date', Text),  # renamed from date
    Column('finish_date', Text),
    Column('daysWindow', BigInteger),
    Column('Updates', Text),
    Column('location', Text),
    Column('stars', Text)
)

# Create the table in the database if it does not exist
metadata.create_all(engine)

# ------------------------------
# API Fetch Function
# ------------------------------
def fetch_wave_tour_events():
    """
    Fetch events from the API.
    Saves raw JSON data to 'wave_tour_events_raw.json' for reference.
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
    
    if response.status_code == 200:
        data = response.json()
        # Save raw data for reference
        with open("wave_tour_events_raw.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        print("Raw event data successfully saved to wave_tour_events_raw.json")
        
        # Extract list of events and normalize the JSON structure
        events = data["data"]["organisationByShortName"]["events"]
        df = pd.json_normalize(events)
        
        # Select only the required columns from the API response
        columns_needed = ["id", "name", "status", "date", "daysWindow"]
        df = df[columns_needed]
        
        # ------------------------------
        # Data Transformations
        # ------------------------------
        # 1. Format the date column into dd/mm/yyyy and rename to start_date.
        df['start_date'] = pd.to_datetime(df['date']).dt.strftime('%d/%m/%Y')
        df.drop(columns=['date'], inplace=True)
        
        # 2. Create finish_date = start_date + daysWindow (in days)
        finish_dt = pd.to_datetime(df['start_date'], format='%d/%m/%Y') + pd.to_timedelta(df['daysWindow'], unit='D')
        df['finish_date'] = finish_dt.dt.strftime('%d/%m/%Y')
        
        # 3. Create location column from the name (first part up to the colon) and convert to proper case.
        df['location'] = df['name'].str.split(':').str[0].str.strip().str.title()
        
        # 4. Create stars column by extracting the number preceding the word "star" in the name column.
        df['stars'] = df['name'].apply(
            lambda x: re.search(r'(\d+)\s*star', x, re.IGNORECASE).group(1) if re.search(r'(\d+)\s*star', x, re.IGNORECASE) else None
        )
        
        # 5. Clean the status column by replacing underscores with spaces and converting to proper case.
        df['status'] = df['status'].str.replace('_', ' ').str.title()
        
        return df
    else:
        print(f"Error fetching data: {response.status_code}, {response.text}")
        return pd.DataFrame()

# ------------------------------
# Comparison Function using ALL_EVENTS table
# ------------------------------
def compare_and_update_events_db(new_df, engine):
    """
    Compare newly fetched events with existing events in the ALL_EVENTS database table.
    Marks new events or events whose status has changed in the 'Updates' column.
    Returns the updated DataFrame and a flag indicating if changes were found.
    """
    try:
        # Query the current data from the ALL_EVENTS table
        old_df = pd.read_sql("SELECT * FROM ALL_EVENTS", engine)
        print("Current contents of the ALL_EVENTS table:")
        print(old_df)
    except Exception as e:
        print("An error occurred while fetching the table data:")
        print(e)
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

    return new_df, changes_found

# ------------------------------
# Upsert Function for MySQL
# ------------------------------
def upsert_all_events(engine, table, df):
    """
    Upsert events into the MySQL database table.
    For each row, if the event id exists it will update the record,
    otherwise it will insert a new row.
    """
    with engine.begin() as conn:  # This begins a transaction that commits on exit.
        for idx, row in df.iterrows():
            data = row.to_dict()
            # Convert id to integer since the DB table expects a BigInteger
            try:
                data['id'] = int(data['id'])
            except Exception as e:
                print(f"Error converting id {data['id']} to integer: {e}")
                continue
            stmt = insert(table).values(**data)
            # Exclude 'id' from the update values
            update_dict = {c.name: stmt.inserted[c.name] for c in table.c if c.name != 'id'}
            upsert_stmt = stmt.on_duplicate_key_update(**update_dict)
            conn.execute(upsert_stmt)

# ------------------------------
# Main Function
# ------------------------------
def main():
    # 1. Fetch the latest events from the API.
    new_events_df = fetch_wave_tour_events()
    if new_events_df.empty:
        print("No new data fetched. Exiting.")
        return

    # 2. Compare with existing events in the ALL_EVENTS table and update 'Updates' field if necessary.
    updated_events_df, changes_found = compare_and_update_events_db(new_events_df, engine)
    
    # 3. Upsert the latest events data into the MySQL database.
    upsert_all_events(engine, all_events, updated_events_df)
    print("Database update completed.")

if __name__ == "__main__":
    main()
