import os
import requests
import json
import pandas as pd
import re
from sshtunnel import SSHTunnelForwarder
import pymysql
import pymysql.cursors
from datetime import datetime

# ------------------------------
# SSH Tunnel & Database Configuration
# ------------------------------

# SSH Configuration
SSH_HOST = '129.151.144.124'
SSH_USER = 'opc'
SSH_KEY_PATH = r"C:\Users\jackf\.ssh\ssh-key-2025-07-09.key"
HEATWAVE_HOST = '10.0.151.92'
HEATWAVE_PORT = 3306

# Database credentials
DB_USER = 'admin'
DB_PASS = os.getenv('ORACLE_DB_PASSWORD') or input("Enter MySQL password for admin user: ")
DB_NAME = 'jfa_heatwave_db'

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
# Database Setup Function
# ------------------------------
def setup_database_table(connection):
    """
    Create the ALL_EVENTS table if it doesn't exist.
    """
    with connection.cursor() as cursor:
        # Create database if it doesn't exist
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}`")
        cursor.execute(f"USE `{DB_NAME}`")
        
        # Create the ALL_EVENTS table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ALL_EVENTS (
                id BIGINT PRIMARY KEY,
                name TEXT,
                status TEXT,
                start_date TEXT,
                finish_date TEXT,
                daysWindow BIGINT,
                Updates TEXT,
                location TEXT,
                stars TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_status (status(50)),
                INDEX idx_location (location(100)),
                INDEX idx_start_date (start_date(20))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        connection.commit()
        print("✅ ALL_EVENTS table created/verified successfully!")

# ------------------------------
# Comparison Function using ALL_EVENTS table
# ------------------------------
def compare_and_update_events_db(new_df, connection):
    """
    Compare newly fetched events with existing events in the ALL_EVENTS database table.
    Marks new events or events whose status has changed in the 'Updates' column.
    Returns the updated DataFrame and a flag indicating if changes were found.
    """
    try:
        # Query the current data from the ALL_EVENTS table
        old_df = pd.read_sql(f"SELECT * FROM {DB_NAME}.ALL_EVENTS", connection)
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

    return new_df, changes_found

# ------------------------------
# Upsert Function for MySQL/Oracle HeatWave
# ------------------------------
def upsert_all_events(connection, df):
    """
    Upsert events into the MySQL database table.
    For each row, if the event id exists it will update the record,
    otherwise it will insert a new row.
    """
    with connection.cursor() as cursor:
        for idx, row in df.iterrows():
            data = row.to_dict()
            # Convert id to integer since the DB table expects a BIGINT
            try:
                data['id'] = int(data['id'])
            except Exception as e:
                print(f"Error converting id {data['id']} to integer: {e}")
                continue
            
            # Prepare the upsert query (MySQL INSERT ... ON DUPLICATE KEY UPDATE)
            columns = ', '.join(data.keys())
            placeholders = ', '.join(['%s'] * len(data))
            update_clause = ', '.join([f"{col} = VALUES({col})" for col in data.keys() if col != 'id'])
            
            query = f"""
                INSERT INTO {DB_NAME}.ALL_EVENTS ({columns})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {update_clause}
            """
            
            cursor.execute(query, list(data.values()))
        
        connection.commit()

# ------------------------------
# Main Function
# ------------------------------
def main():
    print("\n=== Oracle HeatWave Wave Tour Events Update ===\n")
    
    # Create SSH tunnel
    with SSHTunnelForwarder(
        (SSH_HOST, 22),
        ssh_username=SSH_USER,
        ssh_pkey=SSH_KEY_PATH,
        remote_bind_address=(HEATWAVE_HOST, HEATWAVE_PORT)
    ) as tunnel:
        print(f"✅ SSH Tunnel established on port: {tunnel.local_bind_port}")
        
        try:
            # Connect to Oracle HeatWave via SSH tunnel
            connection = pymysql.connect(
                host='127.0.0.1',
                port=tunnel.local_bind_port,
                user=DB_USER,
                password=DB_PASS,
                connect_timeout=10,
                cursorclass=pymysql.cursors.DictCursor
            )
            print("✅ Connected to Oracle HeatWave MySQL!")
            
            # Setup database and table
            setup_database_table(connection)
            
            # 1. Fetch the latest events from the API.
            print("\n📡 Fetching latest Wave Tour events...")
            new_events_df = fetch_wave_tour_events()
            if new_events_df.empty:
                print("No new data fetched. Exiting.")
                return

            # 2. Compare with existing events in the ALL_EVENTS table and update 'Updates' field if necessary.
            print("\n🔍 Comparing with existing data...")
            updated_events_df, changes_found = compare_and_update_events_db(new_events_df, connection)
            
            # 3. Upsert the latest events data into the MySQL database.
            print("\n💾 Updating database...")
            upsert_all_events(connection, updated_events_df)
            print("✅ Database update completed.")
            
            # 4. Show summary
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) as total FROM {DB_NAME}.ALL_EVENTS")
                total_events = cursor.fetchone()['total']
                
                cursor.execute(f"SELECT COUNT(*) as new_events FROM {DB_NAME}.ALL_EVENTS WHERE Updates LIKE '%New Event%'")
                new_events_count = cursor.fetchone()['new_events']
                
                cursor.execute(f"SELECT COUNT(*) as status_changes FROM {DB_NAME}.ALL_EVENTS WHERE Updates LIKE '%->%'")
                status_changes = cursor.fetchone()['status_changes']
                
                print(f"\n📊 Summary:")
                print(f"   Total events in database: {total_events}")
                print(f"   New events this run: {len(updated_events_df[updated_events_df['Updates'].str.contains('New Event', na=False)])}")
                print(f"   Status changes this run: {len(updated_events_df[updated_events_df['Updates'].str.contains('->', na=False)])}")
                
                if changes_found:
                    print("\n🔄 Changes detected and saved!")
                else:
                    print("\n✅ No changes detected - database is up to date!")
            
            connection.close()
            
        except pymysql.err.OperationalError as e:
            print(f"❌ Database connection error: {e}")
        except pymysql.err.ProgrammingError as e:
            print(f"❌ Database SQL error: {e}")
        except Exception as e:
            print(f"❌ Unexpected error: {type(e).__name__}: {e}")

if __name__ == "__main__":
    main() 