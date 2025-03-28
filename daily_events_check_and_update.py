import os
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, BigInteger, Text

# ------------------------------
# Database Configuration & Setup
# ------------------------------
# db_config = {
#     'user': 'avnadmin',
#     'password': 'AVNS_ND3WBdcIqIQvtuWr2ka',
#     'host': 'mysql-world-wave-tour-database-pwa-iwt-windsurf-stats.l.aivencloud.com',
#     'port': '28343',  # Change if necessary
#     'database': 'defaultdb'
# }

# # Create the MySQL engine
# engine = create_engine(
#     f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
# )

db_config = {
    'user': os.getenv('DB_USER', 'avnadmin'),
    'password': os.getenv('DB_PASSWORD', 'AVNS_ND3WBdcIqIQvtuWr2ka'),
    'host': os.getenv('DB_HOST', 'mysql-world-wave-tour-database-pwa-iwt-windsurf-stats.l.aivencloud.com'),
    'port': os.getenv('DB_PORT', '28343'),
    'database': os.getenv('DB_DATABASE', 'defaultdb')
}

# Build the connection string; if SSL is required, include the ssl_ca parameter.
ssl_ca = os.getenv('MYSQL_SSL_CA')
ssl_args = f"?ssl_ca={ssl_ca}" if ssl_ca else ""

engine = create_engine(
    f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}{ssl_args}"
)


# Define metadata and table schema for ALL_EVENTS
metadata = MetaData()
all_events = Table(
    'ALL_EVENTS',
    metadata,
    Column('id', BigInteger, primary_key=True),
    Column('name', Text),
    Column('status', Text),
    Column('date', Text),
    Column('daysWindow', BigInteger),
    Column('Updates', Text)
)

# ------------------------------
# Recreate the ALL_EVENTS Table
# ------------------------------
# Drop the table if it exists and create it fresh
metadata.drop_all(engine, tables=[all_events])
metadata.create_all(engine)
print("ALL_EVENTS table has been recreated.")

# ------------------------------
# Upload CSV Data to the Database
# ------------------------------
# Replace the path below with the path to your test CSV file
csv_file = "C:/Users/Jack.Andrew/OneDrive - aspirezone.qa/JFA PD/Live Heats Data Scrape/Scrape/LIVE_HEATS_SCRAPE/TEST_wave_tour_events.csv"
df = pd.read_csv(csv_file)

# Insert CSV data into the ALL_EVENTS table
df.to_sql('ALL_EVENTS', engine, if_exists='append', index=False)
print(f"Data from '{csv_file}' has been uploaded to the ALL_EVENTS table.")
