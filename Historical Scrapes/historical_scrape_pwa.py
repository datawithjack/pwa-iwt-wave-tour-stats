# =============================================================================
# PWA Event Webscrape
# =============================================================================

# =============================================================================
# Set Up
# =============================================================================
import time
import csv
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException

# NEW: Import the progression/results functions
import functions_pwa_progression_results_scores as fpprs
# NEW: Import the final rank functions from new_pwa_final_rank
import new_pwa_final_rank


# Set up WebDriver without manually specifying the path
chrome_options = Options()
chrome_options.add_argument("--start-maximized")
# Uncomment the line below to run in headless mode
chrome_options.add_argument("--headless")
driver = webdriver.Chrome(options=chrome_options)

# Increase timeout for waiting for elements
wait = WebDriverWait(driver, 90)  # Adjust the wait time as needed

# Open the target page
url = "https://www.pwaworldtour.com/index.php?id=2310"
driver.get(url)

# =============================================================================
# Get URLs, results codes,  category codes for all events
# =============================================================================

# Use JavaScript to click the dropdown toggle
dropdown_toggle_js = """
var dropdown = document.querySelector('.nav-sub.select-box .label');
if (dropdown) {
    dropdown.click();
    return true;
} else {
    return false;
}
"""
dropdown_toggled = driver.execute_script(dropdown_toggle_js)

if dropdown_toggled:
    print("Dropdown clicked successfully")
else:
    print("Dropdown not found")

# Add a short wait to ensure all year options are fully loaded
time.sleep(3)  # Adjust this wait time if needed

# Wait for the dropdown options to become visible
dropdown_options = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, ".nav-sub.select-box ul")))

# Find all <a> elements within the dropdown (i.e., all years)
year_elements = dropdown_options.find_elements(By.TAG_NAME, "a")

# Collect all year options
year_data = []
for year_element in year_elements:
    year_text = year_element.text.strip()  # e.g., '2020'
    try:
        year_int = int(year_text)
    except ValueError:
        continue  # Skip if conversion fails

    # Only add years 2016 or later
    if year_int < 2016:
        continue

    href = year_element.get_attribute("href")
    year_id = href.split("id=")[-1]  # Extract the year ID from the URL
    year_data.append({"year": year_text, "id": year_id})

# Now proceed to loop through the collected years and scrape event data
event_data_by_year = []

# Loop through the dynamically generated list of years
for year_info in year_data:
    year = year_info["year"]
    year_id = year_info["id"]
    
    # Construct the year URL and navigate
    year_url = f"https://www.pwaworldtour.com/index.php?id={year_id}"
    driver.get(year_url)

    # Wait for the event sections to be visible
    sections = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".event-calendar-grid")))

    # Extract event data from each section, only focusing on "Completed events"
    for section in sections:
        # Extract the section title (e.g., "Upcoming events", "Completed events")
        section_title = section.find_element(By.TAG_NAME, "h3").text.strip()
        
        if section_title.lower() == "completed events":
            # Find all event links in this section
            event_links = section.find_elements(By.CLASS_NAME, "event-calendar-link")
            
            # Extract event details and store them with the section title
            for event in event_links:
                event_title = event.find_element(By.CLASS_NAME, "event-title").text
                event_href = event.get_attribute("href")
                
                event_data_by_year.append({
                    "year": year,
                    "id": year_id,
                    "section": section_title,
                    "event_name": event_title,
                    "event_href": event_href
                })

# Extract event_id from event_href and construct new links, plus retrieve category codes
for event in event_data_by_year:
    event_href = event['event_href']
    
    # Extract event_id from event_href
    try:
        event_id = event_href.split('%5BshowUid%5D=')[-1].split('&')[0]
        event['event_id'] = event_id
    except IndexError:
        print(f"Could not extract event_id from href: {event_href}")
        continue

    # NEW BLOCK: Collect final rank codes and labels using the new function
    try:
        final_rank_data = new_pwa_final_rank.extract_wave_links_with_labels(event_id)
        event['final_rank'] = final_rank_data  # This stores the list of dicts with 'label' and 'href'
    except Exception as e:
        print(f"Error collecting final rank data for event {event['event_name']} (ID: {event_id}): {e}")
        event['final_rank'] = []  # Save an empty list if extraction fails

    # Existing code: Build and visit the URL to extract ladder (elimination) data
    new_url = f"https://www.pwaworldtour.com/index.php?id=1900&type=21&tx_pwaevent_pi1%5Baction%5D=ladders&tx_pwaevent_pi1%5BshowUid%5D={event_id}"
    event['ladder_url'] = new_url
    driver.get(new_url)
    
    # Check for "No elimination ladders" message and process ladder links
    try:
        no_ladders_msg = driver.find_elements(By.CSS_SELECTOR, ".no-entries-found-msg")
        if no_ladders_msg:
            print(f"No elimination ladders for event: {event['event_name']} in year {event['year']}, skipping to next event.")
            continue
    except WebDriverException:
        print(f"Error checking for 'no ladders' message for event: {event['event_name']} in year {event['year']}, skipping to next event.")
        continue

    try:
        ladder_links = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a[href*='tx_pwaevent_pi1%5BshowUid%5D']")))
        for ladder_link in ladder_links:
            ladder_href = ladder_link.get_attribute('href')
            try:
                category_code = ladder_href.split('%5Bladder%5D=')[-1].split('&')[0]  # Extract category code
                elimination_name = ladder_link.text.strip()  # Extract elimination name from the <a> tag text
                print(f"Category Code: {category_code}, Elimination Name: {elimination_name}")
                event.setdefault('category_codes', []).append(category_code)
                event.setdefault('elimination_names', []).append(elimination_name)
            except IndexError:
                print(f"Could not extract category_code from href: {ladder_href}")
                continue
    except WebDriverException:
        print(f"Failed to load or find ladder links for event: {event['event_name']} in year {event['year']}, skipping to next.")
        continue



# =============================================================================
# Filter Final Output
# =============================================================================
# Export event data to CSV (no additional filtering needed)
csv_file = "Historical Scrapes/Data/Raw/pwa_event_data_raw.csv"
# Filter events to include only those with at least one category code
filtered_events = [event for event in event_data_by_year if event.get('final_rank')]

if filtered_events:
    # Collect all unique keys from the dictionaries in filtered_events
    all_keys = set()
    for event in filtered_events:
        all_keys.update(event.keys())
    
    # Ensure each dictionary has the same keys (fill missing keys with None)
    for event in filtered_events:
        for key in all_keys:
            if key not in event:
                event[key] = None
    
    with open(csv_file, 'w', newline='', encoding='utf-8') as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=all_keys)
        dict_writer.writeheader()
        dict_writer.writerows(filtered_events)

    print(f"Data successfully written to {csv_file}")
else:
    print("No event data available to write to CSV.")


# =============================================================================
# PWA EVENT CLEANING
# =============================================================================

import ast
from urllib.parse import urlparse, parse_qs
df = pd.read_csv('Historical Scrapes/Data/Raw/PWA/pwa_event_data_raw.csv')

def clean_event_df(df, output_file=None):
    # -------------------------------
    # Expand final_rank column into final_rank_label and final_rank_code columns
    # -------------------------------
    new_final_rank_rows = []
    for _, row in df.iterrows():
        try:
            # Convert the string representation into a Python list of dictionaries
            final_rank_list = ast.literal_eval(row['final_rank'])
            # For each dictionary in the list, extract the desired information
            for rank in final_rank_list:
                label = rank.get('label')
                href = rank.get('href')
                code = None
                if href:
                    parsed_url = urlparse(href)
                    params = parse_qs(parsed_url.query)
                    # Extract the discipline code from the parameter "tx_pwaevent_pi1[eventDiscipline]"
                    code = params.get('tx_pwaevent_pi1[eventDiscipline]', [None])[0]
                new_row = row.copy()
                new_row['final_rank_label'] = label
                new_row['final_rank_code'] = code
                new_final_rank_rows.append(new_row)
        except Exception as e:
            # In case of error, set the new columns to None
            new_row = row.copy()
            new_row['final_rank_label'] = None
            new_row['final_rank_code'] = None
            new_final_rank_rows.append(new_row)
    df = pd.DataFrame(new_final_rank_rows)
    
    # -------------------------------
    # Clean the 'category_codes' column:
    # - Split on commas, strip whitespace, and remove extraneous characters
    # -------------------------------
    df['category_codes'] = (
        df['category_codes']
        .fillna('')
        .astype(str)
        .apply(lambda x: [item.replace("'", "").replace("[", "").replace("]", "").strip() 
                            for item in x.split(',')])
    )
    
    # -------------------------------
    # Clean the 'elimination_names' column:
    # - Split on commas and strip whitespace
    # -------------------------------
    df['elimination_names'] = (
        df['elimination_names']
        .fillna('')
        .astype(str)
        .apply(lambda x: [item.replace("'", "").replace("[", "").replace("]", "").strip() 
                            for item in x.split(',')])
    )
    
    # Keep only rows where the number of category_codes matches the number of elimination_names
    df = df[df.apply(lambda row: len(row['category_codes']) == len(row['elimination_names']), axis=1)]
    
    # -------------------------------
    # Expand each row so that each combination of category and elimination name becomes its own row
    # -------------------------------
    new_rows = []
    for _, row in df.iterrows():
        for cat, elim in zip(row['category_codes'], row['elimination_names']):
            new_row = row.copy()
            new_row['category_codes'] = cat
            new_row['elimination_names'] = elim
            new_rows.append(new_row)
    new_df = pd.DataFrame(new_rows)
    
    # -------------------------------
    # Filter rows to keep only those where 'elimination_names' contains "wave" (case insensitive)
    # -------------------------------
    new_df = new_df[new_df['elimination_names'].str.contains('wave', case=False, na=False)]
    
    # -------------------------------
    # Additional gender filter:
    # - For "men", catch both "men" and "mens" (ensuring "women" isn't matched) via final_rank_label
    # - For "women", catch both "women" and "womens"
    # -------------------------------
    men_condition = (~new_df['elimination_names'].str.contains(r'\bmen(s)?\b', case=False, regex=True, na=False)) | \
                    (new_df['final_rank_label'].str.contains(r'\bmen(s)?\b', case=False, regex=True, na=False))
    women_condition = (~new_df['elimination_names'].str.contains(r'\bwomen(s)?\b', case=False, regex=True, na=False)) | \
                      (new_df['final_rank_label'].str.contains(r'\bwomen(s)?\b', case=False, regex=True, na=False))
    new_df = new_df[men_condition & women_condition]
    
    # -------------------------------
    # Extract new column 'sex' from final_rank_label (either "Men" or "Women")
    # -------------------------------
    new_df['sex'] = new_df['final_rank_label'].str.extract(r'(?i)\b(men|women)\b')[0].str.capitalize()
    
    # -------------------------------
    # Drop the original final_rank column
    # -------------------------------
    new_df = new_df.drop(columns=['final_rank'])
    
    # -------------------------------
    # Append "pwa_" prefix to all column names
    # -------------------------------
    new_df.columns = ['pwa_' + col for col in new_df.columns]
    
    # Write the cleaned DataFrame to a CSV file if an output file path is provided
    if output_file:
        new_df.to_csv(output_file, index=False)
    
    return new_df



# =============================================================================
# Extract Heat Data Using PWA Progression/Results Functions
# =============================================================================
# Initialize empty DataFrames to aggregate the data from each event.
all_heat_results_df = pd.DataFrame()
all_heat_progression_df = pd.DataFrame()
all_heat_scores_df = pd.DataFrame()

for event in filtered_events:
    event_id = event.get('event_id')
    category_codes = event.get('category_codes', [])
    for category_code in category_codes:
        print(f"Processing event_id: {event_id} with category_code: {category_code}")
        # Call the function that extracts XML data (heat results and progression)
        heat_results_df, heat_progression_df, heat_ids = fpprs.export_heat_progression_and_results(event_id, category_code)
        if heat_results_df is not None:
            all_heat_results_df = pd.concat([all_heat_results_df, heat_results_df], ignore_index=True)
            all_heat_progression_df = pd.concat([all_heat_progression_df, heat_progression_df], ignore_index=True)
            # If heat IDs were found, extract the heat scores from JSON
            if heat_ids:
                heat_scores_df = fpprs.export_heat_scores(event_id, category_code, heat_ids)
                all_heat_scores_df = pd.concat([all_heat_scores_df, heat_scores_df], ignore_index=True)

# Optionally, export the aggregated dataframes to CSV files
all_heat_results_df.to_csv('aggregated_heat_results.csv', index=False, encoding='utf-8-sig')
all_heat_progression_df.to_csv('aggregated_heat_progression.csv', index=False, encoding='utf-8-sig')
all_heat_scores_df.to_csv('aggregated_heat_scores.csv', index=False, encoding='utf-8-sig')

print("Aggregated heat data exported to CSV files.")