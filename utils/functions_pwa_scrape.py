import requests
import xml.etree.ElementTree as ET
import pandas as pd
import unicodedata
import ast

def export_heat_progression_and_results(event_id, category_code):
    """
    Fetch XML data for the given category_code, extract heat progression and
    sailor-level heat results. Instead of immediately writing CSV files,
    return the heat results DataFrame, heat progression DataFrame, and the list
    of unique heat IDs.
    """
    xml_url = f'https://www.pwaworldtour.com/fileadmin/live_ladder/live_ladder_{category_code}.xml'
    response = requests.get(xml_url)
    
    if response.status_code != 200:
        print(f"Failed to fetch XML for category code {category_code}. Status code: {response.status_code}")
        return None, None, []
    
    xml_content = response.content
    root = ET.fromstring(xml_content)
    
    all_data = []                # Sailor-level heat results
    heat_progression_data = []   # Heat progression information
    
    # Process each elimination block in the XML
    for elimination in root.findall('elimination'):
        discipline = elimination.find('discipline').text if elimination.find('discipline') is not None else None
        if discipline != 'wave':
            continue  # Only process 'wave' discipline
        
        event = elimination.find('event').text if elimination.find('event') is not None else None
        elimination_name = elimination.find('name').text if elimination.find('name') is not None else None
        sex = elimination.find('sex').text if elimination.find('sex') is not None else None
        event_division_id = elimination.find('eventDivisionId').text if elimination.find('eventDivisionId') is not None else None
        # Extract toAdvance from elimination level
        elimination_toadvance = elimination.find('toAdvance').text if elimination.find('toAdvance') is not None else None

        rounds = elimination.find('rounds')
        if rounds is None:
            continue
        
        # Loop through each round
        for round_elem in rounds.findall('round'):
            round_name_raw = round_elem.find('name').text if round_elem.find('name') is not None else None
            # Try to extract toAdvance from the round; if missing, fall back to elimination level
            round_toadvance_elem = round_elem.find('toAdvance')
            if round_toadvance_elem is not None and round_toadvance_elem.text is not None:
                toadvance = round_toadvance_elem.text
            else:
                toadvance = elimination_toadvance
            
            # Compute round_order (numeric round value minus 1) and add "Round " prefix to round_name
            if round_name_raw and round_name_raw.isdigit():
                round_order = int(round_name_raw) - 1
                round_name = f"Round {round_name_raw}"
            else:
                round_order = None
                round_name = f"Round {round_name_raw}" if round_name_raw else None
            
            # --- Extract heat progression data ---
            for heat_group in round_elem.findall('heats/heatGroup'):
                for heat in heat_group.findall('heat'):
                    heat_id = heat.find('heatId').text if heat.find('heatId') is not None else None
                    heat_name = heat.find('heatName').text if heat.find('heatName') is not None else None
                    
                    heat_progression_data.append({
                        'event_id': event_id,
                        'eventDivisionId': category_code,
                        'sex': sex,
                        'round_name': round_name,
                        'round_order': round_order,
                        'heat_id': heat_id,
                        'heat_order': heat_name,
                        'total_winners_progressing': toadvance,
                        'winners_progressing_to_round_order': '',
                        'total_losers_progressing': '',
                        'losers_progressing_to_round_order': ''
                    })
            
            # --- Extract sailor-level heat results ---
            for heat_group in round_elem.findall('heats/heatGroup'):
                for heat in heat_group.findall('heat'):
                    heat_id = heat.find('heatId').text if heat.find('heatId') is not None else None
                    heat_name = heat.find('heatName').text if heat.find('heatName') is not None else None
                    sailors_node = heat.find('sailors')
                    if sailors_node is None:
                        continue
                    for sailor in sailors_node.findall('sailor'):
                        sailor_name = sailor.find('sailorName').text if sailor.find('sailorName') is not None else None
                        sail_nr = sailor.find('sailNr').text if sailor.find('sailNr') is not None else None
                        place = sailor.find('place').text if sailor.find('place') is not None else None
                        
                        all_data.append({
                            'event_id': event_id,
                            'Event': event,
                            'Elimination Name': elimination_name,
                            'Discipline': discipline,
                            'eventDivision': sex,
                            'Round': round_name,
                            'Heat ID': heat_id,
                            'Heat Name': heat_name,
                            'Sailor Name': sailor_name,
                            'Sailor Number': sail_nr,
                            'Place': place,
                            'Category Code': category_code
                        })
    
    # Convert lists to DataFrames
    final_df = pd.DataFrame(all_data)
    heat_progression_df = pd.DataFrame(heat_progression_data)
    
    # If no wave events were processed, return empty results immediately.
    if final_df.empty:
        print(f"No wave events found for event_id: {event_id} and category_code: {category_code}")
        return final_df, heat_progression_df, []
    
    # Create an athlete_id by combining sailor name and number
    final_df['athlete_id'] = final_df['Sailor Name'].astype(str) + '_' + final_df['Sailor Number'].astype(str)
    
    # Map sex values: "male" -> "Men" and "female" -> "Women"
    sex_mapping = {'male': 'Men', 'female': 'Women'}
    heat_progression_df['sex'] = heat_progression_df['sex'].map(sex_mapping)
    final_df['eventDivision'] = final_df['eventDivision'].map(sex_mapping)
    
    # Add source column with the value 'PWA'
    final_df['source'] = 'PWA'
    heat_progression_df['source'] = 'PWA'
    
    # --- Update the pwa_heat_results export ---
    # Drop unwanted columns
    columns_to_drop = [
        'Heat Name',
        'Event',
        'Sailor Name',
        'Sailor Number',
        'Elimination Name',
        'Discipline',
        'eventDivision',
        'Round'
    ]
    final_df.drop(columns=columns_to_drop, inplace=True, errors='ignore')
    
    # Rename columns as specified
    final_df.rename(columns={
        'Heat ID': 'heat_id',
        'Category Code': 'eventDivisionId',
        'athlete_id': 'athleteId',
        'Place': 'place'
    }, inplace=True)
    
    # Add empty columns for result_total, winBy, and needs (result_total to be merged later)
    final_df['result_total'] = ''
    final_df['winBy'] = ''
    final_df['needs'] = ''
    
    # Reorder columns to the desired order
    final_df = final_df[['source', 'event_id', 'eventDivisionId', 'heat_id', 'athleteId',
                         'result_total', 'winBy', 'needs', 'place']]
    
    # Get unique heat IDs for the heat scores extraction
    unique_heat_ids = final_df['heat_id'].dropna().unique().tolist()
    
    return final_df, heat_progression_df, unique_heat_ids


def export_heat_scores(event_id, category_code, heat_ids):
    """
    For each heat_id provided, fetch heat scores from the JSON API.
    Instead of writing the CSV immediately, return the heat scores DataFrame.
    The event_id (from the XML export) is included in the data.
    """
    api_base_url = "https://www.pwaworldtour.com/fileadmin/live_score/"
    heat_data = []
    
    for heat_id in heat_ids:
        try:
            api_url = f"{api_base_url}{heat_id}.json"
            response = requests.get(api_url)
            response.raise_for_status()
            heatsheet_json = response.json()
            
            # Basic heat info
            heat_info = {
                'Heat ID': heatsheet_json['heat']['heatId'],
                'Heat No': heatsheet_json['heat']['heatNo'],
                'Wave Count': heatsheet_json['heat']['waveCount'],
                'Jumps Count': heatsheet_json['heat']['jumpsCount'],
                'Wave Factor': heatsheet_json['heat']['waveFactor'],
                'Jump Factor': heatsheet_json['heat']['jumpFactor'],
            }
            
            # Process each sailor in the heat
            for sailor_info in heatsheet_json['heat']['sailors']:
                sailor = sailor_info['sailor']
                # Use .get() so that missing keys return an empty string
                sailor_name = unicodedata.normalize('NFKD', sailor.get('sailorName', ''))
                base_info = {
                    'event_id': event_id,   # Include event_id from XML extraction
                    'Category': category_code,
                    'Sailor Name': sailor.get('sailorName', ''),
                    'Sail Number': sailor.get('sailNo', ''),
                    'Heatsheet ID': heat_id,
                    'Heat No': heatsheet_json['heat']['heatNo'],
                    'Total Wave': sailor.get('totalWave', ''),
                    'Total Jump': sailor.get('totalJump', ''),
                    'Total Points': sailor.get('totalPoints', ''),
                    'Position': sailor.get('totalPos', ''),
                }
                
                combined_info = {**heat_info, **base_info}
                
                # Process each score (wave or jump)
                for score_type, score_list in sailor.get('scores', {}).items():
                    for score in score_list:
                        if not isinstance(score, dict):
                            continue
                        row = combined_info.copy()
                        row['Type'] = 'Wave' if score_type == 'wave' else score.get('type', '')
                        row['Score'] = score.get('score', None)
                        row['Counting'] = 'Yes' if score.get('counting') else 'No'
                        
                        heat_data.append(row)
            
            print(f"Successfully parsed heatsheet for Heat ID {heat_id}")
        except Exception as e:
            print(f"Failed to retrieve or parse heatsheet for Heat ID {heat_id}. Error: {e}")
            continue
    
    # Define the required columns for the output DataFrame
    required_columns = [
        'source',
        'event_id',
        'heat_id',
        'eventDivisionId',
        'athleteId',
        'score',
        'modified_total',
        'modifier',
        'type',
        'counting',
        'total_wave',
        'total_jump',
        'total_points'
    ]
    
    # Create a DataFrame from the collected heat data
    heatsheet_df = pd.DataFrame(heat_data)
    
    # If no data was collected, or required keys are missing, return an empty DataFrame.
    if heatsheet_df.empty:
        print("No heat scores data available; returning empty DataFrame.")
        return pd.DataFrame(columns=required_columns)
    
    if 'Sailor Name' not in heatsheet_df.columns or 'Sail Number' not in heatsheet_df.columns:
        print("Required keys ('Sailor Name' or 'Sail Number') not found in heat scores data; skipping export for this category_code.")
        return pd.DataFrame(columns=required_columns)
    
    # Add source column with value 'PWA'
    heatsheet_df['source'] = 'PWA'
    
    # Create new column 'athleteId' by combining Sailor Name and Sail Number
    heatsheet_df['athleteId'] = (
        heatsheet_df['Sailor Name'].astype(str) +
        '_' +
        heatsheet_df['Sail Number'].astype(str)
    )
    
    # Drop unwanted columns
    columns_to_drop = [
        'Wave Count', 'Jumps Count', 'Wave Factor', 'Jump Factor',
        'Sailor Name', 'Sail Number'
    ]
    heatsheet_df.drop(columns=columns_to_drop, inplace=True, errors='ignore')
    
    # Rename columns to match the new specification
    heatsheet_df.rename(columns={
        'Heat ID': 'heat_id',
        'Category': 'eventDivisionId',
        'Score': 'score',
        'Type': 'type',
        'Counting': 'counting',
        'Total Wave': 'total_wave',
        'Total Jump': 'total_jump',
        'Total Points': 'total_points'
    }, inplace=True)
    
    # Add empty columns for modified_total and modifier
    heatsheet_df['modified_total'] = ''
    heatsheet_df['modifier'] = ''
    
    # Reorder columns as specified
    heatsheet_df = heatsheet_df[[ 
        'source',
        'event_id',
        'heat_id',
        'eventDivisionId',
        'athleteId',
        'score',
        'modified_total',
        'modifier',
        'type',
        'counting',
        'total_wave',
        'total_jump',
        'total_points'
    ]]
    
    return heatsheet_df


def export_heat_data(event_id, category_code):
    """
    Main function that takes an event_id and category_code as inputs.
    It extracts data from XML and JSON sources, merges the total points from
    the heat scores into the heat results export (as result_total), and writes
    all CSV files after all data is available.
    
    CSV files created:
      - pwa_heat_results.csv (with result_total merged)
      - pwa_heat_progression_format.csv
      - pwa_heat_scores.csv
    """
    # Extract heat progression and results from XML
    heat_results_df, heat_progression_df, heat_ids = export_heat_progression_and_results(event_id, category_code)
    
    if not heat_ids:
        print("No heat IDs found. Skipping heat scores export.")
        return
    
    # Extract heat scores from JSON
    heat_scores_df = export_heat_scores(event_id, category_code, heat_ids)
    
    # Merge total_points into heat_results_df by matching on event_id, heat_id, and athleteId.
    # We first extract unique rows from the heat_scores_df.
    total_points_df = heat_scores_df[['event_id', 'heat_id', 'athleteId', 'total_points']].drop_duplicates()
    
    # Merge on the common keys. This will add the total_points column to heat_results_df.
    merged_df = pd.merge(heat_results_df, total_points_df, on=['event_id', 'heat_id', 'athleteId'], how='left')
    
    # Update the result_total column with the merged total_points values.
    merged_df['result_total'] = merged_df['total_points']
    # Optionally, drop the extra total_points column if not needed.
    merged_df.drop(columns=['total_points'], inplace=True)
    
    # Write CSV files only after all data is available
    merged_df.to_csv('pwa_heat_results.csv', encoding='utf-8-sig', index=False)
    heat_progression_df.to_csv('pwa_heat_progression_format.csv', encoding='utf-8-sig', index=False)
    heat_scores_df.to_csv('pwa_heat_scores.csv', encoding='utf-8-sig', index=False)
    
    print("Exported pwa_heat_results.csv, pwa_heat_progression_format.csv, and pwa_heat_scores.csv")


# Example usage:
# Replace with your actual event_id and category_code values
# if __name__ == '__main__':
# export_heat_data(363, 931)

import requests
from bs4 import BeautifulSoup
import pandas as pd

import re
import requests
from bs4 import BeautifulSoup

def extract_wave_links_with_labels(event_id):
    """
    Extracts wave links and their labels from the PWA website for a given event_id.
    It filters links where:
      - The link label contains "wave" (case-insensitive)
      - The href includes a numeric code immediately following the sequence 
        "tx_pwaevent_pi1%5BeventDiscipline%5D="
    
    Returns:
      dict: A dictionary with keys as labels and values as the extracted numeric code.
    """
    url = f"https://www.pwaworldtour.com/index.php?id=193&type=21&tx_pwaevent_pi1%5Baction%5D=results&tx_pwaevent_pi1%5BshowUid%5D={event_id}.xml"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to retrieve data: {response.status_code}")
    
    soup = BeautifulSoup(response.content, 'lxml')
    
    # Look for the first <ul> container.
    container = soup.find('ul')
    links = container.find_all('a', href=True) if container else soup.find_all('a', href=True)
    
    # Regex pattern to extract the number after "tx_pwaevent_pi1%5BeventDiscipline%5D="
    pattern = r"tx_pwaevent_pi1%5BeventDiscipline%5D=(\d+)"
    
    wave_links = {}
    for link in links:
        label = link.get_text(strip=True)
        href = link.get('href')
        # Check if the label contains "wave" (case-insensitive)
        if "wave" in label.lower():
            match = re.search(pattern, href)
            if match:
                # Extract just the numeric part.
                wave_links[label] = match.group(1)
    return wave_links

# # Example usage:
# if __name__ == "__main__":
#     event_id = 357  # Replace with the desired event_id
#     result = extract_wave_links_with_labels(event_id)
#     print(result)



def extract_pwa_results(event_id, discipline_code):
    """
    Extracts event results from the PWA XML page and returns a DataFrame.

    Parameters:
      event_id (str/int): The event identifier supplied to the function.
      discipline_code (str/int): The discipline code, used in the URL as eventDivisionid.

    Returns:
      pd.DataFrame: A DataFrame with columns: source, event_id, eventDivisionid, Name, sail_no, athlete_id, place, Points.
    """
    # Build the URL with event_id and discipline_code
    url = f"https://www.pwaworldtour.com/index.php?id=193&type=21&tx_pwaevent_pi1%5Baction%5D=results&tx_pwaevent_pi1%5BshowUid%5D={event_id}.xml&tx_pwaevent_pi1%5BeventDiscipline%5D={discipline_code}"
    
    # Request the XML/HTML content from the URL
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to retrieve data: {response.status_code}")
    
    content = response.content
    soup = BeautifulSoup(content, 'lxml')
    
    # Find the table containing the results
    table = soup.find('table')
    if table is None:
        raise Exception("No results table found in the XML/HTML content.")
    
    # Parse table rows (skip the header row)
    rows = table.find_all('tr')
    results = []
    
    for row in rows[1:]:
        cols = row.find_all('td')
        if len(cols) < 6:
            continue  # Skip rows that do not have enough columns
        
        # Extract the required fields
        place = cols[0].get_text(strip=True)
        name_div = cols[1].find('div', class_='rank-name')
        name = name_div.get_text(strip=True) if name_div else ""
        sail_no = cols[2].get_text(strip=True)
        points = cols[5].get_text(strip=True)
        
        # Create athlete_id by combining parts of the name with sail_no
        parts = name.split(" ", 1)
        if len(parts) > 1:
            athlete_id = f"{parts[1]}_{sail_no}"
        else:
            athlete_id = f"{name}_{sail_no}"
        
        record = {
            "source": "PWA",                   # Inserted column at the start
            "event_id": event_id,              # Supplied event_id
            "eventDivisionid": discipline_code,  # discipline_code used as eventDivisionid
            "Name": name,
            "sail_no": sail_no,
            "athlete_id": athlete_id,          # New athlete_id column
            "place": place,
            "Points": points
        }
        results.append(record)
    
    # Convert list of records to a DataFrame with the desired column order
    df = pd.DataFrame(results)
    cols_order = ["source", "event_id", "eventDivisionid", "Name", "sail_no", "athlete_id", "place", "Points"]
    df = df[cols_order]
    
    return df

# if __name__ == "__main__":
#     # Read the cleaned event data CSV with prefixed columns
#     event_data = pd.read_csv("Historical Scrapes/Data/Clean/pwa_event_data_cleaned.csv")
    
#     # Deduplicate rows based on the pwa_event_id and pwa_final_rank_code pairs
#     dedup_event_data = event_data.drop_duplicates(subset=['pwa_event_id', 'pwa_final_rank_code'])
#     print(f"Deduplicated to {len(dedup_event_data)} unique event/discipline pairs out of {len(event_data)} total rows.")
    
#     # List to store DataFrame results from each event/discipline pair
#     df_list = []
    
#     # Loop through each row using the new column names: pwa_event_id and pwa_final_rank_code
#     for idx, row in dedup_event_data.iterrows():
#         event_id = row['pwa_event_id']
#         discipline_code = row['pwa_final_rank_code']
#         try:
#             df_result = extract_pwa_results(event_id, discipline_code)
#             df_list.append(df_result)
#             print(f"Processed pwa_event_id: {event_id}, pwa_final_rank_code: {discipline_code}")
#         except Exception as e:
#             print(f"Error processing pwa_event_id: {event_id}, pwa_final_rank_code: {discipline_code}: {e}")
    
#     # Combine all results into a single DataFrame and save as CSV
#     if df_list:
#         final_df = pd.concat(df_list, ignore_index=True)
#         final_df.to_csv("Historical Scrapes/Data/Raw/pwa_final_ranks_raw.csv", index=False)
#         print("Saved final results to Historical Scrapes/Data/Raw/pwa_final_ranks_raw.csv")
#     else:
#         print("No results to save.")

#extract_wave_links_with_labels(357)
