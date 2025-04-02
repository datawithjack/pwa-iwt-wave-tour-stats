import requests
from bs4 import BeautifulSoup
import pandas as pd

def extract_pwa_results(event_id, category_code):
    """
    Extracts event results from the PWA XML page and returns a DataFrame.

    Parameters:
      event_id (str/int): The event identifier supplied to the function.
      category_code (str/int): The category code, used in the URL and stored as eventDivisionid.

    Returns:
      pd.DataFrame: A DataFrame with columns: source, event_id, eventDivisionid, Name, sail_no, athlete_id, place, Points.
    """
    # Build the URL with event_id and category_code
    url = f"https://www.pwaworldtour.com/index.php?id=193&type=21&tx_pwaevent_pi1%5Baction%5D=results&tx_pwaevent_pi1%5BshowUid%5D={event_id}.xml&tx_pwaevent_pi1%5BeventDiscipline%5D={category_code}"
    
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
        
        # Split Name on the first space and combine the second part with sail_no to create athlete_id
        parts = name.split(" ", 1)
        if len(parts) > 1:
            athlete_id = f"{parts[1]}_{sail_no}"
        else:
            athlete_id = f"{name}_{sail_no}"
        
        record = {
            "source": "PWA",               # Inserted column at the start
            "event_id": event_id,          # Supplied event_id
            "eventDivisionid": category_code,  # category_code stored as eventDivisionid
            "Name": name,
            "sail_no": sail_no,
            "athlete_id": athlete_id,      # New athlete_id column
            "place": place,
            "Points": points
        }
        results.append(record)
    
    # Convert list of records to a DataFrame with the desired column order
    df = pd.DataFrame(results)
    cols_order = ["source", "event_id", "eventDivisionid", "Name", "sail_no", "athlete_id", "place", "Points"]
    df = df[cols_order]
    
    return df

if __name__ == "__main__":
    # Read the event data CSV which should contain columns: event_id and category_code
    event_data = pd.read_csv("pwa_event_data_cleaned.csv")
    
    # List to store DataFrame results from each event/category pair
    df_list = []
    
    # Loop through each event_id and category_code
    for idx, row in event_data.iterrows():
        event_id = row['event_id']
        category_code = row['category_codes']
        try:
            df_result = extract_pwa_results(event_id, category_code)
            df_list.append(df_result)
            print(f"Processed event_id: {event_id}, category_code: {category_code}")
        except Exception as e:
            print(f"Error processing event_id: {event_id}, category_code: {category_code}: {e}")
    
    # Combine all results into a single DataFrame and save as CSV
    if df_list:
        final_df = pd.concat(df_list, ignore_index=True)
        final_df.to_csv("pwa_final_ranks.csv", index=False)
        print("Saved final results to pwa_final_ranks.csv")
    else:
        print("No results to save.")
