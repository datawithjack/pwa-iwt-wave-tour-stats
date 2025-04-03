import requests
from bs4 import BeautifulSoup
import re
import time
import pandas as pd
from datetime import datetime

base_url = "https://www.pwaworldtour.com/"
initial_url = base_url + "index.php?id=7"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
}

response = requests.get(initial_url, headers=headers)
soup = BeautifulSoup(response.content, 'html.parser')

# Debug: Check if the pagination section is present
pagination_div = soup.find("div", class_="page-browser")
if pagination_div:
    page_links = pagination_div.select("div.page-browser a.page.small-header")
    # Or, alternatively, search recursively:
    # page_links = pagination_div.find_all("a", class_="page small-header")

# Extract page URLs from the pagination section
page_links = soup.select("div.page-browser div.page-browser a.page")

page_urls = []
for link in page_links:
    href = link.get("href")
    # Replace HTML entities to get a proper URL
    href = href.replace("&amp;", "&")
    full_url = base_url + href
    if full_url not in page_urls:
        page_urls.append(full_url)

print("Extracted page URLs:", page_urls)


# Now, iterate through the page URLs to extract profile links.
profile_links = []
# Regex to match profile hrefs (adjust if necessary)
pattern = re.compile(r'index\.php\?id=7&amp;tx_pwasailor_pi1%5BshowUid%5D=\d+&amp;cHash=[a-f0-9]+')

for url in page_urls:
    print(f"Scraping page: {url}")
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    for a_tag in soup.find_all('a', href=True):
        # Check if the anchor tag matches our pattern
        if pattern.search(str(a_tag)):
            href = a_tag['href'].replace("&amp;", "&")
            full_profile_url = base_url + href
            if full_profile_url not in profile_links:
                profile_links.append(full_profile_url)
    
    time.sleep(0.5)  # be polite

# Now profile_links should contain URLs from all pages.
print("Total profile links found:", len(profile_links))

# The rest of your extraction code goes here...
data = []

for url in profile_links:
    print(f"Extracting from {url}")
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Extract name
    try:
        name = soup.select_one('.sailor-details-info-top h2').text.strip()
    except:
        name = None
    
    # Extract sail number
    try:
        sail_no = soup.select_one('.sail-no').text.strip()
    except:
        sail_no = None
    
    # Extract age and nationality from the base info section
    try:
        base_info = soup.select_one('.sailor-details-info-base')
        raw_text = base_info.get_text(separator="\n")
        
        age_match = re.search(r'Age:\s*(\d+)', raw_text)
        nationality_match = re.search(r'Nationality:\s*([^\n]+)', raw_text)
        age = int(age_match.group(1)) if age_match else None
        nationality = nationality_match.group(1) if nationality_match else None
    except:
        age = None
        nationality = None

    # Extract current sponsor from the sponsors div
    try:
        sponsor_div = soup.find("div", class_="sponsors")
        if sponsor_div:
            sponsor_text = sponsor_div.get_text(separator=" ", strip=True)
            # Remove the header "Sponsors" if present
            if sponsor_text.startswith("Sponsors"):
                current_sponsor = sponsor_text[len("Sponsors"):].strip()
            else:
                current_sponsor = sponsor_text
        else:
            current_sponsor = None
    except Exception as e:
        current_sponsor = None
    
    data.append({
        'name': name,
        'age': age,
        'nationality': nationality,
        'sail_no': sail_no,
        'pwa_url': url,
        'current_sponsors': current_sponsor
    })

    time.sleep(0.5)  # Avoid hammering the server

# Convert to DataFrame
df = pd.DataFrame(data)
print(df.head())

# print(df)

# Write to CSV
df.to_csv('pwa_sailors_raw.csv', index=False, encoding='utf-8')
print("Data saved to pwa_sailors_raw.csv")

############### CLEAN PWA DATA ###############

df = pd.read_csv("Athlete Database/Raw Data/pwa_sailors_raw.csv")

# 1. Remove extra spaces from the 'name' column
df["name"] = df["name"].astype(str).apply(lambda x: re.sub(r'\s+', ' ', x).strip())

# Remove rows where 'name' is null or the string "nan"
df = df[df["name"].notna()]
df = df[df["name"].str.lower() != "nan"]

# 3. Add an approximate year of birth column based on age
current_year = datetime.now().year

def calc_year_of_birth(age):
    try:
        age_val = float(age)
        return int(current_year - age_val)
    except (ValueError, TypeError):
        return None

df["yob"] = df["age"].apply(calc_year_of_birth)

# Remove rows with a 'sail_no' that does not contain any digit 
df = df[df["sail_no"].str.contains(r'\d', na=False)]

# Remove specific unwanted sail numbers
unwanted_sailnos = ["CRO-751", "E-4", "SGP-21"]
df = df[~df["sail_no"].isin(unwanted_sailnos)]


# Remove rows with specific unwanted URLs (assuming the column is named 'url')
unwanted_urls = [
    "https://www.pwaworldtour.com/index.php?id=7&tx_pwasailor_pi1%5BshowUid%5D=1835&cHash=8dceba137fb296ded96b0060f0ae19ff",
    "https://www.pwaworldtour.com/index.php?id=7&tx_pwasailor_pi1%5BshowUid%5D=2013&cHash=550d91a0ea5b520e5e18832faec27208",
    "https://www.pwaworldtour.com/index.php?id=7&tx_pwasailor_pi1%5BshowUid%5D=1962&cHash=2e3396508dc5475661161d20dc52c47d",
    "https://www.pwaworldtour.com/index.php?id=7&tx_pwasailor_pi1%5BshowUid%5D=1977&cHash=524a083aa25c235432177b8a3dcc7b88",
    "https://www.pwaworldtour.com/index.php?id=7&tx_pwasailor_pi1%5BshowUid%5D=2053&cHash=1618f0e41b729f1c76eca1549ece7b38"
]
df = df[~df["pwa_url"].isin(unwanted_urls)]

# Remove rows with name "Marc" and "Farrah Hall"
df = df[(df["name"] != "Marc") & (df["name"] != "Farrah Hall")]

# Replace sail_no for athlete "Julian Salmonn" with "G-901"
df.loc[df["name"] == "Julian Salmonn", "sail_no"] = "G-901"

# prefix with pwa_
df.columns = ['pwa_' + col for col in df.columns]
df.rename(columns={'pwa_pwa_url': 'pwa_url'}, inplace=True)
# Display the first few rows of the cleaned DataFrame
print(df.head())



# Write back the cleaned DataFrame to a new CSV file
df.to_csv("Athlete Database/Clean Data/pwa_sailors_clean.csv", index=False)
