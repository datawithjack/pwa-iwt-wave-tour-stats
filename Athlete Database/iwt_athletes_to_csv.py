import json
import csv

def json_to_csv(json_file, csv_file):
    # Load JSON data using UTF-8 encoding
    with open(json_file, 'r', encoding='utf-8') as jf:
        data = json.load(jf)

    # Open CSV file for writing with utf-8-sig encoding to handle special characters properly
    with open(csv_file, 'w', newline='', encoding='utf-8-sig') as cf:
        writer = csv.writer(cf)
        # Write header row including the new "year_of_birth" column
        writer.writerow(["id", "name", "image", "dob", "nationality", "year_of_birth"])
        
        # Process each record in the JSON data
        for item in data:
            dob = item.get("dob")
            # Extract the year from dob if available, otherwise leave empty
            year_of_birth = dob.split('-')[0] if dob else ""
            writer.writerow([
                item.get("id"),
                item.get("name"),
                item.get("image"),
                dob,
                item.get("nationality"),
                year_of_birth
            ])

if __name__ == "__main__":
    json_to_csv("unique_athletes.json", "unique_athletes.csv")
