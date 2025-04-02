import pandas as pd

def clean_csv(input_file, output_file):
    # Read the CSV file
    df = pd.read_csv(input_file)
    
    # Clean the 'category_code' column:
    # - Split on commas, strip whitespace, and remove empty strings or literal "[]"
    df['category_codes'] = (
        df['category_codes']
        .fillna('')
        .astype(str)
        .apply(lambda x: [
            item.replace("'", "").replace("[", "").replace("]", "").strip() 
            for item in x.split(',')
        ])
    )
    
    # Clean the 'elimination' column:
    # - Split on commas and strip whitespace
    df['elimination_names'] = (
        df['elimination_names']
        .fillna('')
        .astype(str)
        .apply(lambda x: [
            item.replace("'", "").replace("[", "").replace("]", "").strip() 
            for item in x.split(',')
        ])
    )

    
    # Keep only rows where the number of category_codes matches the number of elimination names
    df = df[df.apply(lambda row: len(row['category_codes']) == len(row['elimination_names']), axis=1)]
    
    # For each row, zip the two lists and create a new row for each pair
    new_rows = []
    for _, row in df.iterrows():
        for cat, elim in zip(row['category_codes'], row['elimination_names']):
            new_row = row.copy()
            new_row['category_codes'] = cat
            new_row['elimination_names'] = elim
            new_rows.append(new_row)
    
    new_df = pd.DataFrame(new_rows)
    
    # Filter rows to keep only those where 'elimination' contains "wave" (case insensitive)
    new_df = new_df[new_df['elimination_names'].str.contains('wave', case=False, na=False)]
    
    # Write the cleaned DataFrame to a new CSV file
    new_df.to_csv(output_file, index=False)

if __name__ == '__main__':
    input_csv = 'pwa_event_data.csv'
    output_csv = 'pwa_event_data_cleaned.csv'
    clean_csv(input_csv, output_csv)
