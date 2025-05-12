
df_raw = pd.read_csv('Historical Scrapes/Data/Raw/PWA/pwa_event_data_raw.csv')
cleaned = clean_event_df(df_raw, output_file="Historical Scrapes/Data/Clean/PWA/pwa_event_data_clean.csv")
