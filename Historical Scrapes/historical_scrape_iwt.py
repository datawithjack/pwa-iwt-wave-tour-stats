########## GET ALL EVENT DATA FROM 'WORLD WAVE TOUR' ON LIVE HEATS ############
import pandas as pd
from functions_iwt_progression_results_scores import (
    fetch_wave_tour_events,
    extract_results_published_events,
    fetch_event_divisions,
    fetch_event_division_results,
    flatten_heat_progression,
    flatten_heat_results_and_scores,
    process_event_division,
    clean_heat_order
)

def main():
    # Step 1: Fetch events
    print("Fetching Wave Tour events...")
    fetch_wave_tour_events()
    event_ids = extract_results_published_events()

    # Prepare lists for collected DataFrames
    progression_dfs = []
    results_dfs = []
    scores_dfs = []
    final_rank_dfs = []

    # Process each event and division
    for event_id in event_ids:
        division_ids = fetch_event_divisions(event_id)
        for division_id in division_ids:
            print(f"Processing Event {event_id}, Division {division_id}...")
            data = fetch_event_division_results(event_id, division_id)
            if not data:
                print(f"Skipping Event {event_id}, Division {division_id} due to missing data.")
                continue

            # Flatten progression and clean
            df_prog = flatten_heat_progression(data, event_id, division_id)
            if df_prog is not None:
                df_prog = clean_heat_order(df_prog, "heat_order")
                progression_dfs.append(df_prog)

            # Flatten results and scores
            df_res, df_scr = flatten_heat_results_and_scores(data, event_id, division_id)
            if df_res is not None:
                results_dfs.append(df_res)
            if df_scr is not None:
                scores_dfs.append(df_scr)

            # Final ranking
            event_data = process_event_division(data, event_id, division_id)
            df_final = event_data.get("df_final_rank")
            if df_final is not None:
                final_rank_dfs.append(df_final)

    # Combine and export utility
    def combine_and_export(dfs, filename):
        if dfs:
            df_combined = pd.concat(dfs, ignore_index=True)
            df_combined.to_csv(filename, index=False)
            print(f"Exported {filename}")

    # Export all CSVs
    combine_and_export(progression_dfs, "combined_iwt_heat_progression.csv")
    combine_and_export(results_dfs, "combined_iwt_heat_results.csv")
    combine_and_export(scores_dfs, "combined_iwt_heat_scores.csv")
    combine_and_export(final_rank_dfs, "combined_iwt_final_ranks.csv")

if __name__ == "__main__":
    main()
