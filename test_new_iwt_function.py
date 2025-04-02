import json
import pandas as pd
from functions_iwt_progression_results_scores import (
    create_final_rank_no_heat_info,
    flatten_heat_results_and_scores,
    calculate_final_rank_heat_info
)

def is_no_heat_info(json_data):
    """
    Determines whether the JSON is in the no-heat-info format.
    This checks if, for each result in the first heat, the following hold true:
      - total, the first ride's total, and place (converted to int) are all equal.
    """
    try:
        heats = json_data["data"]["eventDivision"]["heats"]
        if not heats:
            return False
        first_heat = heats[0]
        results = first_heat.get("result", [])
        if not results:
            return False
        for res in results:
            total_val = res.get("total")
            place_val = res.get("place")
            if total_val is None or place_val is None:
                return False
            try:
                place_int = int(place_val)
            except Exception:
                return False
            # Check if total converted to int equals the place
            if int(total_val) != place_int:
                return False
            # If rides exist, check the first ride's total as well
            rides = res.get("rides")
            if rides and isinstance(rides, dict):
                first_key = next(iter(rides))
                ride_list = rides[first_key]
                if ride_list and len(ride_list) > 0:
                    ride_total = ride_list[0].get("total")
                    if ride_total is None or int(ride_total) != place_int:
                        return False
        return True
    except Exception as e:
        print("Error in is_no_heat_info:", e)
        return False

def main():
    # Load the test JSON file.
    # Change the filename to "no_heat_info.json" to test a no heat info file,
    # or "heat_info.json" to test a JSON with detailed heat info.
    with open("event_results/event_72489_division_168257.json", "r") as f:
        json_data = json.load(f)
    # Alternatively:
    # with open("heat_info.json", "r") as f:
    #     json_data = json.load(f)

    # For testing, we use a fixed event id.
    event_id = "584940"
    # Use the eventDivision id from the JSON for division_id.
    division_id = json_data["data"]["eventDivision"]["id"]

    if is_no_heat_info(json_data):
        print("JSON detected as NO HEAT INFO.")
        df_final_rank = create_final_rank_no_heat_info(json_data, event_id, division_id)
    else:
        print("JSON detected as HEAT INFO.")
        # For heat info, first extract detailed results.
        df_results, _ = flatten_heat_results_and_scores(json_data, event_id, division_id)
        # Ensure there's a roundPosition column
        if df_results is not None and "roundPosition" not in df_results.columns:
            df_results["roundPosition"] = 0
        df_final_rank = calculate_final_rank_heat_info(df_results)

    print("\nFinal Ranking DataFrame:")
    print(df_final_rank)

if __name__ == "__main__":
    main()