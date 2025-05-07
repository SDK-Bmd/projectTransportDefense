"""
Navitia API data extraction script for La Défense
Extracts real-time schedule information for all transport modes
"""
import requests
import json
from datetime import datetime
import os
import sys

# Add the parent directory to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from dotenv import load_dotenv
from utils.data_lake_utils import get_s3_client, save_json_to_data_lake
from config.config import DATA_LAKE, LADEFENSE_COORDINATES, API_ENDPOINTS
from data_extraction.api_utils import get_with_retries

# Load environment variables
load_dotenv()

def extract_navitia_schedules():
    """Extract real-time schedules for La Défense using Navitia API"""
    # Get API key from environment variables
    api_key = os.getenv("NAVITIA_API_KEY")

    if not api_key:
        print("Error: Navitia API key not found in environment variables")
        print("Please add NAVITIA_API_KEY=your_key to your .env file")
        return False

    # Configuration
    bucket_name = DATA_LAKE["bucket_name"]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Coordinates for La Défense
    lat, lon = LADEFENSE_COORDINATES["lat"], LADEFENSE_COORDINATES["lon"]

    # Base URL for Navitia API
    base_url = API_ENDPOINTS['navitia_base']

    # Headers for authentication
    headers = {
        "Authorization": api_key
    }

    # Data to collect
    navitia_data = {
        "extraction_time": datetime.now().isoformat(),
        "location": "La Défense",
        "coordinates": {"lat": lat, "lon": lon},
        "stops": [],
        "departures": []
    }

    try:
        # Step 1: Find stops around La Défense
        # The coverage is for Île-de-France region
        coverage_url = f"{base_url}/coverage/fr-idf/coords/{lon};{lat}/stop_areas"
        params = {
            "distance": 500,  # 500 meters radius
            "count": 20  # Up to 20 stops
        }

        stops_response = get_with_retries(coverage_url, headers=headers, params=params, max_retries=3)

        if not stops_response or stops_response.status_code != 200:
            print(
                f"Error retrieving stops around La Défense: {stops_response.status_code if stops_response else 'No response'}")
            return False

        stops_data = stops_response.json()
        stop_areas = stops_data.get("stop_areas", [])

        # Only keep stops with "Defense" in the name or that are known to be at La Défense
        defense_stops = []
        for stop in stop_areas:
            stop_name = stop.get("name", "").lower()
            if "defense" in stop_name or "défense" in stop_name:
                defense_stops.append(stop)

                # Also add to our result data
                navitia_data["stops"].append({
                    "id": stop.get("id"),
                    "name": stop.get("name"),
                    "coordinates": {
                        "lat": stop.get("coord", {}).get("lat"),
                        "lon": stop.get("coord", {}).get("lon")
                    },
                    "lines": [line.get("code") for line in stop.get("lines", [])]
                })

        print(f"Found {len(defense_stops)} stops at La Défense")

        # Step 2: Get departures for each stop area
        for stop in defense_stops:
            stop_id = stop.get("id")

            # Get next departures
            departures_url = f"{base_url}/coverage/fr-idf/stop_areas/{stop_id}/departures"
            params = {
                "count": 20,  # Up to 20 departures
                "duration": 3600,  # Look for departures within the next hour
                "data_freshness": "realtime"  # Request real-time data
            }

            departures_response = get_with_retries(departures_url, headers=headers, params=params, max_retries=3)

            if not departures_response or departures_response.status_code != 200:
                print(
                    f"Error retrieving departures for stop {stop.get('name')}: {departures_response.status_code if departures_response else 'No response'}")
                continue

            departures_data = departures_response.json()

            # Process departures
            for departure in departures_data.get("departures", []):
                display_info = departure.get("display_informations", {})
                stop_date_time = departure.get("stop_date_time", {})

                departure_info = {
                    "stop_id": stop_id,
                    "stop_name": stop.get("name"),
                    "route_id": display_info.get("route_id"),
                    "trip_id": display_info.get("trip_id"),
                    "direction": display_info.get("direction"),
                    "headsign": display_info.get("headsign"),
                    "line_code": display_info.get("code"),
                    "network": display_info.get("network"),
                    "mode": display_info.get("commercial_mode"),
                    "departure_time": stop_date_time.get("departure_date_time"),
                    "status": "scheduled" if stop_date_time.get("data_freshness") == "base_schedule" else "realtime"
                }

                navitia_data["departures"].append(departure_info)

            print(f"Retrieved {len(departures_data.get('departures', []))} departures for {stop.get('name')}")

        # Save to data lake
        s3_key = f"landing/transport/navitia_ladefense_{timestamp}.json"
        save_json_to_data_lake(bucket_name, s3_key, navitia_data)

        # Also save a latest version
        save_json_to_data_lake(bucket_name, "landing/transport/navitia_ladefense_latest.json", navitia_data)

        print(
            f"Navitia data extracted and saved: {len(navitia_data['departures'])} departures across {len(navitia_data['stops'])} stops")
        return True

    except Exception as e:
        print(f"Error extracting Navitia data: {str(e)}")
        return False


if __name__ == "__main__":
    extract_navitia_schedules()