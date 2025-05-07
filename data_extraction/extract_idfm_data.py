"""
IDFM API data extraction script for La Défense
Extracts real-time schedule information and station data
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
from configuration.config import DATA_LAKE, LADEFENSE_COORDINATES

# Load environment variables
load_dotenv()

def extract_idfm_data():
    """Extract real-time schedules and station data for La Défense using IDFM API"""
    # Get API key from environment variables
    api_key = os.getenv("IDFM_API_KEY")

    if not api_key:
        print("Error: IDFM API key not found in environment variables")
        print("Please add IDFM_API_KEY=your_key to your .env file")
        return False

    # Configuration
    bucket_name = DATA_LAKE["bucket_name"]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Coordinates for La Défense
    lat, lon = LADEFENSE_COORDINATES["lat"], LADEFENSE_COORDINATES["lon"]

    # Base URL for IDFM API
    base_url = "https://prim.iledefrance-mobilites.fr/marketplace/general-message"

    # Headers for authentication
    headers = {
        "Accept": "application/json",
        "Authorization": api_key
    }

    # Data to collect
    idfm_data = {
        "extraction_time": datetime.now().isoformat(),
        "location": "La Défense",
        "coordinates": {"lat": lat, "lon": lon},
        "stops": [],
        "departures": [],
        "traffic_status": []
    }

    try:
        # Step 1: Get traffic status information
        traffic_params = {
            "LineRef": "",  # Empty for all lines
            "StopPointRef": ""  # Empty for all stops
        }

        traffic_response = requests.get(
            base_url,
            headers=headers,
            params=traffic_params
        )

        if traffic_response.status_code == 200:
            traffic_data = traffic_response.json()

            # Process traffic data
            if "ServiceDelivery" in traffic_data and "GeneralMessageDelivery" in traffic_data["ServiceDelivery"]:
                messages = traffic_data["ServiceDelivery"]["GeneralMessageDelivery"].get("GeneralMessage", [])

                for message in messages:
                    # Extract relevant information
                    info = {
                        "id": message.get("InfoMessageIdentifier", ""),
                        "title": message.get("InfoMessageVersion", {}).get("content", ""),
                        "severity": message.get("InfoMessageVersion", {}).get("severity", ""),
                        "message": message.get("InfoMessageVersion", {}).get("MessageText", {}).get("value", ""),
                        "start_time": message.get("RecordedAtTime", ""),
                        "valid_until": message.get("ValidUntilTime", "")
                    }

                    # Add affected lines/stations
                    affected_lines = []
                    affected_stops = []

                    for ref in message.get("InfoMessageVersion", {}).get("InfoChannelRef", []):
                        if ref.get("type") == "Line":
                            affected_lines.append(ref.get("ref", ""))
                        elif ref.get("type") == "StopPoint":
                            affected_stops.append(ref.get("ref", ""))

                    info["affected_lines"] = affected_lines
                    info["affected_stops"] = affected_stops

                    idfm_data["traffic_status"].append(info)

            print(f"Retrieved {len(idfm_data['traffic_status'])} traffic status messages")
        else:
            print(f"Error retrieving traffic status: {traffic_response.status_code}")

        # Step 2: Get stations data for La Défense area
        # Using the stop-points endpoint
        stops_url = "https://prim.iledefrance-mobilites.fr/marketplace/stop-points"
        stop_params = {
            "BoundingBoxStructure.UpperLeft.Longitude": lon - 0.01,
            "BoundingBoxStructure.UpperLeft.Latitude": lat + 0.01,
            "BoundingBoxStructure.LowerRight.Longitude": lon + 0.01,
            "BoundingBoxStructure.LowerRight.Latitude": lat - 0.01
        }

        stops_response = requests.get(
            stops_url,
            headers=headers,
            params=stop_params
        )

        if stops_response.status_code == 200:
            stops_data = stops_response.json()

            # Process stops data
            if "StopPoints" in stops_data:
                for stop in stops_data["StopPoints"]:
                    stop_info = {
                        "id": stop.get("id", ""),
                        "name": stop.get("name", ""),
                        "type": stop.get("type", ""),
                        "coordinates": {
                            "lat": stop.get("Location", {}).get("Latitude", 0),
                            "lon": stop.get("Location", {}).get("Longitude", 0)
                        },
                        "lines": stop.get("lines", []),
                        "accessibility": stop.get("AccessibilityAssessment", {})
                    }

                    idfm_data["stops"].append(stop_info)

            print(f"Retrieved {len(idfm_data['stops'])} stops in La Défense area")
        else:
            print(f"Error retrieving stops: {stops_response.status_code}")

        # Step 3: Get real-time departures for stops in La Défense
        # Using stop-monitoring endpoint for each stop
        departures_url = "https://prim.iledefrance-mobilites.fr/marketplace/stop-monitoring"

        for stop in idfm_data["stops"]:
            stop_id = stop["id"]

            departure_params = {
                "MonitoringRef": stop_id,
                "MaximumStopVisits": 10  # Get up to 10 departures per stop
            }

            departures_response = requests.get(
                departures_url,
                headers=headers,
                params=departure_params
            )

            if departures_response.status_code == 200:
                departures_data = departures_response.json()

                # Process departures data
                if "ServiceDelivery" in departures_data and "StopMonitoringDelivery" in departures_data["ServiceDelivery"]:
                    visits = departures_data["ServiceDelivery"]["StopMonitoringDelivery"].get("MonitoredStopVisit", [])

                    for visit in visits:
                        if "MonitoredVehicleJourney" in visit:
                            journey = visit["MonitoredVehicleJourney"]

                            # Extract basic info
                            departure_info = {
                                "stop_id": stop_id,
                                "stop_name": stop["name"],
                                "line_id": journey.get("LineRef", {}).get("value", ""),
                                "line_name": journey.get("PublishedLineName", {}).get("value", ""),
                                "direction": journey.get("DirectionName", {}).get("value", ""),
                                "destination": journey.get("DestinationName", {}).get("value", ""),
                                "operator": journey.get("OperatorRef", {}).get("value", "")
                            }

                            # Extract timing info
                            if "MonitoredCall" in journey:
                                call = journey["MonitoredCall"]

                                expected_time = call.get("ExpectedDepartureTime", "")
                                aimed_time = call.get("AimedDepartureTime", "")

                                departure_info["expected_time"] = expected_time
                                departure_info["aimed_time"] = aimed_time
                                departure_info["is_realtime"] = expected_time != aimed_time

                                # Calculate delay in minutes if both times are available
                                if expected_time and aimed_time:
                                    expected_dt = datetime.fromisoformat(expected_time.replace('Z', '+00:00'))
                                    aimed_dt = datetime.fromisoformat(aimed_time.replace('Z', '+00:00'))
                                    delay_seconds = (expected_dt - aimed_dt).total_seconds()
                                    departure_info["delay_minutes"] = int(delay_seconds / 60)

                            idfm_data["departures"].append(departure_info)

                print(f"Retrieved {len(idfm_data['departures'])} departures for stop {stop['name']}")
            else:
                print(f"Error retrieving departures for stop {stop['name']}: {departures_response.status_code}")

        # Save to data lake
        s3_key = f"landing/transport/idfm_ladefense_{timestamp}.json"
        save_json_to_data_lake(bucket_name, s3_key, idfm_data)

        # Also save a latest version
        save_json_to_data_lake(bucket_name, "landing/transport/idfm_ladefense_latest.json", idfm_data)

        print(
            f"IDFM data extracted and saved: {len(idfm_data['departures'])} departures across {len(idfm_data['stops'])} stops with {len(idfm_data['traffic_status'])} traffic messages")
        return True

    except Exception as e:
        print(f"Error extracting IDFM data: {str(e)}")
        return False


if __name__ == "__main__":
    extract_idfm_data()