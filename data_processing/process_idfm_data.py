"""
IDFM data processing script for La Défense
Processes raw IDFM data and saves refined versions
"""
import pandas as pd
import json
from datetime import datetime



from utils.data_lake_utils import get_s3_client, read_json_from_data_lake, save_parquet_to_data_lake
from configuration.config import DATA_LAKE


def process_idfm_stops(stops_data):
    """
    Process IDFM stops data

    Args:
        stops_data: List of stop data from IDFM API

    Returns:
        pd.DataFrame: Processed stops data
    """
    stops_list = []

    try:
        for stop in stops_data:
            stop_info = {
                "id": stop.get("id", ""),
                "name": stop.get("name", ""),
                "type": stop.get("type", ""),
                "lat": stop.get("coordinates", {}).get("lat", 0),
                "lon": stop.get("coordinates", {}).get("lon", 0),
                "lines": ", ".join(stop.get("lines", [])),
                "wheelchair_accessible": stop.get("accessibility", {}).get("wheelchairAccessible", "unknown"),
                "elevator_available": "unknown",  # IDFM might not have this info
                "escalator_available": "unknown",
                "extraction_time": datetime.now().isoformat()
            }
            stops_list.append(stop_info)

        return pd.DataFrame(stops_list) if stops_list else pd.DataFrame()

    except Exception as e:
        print(f"Error processing IDFM stops: {str(e)}")
        return pd.DataFrame()


def process_idfm_departures(departures_data):
    """
    Process IDFM departures data

    Args:
        departures_data: List of departure data from IDFM API

    Returns:
        pd.DataFrame: Processed departures data
    """
    departures_list = []

    try:
        for departure in departures_data:
            departure_info = {
                "extraction_time": datetime.now().isoformat(),
                "stop_id": departure.get("stop_id", ""),
                "stop_name": departure.get("stop_name", ""),
                "transport_type": "idfm",  # Generic type for IDFM data
                "line": departure.get("line_name", ""),
                "direction": departure.get("direction", ""),
                "destination": departure.get("destination", ""),
                "expected_time": departure.get("expected_time", ""),
                "aimed_time": departure.get("aimed_time", ""),
                "is_realtime": departure.get("is_realtime", False),
                "delay_minutes": departure.get("delay_minutes", 0),
                "operator": departure.get("operator", ""),
                "message": f"Direction {departure.get('direction', '')} - {departure.get('destination', '')}"
            }
            departures_list.append(departure_info)

        return pd.DataFrame(departures_list) if departures_list else pd.DataFrame()

    except Exception as e:
        print(f"Error processing IDFM departures: {str(e)}")
        return pd.DataFrame()


def process_idfm_traffic_status(traffic_data):
    """
    Process IDFM traffic status data

    Args:
        traffic_data: List of traffic status data from IDFM API

    Returns:
        pd.DataFrame: Processed traffic status data
    """
    traffic_list = []

    try:
        for traffic_msg in traffic_data:
            # Determine severity level
            severity = traffic_msg.get("severity", "").lower()
            if severity in ["high", "critical"]:
                status = "critical"
            elif severity in ["medium", "moderate"]:
                status = "major"
            elif severity in ["low", "minor"]:
                status = "minor"
            else:
                status = "normal"

            traffic_info = {
                "extraction_time": datetime.now().isoformat(),
                "transport_type": "idfm",
                "line": ", ".join(traffic_msg.get("affected_lines", [])),
                "title": traffic_msg.get("title", ""),
                "message": traffic_msg.get("message", ""),
                "status": status,
                "severity": traffic_msg.get("severity", ""),
                "start_time": traffic_msg.get("start_time", ""),
                "valid_until": traffic_msg.get("valid_until", ""),
                "affected_lines": ", ".join(traffic_msg.get("affected_lines", [])),
                "affected_stops": ", ".join(traffic_msg.get("affected_stops", []))
            }
            traffic_list.append(traffic_info)

        return pd.DataFrame(traffic_list) if traffic_list else pd.DataFrame()

    except Exception as e:
        print(f"Error processing IDFM traffic status: {str(e)}")
        return pd.DataFrame()


def process_idfm_data():
    """Process IDFM data and save refined versions"""
    bucket_name = DATA_LAKE["bucket_name"]

    print("Processing IDFM data...")

    try:
        # Read raw IDFM data
        idfm_data = read_json_from_data_lake(bucket_name, "landing/transport/idfm_ladefense_latest.json")

        if not idfm_data:
            print("No IDFM data found")
            return

        print(f"Processing IDFM data extracted at: {idfm_data.get('extraction_time')}")

        # Process stops
        stops_data = idfm_data.get("stops", [])
        if stops_data:
            stops_df = process_idfm_stops(stops_data)
            if not stops_df.empty:
                save_parquet_to_data_lake(
                    bucket_name,
                    "refined/stations/idfm_stops_latest.parquet",
                    stops_df
                )
                print(f"Processed {len(stops_df)} IDFM stops")

        # Process departures
        departures_data = idfm_data.get("departures", [])
        if departures_data:
            departures_df = process_idfm_departures(departures_data)
            if not departures_df.empty:
                save_parquet_to_data_lake(
                    bucket_name,
                    "refined/transport/idfm_schedules_latest.parquet",
                    departures_df
                )
                print(f"Processed {len(departures_df)} IDFM departures")

        # Process traffic status
        traffic_data = idfm_data.get("traffic_status", [])
        if traffic_data:
            traffic_df = process_idfm_traffic_status(traffic_data)
            if not traffic_df.empty:
                save_parquet_to_data_lake(
                    bucket_name,
                    "refined/transport/idfm_traffic_latest.parquet",
                    traffic_df
                )
                print(f"Processed {len(traffic_df)} IDFM traffic messages")

        print("IDFM data processing completed!")

    except Exception as e:
        print(f"Error processing IDFM data: {str(e)}")


if __name__ == "__main__":
    process_idfm_data()