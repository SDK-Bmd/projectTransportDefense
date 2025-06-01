"""
Transport data processing script for La Défense
Processes raw transport data and saves refined versions
"""
import pandas as pd
import json
from datetime import datetime
import sys
import os


from utils.data_lake_utils import get_s3_client, read_json_from_data_lake, save_parquet_to_data_lake, \
    read_parquet_from_data_lake
from configuration.config import DATA_LAKE


def process_schedules(schedules_data, transport_type, line):
    """
    Process schedule data from RATP API response

    Args:
        schedules_data: Raw schedule data from API
        transport_type: Type of transport (metro, rers, transilien, buses)
        line: Line number/letter

    Returns:
        pd.DataFrame: Processed schedules data
    """
    schedules_list = []

    try:
        # Handle different API response structures
        if isinstance(schedules_data, dict):
            # Check for RATP API structure
            result = schedules_data.get("result", {})

            if "schedules" in result:
                # RATP API format
                for schedule in result["schedules"]:
                    schedule_info = {
                        "extraction_time": datetime.now().isoformat(),
                        "transport_type": transport_type,
                        "line": line,
                        "station": result.get("station", ""),
                        "direction": schedule.get("direction", ""),
                        "destination": schedule.get("destination", ""),
                        "message": schedule.get("message", ""),
                        "code": schedule.get("code", "")
                    }
                    schedules_list.append(schedule_info)

            # Handle error cases
            elif "error" in schedules_data:
                print(f"Error in schedules data for {transport_type} {line}: {schedules_data['error']}")
                return pd.DataFrame()

        # Convert to DataFrame
        if schedules_list:
            return pd.DataFrame(schedules_list)
        else:
            # Return empty DataFrame with correct columns if no data
            return pd.DataFrame(columns=[
                "extraction_time", "transport_type", "line", "station",
                "direction", "destination", "message", "code"
            ])

    except Exception as e:
        print(f"Error processing schedules for {transport_type} {line}: {str(e)}")
        return pd.DataFrame()


def process_traffic_status(traffic_data, transport_type, line):
    """
    Process traffic status data from RATP API response

    Args:
        traffic_data: Raw traffic data from API
        transport_type: Type of transport (metro, rers, transilien, buses)
        line: Line number/letter

    Returns:
        pd.DataFrame: Processed traffic status data
    """
    traffic_list = []

    try:
        # Handle different API response structures
        if isinstance(traffic_data, dict):
            # Check for RATP API structure
            result = traffic_data.get("result", {})

            if "line" in result and "slug" in result:
                # RATP API format - single line status
                traffic_info = {
                    "extraction_time": datetime.now().isoformat(),
                    "transport_type": transport_type,
                    "line": line,
                    "slug": result.get("slug", ""),
                    "title": result.get("title", ""),
                    "message": result.get("message", ""),
                    "status": determine_status_from_message(result.get("message", ""))
                }
                traffic_list.append(traffic_info)

            # Handle multiple traffic reports
            elif isinstance(result, list):
                for traffic_item in result:
                    traffic_info = {
                        "extraction_time": datetime.now().isoformat(),
                        "transport_type": transport_type,
                        "line": line,
                        "slug": traffic_item.get("slug", ""),
                        "title": traffic_item.get("title", ""),
                        "message": traffic_item.get("message", ""),
                        "status": determine_status_from_message(traffic_item.get("message", ""))
                    }
                    traffic_list.append(traffic_info)

            # Handle error cases
            elif "error" in traffic_data:
                print(f"Error in traffic data for {transport_type} {line}: {traffic_data['error']}")
                return pd.DataFrame()

        # Convert to DataFrame
        if traffic_list:
            return pd.DataFrame(traffic_list)
        else:
            # Return empty DataFrame with correct columns if no data
            return pd.DataFrame(columns=[
                "extraction_time", "transport_type", "line", "slug",
                "title", "message", "status"
            ])

    except Exception as e:
        print(f"Error processing traffic status for {transport_type} {line}: {str(e)}")
        return pd.DataFrame()


def determine_status_from_message(message):
    """
    Determine traffic status based on message content

    Args:
        message: Traffic message string

    Returns:
        str: Status level (normal, minor, major, critical)
    """
    if not message:
        return "unknown"

    message_lower = message.lower()

    # Define keywords for different status levels
    critical_keywords = ["interrompu", "fermé", "suspendu", "bloqué", "arrêt total"]
    major_keywords = ["perturbé", "retard important", "ralenti", "incident"]
    minor_keywords = ["retard", "léger ralentissement", "travaux"]
    normal_keywords = ["normal", "reprise", "rétabli"]

    # Check for critical issues
    if any(keyword in message_lower for keyword in critical_keywords):
        return "critical"

    # Check for major issues
    elif any(keyword in message_lower for keyword in major_keywords):
        return "major"

    # Check for minor issues
    elif any(keyword in message_lower for keyword in minor_keywords):
        return "minor"

    # Check for normal service
    elif any(keyword in message_lower for keyword in normal_keywords):
        return "normal"

    # Default to normal if no keywords match
    else:
        return "normal"


def combine_all_transport_data():
    """
    Combine all processed transport data into unified datasets
    """
    bucket_name = DATA_LAKE["bucket_name"]

    all_schedules = []
    all_traffic = []

    # Define all transport types and lines
    transport_config = {
        "metro": ["1"],
        "rers": ["A", "E"],
        "transilien": ["L"],
        "buses": ["73", "144", "158", "163", "174", "178", "258", "262", "272", "275"]
    }

    for transport_type, lines in transport_config.items():
        for line in lines:
            try:
                # Load schedules
                schedules_key = f'refined/transport/{transport_type}_{line}_schedules_latest.parquet'
                line_schedules = read_parquet_from_data_lake(bucket_name, schedules_key)
                if not line_schedules.empty:
                    all_schedules.append(line_schedules)

                # Load traffic status
                traffic_key = f'refined/transport/{transport_type}_{line}_traffic_latest.parquet'
                line_traffic = read_parquet_from_data_lake(bucket_name, traffic_key)
                if not line_traffic.empty:
                    all_traffic.append(line_traffic)

            except Exception as e:
                print(f"Could not load data for {transport_type} {line}: {str(e)}")
                continue

    # Combine all data
    if all_schedules:
        combined_schedules = pd.concat(all_schedules, ignore_index=True)
        save_parquet_to_data_lake(
            bucket_name,
            "refined/transport/schedules_latest.parquet",
            combined_schedules
        )
        print(f"Combined schedules saved: {len(combined_schedules)} records")

    if all_traffic:
        combined_traffic = pd.concat(all_traffic, ignore_index=True)
        save_parquet_to_data_lake(
            bucket_name,
            "refined/transport/traffic_latest.parquet",
            combined_traffic
        )
        print(f"Combined traffic status saved: {len(combined_traffic)} records")


def process_transport_data():
    """Process transport data for all transport types"""
    bucket_name = DATA_LAKE["bucket_name"]

    # Define all transport types and lines (updated with new ones)
    transport_config = {
        "metro": ["1"],
        "rers": ["A", "E"],  # Added E
        "transilien": ["L"],  # NEW
        "buses": ["73", "144", "158", "163", "174", "178", "258", "262", "272", "275"]  # NEW
    }

    print("Processing transport data for all lines...")

    for transport_type, lines in transport_config.items():
        print(f"Processing {transport_type} lines: {', '.join(lines)}")

        for line in lines:
            try:
                # Read raw data
                latest_key = f"landing/transport/{transport_type}_{line}_latest.json"
                data = read_json_from_data_lake(bucket_name, latest_key)

                if data:
                    print(f"Processing {transport_type} {line}...")

                    # Process schedules
                    schedules_df = process_schedules(
                        data.get("schedules", {}),
                        transport_type,
                        line
                    )

                    # Process traffic status
                    traffic_df = process_traffic_status(
                        data.get("traffic", {}),
                        transport_type,
                        line
                    )

                    # Save processed data
                    if not schedules_df.empty:
                        schedules_key = f"refined/transport/{transport_type}_{line}_schedules_latest.parquet"
                        save_parquet_to_data_lake(bucket_name, schedules_key, schedules_df)
                        print(f"  Saved {len(schedules_df)} schedule records")

                    if not traffic_df.empty:
                        traffic_key = f"refined/transport/{transport_type}_{line}_traffic_latest.parquet"
                        save_parquet_to_data_lake(bucket_name, traffic_key, traffic_df)
                        print(f"  Saved {len(traffic_df)} traffic status records")

                else:
                    print(f"No data found for {transport_type} {line}")

            except Exception as e:
                print(f"Error processing {transport_type} {line}: {str(e)}")
                continue

    # Combine all processed data
    print("Combining all transport data...")
    combine_all_transport_data()

    print("Transport data processing completed!")


if __name__ == "__main__":
    process_transport_data()