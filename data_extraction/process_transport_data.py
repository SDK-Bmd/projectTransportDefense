"""
Transport data processing script for La Défense
Transforms raw transportation data into analysis-ready formats
"""
import boto3
import json
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
import os
import config
from botocore.client import Config


def get_s3_client():
    """Create and return an S3 client connected to MinIO"""
    return boto3.client(
        's3',
        endpoint_url=config.DATA_LAKE["endpoint_url"],
        aws_access_key_id=config.DATA_LAKE["access_key"],
        aws_secret_access_key=config.DATA_LAKE["secret_key"],
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )


def process_transport_data():
    """Process transportation data from the landing zone"""
    s3 = get_s3_client()
    bucket_name = config.DATA_LAKE["bucket_name"]

    # Get the latest data for each transport type
    transport_types = {
        "metro_1": "landing/transport/metro_1_latest.json",
        "rer_A": "landing/transport/rers_A_latest.json",
        "tram_2": "landing/transport/tramways_2_latest.json"
    }

    schedules_data = []
    traffic_data = []

    # Process each transport type
    for transport_name, file_key in transport_types.items():
        try:
            # Get the data
            response = s3.get_object(Bucket=bucket_name, Key=file_key)
            content = response['Body'].read().decode('utf-8')
            data = json.loads(content)

            # Extract transport type information
            transport_type = data.get("transport_type", "")
            line = data.get("line", "")
            station = data.get("station", "")
            extraction_time = data.get("extraction_time", "")

            # Process schedules data
            if "schedules" in data and "result" in data["schedules"]:
                for direction in data["schedules"]["result"].get("schedules", []):
                    direction_name = direction.get("destination", "")
                    for schedule in direction.get("schedules", []):
                        schedule_entry = {
                            "extraction_time": extraction_time,
                            "transport_type": transport_type,
                            "line": line,
                            "station": station.replace("+", " "),
                            "direction": direction_name,
                            "message": schedule.get("message", ""),
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                        schedules_data.append(schedule_entry)

            # Process traffic data
            if "traffic" in data and "result" in data["traffic"]:
                traffic_info = data["traffic"]["result"]
                traffic_entry = {
                    "extraction_time": extraction_time,
                    "transport_type": transport_type,
                    "line": line,
                    "title": traffic_info.get("title", ""),
                    "message": traffic_info.get("message", ""),
                    "status": traffic_info.get("slug", ""),
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                traffic_data.append(traffic_entry)

            print(f"Processed {transport_name} data")

        except Exception as e:
            print(f"Error processing {transport_name}: {str(e)}")

    # Create DataFrames
    schedules_df = pd.DataFrame(schedules_data)
    traffic_df = pd.DataFrame(traffic_data)

    # Save to refined zone
    timestamp = datetime.now().strftime("%Y%m%d")

    # Save schedules
    if not schedules_df.empty:
        schedules_bytes = BytesIO()
        schedules_df.to_parquet(schedules_bytes)
        schedules_key = f"refined/transport/schedules_{timestamp}.parquet"
        s3.put_object(
            Bucket=bucket_name,
            Key=schedules_key,
            Body=schedules_bytes.getvalue()
        )

        # Also save latest
        latest_schedules_bytes = BytesIO()
        schedules_df.to_parquet(latest_schedules_bytes)
        s3.put_object(
            Bucket=bucket_name,
            Key="refined/transport/schedules_latest.parquet",
            Body=latest_schedules_bytes.getvalue()
        )

        print(f"Saved {len(schedules_df)} schedule entries to {schedules_key}")
    else:
        print("No schedule data to save")

    # Save traffic
    if not traffic_df.empty:
        traffic_bytes = BytesIO()
        traffic_df.to_parquet(traffic_bytes)
        traffic_key = f"refined/transport/traffic_{timestamp}.parquet"
        s3.put_object(
            Bucket=bucket_name,
            Key=traffic_key,
            Body=traffic_bytes.getvalue()
        )

        # Also save latest
        latest_traffic_bytes = BytesIO()
        traffic_df.to_parquet(latest_traffic_bytes)
        s3.put_object(
            Bucket=bucket_name,
            Key="refined/transport/traffic_latest.parquet",
            Body=latest_traffic_bytes.getvalue()
        )

        print(f"Saved {len(traffic_df)} traffic entries to {traffic_key}")
    else:
        print("No traffic data to save")

    return True


if __name__ == "__main__":
    process_transport_data()