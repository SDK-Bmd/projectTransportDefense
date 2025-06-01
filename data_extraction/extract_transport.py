"""
Transport data extraction script for La Défense
"""
import json
from datetime import datetime
import boto3
from botocore.client import Config
from configuration import config
from api_utils import get_with_retries, API_ENDPOINTS  # Import the new API utilities

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

def extract_ratp_transport_data():
    """Extract transport data from RATP API for La Défense stations"""
    # Configuration
    s3 = get_s3_client()
    bucket_name = config.DATA_LAKE["bucket_name"]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # EXPANDED STATIONS CONFIGURATION
    stations = {
        "metro": [
            {"line": "1", "station": "la+defense"}
        ],
        "rers": [
            {"line": "A", "station": "la+defense"},
            {"line": "E", "station": "la+defense"}  # NEW
        ],
        "transilien": [
            {"line": "L", "station": "la+defense"}  # NEW
        ],
        "buses": [  # NEW SECTION
            {"line": "73", "station": "grande+arche"},
            {"line": "144", "station": "la+defense"},
            {"line": "158", "station": "esplanade+de+la+defense"},
            {"line": "163", "station": "cnit"},
            {"line": "174", "station": "la+defense"},
            {"line": "178", "station": "quatre+temps"},
            {"line": "258", "station": "pont+de+neuilly"},
            {"line": "262", "station": "la+defense"},
            {"line": "272", "station": "les+bergeries"},
            {"line": "275", "station": "la+defense"}
        ]
    }

    # Extract data by transport type
    for transport_type, stations_list in stations.items():
        for station_info in stations_list:
            line = station_info["line"]
            station = station_info["station"]

            # API endpoints using the constants from api_utils
            schedules_url = API_ENDPOINTS['ratp_schedules'].format(
                type=transport_type, line=line, station=station
            )
            traffic_url = API_ENDPOINTS['ratp_traffic'].format(
                type=transport_type, line=line
            )

            try:
                # Get schedule data with retry logic
                schedules_response = get_with_retries(schedules_url, max_retries=3)
                if schedules_response and schedules_response.status_code == 200:
                    schedules_data = schedules_response.json()
                else:
                    schedules_data = {"error": f"Failed to retrieve schedules (status: {schedules_response.status_code if schedules_response else 'No response'})"}

                # Get traffic data with retry logic
                traffic_response = get_with_retries(traffic_url, max_retries=3)
                if traffic_response and traffic_response.status_code == 200:
                    traffic_data = traffic_response.json()
                else:
                    traffic_data = {"error": f"Failed to retrieve traffic (status: {traffic_response.status_code if traffic_response else 'No response'})"}

                # Combine data with metadata
                combined_data = {
                    "extraction_time": datetime.now().isoformat(),
                    "transport_type": transport_type,
                    "line": line,
                    "station": station,
                    "schedules": schedules_data,
                    "traffic": traffic_data
                }

                # Save to data lake
                s3_key = f"landing/transport/{transport_type}_{line}_{timestamp}.json"
                s3.put_object(
                    Bucket=bucket_name,
                    Key=s3_key,
                    Body=json.dumps(combined_data)
                )

                # Also save a latest version
                latest_key = f"landing/transport/{transport_type}_{line}_latest.json"
                s3.put_object(
                    Bucket=bucket_name,
                    Key=latest_key,
                    Body=json.dumps(combined_data)
                )

                print(f"Transport data extracted and saved for {transport_type} {line} at La Défense")

            except Exception as e:
                print(f"Error extracting data for {transport_type} {line}: {str(e)}")

if __name__ == "__main__":
    extract_ratp_transport_data()