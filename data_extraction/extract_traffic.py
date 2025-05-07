"""
Traffic data extraction script for La Défense area
"""
import json
from datetime import datetime
import os
import boto3
from botocore.client import Config
from dotenv import load_dotenv
from config import config
from api_utils import get_with_retries, API_ENDPOINTS

# Load environment variables
load_dotenv()

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

def extract_traffic_data():
    """Extract traffic data for La Défense area"""
    # Configuration
    s3 = get_s3_client()
    bucket_name = config.DATA_LAKE["bucket_name"]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # TomTom API key
    tomtom_api_key = os.getenv("TOMTOM_API_KEY")

    # Coordinates for La Défense
    lat, lon = config.LADEFENSE_COORDINATES["lat"], config.LADEFENSE_COORDINATES["lon"]
    bbox = config.LADEFENSE_COORDINATES["bbox"]

    # Data to collect
    traffic_data = {
        "extraction_time": datetime.now().isoformat(),
        "location": "La Défense",
        "coordinates": {"lat": lat, "lon": lon},
        "sources": []
    }

    # 1. TomTom data (if API key is available)
    if tomtom_api_key:
        try:
            # Traffic flow data
            flow_url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?point={lat},{lon}&unit=KMPH&openLr=false&key={tomtom_api_key}"

            # Incidents data
            incidents_url = f"https://api.tomtom.com/traffic/services/5/incidentDetails?bbox={bbox['min_lon']},{bbox['min_lat']},{bbox['max_lon']},{bbox['max_lat']}&fields={{incidents{{type,geometry{{type,coordinates}},properties{{iconCategory,magnitudeOfDelay,events{{description,code}},startTime,endTime,from,to,length,delay,roadNumbers,timeValidity}}}}&language=fr-FR&timeValidityFilter=present&key={tomtom_api_key}"

            # Get flow data with retry logic
            flow_response = get_with_retries(flow_url, max_retries=3)
            if flow_response and flow_response.status_code == 200:
                traffic_data["tomtom_flow"] = flow_response.json()
                traffic_data["sources"].append("TomTom Flow")

            # Get incidents data with retry logic
            incidents_response = get_with_retries(incidents_url, max_retries=3)
            if incidents_response and incidents_response.status_code == 200:
                traffic_data["tomtom_incidents"] = incidents_response.json()
                if "TomTom Flow" not in traffic_data["sources"]:
                    traffic_data["sources"].append("TomTom")

            print("TomTom traffic data retrieved successfully")
        except Exception as e:
            print(f"Error retrieving TomTom traffic data: {str(e)}")
    else:
        print("No TomTom API key found. Skipping TomTom traffic data.")

    # 2. Sytadin data (with updated URL)
    try:
        # Using the updated Sytadin URL from API_ENDPOINTS
        sytadin_url = API_ENDPOINTS['sytadin_traffic']

        # Get Sytadin data with retry logic
        sytadin_response = get_with_retries(sytadin_url, max_retries=3)

        if sytadin_response and sytadin_response.status_code == 200:
            try:
                # Try to parse as JSON
                data = sytadin_response.json()
                traffic_data["sytadin"] = data
                traffic_data["sources"].append("Sytadin")
                print("Sytadin traffic data retrieved successfully")
            except ValueError:
                # If not JSON, it might be HTML or another format
                print("Sytadin response is not in JSON format")
                traffic_data["sytadin_error"] = "Response not in expected format"
        else:
            print(f"Error retrieving Sytadin traffic data: {sytadin_response.status_code if sytadin_response else 'No response'}")
    except Exception as e:
        print(f"Error processing public traffic data: {str(e)}")

    # Save to data lake
    s3_key = f"landing/traffic/traffic_ladefense_{timestamp}.json"
    s3.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=json.dumps(traffic_data)
    )

    # Also save a latest version
    s3.put_object(
        Bucket=bucket_name,
        Key="landing/traffic/traffic_ladefense_latest.json",
        Body=json.dumps(traffic_data)
    )

    print(f"Traffic data extracted and saved to {s3_key}")
    return True

if __name__ == "__main__":
    extract_traffic_data()