"""
Weather data extraction script using Visual Crossing Weather API
"""
import requests
import json
from datetime import datetime, timedelta
import os
import boto3
from botocore.client import Config
from dotenv import load_dotenv
from config import config

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


def extract_visual_crossing_data():
    """Extract weather data from Visual Crossing API and store in data lake"""
    # Get API key from environment variables
    api_key = os.getenv("VISUAL_CROSSING_API_KEY")

    if not api_key:
        print("Error: Visual Crossing API key not found in environment variables")
        print("Please add VISUAL_CROSSING_API_KEY=your_key to your .env file")
        return False

    # Configuration
    s3 = get_s3_client()
    bucket_name = config.DATA_LAKE["bucket_name"]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Coordinates for La Défense
    lat, lon = config.LADEFENSE_COORDINATES["lat"], config.LADEFENSE_COORDINATES["lon"]
    location = "La Défense, Paris, France"

    # Get current date and time
    current_date = datetime.now()

    # For forecast: current date + 7 days
    end_date = (current_date + timedelta(days=7)).strftime("%Y-%m-%d")

    # For historical: past 3 days + current date
    start_date = (current_date - timedelta(days=3)).strftime("%Y-%m-%d")
    current_date_str = current_date.strftime("%Y-%m-%d")

    # Base URL for Visual Crossing Timeline API
    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{location}/{start_date}/{end_date}"

    # Parameters for the request
    params = {
        "key": api_key,
        "unitGroup": "metric",  # Use metric units
        "include": "days,hours,current,alerts",  # Include daily, hourly data, current conditions, and alerts
        "contentType": "json",
        "elements": "datetime,temp,feelslike,humidity,precip,precipprob,preciptype,windspeed,winddir,pressure,cloudcover,visibility,uvindex,conditions,description",
        "locations": f"{lat},{lon}"  # Specific coordinates for La Défense
    }

    try:
        # Make the API request
        print(f"Fetching weather data for La Défense from Visual Crossing...")
        response = requests.get(url, params=params)

        if response.status_code == 200:
            data = response.json()

            # Enhance the data with extraction metadata
            weather_data = {
                "extraction_time": datetime.now().isoformat(),
                "source": "Visual Crossing Weather API",
                "location": data.get("resolvedAddress", location),
                "coordinates": {
                    "lat": data.get("latitude", lat),
                    "lon": data.get("longitude", lon)
                },
                "timezone": data.get("timezone"),
                "current_conditions": data.get("currentConditions", {}),
                "days": data.get("days", []),
                "alerts": data.get("alerts", [])
            }

            # Save to data lake
            s3_key = f"landing/weather/visual_crossing_{timestamp}.json"
            s3.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=json.dumps(weather_data)
            )

            # Also save a "latest" version for easy access
            s3_latest_key = "landing/weather/visual_crossing_latest.json"
            s3.put_object(
                Bucket=bucket_name,
                Key=s3_latest_key,
                Body=json.dumps(weather_data)
            )

            print(f"Weather data successfully extracted and saved to {s3_key}")
            return True
        else:
            print(f"Error accessing Visual Crossing API: {response.status_code}")
            if response.status_code == 429:
                print("You have exceeded your API request quota. Check your subscription limits.")
            elif response.status_code == 401:
                print("Unauthorized: Please check your API key.")
            else:
                print(f"Response content: {response.text}")
            return False
    except Exception as e:
        print(f"Exception when accessing Visual Crossing API: {str(e)}")
        return False


if __name__ == "__main__":
    # Check if .env file exists
    if not os.path.exists('.env'):
        with open('.env', 'a') as f:
            f.write("# Add your Visual Crossing API key here\n")
            f.write("VISUAL_CROSSING_API_KEY=your_key_here\n")
        print("Created .env file. Please add your Visual Crossing API key.")

    # Run the extraction
    extract_visual_crossing_data()