"""
Weather data processing script for La Défense
Transforms Visual Crossing weather data into analysis-ready formats
"""
import sys
import os
import pandas as pd
from datetime import datetime
import logging

# Add the parent directory to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from utils.data_lake_utils import read_json_from_data_lake, save_parquet_to_data_lake
from config.config import DATA_LAKE

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('process_weather')

def process_weather_data():
    """Process raw weather data from the landing zone to the refined zone"""
    bucket_name = DATA_LAKE["bucket_name"]

    # Get the latest weather data
    try:
        latest_key = "landing/weather/visual_crossing_latest.json"
        data = read_json_from_data_lake(bucket_name, latest_key)

        if not data:
            logger.error(f"No data found in {latest_key}")
            return False

        logger.info(f"Processing weather data from {latest_key}...")

        # Extract current conditions
        current = data.get("current_conditions", {})
        current_df = pd.DataFrame([{
            "timestamp": current.get("datetime", ""),
            "temperature": current.get("temp", None),
            "feels_like": current.get("feelslike", None),
            "humidity": current.get("humidity", None),
            "wind_speed": current.get("windspeed", None),
            "wind_direction": current.get("winddir", None),
            "pressure": current.get("pressure", None),
            "cloud_cover": current.get("cloudcover", None),
            "visibility": current.get("visibility", None),
            "uv_index": current.get("uvindex", None),
            "conditions": current.get("conditions", ""),
            "precipitation": current.get("precip", 0),
            "precipitation_probability": current.get("precipprob", 0),
            "extraction_time": data.get("extraction_time", "")
        }])

        # Extract daily forecast
        daily_records = []
        for day in data.get("days", []):
            daily_records.append({
                "date": day.get("datetime", ""),
                "temperature_max": day.get("tempmax", None),
                "temperature_min": day.get("tempmin", None),
                "temperature_avg": day.get("temp", None),
                "humidity": day.get("humidity", None),
                "precipitation": day.get("precip", 0),
                "precipitation_probability": day.get("precipprob", 0),
                "precipitation_type": ",".join(day.get("preciptype", [])) if day.get("preciptype") else "",
                "wind_speed": day.get("windspeed", None),
                "wind_direction": day.get("winddir", None),
                "conditions": day.get("conditions", ""),
                "description": day.get("description", ""),
                "extraction_time": data.get("extraction_time", "")
            })
        daily_df = pd.DataFrame(daily_records)

        # Extract hourly forecast
        hourly_records = []
        for day in data.get("days", []):
            for hour in day.get("hours", []):
                hourly_records.append({
                    "datetime": f"{day.get('datetime', '')}T{hour.get('datetime', '')}",
                    "temperature": hour.get("temp", None),
                    "feels_like": hour.get("feelslike", None),
                    "humidity": hour.get("humidity", None),
                    "precipitation": hour.get("precip", 0),
                    "precipitation_probability": hour.get("precipprob", 0),
                    "precipitation_type": ",".join(hour.get("preciptype", [])) if hour.get("preciptype") else "",
                    "wind_speed": hour.get("windspeed", None),
                    "wind_direction": hour.get("winddir", None),
                    "pressure": hour.get("pressure", None),
                    "cloud_cover": hour.get("cloudcover", None),
                    "visibility": hour.get("visibility", None),
                    "conditions": hour.get("conditions", ""),
                    "extraction_time": data.get("extraction_time", "")
                })
        hourly_df = pd.DataFrame(hourly_records)

        # Save processed data to refined zone
        timestamp = datetime.now().strftime("%Y%m%d")

        # Save current conditions
        current_key = f"refined/weather/current_{timestamp}.parquet"
        save_parquet_to_data_lake(bucket_name, current_key, current_df)
        logger.info(f"Saved current conditions to {current_key}")

        # Save daily forecast
        daily_key = f"refined/weather/daily_{timestamp}.parquet"
        save_parquet_to_data_lake(bucket_name, daily_key, daily_df)
        logger.info(f"Saved daily forecast to {daily_key}")

        # Save hourly forecast
        hourly_key = f"refined/weather/hourly_{timestamp}.parquet"
        save_parquet_to_data_lake(bucket_name, hourly_key, hourly_df)
        logger.info(f"Saved hourly forecast to {hourly_key}")

        # Also save the latest versions for easy access
        save_parquet_to_data_lake(bucket_name, "refined/weather/current_latest.parquet", current_df)
        save_parquet_to_data_lake(bucket_name, "refined/weather/daily_latest.parquet", daily_df)
        save_parquet_to_data_lake(bucket_name, "refined/weather/hourly_latest.parquet", hourly_df)

        logger.info("Weather data processing completed successfully")
        return True
    except Exception as e:
        logger.error(f"Error processing weather data: {str(e)}")
        return False


if __name__ == "__main__":
    process_weather_data()