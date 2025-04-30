"""
Weather data processing script for La Défense
Transforms Visual Crossing weather data into analysis-ready formats
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


def process_weather_data():
    """Process raw weather data from the landing zone to the refined zone"""
    s3 = get_s3_client()
    bucket_name = config.DATA_LAKE["bucket_name"]

    # Get the latest weather data
    try:
        latest_key = "landing/weather/visual_crossing_latest.json"
        response = s3.get_object(Bucket=bucket_name, Key=latest_key)
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)

        print(f"Processing weather data from {latest_key}...")

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
        current_bytes = BytesIO()
        current_df.to_parquet(current_bytes)
        current_key = f"refined/weather/current_{timestamp}.parquet"
        s3.put_object(
            Bucket=bucket_name,
            Key=current_key,
            Body=current_bytes.getvalue()
        )
        print(f"Saved current conditions to {current_key}")

        # Save daily forecast
        daily_bytes = BytesIO()
        daily_df.to_parquet(daily_bytes)
        daily_key = f"refined/weather/daily_{timestamp}.parquet"
        s3.put_object(
            Bucket=bucket_name,
            Key=daily_key,
            Body=daily_bytes.getvalue()
        )
        print(f"Saved daily forecast to {daily_key}")

        # Save hourly forecast
        hourly_bytes = BytesIO()
        hourly_df.to_parquet(hourly_bytes)
        hourly_key = f"refined/weather/hourly_{timestamp}.parquet"
        s3.put_object(
            Bucket=bucket_name,
            Key=hourly_key,
            Body=hourly_bytes.getvalue()
        )
        print(f"Saved hourly forecast to {hourly_key}")

        # Also save the latest versions for easy access
        current_latest_bytes = BytesIO()
        current_df.to_parquet(current_latest_bytes)
        s3.put_object(
            Bucket=bucket_name,
            Key="refined/weather/current_latest.parquet",
            Body=current_latest_bytes.getvalue()
        )

        daily_latest_bytes = BytesIO()
        daily_df.to_parquet(daily_latest_bytes)
        s3.put_object(
            Bucket=bucket_name,
            Key="refined/weather/daily_latest.parquet",
            Body=daily_latest_bytes.getvalue()
        )

        hourly_latest_bytes = BytesIO()
        hourly_df.to_parquet(hourly_latest_bytes)
        s3.put_object(
            Bucket=bucket_name,
            Key="refined/weather/hourly_latest.parquet",
            Body=hourly_latest_bytes.getvalue()
        )

        return True
    except Exception as e:
        print(f"Error processing weather data: {str(e)}")
        return False


if __name__ == "__main__":
    process_weather_data()