
"""
Combined station data processor for La Défense
Merges station data from multiple sources into a unified dataset
"""
import boto3
import json
import pandas as pd
from io import BytesIO
from datetime import datetime
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


def process_combined_station_data():
    """Process and combine station data from multiple sources into a unified dataset"""
    s3 = get_s3_client()
    bucket_name = config.DATA_LAKE["bucket_name"]
    timestamp = datetime.now().strftime("%Y%m%d")

    # Sources to combine (using the latest version of each)
    sources = [
        "landing/stations/ratp_stations_latest.json",
        "landing/stations/osm_enhanced_latest.json"
    ]

    # Combined data structure
    combined_data = {
        "processing_time": datetime.now().isoformat(),
        "sources": [],
        "stations": []
    }

    # Station mapping to avoid duplicates (by name similarity)
    station_map = {}

    try:
        # Process each source
        for source_key in sources:
            try:
                # Get the source data
                response = s3.get_object(Bucket=bucket_name, Key=source_key)
                content = response['Body'].read().decode('utf-8')
                source_data = json.loads(content)

                # Record the source
                source_info = {
                    "key": source_key,
                    "name": source_data.get("source", source_key.split("/")[-1]),
                    "extraction_time": source_data.get("extraction_time", "")
                }
                combined_data["sources"].append(source_info)

                # Process stations from this source
                if "stations" in source_data:
                    for station in source_data["stations"]:
                        station_name = station.get("name", "").lower()
                        if not station_name:
                            continue

                        # Check if we already have this station
                        found = False
                        for key in station_map:
                            # Simple name matching (could be improved with fuzzy matching)
                            if "defense" in station_name and "defense" in key:
                                found = True
                                station_idx = station_map[key]

                                # Merge with existing station data
                                existing = combined_data["stations"][station_idx]

                                # Add source reference
                                if "sources" not in existing:
                                    existing["sources"] = []
                                existing["sources"].append(source_info["name"])

                                # Merge coordinates if not present
                                if "coordinates" not in existing and "coordinates" in station:
                                    existing["coordinates"] = station["coordinates"]

                                # Merge accessibility data
                                if "accessibility" in station:
                                    if "accessibility" not in existing:
                                        existing["accessibility"] = {}
                                    existing["accessibility"].update(station["accessibility"])

                                # Merge equipment data
                                if "equipment" in station:
                                    if "equipment" not in existing:
                                        existing["equipment"] = {}
                                    existing["equipment"].update(station["equipment"])

                                # Merge routes/lines data
                                if "routes" in station or "lines" in station:
                                    if "routes" not in existing:
                                        existing["routes"] = []
                                    routes_to_add = station.get("routes", []) or station.get("lines", [])
                                    if isinstance(routes_to_add, list):
                                        existing["routes"].extend(routes_to_add)

                                # Add any additional fields not already present
                                for key, value in station.items():
                                    if key not in existing and key not in ["name", "sources", "coordinates",
                                                                           "accessibility", "equipment", "routes",
                                                                           "lines"]:
                                        existing[key] = value

                                break

                        if not found:
                            # Add as a new station
                            new_station = dict(station)
                            if "sources" not in new_station:
                                new_station["sources"] = []
                            new_station["sources"].append(source_info["name"])

                            # Add to combined data
                            station_map[station_name] = len(combined_data["stations"])
                            combined_data["stations"].append(new_station)

                # Process additional data (entrances, platforms, amenities)
                for data_type in ["entrances", "platforms", "amenities"]:
                    if data_type in source_data and len(source_data[data_type]) > 0:
                        # Find La Défense station(s) to attach this data to
                        for item in source_data[data_type]:
                            # Try to find a related station
                            found = False
                            for station_idx, station in enumerate(combined_data["stations"]):
                                station_name = station.get("name", "").lower()

                                if "defense" in station_name:
                                    # Add the item to the station
                                    if data_type not in station:
                                        station[data_type] = []
                                    station[data_type].append(item)
                                    found = True
                                    break

                            # If no match found, add to all La Défense stations
                            if not found:
                                for station in combined_data["stations"]:
                                    station_name = station.get("name", "").lower()
                                    if "defense" in station_name:
                                        if data_type not in station:
                                            station[data_type] = []
                                        station[data_type].append(item)

                print(f"Processed source: {source_key}")

            except Exception as e:
                print(f"Error processing source {source_key}: {str(e)}")

        # Deduplicate and clean up
        for station in combined_data["stations"]:
            # Deduplicate routes
            if "routes" in station and isinstance(station["routes"], list):
                unique_routes = []
                route_ids = set()
                for route in station["routes"]:
                    if isinstance(route, dict) and "id" in route:
                        if route["id"] not in route_ids:
                            route_ids.add(route["id"])
                            unique_routes.append(route)
                    elif isinstance(route, str) and route not in route_ids:
                        route_ids.add(route)
                        unique_routes.append(route)
                station["routes"] = unique_routes

            # Deduplicate sources
            if "sources" in station:
                station["sources"] = list(set(station["sources"]))

        print(f"Combined data contains {len(combined_data['stations'])} unique stations")

        # Save both JSON and Parquet formats
        # 1. JSON for full data
        json_key = f"refined/stations/combined_stations_{timestamp}.json"
        s3.put_object(
            Bucket=bucket_name,
            Key=json_key,
            Body=json.dumps(combined_data)
        )

        # Also save latest version
        s3.put_object(
            Bucket=bucket_name,
            Key="refined/stations/combined_stations_latest.json",
            Body=json.dumps(combined_data)
        )

        # 2. Parquet for easier analysis
        # Flatten the stations to a dataframe
        stations_flat = []
        for station in combined_data["stations"]:
            flat_station = {
                "name": station.get("name", ""),
                "id": station.get("id", ""),
                "type": station.get("type", ""),
                "lat": station.get("coordinates", {}).get("lat", 0),
                "lon": station.get("coordinates", {}).get("lon", 0),
                "wheelchair_accessible": station.get("accessibility", {}).get("wheelchair", "unknown"),
                "elevator_available": "yes" if station.get("equipment", {}).get("elevators", {}).get("count",
                                                                                                     0) > 0 else "no",
                "escalator_available": "yes" if station.get("equipment", {}).get("escalators", {}).get("count",
                                                                                                       0) > 0 else "no",
                "num_lines": len(station.get("routes", [])),
                "lines": ";".join([str(r.get("short_name", r)) if isinstance(r, dict) else str(r)
                                   for r in station.get("routes", [])]),
                "num_entrances": len(station.get("entrances", [])),
                "num_platforms": len(station.get("platforms", [])),
                "data_sources": ";".join(station.get("sources", []))
            }
            stations_flat.append(flat_station)

        stations_df = pd.DataFrame(stations_flat)

        # Save as parquet
        parquet_bytes = BytesIO()
        stations_df.to_parquet(parquet_bytes)

        parquet_key = f"refined/stations/combined_stations_{timestamp}.parquet"
        s3.put_object(
            Bucket=bucket_name,
            Key=parquet_key,
            Body=parquet_bytes.getvalue()
        )

        # Also save latest version
        latest_parquet_bytes = BytesIO()
        stations_df.to_parquet(latest_parquet_bytes)
        s3.put_object(
            Bucket=bucket_name,
            Key="refined/stations/combined_stations_latest.parquet",
            Body=latest_parquet_bytes.getvalue()
        )

        print(f"Combined station data processed and saved to {json_key} and {parquet_key}")
        return True

    except Exception as e:
        print(f"Error processing combined station data: {str(e)}")
        return False


if __name__ == "__main__":
    process_combined_station_data()