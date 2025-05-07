"""
Basic data quality monitoring for La Défense mobility data lake
Performs essential checks on data completeness and structure
"""
import json
import logging
from datetime import datetime
import os
import sys

# Add the parent directory to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from configuration.config import DATA_LAKE
from utils.data_lake_utils import check_file_exists, read_json_from_data_lake

# Set up logging
logging.basicConfig(
    filename=os.path.join(parent_dir, 'data_quality.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('data_quality')

def check_json_structure(bucket, key, required_fields):
    """Check if a JSON file has the required fields"""
    data = read_json_from_data_lake(bucket, key)

    if not data:
        logger.error(f"❌ Error reading JSON: {key}")
        return False

    missing_fields = []
    for field in required_fields:
        if field not in data:
            missing_fields.append(field)

    if not missing_fields:
        logger.info(f"✅ JSON structure valid: {key}")
        return True
    else:
        logger.error(f"❌ JSON missing fields: {key} - {', '.join(missing_fields)}")
        return False


def run_basic_checks():
    """Run basic quality checks on the data lake"""
    bucket = DATA_LAKE["bucket_name"]
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"Running data quality checks at {timestamp}...")
    logger.info(f"Starting data quality check run at {timestamp}")

    # List of checks with their status
    checks = []

    # Weather data checks
    weather_exists = check_file_exists(bucket, "landing/weather/visual_crossing_latest.json")
    checks.append(("Weather data exists", weather_exists))

    if weather_exists:
        weather_structure = check_json_structure(
            bucket,
            "landing/weather/visual_crossing_latest.json",
            ["extraction_time", "source", "location", "coordinates", "current_conditions", "days"]
        )
        checks.append(("Weather data structure", weather_structure))

    # Transport data checks - RATP Source
    transport_types = {
        "metro": "1",
        "rers": "A",
        "tramways": "2"
    }

    for transport_type, line in transport_types.items():
        key = f"landing/transport/{transport_type}_{line}_latest.json"
        transport_exists = check_file_exists(bucket, key)
        checks.append((f"{transport_type.capitalize()} {line} data exists", transport_exists))

        if transport_exists:
            transport_structure = check_json_structure(
                bucket,
                key,
                ["extraction_time", "transport_type", "line", "station", "schedules", "traffic"]
            )
            checks.append((f"{transport_type.capitalize()} {line} data structure", transport_structure))

    # IDFM data checks
    idfm_exists = check_file_exists(bucket, "landing/transport/idfm_ladefense_latest.json")
    checks.append(("IDFM data exists", idfm_exists))

    if idfm_exists:
        idfm_structure = check_json_structure(
            bucket,
            "landing/transport/idfm_ladefense_latest.json",
            ["extraction_time", "location", "coordinates", "stops", "departures", "traffic_status"]
        )
        checks.append(("IDFM data structure", idfm_structure))

    # Processed IDFM data
    processed_transport_exists = check_file_exists(bucket, "refined/transport/schedules_latest.parquet")
    checks.append(("Processed transport schedules exist", processed_transport_exists))

    processed_traffic_exists = check_file_exists(bucket, "refined/transport/traffic_latest.parquet")
    checks.append(("Processed traffic status exists", processed_traffic_exists))

    # Station data checks
    ratp_stations_exists = check_file_exists(bucket, "landing/stations/ratp_stations_latest.json")
    checks.append(("RATP stations data exists", ratp_stations_exists))

    osm_stations_exists = check_file_exists(bucket, "landing/stations/osm_enhanced_latest.json")
    checks.append(("OSM stations data exists", osm_stations_exists))

    combined_stations_exists = check_file_exists(bucket, "refined/stations/combined_stations_latest.parquet")
    checks.append(("Combined stations data exists", combined_stations_exists))

    # Traffic data check
    traffic_exists = check_file_exists(bucket, "landing/traffic/traffic_ladefense_latest.json")
    checks.append(("Traffic data exists", traffic_exists))

    # Print summary
    total_checks = len(checks)
    passed_checks = sum(1 for _, passed in checks if passed)

    print(f"\nData Quality Summary: {passed_checks}/{total_checks} checks passed")
    print("-" * 50)

    for name, passed in checks:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} | {name}")

    print("-" * 50)
    logger.info(f"Data quality check completed: {passed_checks}/{total_checks} checks passed")

    return checks

if __name__ == "__main__":
    run_basic_checks()