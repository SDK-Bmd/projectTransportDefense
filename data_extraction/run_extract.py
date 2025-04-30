"""
Automated extraction scheduler for La Défense mobility data lake
This script orchestrates all data extraction and processing tasks
"""
import schedule
import time
import os
from datetime import datetime
import data_extraction.config as config
from api_utils import get_with_retries, API_ENDPOINTS



def run_transport_extraction():
    """Run transport data extraction and processin
    g"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Running transport data extraction...")
    os.system('python extract_transport.py')
    os.system('python process_transport_data.py')
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Transport data extraction complete")


def run_weather_extraction():
    """Run weather data extraction and processing"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Running weather data extraction...")
    os.system('python extract_visual_crossing_weather.py')
    os.system('python process_weather_data.py')
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Weather data extraction complete")


def run_traffic_extraction():
    """Run traffic data extraction"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Running traffic data extraction...")
    os.system('python extract_traffic.py')
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Traffic data extraction complete")


def run_quality_check():
    """Run basic data quality checks"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Running data quality checks...")

    os.system('python data_quality.py')

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Data quality checks complete")

def run_station_extraction():
    """Run station data extraction and processing"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Running station data extraction...")
    os.system('python extract_ratp_stations.py')
    os.system('python extract_osm_stations.py')
    os.system('python process_stations_data.py')
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Station data extraction complete")


def run_all_extractions():
    """Run all data extraction processes"""
    print(f"\n--- Starting complete extraction at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---\n")

    run_transport_extraction()
    run_weather_extraction()
    run_traffic_extraction()
    run_station_extraction()
    run_quality_check()

    print(f"\n--- Completed extraction at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---\n")


def setup_schedule():
    """Set up the extraction schedule based on configuration"""
    # Get frequencies from configuration
    transport_freq = config.EXTRACTION_CONFIG["transport"]["frequency_minutes"]
    weather_freq = config.EXTRACTION_CONFIG["weather"]["frequency_minutes"]
    traffic_freq = config.EXTRACTION_CONFIG["traffic"]["frequency_minutes"]
    stations_freq = config.EXTRACTION_CONFIG["stations"]["frequency_minutes"]
    dataquality_freq = config.EXTRACTION_CONFIG["dataQuality"]["frequency_minutes"]

    # Schedule transport extraction
    schedule.every(transport_freq).minutes.do(run_transport_extraction)

    # Schedule weather extraction
    schedule.every(weather_freq).minutes.do(run_weather_extraction)

    # Schedule traffic extraction
    schedule.every(traffic_freq).minutes.do(run_traffic_extraction)

    # Schedule station extraction (typically once per day)
    schedule.every(stations_freq).minutes.do(run_station_extraction)
    # Schedule station extraction (typically once per day)
    schedule.every(dataquality_freq).minutes.do(run_station_extraction)

    print("Extraction schedule configured:")
    print(f"- Transport data: every {transport_freq} minutes")
    print(f"- Weather data: every {weather_freq} minutes")
    print(f"- Traffic data: every {traffic_freq} minutes")
    print(f"- Station data: every {stations_freq} minutes")
    print("\nPress Ctrl+C to stop the scheduler.")


if __name__ == "__main__":
    # Run all extractions immediately on startup
    run_all_extractions()

    # Set up scheduled runs
    setup_schedule()

    # Run the scheduler
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nScheduler stopped by user.")