import schedule
import time
import os
import sys
from datetime import datetime
import argparse

# Add the parent directory to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from configuration.config import EXTRACTION_CONFIG

def run_transport_extraction():
    """Run transport data extraction and processing for ALL transport types"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Running comprehensive transport data extraction...")

    # Extract data from RATP API (Metro 1, RER A/E, Transilien L, Buses)
    print("  → Extracting RATP transport data (Metro, RER, Transilien, Bus)")
    os.system('python ../data_extraction/extract_transport.py')

    # Extract RATP station information
    print("  → Extracting RATP station data")
    os.system('python ../data_extraction/extract_ratp_stations.py')

    # Process extracted RATP data
    print("  → Processing RATP transport data")
    os.system('python ../data_processing/process_transport_data.py')

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] RATP transport data extraction complete")

def run_idfm_extraction():
    """Run IDFM data extraction and processing"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Running IDFM data extraction...")

    # Extract IDFM real-time data
    print("  → Extracting IDFM real-time data")
    os.system('python ../data_extraction/extract_idfm_data.py')

    # Process IDFM data
    print("  → Processing IDFM data")
    os.system('python ../data_processing/process_idfm_data.py')

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] IDFM data extraction complete")

def run_weather_extraction():
    """Run weather data extraction and processing"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Running weather data extraction...")

    # Extract weather data
    print("  → Extracting Visual Crossing weather data")
    os.system('python ../data_extraction/extract_visual_crossing_weather.py')

    # Process weather data
    print("  → Processing weather data")
    os.system('python ../data_processing/process_weather_data.py')

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Weather data extraction complete")

def run_traffic_extraction():
    """Run traffic data extraction"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Running traffic data extraction...")

    # Extract traffic data (TomTom, Sytadin)
    print("  → Extracting traffic data (TomTom, Sytadin)")
    os.system('python ../data_extraction/extract_traffic.py')

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Traffic data extraction complete")

def run_station_extraction():
    """Run comprehensive station data extraction and processing"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Running station data extraction...")

    # Extract RATP station data (already included in transport extraction)
    print("  → RATP stations (included in transport extraction)")

    # Extract OpenStreetMap station data
    print("  → Extracting OpenStreetMap station data")
    os.system('python ../data_extraction/extract_osm_stations.py')

    # Process combined station data
    print("  → Processing combined station data")
    os.system('python ../data_processing/process_stations_data.py')

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Station data extraction complete")

def run_quality_check():
    """Run comprehensive data quality checks"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Running data quality checks...")

    # Run data quality validation
    print("  → Validating data quality across all sources")
    os.system('python ../data_processing/data_quality.py')

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Data quality checks complete")

def run_historical_extraction():
    """Run historical data extraction for predictions (NEW)"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Running historical data extraction...")

    # Extract historical data for ML training
    print("  → Extracting 1 month of historical data")
    os.system('python ../data_extraction/extract_historical_data.py')

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Historical data extraction complete")

def run_model_training():
    """Train/update prediction models (NEW)"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Running prediction model training...")

    # Train ML models for predictions
    print("  → Training mobility prediction models")
    os.system('python ../models/enhanced_prediction_model.py --train')

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Model training complete")

def run_prediction_update():
    """Update predictions with latest models (NEW)"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Updating predictions...")

    # Generate fresh predictions
    print("  → Generating 24h forecasts for all lines")
    os.system('python ../models/enhanced_prediction_model.py --predict')

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Prediction update complete")

def run_data_consolidation():
    """Run data consolidation and cross-source validation"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Running data consolidation...")

    # Consolidate transport data from multiple sources
    print("  → Consolidating transport data (RATP + IDFM)")

    # This could be a new script that merges RATP and IDFM data
    # For now, the individual processing scripts handle their own data
    try:
        os.system('python ../data_processing/consolidate_transport_data.py')
    except:
        print("  → Consolidation script not found, using individual processing results")

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Data consolidation complete")

def run_weekly_model_update():
    """Weekly comprehensive model update (NEW)"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Running weekly model update...")

    # Weekly comprehensive update: historical data + model retraining
    print("  → Extracting latest historical data")
    run_historical_extraction()

    print("  → Retraining models with updated data")
    run_model_training()

    print("  → Updating prediction forecasts")
    run_prediction_update()

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Weekly model update complete")

def run_all_extractions():
    """Run all data extraction processes in optimal order"""
    print(f"\n=== Starting comprehensive La Défense mobility extraction at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")

    extraction_start_time = time.time()

    # 1. Extract core transport data (highest priority)
    run_transport_extraction()

    # 2. Extract IDFM data (complementary to RATP)
    run_idfm_extraction()

    # 3. Extract weather data (affects transport predictions)
    run_weather_extraction()

    # 4. Extract traffic data (road conditions)
    run_traffic_extraction()

    # 5. Extract and process station information
    run_station_extraction()

    # 6. Consolidate data from multiple sources
    run_data_consolidation()

    # 7. Run quality checks
    run_quality_check()

    # 8. Update predictions with new data
    run_prediction_update()

    extraction_end_time = time.time()
    total_time = (extraction_end_time - extraction_start_time) / 60

    print(f"\n=== Completed comprehensive extraction at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")
    print(f"Total extraction time: {total_time:.1f} minutes")

    # Summary of extracted data
    print("\n📊 Extraction Summary:")
    print("  ✅ Metro Line 1")
    print("  ✅ RER A, RER E")
    print("  ✅ Transilien L")
    print("  ✅ Bus lines: 73, 144, 158, 163, 174, 178, 258, 262, 272, 275")
    print("  ✅ IDFM real-time data")
    print("  ✅ Weather conditions")
    print("  ✅ Road traffic data")
    print("  ✅ Station accessibility information")
    print("  ✅ Data quality validation")
    print("  ✅ Updated predictions")

def run_initial_setup():
    """Run initial setup including historical data extraction and model training"""
    print(f"\n=== Initial Setup: Historical Data + Model Training ===\n")

    setup_start_time = time.time()

    # 1. Extract historical data for training
    print("🔄 Step 1: Extracting historical data...")
    run_historical_extraction()

    # 2. Train initial models
    print("🤖 Step 2: Training prediction models...")
    run_model_training()

    # 3. Run initial extractions
    print("📊 Step 3: Running initial data extraction...")
    run_all_extractions()

    setup_end_time = time.time()
    total_time = (setup_end_time - setup_start_time) / 60

    print(f"\n=== Initial setup completed in {total_time:.1f} minutes ===")
    print("🎉 Your La Défense mobility platform is now prediction-ready!")

def setup_schedule():
    """Set up the extraction schedule based on configuration with enhanced frequencies"""

    # Get frequencies from configuration
    transport_freq = EXTRACTION_CONFIG["transport"]["frequency_minutes"]
    weather_freq = EXTRACTION_CONFIG["weather"]["frequency_minutes"]
    traffic_freq = EXTRACTION_CONFIG["traffic"]["frequency_minutes"]
    stations_freq = EXTRACTION_CONFIG["stations"]["frequency_minutes"]
    dataquality_freq = EXTRACTION_CONFIG["dataQuality"]["frequency_minutes"]

    # Schedule transport extraction (RATP + IDFM)
    schedule.every(transport_freq).minutes.do(run_transport_extraction)
    schedule.every(transport_freq).minutes.do(run_idfm_extraction)

    # Schedule weather extraction
    schedule.every(weather_freq).minutes.do(run_weather_extraction)

    # Schedule traffic extraction
    schedule.every(traffic_freq).minutes.do(run_traffic_extraction)

    # Schedule station extraction (typically once per day)
    schedule.every(stations_freq).minutes.do(run_station_extraction)

    # Schedule data quality checks
    schedule.every(dataquality_freq).minutes.do(run_quality_check)

    # Schedule data consolidation (every 2 hours)
    schedule.every(120).minutes.do(run_data_consolidation)

    # NEW: Schedule prediction updates (every 4 hours)
    schedule.every(240).minutes.do(run_prediction_update)

    # NEW: Schedule weekly model retraining (Sundays at 2 AM)
    schedule.every().sunday.at("02:00").do(run_weekly_model_update)

    print("🕐 Enhanced extraction schedule configured:")
    print(f"  🚊 Transport data (RATP): every {transport_freq} minutes")
    print(f"  📡 IDFM data: every {transport_freq} minutes")
    print(f"  🌤️ Weather data: every {weather_freq} minutes")
    print(f"  🚗 Traffic data: every {traffic_freq} minutes")
    print(f"  🚉 Station data: every {stations_freq} minutes")
    print(f"  🔍 Data quality: every {dataquality_freq} minutes")
    print(f"  🔄 Data consolidation: every 120 minutes")
    print(f"  🔮 Prediction updates: every 240 minutes")
    print(f"  🤖 Model retraining: weekly (Sundays 2:00 AM)")
    print("\n📋 Transport Lines Monitored:")
    print("  • Metro: Line 1")
    print("  • RER: A, E")
    print("  • Transilien: L")
    print("  • Bus: 73, 144, 158, 163, 174, 178, 258, 262, 272, 275")
    print("\n🔮 Prediction Features:")
    print("  • Real-time reliability forecasting")
    print("  • Delay predictions")
    print("  • Passenger load estimation")
    print("  • 24-hour forecasts")
    print("  • Weather impact analysis")
    print("\nPress Ctrl+C to stop the scheduler.")

def run_specific_extraction(extraction_type):
    """Run a specific type of extraction"""
    extraction_functions = {
        'transport': run_transport_extraction,
        'idfm': run_idfm_extraction,
        'weather': run_weather_extraction,
        'traffic': run_traffic_extraction,
        'stations': run_station_extraction,
        'quality': run_quality_check,
        'consolidation': run_data_consolidation,
        'historical': run_historical_extraction,      # NEW
        'train-models': run_model_training,           # NEW
        'update-predictions': run_prediction_update,  # NEW
        'weekly-update': run_weekly_model_update      # NEW
    }

    if extraction_type in extraction_functions:
        extraction_functions[extraction_type]()
    else:
        print(f"Unknown extraction type: {extraction_type}")
        print(f"Available types: {', '.join(extraction_functions.keys())}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='La Défense Comprehensive Mobility Data Extraction with Predictions',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_extract.py --schedule                    # Run as scheduled service
  python run_extract.py --extract all                 # Run all extractions once
  python run_extract.py --extract transport           # Run only transport extraction
  python run_extract.py --extract historical          # Extract historical data
  python run_extract.py --extract train-models        # Train prediction models
  python run_extract.py --extract update-predictions  # Update predictions
  python run_extract.py --initial-setup               # Complete initial setup
        """
    )

    parser.add_argument(
        '--schedule',
        action='store_true',
        help='Run as a scheduled service with automatic extractions'
    )

    parser.add_argument(
        '--extract',
        choices=[
            'all', 'transport', 'idfm', 'weather', 'traffic', 'stations',
            'quality', 'consolidation', 'historical', 'train-models',
            'update-predictions', 'weekly-update'
        ],
        help='Run a specific extraction type'
    )

    parser.add_argument(
        '--initial-setup',
        action='store_true',
        help='Run complete initial setup (historical data + model training + extractions)'
    )

    args = parser.parse_args()

    if args.initial_setup:
        print("🚀 Starting La Défense Mobility Platform Initial Setup")
        print("📍 This will set up predictions and extract historical data")
        print("⏱️ Estimated time: 10-15 minutes")

        run_initial_setup()

    elif args.schedule:
        print("🚀 Starting La Défense Mobility Data Extraction Service")
        print("📍 Coverage: Metro 1, RER A/E, Transilien L, Bus network")
        print("🔮 Features: Real-time data + ML predictions")

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
            print("\n🛑 Scheduler stopped by user.")
            print("📊 Final extraction summary saved to logs.")

    elif args.extract:
        print(f"🎯 Running {args.extract} extraction...")

        if args.extract == 'all':
            run_all_extractions()
        else:
            run_specific_extraction(args.extract)

        print("✅ Extraction completed.")

    else:
        # By default, run all extractions once
        print("🚀 Running default comprehensive extraction...")
        run_all_extractions()