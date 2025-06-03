"""
Simplified setup script without Unicode characters for Windows compatibility
Complete integration script for historical data extraction and prediction model training
"""
import os
import sys
import time
from datetime import datetime

# Add the parent directory to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)


def run_historical_extraction():
    """Run the historical data extraction"""
    print("=" * 60)
    print("STEP 1: Historical Data Extraction")
    print("=" * 60)

    try:
        # Import and run the historical extractor
        from data_extraction.extract_historical_data import HistoricalDataExtractor

        extractor = HistoricalDataExtractor()
        success = extractor.run_full_extraction()

        if success:
            print("[SUCCESS] Historical data extraction completed successfully!")
            return True
        else:
            print("[ERROR] Historical data extraction failed")
            return False

    except Exception as e:
        print(f"[ERROR] Error during historical extraction: {str(e)}")
        return False


def train_prediction_models():
    """Train the prediction models"""
    print("\n" + "=" * 60)
    print("STEP 2: Training Prediction Models")
    print("=" * 60)

    try:
        # Import and run the model trainer
        from models.enhanced_prediction_model import MobilityPredictor

        predictor = MobilityPredictor()
        success = predictor.train_all_models()

        if success:
            print("[SUCCESS] Model training completed successfully!")
            return True
        else:
            print("[ERROR] Model training failed")
            return False

    except Exception as e:
        print(f"[ERROR] Error during model training: {str(e)}")
        return False


def test_predictions():
    """Test the trained models"""
    print("\n" + "=" * 60)
    print("STEP 3: Testing Predictions")
    print("=" * 60)

    try:
        from models.enhanced_prediction_model import PredictionService

        service = PredictionService()
        if not service.initialize():
            print("[ERROR] Failed to initialize prediction service")
            return False

        # Test predictions for different transport types
        test_cases = [
            {"transport_type": "metro", "line": "1"},
            {"transport_type": "rers", "line": "A"},
            {"transport_type": "rers", "line": "E"},
            {"transport_type": "transilien", "line": "L"},
            {"transport_type": "buses", "line": "144"},
        ]

        print("Running prediction tests...")

        for test_case in test_cases:
            transport_type = test_case["transport_type"]
            line = test_case["line"]

            print(f"\nTesting {transport_type} {line}:")

            # Test current prediction
            prediction = service.get_transport_prediction(transport_type, line)

            if prediction:
                reliability = prediction.get('reliability', 'N/A')
                delay = prediction.get('expected_delay_minutes', 'N/A')
                load = prediction.get('passenger_load', 'N/A')
                congestion = prediction.get('congestion_level', 'N/A')

                print(f"  Reliability: {reliability:.2%}" if isinstance(reliability, float) else f"  Reliability: {reliability}")
                print(f"  Expected delay: {delay:.1f} min" if isinstance(delay, float) else f"  Expected delay: {delay}")
                print(f"  Passenger load: {load:.1%}" if isinstance(load, float) else f"  Passenger load: {load}")
                print(f"  Congestion: {congestion}")
            else:
                print("  [ERROR] Prediction failed")

        print("\n[SUCCESS] Prediction testing completed!")
        return True

    except Exception as e:
        print(f"[ERROR] Error during prediction testing: {str(e)}")
        return False


def update_dashboard_predictions():
    """Update the dashboard to use real predictions"""
    print("\n" + "=" * 60)
    print("STEP 4: Dashboard Integration")
    print("=" * 60)

    dashboard_instructions = """
    To integrate predictions into your dashboard:
    
    1. Add this import to your dash_app/app.py:
       from models.enhanced_prediction_model import PredictionService
    
    2. Initialize the prediction service in your app:
       prediction_service = PredictionService()
       prediction_service.initialize()
    
    3. Use predictions in your pages:
       # Get reliability prediction
       prediction = prediction_service.get_transport_prediction("metro", "1")
       reliability = prediction.get('reliability', 0.9)
       
       # Get 24h forecast  
       forecast = prediction_service.get_24h_forecast("metro", "1")
    
    4. Update your prediction visualizations to use real data instead of mock data.
    """

    print(dashboard_instructions)

    # Create a simple integration example
    integration_code = '''
# Add this function to your dash_app/app.py

@st.cache_data(ttl=900)  # Cache for 15 minutes
def load_predictions():
    """Load real-time predictions for all transport lines"""
    try:
        from models.enhanced_prediction_model import PredictionService
        
        service = PredictionService()
        if not service.initialize():
            return {}
        
        # Get predictions for all lines
        lines_to_predict = [
            {"transport_type": "metro", "line": "1"},
            {"transport_type": "rers", "line": "A"},
            {"transport_type": "rers", "line": "E"}, 
            {"transport_type": "transilien", "line": "L"},
            {"transport_type": "buses", "line": "144"},
        ]
        
        predictions = {}
        for line_info in lines_to_predict:
            key = f"{line_info['transport_type']}_{line_info['line']}"
            prediction = service.get_transport_prediction(
                line_info['transport_type'], 
                line_info['line']
            )
            if prediction:
                predictions[key] = prediction
        
        return predictions
        
    except Exception as e:
        st.error(f"Error loading predictions: {str(e)}")
        return {}

# Use in your dashboard pages:
# predictions = load_predictions()
# metro_1_reliability = predictions.get('metro_1', {}).get('reliability', 0.9)
'''

    # Save integration code to file with UTF-8 encoding
    try:
        with open('dashboard_integration_code.py', 'w', encoding='utf-8') as f:
            f.write(integration_code)
        print("[SUCCESS] Integration code saved to: dashboard_integration_code.py")
    except Exception as e:
        print(f"[ERROR] Failed to save integration code: {str(e)}")
        return False

    return True


def setup_automation():
    """Set up automated model retraining"""
    print("\n" + "=" * 60)
    print("STEP 5: Automation Setup")
    print("=" * 60)

    automation_script = '''
"""
Automated model retraining script
Run this weekly to retrain models with fresh data
"""
import sys
import os
from datetime import datetime, timedelta

def weekly_model_update():
    """Weekly model retraining process"""
    print(f"Starting weekly model update: {datetime.now()}")
    
    # Run historical data extraction for past week
    from data_extraction.extract_historical_data import HistoricalDataExtractor
    extractor = HistoricalDataExtractor()
    
    # Extract just the past week
    extractor.start_date = datetime.now() - timedelta(days=7)
    extractor.end_date = datetime.now()
    
    if extractor.run_full_extraction():
        print("[SUCCESS] New historical data extracted")
        
        # Retrain models
        from models.enhanced_prediction_model import MobilityPredictor
        predictor = MobilityPredictor()
        
        if predictor.train_all_models():
            print("[SUCCESS] Models retrained successfully")
        else:
            print("[ERROR] Model retraining failed")
    else:
        print("[ERROR] Historical data extraction failed")

if __name__ == "__main__":
    weekly_model_update()
'''

    try:
        with open('weekly_model_update.py', 'w', encoding='utf-8') as f:
            f.write(automation_script)

        print("[SUCCESS] Weekly automation script created: weekly_model_update.py")
        print("   Run this script weekly to keep models updated with fresh data")

        # Add to existing automation
        automation_instructions = """
    To add model retraining to your automation:
    
    1. Add this to automation/run_extract.py in the schedule setup:
       schedule.every().sunday.at("02:00").do(run_weekly_model_update)
    
    2. Add this function to run_extract.py:
       def run_weekly_model_update():
           print("Running weekly model update...")
           os.system('python weekly_model_update.py')
    """

        print(automation_instructions)
        return True

    except Exception as e:
        print(f"[ERROR] Failed to create automation script: {str(e)}")
        return False


def main():
    """Main function to run the complete setup"""
    print("La Defense Mobility Prediction Setup")
    print("This will extract 1 month of historical data and train prediction models")
    print("Estimated time: 5-10 minutes")

    start_time = time.time()

    # Step 1: Extract historical data
    if not run_historical_extraction():
        print("[ERROR] Setup failed at historical data extraction")
        return False

    # Step 2: Train models
    if not train_prediction_models():
        print("[ERROR] Setup failed at model training")
        return False

    # Step 3: Test predictions
    if not test_predictions():
        print("[ERROR] Setup failed at prediction testing")
        return False

    # Step 4: Dashboard integration
    if not update_dashboard_predictions():
        print("[ERROR] Setup failed at dashboard integration")
        return False

    # Step 5: Setup automation
    if not setup_automation():
        print("[ERROR] Setup failed at automation setup")
        return False

    # Success summary
    end_time = time.time()
    duration = (end_time - start_time) / 60

    print("\n" + "=" * 60)
    print("SETUP COMPLETED SUCCESSFULLY!")
    print("=" * 60)
    print(f"Total setup time: {duration:.1f} minutes")
    print()
    print("Files created:")
    print("  [OK] mobility_prediction_models.pkl (trained models)")
    print("  [OK] model_metadata.json (model information)")
    print("  [OK] dashboard_integration_code.py (integration code)")
    print("  [OK] weekly_model_update.py (automation script)")
    print()
    print("Your mobility platform now has:")
    print("  [OK] Real-time reliability predictions")
    print("  [OK] Delay forecasting")
    print("  [OK] Passenger load predictions")
    print("  [OK] Congestion level classification")
    print("  [OK] 24-hour forecasting")
    print()
    print("Next steps:")
    print("  1. Integrate prediction code into your dashboard")
    print("  2. Test the predictions in your Streamlit app")
    print("  3. Set up weekly model retraining")
    print()
    print("Your La Defense mobility platform is now prediction-ready!")

    return True


if __name__ == "__main__":
    main()