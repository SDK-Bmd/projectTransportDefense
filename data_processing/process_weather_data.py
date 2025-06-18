import pandas as pd
import json
from datetime import datetime
import sys
import os

# Add paths for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from utils.data_lake_utils import get_s3_client, read_json_from_data_lake, save_parquet_to_data_lake
from configuration.config import DATA_LAKE

def safe_float_conversion(value, default=0):
    """Safely convert values to float with fallback"""
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

def safe_string_conversion(value, default="Unknown"):
    """Safely convert values to string with fallback"""
    if value is None or value == "":
        return default
    try:
        return str(value)
    except:
        return default

def process_visual_crossing_data():
    """Enhanced processing of Visual Crossing weather data with better error handling"""
    bucket_name = DATA_LAKE["bucket_name"]

    try:
        # Read the latest Visual Crossing data
        raw_data = read_json_from_data_lake(bucket_name, "landing/weather/visual_crossing_latest.json")

        if not raw_data:
            print("❌ No weather data found")
            return False

        print("✅ Raw weather data loaded successfully")
        print(f"Data keys: {list(raw_data.keys()) if isinstance(raw_data, dict) else 'Not a dict'}")

        # Debug: Print structure
        if isinstance(raw_data, dict):
            if 'days' in raw_data:
                print(f"Found {len(raw_data['days'])} days of forecast data")
                if raw_data['days']:
                    first_day = raw_data['days'][0]
                    print(f"First day keys: {list(first_day.keys())}")
                    if 'hours' in first_day:
                        print(f"First day has {len(first_day['hours'])} hours of data")

        # Process current conditions
        current_df = pd.DataFrame()
        if 'current_conditions' in raw_data:
            current_conditions = raw_data['current_conditions']
            current_df = pd.DataFrame([{
                'temperature': safe_float_conversion(current_conditions.get('temp'), 15),
                'feels_like': safe_float_conversion(current_conditions.get('feelslike'), 15),
                'humidity': safe_float_conversion(current_conditions.get('humidity'), 50),
                'precipitation': safe_float_conversion(current_conditions.get('precip'), 0),
                'precipitation_probability': safe_float_conversion(current_conditions.get('precipprob'), 0),
                'wind_speed': safe_float_conversion(current_conditions.get('windspeed'), 0),
                'pressure': safe_float_conversion(current_conditions.get('pressure'), 1013),
                'visibility': safe_float_conversion(current_conditions.get('visibility'), 10),
                'conditions': safe_string_conversion(current_conditions.get('conditions'), 'Unknown'),
                'timestamp': datetime.now().isoformat()
            }])
            print(f"✅ Current weather processed: {len(current_df)} record")
            print(f"Current weather sample: Temperature={current_df['temperature'].iloc[0]}°C, "
                  f"Precipitation={current_df['precipitation'].iloc[0]}mm")

        # Process daily forecast with enhanced error handling
        daily_df = pd.DataFrame()
        if 'days' in raw_data and raw_data['days']:
            daily_data = []

            print(f"Processing {len(raw_data['days'])} days of forecast...")

            for i, day in enumerate(raw_data['days']):
                date_str = day.get('datetime', '')
                if not date_str:
                    print(f"❌ Day {i}: No datetime field")
                    continue

                try:
                    date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
                except ValueError:
                    print(f"❌ Day {i}: Could not parse date: {date_str}")
                    continue

                # Extract temperature values with multiple fallbacks
                temp_max = safe_float_conversion(day.get('tempmax'),
                          safe_float_conversion(day.get('temp'), 15))
                temp_min = safe_float_conversion(day.get('tempmin'),
                          safe_float_conversion(day.get('temp'), 15))
                temp_avg = safe_float_conversion(day.get('temp'), 15)

                # Ensure min <= avg <= max logic
                if temp_min > temp_max:
                    temp_min, temp_max = temp_max, temp_min
                if temp_avg < temp_min:
                    temp_avg = temp_min
                elif temp_avg > temp_max:
                    temp_avg = temp_max

                daily_record = {
                    'date': date_obj,
                    'temperature_max': temp_max,
                    'temperature_min': temp_min,
                    'temperature_avg': temp_avg,
                    'precipitation': safe_float_conversion(day.get('precip'), 0),
                    'precipitation_probability': safe_float_conversion(day.get('precipprob'), 0),
                    'wind_speed': safe_float_conversion(day.get('windspeed'), 0),
                    'humidity': safe_float_conversion(day.get('humidity'), 50),
                    'pressure': safe_float_conversion(day.get('pressure'), 1013),
                    'visibility': safe_float_conversion(day.get('visibility'), 10),
                    'conditions': safe_string_conversion(day.get('conditions'), 'Unknown'),
                    'description': safe_string_conversion(day.get('description'), '')
                }
                daily_data.append(daily_record)

                print(f"  Day {i} ({date_str}): {temp_min}°C to {temp_max}°C, "
                      f"precip={daily_record['precipitation']}mm")

            daily_df = pd.DataFrame(daily_data)
            print(f"✅ Daily forecast processed: {len(daily_df)} days")

            # Validate daily data
            if not daily_df.empty:
                print("Daily forecast validation:")
                print(f"- Date range: {daily_df['date'].min()} to {daily_df['date'].max()}")
                print(f"- Temperature max range: {daily_df['temperature_max'].min():.1f}°C to {daily_df['temperature_max'].max():.1f}°C")
                print(f"- Temperature min range: {daily_df['temperature_min'].min():.1f}°C to {daily_df['temperature_min'].max():.1f}°C")
                print(f"- Total precipitation: {daily_df['precipitation'].sum():.1f}mm")

                # Check for data integrity
                null_dates = daily_df['date'].isna().sum()
                null_temps = daily_df['temperature_max'].isna().sum()
                if null_dates > 0:
                    print(f"⚠️ Warning: {null_dates} rows with null dates")
                if null_temps > 0:
                    print(f"⚠️ Warning: {null_temps} rows with null temperatures")

        # Process hourly forecast
        hourly_df = pd.DataFrame()
        if 'days' in raw_data and raw_data['days']:
            hourly_data = []

            for day in raw_data['days']:
                date_str = day.get('datetime', '')

                if 'hours' in day and day['hours']:
                    for hour in day['hours']:
                        time_str = hour.get('datetime', '12:00:00')
                        try:
                            # Handle different time formats
                            if len(time_str) == 5:  # HH:MM format
                                time_str += ":00"
                            elif len(time_str) == 8 and time_str.count(':') == 2:  # Already HH:MM:SS
                                pass
                            else:
                                print(f"⚠️ Unusual time format: {time_str}, using default")
                                time_str = "12:00:00"

                            datetime_str = f"{date_str} {time_str}"
                            datetime_obj = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
                        except ValueError:
                            print(f"❌ Could not parse datetime: {datetime_str}")
                            continue

                        hourly_record = {
                            'datetime': datetime_obj,
                            'date': datetime_obj.date(),
                            'hour': datetime_obj.hour,
                            'temperature': safe_float_conversion(hour.get('temp'), 15),
                            'feels_like': safe_float_conversion(hour.get('feelslike'), 15),
                            'humidity': safe_float_conversion(hour.get('humidity'), 50),
                            'precipitation': safe_float_conversion(hour.get('precip'), 0),
                            'precipitation_probability': safe_float_conversion(hour.get('precipprob'), 0),
                            'wind_speed': safe_float_conversion(hour.get('windspeed'), 0),
                            'pressure': safe_float_conversion(hour.get('pressure'), 1013),
                            'visibility': safe_float_conversion(hour.get('visibility'), 10),
                            'conditions': safe_string_conversion(hour.get('conditions'), 'Unknown')
                        }
                        hourly_data.append(hourly_record)

            hourly_df = pd.DataFrame(hourly_data)
            print(f"✅ Hourly forecast processed: {len(hourly_df)} hours")

            if not hourly_df.empty:
                print("Hourly forecast validation:")
                print(f"- Time range: {hourly_df['datetime'].min()} to {hourly_df['datetime'].max()}")
                print(f"- Temperature range: {hourly_df['temperature'].min():.1f}°C to {hourly_df['temperature'].max():.1f}°C")

        # Save processed data to data lake
        success_count = 0

        if not current_df.empty:
            try:
                save_parquet_to_data_lake(bucket_name, "refined/weather/current_latest.parquet", current_df)
                print("✅ Current weather saved to data lake")
                success_count += 1
            except Exception as e:
                print(f"❌ Failed to save current weather: {str(e)}")

        if not daily_df.empty:
            try:
                save_parquet_to_data_lake(bucket_name, "refined/weather/daily_latest.parquet", daily_df)
                print("✅ Daily forecast saved to data lake")
                success_count += 1

                # Debug: Verify what was saved
                print("Daily data sample saved:")
                print(daily_df[['date', 'temperature_max', 'temperature_min', 'precipitation']].head())
            except Exception as e:
                print(f"❌ Failed to save daily forecast: {str(e)}")

        if not hourly_df.empty:
            try:
                save_parquet_to_data_lake(bucket_name, "refined/weather/hourly_latest.parquet", hourly_df)
                print("✅ Hourly forecast saved to data lake")
                success_count += 1
            except Exception as e:
                print(f"❌ Failed to save hourly forecast: {str(e)}")

        if success_count > 0:
            print(f"🎉 Weather data processing completed successfully! ({success_count}/3 datasets processed)")

            # Final verification
            print("\n=== FINAL DATA VERIFICATION ===")
            if not daily_df.empty:
                print(f"Daily forecast: {len(daily_df)} records")
                print(f"Temperature columns: {[col for col in daily_df.columns if 'temp' in col.lower()]}")
                print(f"Date column type: {daily_df['date'].dtype}")
                print(f"Sample record: {daily_df.iloc[0].to_dict()}")

            return True
        else:
            print("❌ No weather data was successfully processed")
            return False

    except Exception as e:
        print(f"❌ Error processing weather data: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def debug_existing_data():
    """Debug function to check current state of weather data"""
    bucket_name = DATA_LAKE["bucket_name"]

    print("=== DEBUGGING EXISTING WEATHER DATA ===")

    # Check raw data
    try:
        raw_data = read_json_from_data_lake(bucket_name, "landing/weather/visual_crossing_latest.json")
        if raw_data:
            print("✅ Raw weather data exists")
            if 'days' in raw_data:
                print(f"- Contains {len(raw_data['days'])} days")
                if raw_data['days']:
                    first_day = raw_data['days'][0]
                    print(f"- First day structure: {list(first_day.keys())}")
                    print(f"- Sample temp data: tempmax={first_day.get('tempmax')}, tempmin={first_day.get('tempmin')}")
        else:
            print("❌ No raw weather data found")
    except Exception as e:
        print(f"❌ Error reading raw data: {str(e)}")

    # Check processed data
    try:
        from utils.data_lake_utils import read_parquet_from_data_lake
        daily_df = read_parquet_from_data_lake(bucket_name, "refined/weather/daily_latest.parquet")
        if not daily_df.empty:
            print(f"✅ Processed daily data exists: {len(daily_df)} records")
            print(f"- Columns: {list(daily_df.columns)}")
            print(f"- Temperature range: {daily_df['temperature_max'].min():.1f} to {daily_df['temperature_max'].max():.1f}°C")
        else:
            print("❌ No processed daily data found")
    except Exception as e:
        print(f"❌ Error reading processed data: {str(e)}")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Process Visual Crossing weather data')
    parser.add_argument('--debug', action='store_true', help='Debug existing data instead of processing')
    args = parser.parse_args()

    if args.debug:
        debug_existing_data()
    else:
        process_visual_crossing_data()