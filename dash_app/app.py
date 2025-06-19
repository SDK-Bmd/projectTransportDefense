import plotly.express as px

import pandas as pd

from datetime import datetime, timedelta

import streamlit as st

from enhanced_dashboard_transport import create_transport_dashboard_section, render_enhanced_schedule_summary

from utils.cache_integration import add_enhanced_cache_management_sidebar, calculate_routes_cached_with_time_fix

try:
    from models.enhanced_prediction_model import PredictionService
    PREDICTIONS_AVAILABLE = True
except ImportError:
    PREDICTIONS_AVAILABLE = False
import sys
import os

# Add paths for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# Import project modules
from configuration.config import DATA_LAKE
from utils.data_lake_utils import read_parquet_from_data_lake, read_json_from_data_lake
from dash_app.components.maps import render_station_map
from dash_app.components.weather import render_weather_section
from dash_app.components.transport import (
    render_transport_status, render_schedules, render_transport_usage_chart, render_line_performance_metrics
)
from dash_app.components.stations import render_station_details


def reset_route_planner_state():
    """Reset route planner session state - call this if you need to clear everything"""
    keys_to_reset = [
        'route_departure_time', 'route_origin', 'route_destination',
        'route_transport_modes', 'route_preferences', 'route_journey_type',
        'route_accessibility', 'route_eco_friendly'
    ]

    for key in keys_to_reset:
        if key in st.session_state:
            del st.session_state[key]

def add_route_planner_reset_button():
    """Add this to your sidebar to allow users to reset route planner state"""
    if st.sidebar.button("🔄 Reset Route Planner", help="Clear all route planner selections"):
        reset_route_planner_state()
        st.rerun()

def initialize_route_planner_state():
    """Initialize session state for route planner if not already set"""
    if 'route_departure_time' not in st.session_state:
        st.session_state.route_departure_time = datetime.now().time()

    if 'route_origin' not in st.session_state:
        st.session_state.route_origin = "La Défense Grande Arche"

    if 'route_destination' not in st.session_state:
        st.session_state.route_destination = None

    if 'route_transport_modes' not in st.session_state:
        st.session_state.route_transport_modes = ["Metro", "RER", "Bus"]

    if 'route_preferences' not in st.session_state:
        st.session_state.route_preferences = {
            'time_pref': 1.0,
            'transfer_pref': 0.3,
            'comfort_pref': 0.5,
            'cost_pref': 0.2
        }

    if 'route_journey_type' not in st.session_state:
        st.session_state.route_journey_type = "Now"

    if 'route_accessibility' not in st.session_state:
        st.session_state.route_accessibility = False

    if 'route_eco_friendly' not in st.session_state:
        st.session_state.route_eco_friendly = False

def safe_get_weather_value(current_data, key, default=0):
    """Safely extract weather values with None handling"""
    value = current_data.get(key, default)
    if value is None or value == "" or str(value).lower() in ['none', 'nan', 'null', 'n/a']:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

# Load data functions
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_weather_data():
    """Load weather data from data lake"""
    bucket_name = DATA_LAKE["bucket_name"]

    try:
        # Get current weather
        current_df = read_parquet_from_data_lake(bucket_name, 'refined/weather/current_latest.parquet')

        # Get daily forecast
        daily_df = read_parquet_from_data_lake(bucket_name, 'refined/weather/daily_latest.parquet')

        # Get hourly forecast
        hourly_df = read_parquet_from_data_lake(bucket_name, 'refined/weather/hourly_latest.parquet')

        return current_df, daily_df, hourly_df
    except Exception as e:
        st.error(f"Error loading weather data: {str(e)}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

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


@st.cache_data(ttl=1800)  # Cache for 30 minutes
def load_24h_forecasts():
    """Load 24-hour forecasts for key transport lines"""
    if not PREDICTIONS_AVAILABLE:
        return {}

    try:
        service = PredictionService()
        if not service.initialize():
            return {}

        # Get 24h forecasts for main lines
        main_lines = [
            {"transport_type": "metro", "line": "1"},
            {"transport_type": "rers", "line": "A"},
            {"transport_type": "rers", "line": "E"},
        ]

        forecasts = {}
        for line_info in main_lines:
            key = f"{line_info['transport_type']}_{line_info['line']}"
            forecast = service.get_24h_forecast(
                line_info['transport_type'],
                line_info['line']
            )
            if not forecast.empty:
                forecasts[key] = forecast

        return forecasts

    except Exception as e:
        st.error(f"Error loading forecasts: {str(e)}")
        return {}

@st.cache_data(ttl=1800)  # Cache for 30 minutes (more frequent updates for transport)
def load_transport_data():
    """Load transport schedules and traffic status for ALL transport types"""
    bucket_name = DATA_LAKE["bucket_name"]

    all_schedules = []
    all_traffic = []

    # UPDATED: Define all transport types and lines including new ones
    transport_config = {
        "metro": ["1"],
        "rers": ["A", "E"],  # Added RER E
        "transilien": ["L"],  # Added Transilien L
        "buses": ["73", "144", "158", "163", "174", "178", "258", "262", "272", "275"]  # Added buses
    }

    data_sources = {
        "primary": "refined/transport/",
        "idfm": "refined/transport/idfm_",
        "combined": "refined/transport/"
    }

    # Try to load combined data first (most recent approach)
    try:
        combined_schedules = read_parquet_from_data_lake(bucket_name, 'refined/transport/schedules_latest.parquet')
        combined_traffic = read_parquet_from_data_lake(bucket_name, 'refined/transport/traffic_latest.parquet')

        if not combined_schedules.empty and not combined_traffic.empty:
            st.sidebar.success("✅ Using combined transport data")
            return combined_schedules, combined_traffic
    except Exception:
        pass

    # Fallback: Load individual transport line data
    for transport_type, lines in transport_config.items():
        for line in lines:
            try:
                # Load schedules
                schedules_key = f'refined/transport/{transport_type}_{line}_schedules_latest.parquet'
                line_schedules = read_parquet_from_data_lake(bucket_name, schedules_key)
                if not line_schedules.empty:
                    all_schedules.append(line_schedules)

                # Load traffic status
                traffic_key = f'refined/transport/{transport_type}_{line}_traffic_latest.parquet'
                line_traffic = read_parquet_from_data_lake(bucket_name, traffic_key)
                if not line_traffic.empty:
                    all_traffic.append(line_traffic)

            except Exception as e:
                print(f"Could not load data for {transport_type} {line}: {str(e)}")
                continue

    # Try IDFM data as additional source
    try:
        idfm_schedules = read_parquet_from_data_lake(bucket_name, 'refined/transport/idfm_schedules_latest.parquet')
        idfm_traffic = read_parquet_from_data_lake(bucket_name, 'refined/transport/idfm_traffic_latest.parquet')

        if not idfm_schedules.empty:
            all_schedules.append(idfm_schedules)
        if not idfm_traffic.empty:
            all_traffic.append(idfm_traffic)

        if not idfm_schedules.empty or not idfm_traffic.empty:
            st.sidebar.info("📡 Including IDFM data")

    except Exception:
        pass

    # Combine all data
    schedules_df = pd.concat(all_schedules, ignore_index=True) if all_schedules else pd.DataFrame()
    traffic_df = pd.concat(all_traffic, ignore_index=True) if all_traffic else pd.DataFrame()

    try:
        # Try to load existing data
        schedules_df = read_parquet_from_data_lake(bucket_name, 'refined/transport/schedules_latest.parquet')
        traffic_df = read_parquet_from_data_lake(bucket_name, 'refined/transport/traffic_latest.parquet')

        # If empty, try RATP fallback
        if schedules_df.empty:
            schedules_df = read_parquet_from_data_lake(bucket_name, 'refined/transport/ratp_schedules_latest.parquet')
        if traffic_df.empty:
            traffic_df = read_parquet_from_data_lake(bucket_name, 'refined/transport/ratp_traffic_latest.parquet')

        return schedules_df, traffic_df

    except Exception as e:
        st.error(f"Error loading transport data: {str(e)}")
        return pd.DataFrame(), pd.DataFrame()

    # Data source indicator
    if not schedules_df.empty or not traffic_df.empty:
        if len(all_schedules) > 1 or len(all_traffic) > 1:
            st.sidebar.success(f"✅ Multi-source data ({len(all_schedules)} schedule sources, {len(all_traffic)} traffic sources)")
        else:
            st.sidebar.warning("⚠️ Limited transport data available")
    else:
        st.sidebar.error("❌ No transport data available")

    return schedules_df, traffic_df


@st.cache_data(ttl=3600)
def load_station_data():
    """Load station information from multiple sources"""
    bucket_name = DATA_LAKE["bucket_name"]
    all_stations = []

    # Data sources in order of preference
    station_sources = [
        'refined/stations/combined_stations_latest.parquet',
        'refined/stations/idfm_stops_latest.parquet',
        'refined/stations/ratp_osm_combined_latest.parquet'
    ]

    for source in station_sources:
        try:
            stations_df = read_parquet_from_data_lake(bucket_name, source)
            if not stations_df.empty:
                all_stations.append(stations_df)
                st.sidebar.info(f"📍 Loaded stations from {source.split('/')[-1]}")
        except Exception:
            continue

    # Combine all station data
    if all_stations:
        combined_stations = pd.concat(all_stations, ignore_index=True)
        # Remove duplicates based on name and coordinates
        combined_stations = combined_stations.drop_duplicates(subset=['name', 'lat', 'lon'], keep='first')
        return combined_stations
    else:
        st.sidebar.warning("⚠️ No station data available")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_traffic_data():
    """Load road traffic data"""
    bucket_name = DATA_LAKE["bucket_name"]

    try:
        # Get traffic data
        traffic_data = read_json_from_data_lake(bucket_name, 'landing/traffic/traffic_ladefense_latest.json')
        return traffic_data
    except Exception as e:
        st.error(f"Error loading traffic data: {str(e)}")
        return {}


@st.cache_data(ttl=1800)  # Cache for 30 minutes
def load_data_quality_status():
    """Load basic data quality status"""
    # Check if data quality log exists
    log_path = os.path.join(parent_dir, 'data_quality.log')
    if not os.path.exists(log_path):
        return {"status": "Unknown", "details": "No data quality logs found"}

    # Parse the log to extract most recent status
    try:
        with open(log_path, 'r') as f:
            # Get the last 20 lines
            lines = f.readlines()[-20:]

            # Extract info from log lines
            quality_info = {}
            for line in reversed(lines):
                if "Data quality check completed" in line:
                    # Extract the stats (format: x/y checks passed)
                    stats_part = line.split("Data quality check completed:")[1].strip()
                    checks_part = stats_part.split(" checks")[0].strip()
                    passed, total = checks_part.split('/')

                    quality_info["status"] = "Good" if int(passed) == int(total) else "Issues Detected"
                    quality_info["passed"] = int(passed)
                    quality_info["total"] = int(total)
                    quality_info["timestamp"] = line.split(" - INFO - ")[0].strip()
                    break

            if not quality_info:
                return {"status": "Unknown", "details": "No complete quality check found in logs"}

            return quality_info
    except Exception as e:
        return {"status": "Error", "details": f"Error parsing quality logs: {str(e)}"}


@st.cache_data(ttl=3600)
def load_idfm_data():
    """Load raw IDFM data for additional detailed information"""
    bucket_name = DATA_LAKE["bucket_name"]

    try:
        idfm_data = read_json_from_data_lake(bucket_name, 'landing/transport/idfm_ladefense_latest.json')
        return idfm_data
    except Exception as e:
        st.error(f"Error loading IDFM raw data: {str(e)}")
        return {}


# Main function to load all data
def load_all_data():
    with st.spinner("Loading mobility data..."):
        current_weather, daily_weather, hourly_weather = load_weather_data()
        schedules_df, traffic_df = load_transport_data()
        stations_df = load_station_data()
        road_traffic_data = load_traffic_data()
        quality_status = load_data_quality_status()
        idfm_data = load_idfm_data()

        # Add predictions to data loading
        predictions = load_predictions()
        forecasts = load_24h_forecasts()

        return {
            "current_weather": current_weather,
            "daily_weather": daily_weather,
            "hourly_weather": hourly_weather,
            "schedules": schedules_df,
            "traffic_status": traffic_df,
            "stations": stations_df,
            "road_traffic": road_traffic_data,
            "quality_status": quality_status,
            "idfm_raw": idfm_data,
            "predictions": predictions,  # NEW
            "forecasts": forecasts  # NEW
        }

# Page configuration
st.set_page_config(
    page_title="La Défense Mobility Dashboard",
    page_icon="🚆",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Sidebar
st.sidebar.title("🏢 La Défense Mobility")
st.sidebar.markdown("---")

add_enhanced_cache_management_sidebar()

page = st.sidebar.selectbox(
    "Choose a page",
    ["Overview", "Route Planner", "Weather Impact", "Transport Analysis", "Station Information", "Data Quality", "Predictions"]
)

# Data source indicator section
st.sidebar.markdown("### 📊 Data Sources")
data_source = st.sidebar.empty()

# Load the data
all_data = load_all_data()


if st.sidebar.button("🔄 Reset Route Planner", help="Clear all route planner selections"):
    reset_route_planner_state()
    st.rerun()
    add_route_planner_reset_button()

# Transport lines coverage
st.sidebar.markdown("### 🚊 Lines Covered")
st.sidebar.markdown("""
**Metro**: Line 1  
**RER**: A, E  
**Transilien**: L  
**Bus**: 73, 144, 158, 163, 174, 178, 258, 262, 272, 275
""")
st.sidebar.markdown("### 🔮 Prediction Status")
try:
    predictions = all_data.get("predictions", {})
    if PREDICTIONS_AVAILABLE and predictions:
        st.sidebar.success(f"✅ Active ({len(predictions)} lines)")
    else:
        st.sidebar.error("❌ Not available")
except:
    st.sidebar.error("❌ Not available")

# Last refresh time
refresh_time = datetime.now().strftime("%Y-%m-%d %H:%M")
st.sidebar.write(f"🔄 Last refresh: {refresh_time}")

# Button to refresh data
if st.sidebar.button("🔄 Refresh Data", use_container_width=True):
    st.cache_data.clear()
    st.rerun()

# Determine and display data source
transport_data_available = not all_data["schedules"].empty or not all_data["traffic_status"].empty
if all_data["idfm_raw"]:
    data_source.success("📡 Using IDFM + RATP data")
elif transport_data_available:
    data_source.success("🚇 Using RATP data")
else:
    data_source.error("❌ No transport data")

# Prepare the date
current_date = datetime.now().strftime("%Y-%m-%d")
current_time = datetime.now().strftime("%H:%M:%S")

# Pages
if page == "Overview":
    st.title("🏢 La Défense Mobility Dashboard")
    st.subheader(f"Current Status as of {current_date} {current_time}")

    # Enhanced summary metrics
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        if not all_data["current_weather"].empty:
            temp = all_data['current_weather']['temperature'].iloc[0]
            feels_like = all_data['current_weather']['feels_like'].iloc[0]
            st.metric(
                "🌡️ Temperature",
                f"{temp}°C",
                f"{feels_like - temp:+.1f}°C feels like"
            )
        else:
            st.metric("🌡️ Temperature", "N/A")

    with col2:
        if not all_data["traffic_status"].empty:
            # Count lines with issues by status
            status_counts = all_data["traffic_status"]["status"].value_counts()
            normal_lines = status_counts.get("normal", 0)
            total_lines = len(all_data["traffic_status"])
            issues = total_lines - normal_lines

            st.metric(
                "🚊 Transport Lines",
                f"{total_lines} total",
                f"{issues} with issues" if issues > 0 else "All normal"
            )
        else:
            st.metric("🚊 Transport Lines", "N/A")

    with col3:
        # Real reliability prediction
        predictions = all_data.get("predictions", {})
        if predictions:
            # Calculate average reliability across all lines
            reliabilities = [pred.get('reliability', 0) for pred in predictions.values() if 'reliability' in pred]
            if reliabilities:
                avg_reliability = sum(reliabilities) / len(reliabilities)
                st.metric(
                    "🔮 Avg Reliability",
                    f"{avg_reliability:.1%}",
                    f"{len(reliabilities)} lines predicted"
                )
            else:
                st.metric("🔮 Avg Reliability", "N/A")
        else:
            st.metric("🔮 Predictions", "Not available")

    with col4:
        if not all_data["stations"].empty:
            total_stations = len(all_data["stations"])
            accessible_stations = len(all_data["stations"][
                all_data["stations"].get("wheelchair_accessible", "unknown") == "yes"
            ]) if "wheelchair_accessible" in all_data["stations"].columns else 0

            st.metric(
                "🚉 Stations",
                f"{total_stations} total",
                f"{accessible_stations} accessible"
            )
        else:
            st.metric("🚉 Stations", "N/A")

    with col5:
        # Prediction freshness indicator
        if predictions:
            st.metric("🔄 Status", "Live ML", "Real-time predictions")
        elif "tomtom_flow" in all_data["road_traffic"]:
            st.metric("🚗 Road Traffic", "Live data", "TomTom")
        else:
            st.metric("🚗 Data Status", "Static", "No real-time")

    # Map of La Défense area
    st.markdown("---")
    render_station_map(all_data["stations"])

    # Transport status summary
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🚊 Transport Status Summary")
        if not all_data["traffic_status"].empty:
            status_summary = all_data["traffic_status"]["status"].value_counts()

            # Create a mini status display
            for status, count in status_summary.items():
                status_display = {
                    "normal": "✅ Normal Service",
                    "minor": "⚠️ Minor Issues",
                    "major": "🚨 Major Issues",
                    "critical": "❌ Critical Issues"
                }.get(status, f"❓ {status}")

                st.markdown(f"**{status_display}**: {count} lines")
        else:
            st.info("No transport status data available")

    with col2:
        st.subheader("📅 Next Departures")
        render_enhanced_schedule_summary(all_data["schedules"])

elif page == "Route Planner":
    st.title("🗺️ La Défense Route Planner")
    st.subheader("Find the best route based on real-time conditions")

    # Initialize session state
    initialize_route_planner_state()

    # Enhanced route planner with proper state management
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 📍 Journey Details")

        # Origin selection with session state
        available_stations = all_data["stations"]["name"].unique() if not all_data["stations"].empty else []
        if len(available_stations) == 0:
            available_stations = ["La Défense Grande Arche", "Esplanade de La Défense", "Charles de Gaulle-Étoile",
                                  "Châtelet-Les Halles"]

        # Find current index for origin
        try:
            origin_index = list(available_stations).index(st.session_state.route_origin)
        except ValueError:
            origin_index = 0
            st.session_state.route_origin = available_stations[0]

        origin = st.selectbox(
            "🚀 Origin Station",
            available_stations,
            index=origin_index,
            help="Select your starting point",
            key="origin_selectbox"
        )

        # Update session state when origin changes
        if origin != st.session_state.route_origin:
            st.session_state.route_origin = origin
            st.rerun()

        # Transport preferences with session state
        st.markdown("### 🎯 Travel Preferences")

        pref_col1, pref_col2 = st.columns(2)
        with pref_col1:
            time_pref = st.slider(
                "⏱️ Prioritize speed",
                0.0, 1.0,
                st.session_state.route_preferences['time_pref'],
                key="time_slider_unique"
            )
            transfer_pref = st.slider(
                "🔄 Minimize transfers",
                0.0, 1.0,
                st.session_state.route_preferences['transfer_pref'],
                key="transfer_slider_unique"
            )

        with pref_col2:
            comfort_pref = st.slider(
                "💺 Prefer comfort",
                0.0, 1.0,
                st.session_state.route_preferences['comfort_pref'],
                key="comfort_slider_unique"
            )
            cost_pref = st.slider(
                "💰 Minimize cost",
                0.0, 1.0,
                st.session_state.route_preferences['cost_pref'],
                key="cost_slider_unique"
            )

        # Update preferences in session state
        st.session_state.route_preferences = {
            'time_pref': time_pref,
            'transfer_pref': transfer_pref,
            'comfort_pref': comfort_pref,
            'cost_pref': cost_pref
        }

        # Accessibility and environmental preferences with session state
        access_pref = st.checkbox(
            "♿ Require wheelchair accessibility",
            value=st.session_state.route_accessibility,
            key="access_check_unique"
        )
        st.session_state.route_accessibility = access_pref

        eco_friendly = st.checkbox(
            "🌱 Prefer eco-friendly routes",
            value=st.session_state.route_eco_friendly,
            key="eco_check_unique"
        )
        st.session_state.route_eco_friendly = eco_friendly

        # Transport mode preferences with session state
        st.markdown("### 🚊 Preferred Transport")
        transport_modes = st.multiselect(
            "Select preferred transport modes",
            ["Metro", "RER", "Transilien", "Bus", "Walking"],
            default=st.session_state.route_transport_modes,
            help="Choose which transport types to include in route planning",
            key="transport_multiselect_unique"
        )
        st.session_state.route_transport_modes = transport_modes

    with col2:
        st.markdown("### 📍 Destination & Timing")

        # Filter out origin from destination options
        destination_options = [station for station in available_stations if station != origin]
        if len(destination_options) == 0:
            destination_options = ["Châtelet-Les Halles", "Charles de Gaulle-Étoile", "Nation"]

        # Find current index for destination
        try:
            if st.session_state.route_destination and st.session_state.route_destination in destination_options:
                dest_index = destination_options.index(st.session_state.route_destination)
            else:
                dest_index = 0
                st.session_state.route_destination = destination_options[0]
        except (ValueError, IndexError):
            dest_index = 0
            st.session_state.route_destination = destination_options[0] if destination_options else None

        destination = st.selectbox(
            "🎯 Destination",
            destination_options,
            index=dest_index,
            help="Select your destination",
            key="destination_select_unique"
        )

        # Update destination in session state
        if destination != st.session_state.route_destination:
            st.session_state.route_destination = destination

        # FIXED: Departure time with proper session state management
        st.markdown("#### ⏰ Departure Time")

        # Journey type with session state
        journey_type = st.radio(
            "📅 Journey Type",
            ["Now", "Scheduled", "Return Journey"],
            index=["Now", "Scheduled", "Return Journey"].index(st.session_state.route_journey_type),
            horizontal=True,
            key="journey_type_unique"
        )
        st.session_state.route_journey_type = journey_type

        # Conditional departure time input based on journey type
        if journey_type == "Now":
            # For "Now", use current time but don't make it editable
            departure_time = datetime.now().time()
            st.info(f"🕐 Departing now: {departure_time.strftime('%H:%M')}")
            # Update session state to current time
            st.session_state.route_departure_time = departure_time

        elif journey_type == "Scheduled":
            # For "Scheduled", allow time editing with proper state management
            st.write("Select your preferred departure time:")

            # Create time input with session state
            departure_time = st.time_input(
                "Departure time",
                value=st.session_state.route_departure_time,
                help="Choose when you want to depart",
                key="departure_time_input_unique"
            )

            # Update session state when time changes
            if departure_time != st.session_state.route_departure_time:
                st.session_state.route_departure_time = departure_time
                # Don't rerun immediately for time input to allow smooth editing

            # Show selected time
            st.success(f"🕐 Scheduled departure: {departure_time.strftime('%H:%M')}")

        else:  # Return Journey
            # For return journey, show two time inputs
            st.write("Plan your return journey:")

            outbound_time = st.time_input(
                "Outbound departure",
                value=st.session_state.route_departure_time,
                key="outbound_time_unique"
            )

            # Default return time 4 hours later
            return_default = (datetime.combine(datetime.today(), outbound_time) + timedelta(hours=4)).time()
            return_time = st.time_input(
                "Return departure",
                value=return_default,
                key="return_time_unique"
            )

            departure_time = outbound_time  # Use outbound time for calculation
            st.session_state.route_departure_time = departure_time

            st.info(f"🔄 Outbound: {outbound_time.strftime('%H:%M')} | Return: {return_time.strftime('%H:%M')}")

        # Show current selections with better formatting
        st.markdown("### 📝 Current Selection")

        # Create a nice summary box
        selection_summary = f"""
        **🚀 From:** {origin}  
        **🎯 To:** {destination}  
        **🕐 When:** {departure_time.strftime('%H:%M')} ({journey_type.lower()})  
        **🚊 Modes:** {', '.join(transport_modes)}  
        **♿ Accessible:** {'Yes' if access_pref else 'No'}  
        **🌱 Eco-friendly:** {'Yes' if eco_friendly else 'No'}
        """

        st.info(selection_summary)

    # Only show button if transport modes are selected and destinations are different
    if not transport_modes:
        st.warning("⚠️ Please select at least one transport mode to find routes.")
    elif origin == destination:
        st.error("❌ Origin and destination cannot be the same. Please select different stations.")
    else:
        # Add some spacing
        st.markdown("---")

        # Create a more prominent find routes button
        button_col1, button_col2, button_col3 = st.columns([1, 2, 1])
        with button_col2:
            find_routes_clicked = st.button(
                "🔍 Find Routes",
                use_container_width=True,
                type="primary",
                key="find_routes_btn_unique"
            )

        if find_routes_clicked:
            st.markdown("---")
            st.subheader("🛤️ Recommended Routes")

            # Prepare preferences dictionary
            preferences = {
                'time_pref': time_pref,
                'transfer_pref': transfer_pref,
                'comfort_pref': comfort_pref,
                'cost_pref': cost_pref,
                'accessibility': access_pref,
                'eco_pref': 1.0 if eco_friendly else 0.2,
                'accessibility_pref': 1.0 if access_pref else 0.1
            }

            # Calculate actual routes with loading spinner
            with st.spinner(f"🔍 Finding optimal routes from {origin} to {destination}..."):
                try:

                    routes = calculate_routes_cached_with_time_fix(
    origin=origin,
    destination=destination,
    preferences=preferences,
    transport_modes=transport_modes,
    stations_df=all_data["stations"],
    schedules_df=all_data["schedules"],
    traffic_df=all_data["traffic_status"],
    departure_time=departure_time  # ← CRITICAL: This was missing!
)

                except Exception as e:
                    st.error(f"Error calculating routes: {str(e)}")
                    routes = {"Error": {"message": str(e)}}

            # Display results
            if "Error" in routes or "No routes found" in routes:
                st.error(
                    "❌ Could not find routes with the selected criteria. Try selecting more transport modes or different stations.")

                # Suggestions for better results
                st.markdown("### 💡 Suggestions:")
                st.info("• Try selecting more transport modes (Metro + RER + Bus)")
                st.info("• Check if the destination name is spelled correctly")
                st.info("• Try a nearby station if the exact station isn't found")

            else:
                # Display routes with enhanced information
                if len(routes) == 1:
                    route_cols = [st.container()]
                elif len(routes) == 2:
                    route_cols = st.columns(2)
                else:
                    route_cols = st.columns(min(3, len(routes)))

                for idx, (route_name, route_data) in enumerate(routes.items()):
                    col_idx = idx % len(route_cols)

                    with route_cols[col_idx]:
                        # Calculate scores for display
                        time_score = max(0, 100 - (route_data.get("total_time", 30) - 10) * 2)
                        eco_score = max(0, 100 - route_data.get("total_emissions", 50) * 0.5)
                        access_score = route_data.get("accessibility_score", 0.8) * 100

                        # Color coding based on route type
                        if "Metro" in route_name or "🚇" in route_name:
                            border_color = "#1f77b4"
                        elif "RER" in route_name or "🚄" in route_name:
                            border_color = "#ff7f0e"
                        elif "Bus" in route_name or "🚌" in route_name:
                            border_color = "#2ca02c"
                        elif "Walking" in route_name or "🚶" in route_name:
                            border_color = "#d62728"
                        else:
                            border_color = "#9467bd"

                        # Enhanced route card with departure time
                        departure_display = f" at {departure_time.strftime('%H:%M')}" if journey_type != "Now" else ""

                        st.markdown(f"""
                        <div style="
                            border: 2px solid {border_color}; 
                            border-radius: 10px; 
                            padding: 15px; 
                            margin: 10px 0;
                            background: white;
                            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                        ">
                            <h4 style="color: {border_color}; margin-top: 0;">{route_name}</h4>
                            <p><strong>🕐 Departure:</strong> {departure_time.strftime('%H:%M')}</p>
                            <p><strong>⏱️ Total time:</strong> {route_data.get('total_time', 'N/A')} min</p>
                            <p><strong>🔄 Transfers:</strong> {route_data.get('num_transfers', 0)}</p>
                            <p><strong>🌱 Emissions:</strong> {route_data.get('total_emissions', 0)}g CO₂</p>
                            <p><strong>♿ Accessibility:</strong> {access_score:.0f}%</p>
                        </div>
                        """, unsafe_allow_html=True)

                        # Route details with timing information
                        with st.expander("View Route Details", expanded=False):
                            route_details = route_data.get("route_details", [])

                            if route_details:
                                for step in route_details:
                                    transport_display = {
                                        "metro": "🚇 Metro",
                                        "rer": "🚄 RER",
                                        "rers": "🚄 RER",
                                        "bus": "🚌 Bus",
                                        "transilien": "🚂 Transilien",
                                        "walking": "🚶 Walking"
                                    }.get(step.get('transport_type', '').lower(),
                                          step.get('transport_type', 'Transport'))

                                    line_text = f" Line {step.get('line', '')}" if step.get('line') else ""

                                    st.write(f"**{transport_display}{line_text}**")
                                    st.write(f"From: {step.get('from_station', 'N/A')}")
                                    st.write(f"To: {step.get('to_station', 'N/A')}")
                                    st.write(f"Duration: {step.get('travel_time', 0):.0f} minutes")

                                    if step.get('departure_time'):
                                        st.write(f"Departure: {step.get('departure_time')}")
                                    if step.get('arrival_time'):
                                        st.write(f"Arrival: {step.get('arrival_time')}")
                                    if step.get('direction'):
                                        st.write(f"Direction: {step.get('direction')}")
                                    if step.get('emissions_g', 0) > 0:
                                        st.write(f"Emissions: {step.get('emissions_g', 0):.0f}g CO₂")

                                    st.write("---")
                            else:
                                st.write("No detailed route information available")

                # Summary recommendations
                st.markdown("---")
                st.subheader("💡 Route Recommendations")

                if routes:
                    route_values = [v for v in routes.values() if isinstance(v, dict) and 'total_time' in v]

                    if route_values:
                        best_time = min(route_values, key=lambda x: x.get('total_time', 999))
                        best_eco = min(route_values, key=lambda x: x.get('total_emissions', 999))
                        best_access = max(route_values, key=lambda x: x.get('accessibility_score', 0))

                        rec_col1, rec_col2, rec_col3 = st.columns(3)

                        with rec_col1:
                            st.success(f"⚡ **Fastest:** {best_time.get('total_time', 'N/A')} min")

                        with rec_col2:
                            st.success(f"🌱 **Greenest:** {best_eco.get('total_emissions', 'N/A')}g CO₂")

                        with rec_col3:
                            st.success(f"♿ **Most Accessible:** {best_access.get('accessibility_score', 0):.0%}")

elif page == "Weather Impact":
    st.title("🌤️ Weather Impact on Mobility")

    # Current weather with enhanced impact analysis
    if not all_data["current_weather"].empty:
        render_weather_section(
            all_data["current_weather"],
            all_data["daily_weather"],
            all_data["hourly_weather"]
        )

        # Enhanced mobility recommendations
        st.markdown("---")
        st.subheader("🚊 Transport Recommendations by Weather")

        current = all_data["current_weather"].iloc[0]

        # SAFE extraction of weather values (FIXES THE ERROR)
        precip = safe_get_weather_value(current, 'precipitation', 0)
        wind_speed = safe_get_weather_value(current, 'wind_speed', 0)
        temp = safe_get_weather_value(current, 'temperature', 15)
        visibility = safe_get_weather_value(current, 'visibility', 10)


        # Create recommendation cards
        recommendations = []

        # NOW SAFE: Precipitation-based recommendations
        if precip > 5:
            recommendations.extend([
                {"icon": "🚇", "title": "Metro Line 1",
                 "message": "Best choice during heavy rain - fully underground and automated", "priority": "high"},
                {"icon": "🚄", "title": "RER A/E", "message": "Good alternative with covered platforms at La Défense",
                 "priority": "medium"},
                {"icon": "⏱️", "title": "Travel Time", "message": "Allow extra 10-15 minutes due to slower traffic",
                 "priority": "medium"}
            ])
        elif precip > 1:
            recommendations.append(
                {"icon": "🌧️", "title": "Light Rain", "message": "Minor delays possible, consider covered transport",
                 "priority": "low"}
            )

        # Wind-based recommendations
        if wind_speed > 50:
            recommendations.extend([
                {"icon": "🚌", "title": "Bus Services",
                 "message": "May experience significant delays due to strong winds", "priority": "high"},
                {"icon": "🚶‍♀️", "title": "Walking Areas",
                 "message": "Avoid open areas around Grande Arche - dangerous wind conditions", "priority": "high"}
            ])
        elif wind_speed > 30:
            recommendations.extend([
                {"icon": "🚌", "title": "Bus Services", "message": "May experience delays due to moderate winds",
                 "priority": "medium"},
                {"icon": "🚶‍♀️", "title": "Walking Areas",
                 "message": "Take care around Grande Arche - strong wind corridors", "priority": "medium"}
            ])

        # Temperature-based recommendations
        if temp < 0:
            recommendations.extend([
                {"icon": "❄️", "title": "Platform Safety",
                 "message": "Platforms may be icy - allow extra time and wear appropriate footwear",
                 "priority": "high"},
                {"icon": "🏢", "title": "Indoor Routes",
                 "message": "Use Les Quatre Temps for warm pedestrian connections", "priority": "medium"}
            ])
        elif temp < 5:
            recommendations.extend([
                {"icon": "🧥", "title": "Cold Weather", "message": "Dress warmly for outdoor waiting areas",
                 "priority": "low"},
                {"icon": "🏢", "title": "Indoor Routes",
                 "message": "Consider indoor connections through shopping centers", "priority": "low"}
            ])
        elif temp > 35:
            recommendations.extend([
                {"icon": "🔥", "title": "Extreme Heat",
                 "message": "Seek air-conditioned transport, avoid prolonged outdoor waiting", "priority": "high"},
                {"icon": "💧", "title": "Hydration",
                 "message": "Stay hydrated - water fountains available in main stations", "priority": "medium"}
            ])
        elif temp > 28:
            recommendations.extend([
                {"icon": "🔆", "title": "Hot Weather", "message": "Metro Line 1 has air conditioning",
                 "priority": "medium"},
                {"icon": "💧", "title": "Hydration", "message": "Stay hydrated during travel", "priority": "low"}
            ])

        # Visibility-based recommendations
        if visibility < 1:
            recommendations.extend([
                {"icon": "🌫️", "title": "Poor Visibility",
                 "message": "Heavy fog - use underground transport exclusively", "priority": "high"},
                {"icon": "⏱️", "title": "Travel Time", "message": "Allow double the normal travel time",
                 "priority": "high"}
            ])
        elif visibility < 5:
            recommendations.append(
                {"icon": "🌫️", "title": "Reduced Visibility", "message": "Fog conditions - allow extra travel time",
                 "priority": "medium"}
            )

        # Display recommendations by priority
        if recommendations:
            priority_order = ["high", "medium", "low"]
            priority_colors = {"high": "#dc3545", "medium": "#fd7e14", "low": "#28a745"}
            priority_icons = {"high": "🚨", "medium": "⚠️", "low": "💡"}

            for priority in priority_order:
                priority_recs = [r for r in recommendations if r["priority"] == priority]
                if priority_recs:
                    st.markdown(f"### {priority_icons[priority]} {priority.title()} Priority")
                    for rec in priority_recs:
                        st.markdown(f"""
                        <div style="
                            border-left: 4px solid {priority_colors[priority]}; 
                            padding: 10px; 
                            margin: 10px 0; 
                            background-color: #f8f9fa;
                            border-radius: 0 8px 8px 0;
                        ">
                            <h4 style="margin: 0 0 5px 0;">{rec['icon']} {rec['title']}</h4>
                            <p style="margin: 0; color: #666;">{rec['message']}</p>
                        </div>
                        """, unsafe_allow_html=True)

        # Calculate and display overall impact
        st.markdown("---")
        st.subheader("📊 Overall Weather Impact Assessment")

        # Calculate composite impact score
        impact_score = 1.0

        if precip > 10:
            impact_score *= 1.5
        elif precip > 5:
            impact_score *= 1.3
        elif precip > 1:
            impact_score *= 1.1

        if wind_speed > 50:
            impact_score *= 1.4
        elif wind_speed > 30:
            impact_score *= 1.2

        if temp < -5 or temp > 40:
            impact_score *= 1.3
        elif temp < 0 or temp > 35:
            impact_score *= 1.15

        if visibility < 1:
            impact_score *= 1.6
        elif visibility < 5:
            impact_score *= 1.2

        # Cap maximum impact
        impact_score = min(impact_score, 3.0)

        # Display impact assessment
        if impact_score >= 2.0:
            st.error(f"🔴 **Severe Impact** - Travel time multiplier: {impact_score:.1f}x")
            st.error("Consider postponing non-essential travel or using underground transport exclusively.")
        elif impact_score >= 1.5:
            st.warning(f"🟡 **Moderate Impact** - Travel time multiplier: {impact_score:.1f}x")
            st.warning("Allow extra travel time and prefer covered transport options.")
        elif impact_score >= 1.2:
            st.info(f"🟢 **Minor Impact** - Travel time multiplier: {impact_score:.1f}x")
            st.info("Slight delays possible, plan accordingly.")
        else:
            st.success(f"✅ **Minimal Impact** - Travel time multiplier: {impact_score:.1f}x")
            st.success("Excellent conditions for all transport modes!")

        if not recommendations:
            st.success("🌟 Current weather conditions are ideal for all transport modes!")

    else:
        st.warning("No weather data available")

elif page == "Transport Analysis":
    st.title("🚊 Transportation Analysis")
    create_transport_dashboard_section(all_data["schedules"], all_data["traffic_status"])
    # Enhanced transport analysis with new lines
    col1, col2 = st.columns([2, 1])

    with col1:
        # Transport lines status
        render_transport_status(all_data["traffic_status"])

    with col2:
        # Quick stats
        if not all_data["traffic_status"].empty:
            st.markdown("### 📊 Quick Stats")

            total_lines = len(all_data["traffic_status"])
            status_counts = all_data["traffic_status"]["status"].value_counts()

            metrics_data = [
                {"metric": "Total Lines", "value": total_lines, "icon": "🚊"},
                {"metric": "Normal Service", "value": status_counts.get("normal", 0), "icon": "✅"},
                {"metric": "With Issues", "value": total_lines - status_counts.get("normal", 0), "icon": "⚠️"}
            ]

            for metric in metrics_data:
                st.markdown(f"""
                <div style="
                    background: white; 
                    padding: 15px; 
                    border-radius: 8px; 
                    border: 1px solid #ddd;
                    margin: 5px 0;
                    text-align: center;
                ">
                    <div style="font-size: 2em;">{metric['icon']}</div>
                    <div style="font-size: 1.5em; font-weight: bold;">{metric['value']}</div>
                    <div style="color: #666;">{metric['metric']}</div>
                </div>
                """, unsafe_allow_html=True)

    # Departure schedules with filters
    st.markdown("---")
    render_schedules(all_data["schedules"])

    # Transport usage patterns
    st.markdown("---")
    render_transport_usage_chart()

    # Performance metrics
    if not all_data["schedules"].empty and not all_data["traffic_status"].empty:
        st.markdown("---")
        render_line_performance_metrics(all_data["schedules"], all_data["traffic_status"])

elif page == "Station Information":
    st.title("🚉 Station Information")

    # Enhanced station information with better filtering
    tab1, tab2 = st.tabs(["🔍 Station Details", "📊 Station Statistics"])

    with tab1:
        render_station_details(all_data["stations"])


    with tab2:
        if not all_data["stations"].empty:
            st.subheader("📈 Station Network Statistics")

            # Station type distribution
            if "type" in all_data["stations"].columns:
                type_counts = all_data["stations"]["type"].value_counts()

                col1, col2 = st.columns(2)

                with col1:
                    fig_types = px.pie(
                        values=type_counts.values,
                        names=type_counts.index,
                        title="Station Types Distribution"
                    )
                    st.plotly_chart(fig_types, use_container_width=True)

                with col2:
                    # Accessibility stats
                    if "wheelchair_accessible" in all_data["stations"].columns:
                        access_counts = all_data["stations"]["wheelchair_accessible"].value_counts()
                        fig_access = px.bar(
                            x=access_counts.index,
                            y=access_counts.values,
                            title="Wheelchair Accessibility Status"
                        )
                        st.plotly_chart(fig_access, use_container_width=True)

elif page == "Data Quality":
    st.title("🔍 Data Quality Status")

    quality = all_data["quality_status"]

    # Enhanced data quality dashboard
    col1, col2 = st.columns([1, 1])

    # with col1:
    #     # Display quality score
    #     if "passed" in quality and "total" in quality:
    #         quality_percentage = (quality["passed"] / quality["total"]) * 100
    #
    #         # Create a gauge chart
    #         fig = go.Figure(go.Indicator(
    #             mode="gauge+number",
    #             value=quality_percentage,
    #             domain={'x': [0, 1], 'y': [0, 1]},
    #             title={'text': "Data Quality Score"},
    #             gauge={
    #                 'axis': {'range': [0, 100]},
    #                 'bar': {'color': "darkblue"},
    #                 'steps': [
    #                     {'range': [0, 60], 'color': "#ffcccc"},
    #                     {'range': [60, 80], 'color': "#ffffcc"},
    #                     {'range': [80, 100], 'color': "#ccffcc"}
    #                 ],
    #                 'threshold': {
    #                     'line': {'color': "red", 'width': 4},
    #                     'thickness': 0.75,
    #                     'value': 90
    #                 }
    #             }
    #         ))
    #
    #         st.plotly_chart(fig, use_container_width=True)
    #     else:
    #         st.info("No detailed quality metrics available")

    with col1:
        # Data source health
        st.markdown("### 🏥 Data Source Health")

        data_sources = {
            "Weather": not all_data["current_weather"].empty,
            "Transport Schedules": not all_data["schedules"].empty,
            "Transport Status": not all_data["traffic_status"].empty,
            "Station Info": not all_data["stations"].empty,
            "Road Traffic": bool(all_data["road_traffic"]),
            "IDFM Data": bool(all_data["idfm_raw"])
        }

        for source, available in data_sources.items():
            status_icon = "🟢" if available else "🔴"
            status_text = "Operational" if available else "Unavailable"

            st.markdown(f"{status_icon} **{source}**: {status_text}")

    # Detailed quality information
    # st.markdown("---")
    # st.subheader("📋 Quality Details")

    # if "timestamp" in quality:
    #     st.write(f"**Last check**: {quality['timestamp']}")
    #
    # if "passed" in quality and "total" in quality:
    #     st.write(f"**Checks passed**: {quality['passed']}/{quality['total']} ({quality_percentage:.1f}%)")

elif page == "Predictions":
    st.title("🔮 Traffic and Mobility Predictions")
    st.subheader(f"Real-time ML Forecasts for {current_date}")

    # Check if predictions are available
    predictions = all_data.get("predictions", {})
    forecasts = all_data.get("forecasts", {})

    if not predictions and not forecasts:
        st.warning("⚠️ Prediction models not available. Please run model training first.")
        st.info("Run: `python run_historical_and_predictions.py` to set up predictions")

        # Show fallback content
        st.markdown("---")
        st.subheader("📊 Static Predictions (Demo)")

        # Keep your existing static prediction content as fallback
        pred_tab1, pred_tab2, pred_tab3, pred_tab4 = st.tabs([
            "🚗 Traffic Predictions",
            "🌤️ Weather Impact",
            "🚊 Transport Reliability",
            "🗺️ Congestion Zones"
        ])

    else:
        # Real predictions available - show ML-powered content
        pred_tab1, pred_tab2, pred_tab3, pred_tab4 = st.tabs([
            "🚊 Transport Reliability",
            "📈 24-Hour Forecasts",
            "⚡ Real-time Insights",
            "🗺️ Congestion Areas"
        ])

        with pred_tab1:
            st.write("Current reliability predictions for all transport lines")

            if predictions:
                # Create detailed prediction cards
                for line_key, prediction in predictions.items():
                    transport_type, line = line_key.split('_', 1)

                    # Get line display info
                    line_icons = {
                        "metro": "🚇", "rers": "🚄",
                        "transilien": "🚂", "buses": "🚌"
                    }

                    icon = line_icons.get(transport_type, "🚊")
                    line_name = f"{icon} {transport_type.title()} {line}"

                    with st.expander(f"{line_name} - Detailed Prediction", expanded=True):
                        pred_col1, pred_col2, pred_col3 = st.columns(3)

                        with pred_col1:
                            reliability = prediction.get('reliability', 0)
                            st.metric(
                                "Reliability Score",
                                f"{reliability:.1%}",
                                help="Probability of on-time service"
                            )

                        with pred_col2:
                            delay = prediction.get('expected_delay_minutes', 0)
                            st.metric(
                                "Expected Delay",
                                f"{delay:.1f} min",
                                help="Predicted additional travel time"
                            )

                        with pred_col3:
                            load = prediction.get('passenger_load', 0)
                            st.metric(
                                "Passenger Load",
                                f"{load:.0%}",
                                help="Estimated capacity utilization"
                            )

                        # Congestion info if available
                        if 'congestion_level' in prediction:
                            congestion = prediction['congestion_level']
                            st.info(f"🚗 Road Congestion Level: **{congestion}**")
            else:
                st.error("No real-time predictions available")

        with pred_tab2:
            st.write("24-hour reliability forecasts for main transport lines")

            if forecasts:
                # Select line for forecast
                available_lines = list(forecasts.keys())
                selected_line = st.selectbox(
                    "Select transport line for 24h forecast:",
                    available_lines,
                    format_func=lambda x: {
                        "metro_1": "🚇 Metro Line 1",
                        "rers_A": "🚄 RER A",
                        "rers_E": "🚄 RER E"
                    }.get(x, x)
                )

                if selected_line and selected_line in forecasts:
                    forecast_df = forecasts[selected_line]

                    # Create reliability forecast chart
                    fig = px.line(
                        forecast_df,
                        x='hour',
                        y='reliability',
                        title=f'24-Hour Reliability Forecast - {selected_line.replace("_", " ").title()}',
                        labels={'reliability': 'Reliability (%)', 'hour': 'Hour of Day'},
                        range_y=[0.6, 1.0]
                    )

                    # Add current hour marker
                    current_hour = datetime.now().hour
                    fig.add_vline(
                        x=current_hour,
                        line_dash="dash",
                        line_color="red",
                        annotation_text="Now"
                    )

                    # Add peak hour indicators
                    fig.add_vrect(x0=7, x1=9, fillcolor="red", opacity=0.1, annotation_text="Morning Peak")
                    fig.add_vrect(x0=17, x1=19, fillcolor="red", opacity=0.1, annotation_text="Evening Peak")

                    st.plotly_chart(fig, use_container_width=True)

                    # Show next 6 hours details
                    st.subheader("Next 6 Hours Detailed Forecast")
                    next_6h = forecast_df.head(6)

                    forecast_cols = st.columns(6)
                    for i, (_, row) in enumerate(next_6h.iterrows()):
                        with forecast_cols[i]:
                            hour_time = f"{row['hour']:02d}:00"
                            reliability = row.get('reliability', 0)
                            delay = row.get('expected_delay_minutes', 0)

                            color = "#28a745" if reliability >= 0.9 else "#ffc107" if reliability >= 0.8 else "#dc3545"

                            st.markdown(f"""
                            <div style="text-align: center; padding: 10px; border: 1px solid {color}; border-radius: 5px;">
                                <div style="font-weight: bold;">{hour_time}</div>
                                <div style="color: {color}; font-size: 1.2em;">{reliability:.0%}</div>
                                <div style="font-size: 0.8em;">+{delay:.1f}min</div>
                            </div>
                            """, unsafe_allow_html=True)
            else:
                st.error("No 24-hour forecasts available")

        with pred_tab3:
            st.write("Real-time insights and recommendations based on ML predictions")

            if predictions:
                # Generate insights based on predictions
                insights = []

                for line_key, prediction in predictions.items():
                    transport_type, line = line_key.split('_', 1)
                    reliability = prediction.get('reliability', 0)
                    delay = prediction.get('expected_delay_minutes', 0)
                    load = prediction.get('passenger_load', 0)

                    line_display = f"{transport_type.title()} {line}"

                    if reliability < 0.7:
                        insights.append({
                            "type": "warning",
                            "title": f"⚠️ {line_display} - Low Reliability",
                            "message": f"Current reliability: {reliability:.0%}. Consider alternative routes."
                        })
                    elif delay > 5:
                        insights.append({
                            "type": "info",
                            "title": f"🕐 {line_display} - Delays Expected",
                            "message": f"Expected delay: {delay:.1f} minutes. Plan extra travel time."
                        })
                    elif load > 0.8:
                        insights.append({
                            "type": "info",
                            "title": f"👥 {line_display} - High Passenger Load",
                            "message": f"Current load: {load:.0%}. Expect crowded conditions."
                        })
                    elif reliability > 0.95 and delay < 2:
                        insights.append({
                            "type": "success",
                            "title": f"✅ {line_display} - Excellent Service",
                            "message": f"Reliability: {reliability:.0%}, minimal delays expected."
                        })

                if insights:
                    st.subheader("🧠 AI-Generated Insights")
                    for insight in insights:
                        if insight["type"] == "warning":
                            st.warning(f"**{insight['title']}**\n\n{insight['message']}")
                        elif insight["type"] == "info":
                            st.info(f"**{insight['title']}**\n\n{insight['message']}")
                        elif insight["type"] == "success":
                            st.success(f"**{insight['title']}**\n\n{insight['message']}")
                else:
                    st.info("All transport lines are operating normally with good reliability.")
            else:
                st.error("No real-time insights available")

    with pred_tab4:
        st.write("Traffic hotspots and congestion information for La Défense area")

        # Traffic hotspots information
        st.subheader("🚨 Known Congestion Areas")

        congestion_zones = [
            {
                "zone": "Grande Arche Area",
                "peak_hours": "8:00-9:30, 18:00-19:30",
                "congestion_level": "High",
                "recommendation": "Use RER E instead of RER A during peak hours",
                "description": "Heavy pedestrian and vehicle traffic around the iconic Grande Arche monument"
            },
            {
                "zone": "CNIT Complex",
                "peak_hours": "12:00-14:00, 18:00-20:00",
                "congestion_level": "Moderate",
                "recommendation": "Bus 144 provides good alternative routing",
                "description": "Business district with conference center generates midday and evening peaks"
            },
            {
                "zone": "Quatre Temps",
                "peak_hours": "11:00-13:00, 17:00-19:00",
                "congestion_level": "Moderate",
                "recommendation": "Use covered walkways for pedestrian access",
                "description": "Major shopping center creates consistent foot traffic and delivery congestion"
            },
            {
                "zone": "Pont de Neuilly",
                "peak_hours": "7:30-9:00, 17:30-19:00",
                "congestion_level": "Very High",
                "recommendation": "Avoid during rush hours - use public transport",
                "description": "Critical bridge connection experiences severe rush hour bottlenecks"
            }
        ]

        # Display in a 2x2 grid using Streamlit columns
        col1, col2 = st.columns(2)

        for i, zone in enumerate(congestion_zones):
            # Alternate between columns
            current_col = col1 if i % 2 == 0 else col2

            with current_col:
                # Create a container for each zone
                with st.container():
                    # Header with emoji and zone name
                    if zone["congestion_level"] == "Very High":
                        st.error(f"📍 **{zone['zone']}** - {zone['congestion_level']} Congestion")
                    elif zone["congestion_level"] == "High":
                        st.warning(f"📍 **{zone['zone']}** - {zone['congestion_level']} Congestion")
                    else:
                        st.info(f"📍 **{zone['zone']}** - {zone['congestion_level']} Congestion")

                    # Zone details in a clean format
                    st.write(f"**⏰ Peak Hours:** {zone['peak_hours']}")
                    st.write(f"**📝 Description:** {zone['description']}")
                    st.success(f"💡 **Tip:** {zone['recommendation']}")

                    # Add some spacing
                    st.write("")

        # Additional traffic insights
        st.markdown("---")
        st.subheader("📊 Traffic Insights & Tips")

        # Create insight cards using Streamlit columns
        insight_col1, insight_col2, insight_col3, insight_col4 = st.columns(4)

        with insight_col1:
            st.info("""
            **🕐 Best Travel Times**

            Avoid 8:00-9:30 AM and 6:00-7:30 PM for optimal traffic conditions
            """)

        with insight_col2:
            st.info("""
            **🚇 Public Transport Priority**

            Metro Line 1 and RER A/E provide the most reliable alternatives during peak hours
            """)

        with insight_col3:
            st.info("""
            **🚶‍♂️ Pedestrian Networks**

            Underground and covered walkways connect major buildings safely
            """)

        with insight_col4:
            st.info("""
            **📱 Real-time Updates**

            Check transport apps before traveling for live disruption information
            """)

        # Traffic prediction summary
        st.markdown("---")
        st.subheader("🔮 Traffic Predictions Summary")

        # Use expander for detailed predictions
        with st.expander("View Detailed Predictions", expanded=True):
            prediction_col1, prediction_col2 = st.columns(2)

            with prediction_col1:
                st.markdown("**🌅 Morning Period**")
                st.error(
                    "**Morning Rush (8:00-9:30):** Heavy congestion expected around Grande Arche and Pont de Neuilly")
                st.warning("**Mid-Morning (9:30-11:00):** Moderate traffic flow, good travel window")

            with prediction_col2:
                st.markdown("**🌆 Afternoon/Evening Period**")
                st.warning("**Lunch Time (12:00-14:00):** Moderate congestion around business districts and CNIT")
                st.error("**Evening Rush (18:00-19:30):** Peak congestion across all major zones")

        # Additional recommendations
        st.success("""
        **🎯 Smart Travel Recommendations:**
        - Use Metro Line 1 for fastest connections during any hour
        - RER E is less crowded than RER A during peak times  
        - Consider walking through Les Quatre Temps for weather protection
        - Bus 144 provides good local circulation around La Défense
        """)
        # Additional traffic insights
        st.markdown("---")
        st.subheader("📊 Traffic Insights & Tips")

        # Create insight cards
        insights = [
            {
                "icon": "🕐",
                "title": "Best Travel Times",
                "content": "Avoid 8:00-9:30 AM and 6:00-7:30 PM for optimal traffic conditions"
            },
            {
                "icon": "🚇",
                "title": "Public Transport Priority",
                "content": "Metro Line 1 and RER A/E provide the most reliable alternatives during peak hours"
            },
            {
                "icon": "🚶‍♂️",
                "title": "Pedestrian Networks",
                "content": "Underground and covered walkways connect major buildings safely"
            },
            {
                "icon": "📱",
                "title": "Real-time Updates",
                "content": "Check transport apps before traveling for live disruption information"
            }
        ]

        # Traffic prediction summary
        st.markdown("---")
        st.info("""
        **🔮 Traffic Predictions Summary:**
        - **Morning Rush (8:00-9:30)**: Heavy congestion expected around Grande Arche and Pont de Neuilly
        - **Lunch Time (12:00-14:00)**: Moderate congestion around business districts and CNIT
        - **Evening Rush (18:00-19:30)**: Peak congestion across all major zones
        - **Off-Peak Hours**: Generally smooth traffic flow with occasional delays near shopping areas
        """)
# Footer
st.markdown("---")
footer_col1, footer_col2, footer_col3 = st.columns(3)

with footer_col1:
    st.markdown("**🏢 La Défense Mobility Dashboard**")
    st.markdown("Real-time urban mobility optimization")

with footer_col2:
    st.markdown("**📊 Data Sources**")
    st.markdown("RATP • IDFM • Visual Crossing • TomTom • OpenStreetMap")

with footer_col3:
    st.markdown("**🔄 Data Refresh**")
    st.markdown("Every 15-60 minutes • Powered by open data")