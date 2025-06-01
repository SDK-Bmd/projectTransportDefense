"""
Streamlit dashboard for La Défense mobility visualization with predictive features
Updated to support Metro, RER A/E, Transilien L, and multiple bus lines
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import io
import sys
import os

# Add paths for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# Import project modules
from configuration.config import DATA_LAKE, LADEFENSE_COORDINATES
from utils.data_lake_utils import get_s3_client, read_parquet_from_data_lake, read_json_from_data_lake
from dash_app.components.maps import render_station_map
from dash_app.components.weather import render_weather_section
from dash_app.components.transport import (
    render_transport_status, render_schedules, render_schedule_summary,
    render_transport_usage_chart, render_line_performance_metrics
)
from dash_app.components.stations import render_station_details, render_accessibility_overview


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

        return {
            "current_weather": current_weather,
            "daily_weather": daily_weather,
            "hourly_weather": hourly_weather,
            "schedules": schedules_df,
            "traffic_status": traffic_df,
            "stations": stations_df,
            "road_traffic": road_traffic_data,
            "quality_status": quality_status,
            "idfm_raw": idfm_data
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

page = st.sidebar.selectbox(
    "Choose a page",
    ["Overview", "Route Planner", "Weather Impact", "Transport Analysis", "Station Information", "Data Quality", "Predictions"]
)

# Data source indicator section
st.sidebar.markdown("### 📊 Data Sources")
data_source = st.sidebar.empty()

# Transport lines coverage
st.sidebar.markdown("### 🚊 Lines Covered")
st.sidebar.markdown("""
**Metro**: Line 1  
**RER**: A, E  
**Transilien**: L  
**Bus**: 73, 144, 158, 163, 174, 178, 258, 262, 272, 275
""")

# Last refresh time
st.sidebar.markdown("---")
refresh_time = datetime.now().strftime("%Y-%m-%d %H:%M")
st.sidebar.write(f"🔄 Last refresh: {refresh_time}")

# Button to refresh data
if st.sidebar.button("🔄 Refresh Data", use_container_width=True):
    st.cache_data.clear()
    st.rerun()

# Load the data
all_data = load_all_data()

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

    # Data quality indicator
    quality = all_data["quality_status"]
    quality_color = "green" if quality.get("status") == "Good" else "orange" if quality.get(
        "status") == "Issues Detected" else "gray"

    st.markdown(f"""
    <div style="
        padding: 8px 16px; 
        border-radius: 8px; 
        background-color: {quality_color}; 
        color: white;
        display: inline-block;
        margin-bottom: 20px;
        font-weight: bold;">
        🔍 Data Quality: {quality.get("status", "Unknown")}
    </div>
    """, unsafe_allow_html=True)

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
        if not all_data["schedules"].empty:
            # Count unique transport types
            transport_types = all_data["schedules"]["transport_type"].nunique()
            next_departures = len(all_data["schedules"])
            st.metric(
                "📅 Departures",
                f"{next_departures} scheduled",
                f"{transport_types} transport types"
            )
        else:
            st.metric("📅 Departures", "N/A")

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
        if "tomtom_flow" in all_data["road_traffic"]:
            st.metric("🚗 Road Traffic", "Live data", "TomTom")
        elif all_data["road_traffic"]:
            st.metric("🚗 Road Traffic", "Available", "Multiple sources")
        else:
            st.metric("🚗 Road Traffic", "N/A")

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
        render_schedule_summary(all_data["schedules"])

elif page == "Route Planner":
    st.title("🗺️ La Défense Route Planner")
    st.subheader("Find the best route based on real-time conditions")

    # Enhanced route planner with more transport options
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 📍 Journey Details")

        # Origin selection with better filtering
        available_stations = all_data["stations"]["name"].unique() if not all_data["stations"].empty else []
        origin = st.selectbox(
            "🚀 Origin Station",
            ["La Défense Grande Arche"] + list(available_stations),
            help="Select your starting point"
        )

        # Transport preferences with new options
        st.markdown("### 🎯 Travel Preferences")

        pref_col1, pref_col2 = st.columns(2)
        with pref_col1:
            time_pref = st.slider("⏱️ Prioritize speed", 0.0, 1.0, 1.0)
            transfer_pref = st.slider("🔄 Minimize transfers", 0.0, 1.0, 0.3)

        with pref_col2:
            comfort_pref = st.slider("💺 Prefer comfort", 0.0, 1.0, 0.5)
            cost_pref = st.slider("💰 Minimize cost", 0.0, 1.0, 0.2)

        # Accessibility and environmental preferences
        access_pref = st.checkbox("♿ Require wheelchair accessibility")
        eco_friendly = st.checkbox("🌱 Prefer eco-friendly routes")

        # Transport mode preferences
        st.markdown("### 🚊 Preferred Transport")
        transport_modes = st.multiselect(
            "Select preferred transport modes",
            ["Metro", "RER", "Transilien", "Bus", "Walking"],
            default=["Metro", "RER", "Transilien"],
            help="Choose which transport types to include in route planning"
        )

    with col2:
        st.markdown("### 📍 Destination & Timing")

        destination = st.selectbox(
            "🎯 Destination",
            available_stations if len(available_stations) > 0 else ["Please select destination"]
        )

        departure_time = st.time_input(
            "⏰ Departure time",
            datetime.now().time(),
            help="When do you want to depart?"
        )

        # Journey type
        journey_type = st.radio(
            "📅 Journey Type",
            ["Now", "Scheduled", "Return Journey"],
            horizontal=True
        )

    if st.button("🔍 Find Routes", use_container_width=True, type="primary"):
        st.markdown("---")
        st.subheader("🛤️ Recommended Routes")

        # Enhanced route calculation with all transport types
        routes = {
            "🚀 Fastest Route": {
                "route_details": [
                    {
                        "transport_type": "metro",
                        "line": "1",
                        "from_station": origin,
                        "to_station": "Esplanade de La Défense",
                        "travel_time": 5,
                        "congestion_factor": 1.0,
                        "emissions_g": 20
                    },
                    {
                        "transport_type": "walking",
                        "line": "",
                        "from_station": "Esplanade de La Défense",
                        "to_station": destination,
                        "travel_time": 7,
                        "congestion_factor": 1.0,
                        "emissions_g": 0
                    }
                ],
                "total_time": 12,
                "num_transfers": 1,
                "total_emissions": 20,
                "accessibility_score": 0.9
            },
            "🌱 Eco Route": {
                "route_details": [
                    {
                        "transport_type": "rer",
                        "line": "E",
                        "from_station": origin,
                        "to_station": "Grande Arche",
                        "travel_time": 8,
                        "congestion_factor": 1.1,
                        "emissions_g": 12
                    },
                    {
                        "transport_type": "walking",
                        "line": "",
                        "from_station": "Grande Arche",
                        "to_station": destination,
                        "travel_time": 5,
                        "congestion_factor": 1.0,
                        "emissions_g": 0
                    }
                ],
                "total_time": 13,
                "num_transfers": 1,
                "total_emissions": 12,
                "accessibility_score": 0.95
            },
            "🚌 Bus Route": {
                "route_details": [
                    {
                        "transport_type": "bus",
                        "line": "144",
                        "from_station": origin,
                        "to_station": "CNIT",
                        "travel_time": 10,
                        "congestion_factor": 1.3,
                        "emissions_g": 45
                    },
                    {
                        "transport_type": "walking",
                        "line": "",
                        "from_station": "CNIT",
                        "to_station": destination,
                        "travel_time": 3,
                        "congestion_factor": 1.0,
                        "emissions_g": 0
                    }
                ],
                "total_time": 13,
                "num_transfers": 1,
                "total_emissions": 45,
                "accessibility_score": 0.7
            }
        }

        # Filter routes based on preferences
        if eco_friendly:
            # Sort by emissions
            routes = dict(sorted(routes.items(), key=lambda x: x[1]["total_emissions"]))

        # Display routes with enhanced information
        route_cols = st.columns(len(routes))

        for idx, (route_name, route_data) in enumerate(routes.items()):
            with route_cols[idx]:
                # Calculate scores
                time_score = max(0, 100 - (route_data["total_time"] - 10) * 5)
                eco_score = max(0, 100 - route_data["total_emissions"])
                access_score = route_data["accessibility_score"] * 100

                st.markdown(f"""
                <div style="
                    border: 2px solid #e0e0e0; 
                    border-radius: 10px; 
                    padding: 15px; 
                    margin: 10px 0;
                    background: white;
                ">
                    <h4 style="color: #1f77b4; margin-top: 0;">{route_name}</h4>
                    <p><strong>⏱️ Total time:</strong> {route_data['total_time']} min</p>
                    <p><strong>🔄 Transfers:</strong> {route_data['num_transfers']}</p>
                    <p><strong>🌱 Emissions:</strong> {route_data['total_emissions']}g CO₂</p>
                    <p><strong>♿ Accessibility:</strong> {access_score:.0f}%</p>
                </div>
                """, unsafe_allow_html=True)

                # Route details
                with st.expander("View Route Details"):
                    for step in route_data["route_details"]:
                        transport_display = {
                            "metro": "🚇 Metro",
                            "rer": "🚄 RER",
                            "bus": "🚌 Bus",
                            "walking": "🚶 Walking"
                        }.get(step['transport_type'], step['transport_type'])

                        st.write(f"{transport_display} {step['line']} from **{step['from_station']}** to **{step['to_station']}** - {step['travel_time']} min")

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
        precip = current.get('precipitation', 0)
        wind_speed = current.get('wind_speed', 0)
        temp = current.get('temperature', 15)

        # Create recommendation cards
        recommendations = []

        if precip > 5:
            recommendations.extend([
                {"icon": "🚇", "title": "Metro Line 1", "message": "Best choice during heavy rain - fully underground and automated", "priority": "high"},
                {"icon": "🚄", "title": "RER A/E", "message": "Good alternative with covered platforms at La Défense", "priority": "medium"},
                {"icon": "⏱️", "title": "Travel Time", "message": "Allow extra 10-15 minutes due to slower traffic", "priority": "medium"}
            ])

        if wind_speed > 30:
            recommendations.extend([
                {"icon": "🚌", "title": "Bus Services", "message": "May experience delays due to high winds", "priority": "low"},
                {"icon": "🚶‍♀️", "title": "Walking Areas", "message": "Take care around Grande Arche - strong wind corridors", "priority": "high"}
            ])

        if temp < 5:
            recommendations.extend([
                {"icon": "❄️", "title": "Platform Safety", "message": "Platforms may be slippery - allow extra time", "priority": "medium"},
                {"icon": "🏢", "title": "Indoor Routes", "message": "Use Les Quatre Temps for pedestrian connections", "priority": "low"}
            ])

        if temp > 28:
            recommendations.extend([
                {"icon": "🔆", "title": "Metro Comfort", "message": "Air conditioning available on Metro Line 1", "priority": "medium"},
                {"icon": "💧", "title": "Hydration", "message": "Stay hydrated - water fountains available in main stations", "priority": "low"}
            ])

        # Display recommendations by priority
        if recommendations:
            priority_order = ["high", "medium", "low"]
            priority_colors = {"high": "#dc3545", "medium": "#fd7e14", "low": "#28a745"}

            for priority in priority_order:
                priority_recs = [r for r in recommendations if r["priority"] == priority]
                if priority_recs:
                    st.markdown(f"### {priority.title()} Priority")
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
        else:
            st.success("🌟 Current weather conditions are ideal for all transport modes!")

    else:
        st.warning("No weather data available")

elif page == "Transport Analysis":
    st.title("🚊 Transportation Analysis")

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
    tab1, tab2, tab3 = st.tabs(["🔍 Station Details", "♿ Accessibility", "📊 Station Statistics"])

    with tab1:
        render_station_details(all_data["stations"])

    with tab2:
        render_accessibility_overview(all_data["stations"])

    with tab3:
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

    with col1:
        # Display quality score
        if "passed" in quality and "total" in quality:
            quality_percentage = (quality["passed"] / quality["total"]) * 100

            # Create a gauge chart
            fig = go.Figure(go.Indicator(
                mode="gauge+number",
                value=quality_percentage,
                domain={'x': [0, 1], 'y': [0, 1]},
                title={'text': "Data Quality Score"},
                gauge={
                    'axis': {'range': [0, 100]},
                    'bar': {'color': "darkblue"},
                    'steps': [
                        {'range': [0, 60], 'color': "#ffcccc"},
                        {'range': [60, 80], 'color': "#ffffcc"},
                        {'range': [80, 100], 'color': "#ccffcc"}
                    ],
                    'threshold': {
                        'line': {'color': "red", 'width': 4},
                        'thickness': 0.75,
                        'value': 90
                    }
                }
            ))

            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No detailed quality metrics available")

    with col2:
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
    st.markdown("---")
    st.subheader("📋 Quality Details")

    if "timestamp" in quality:
        st.write(f"**Last check**: {quality['timestamp']}")

    if "passed" in quality and "total" in quality:
        st.write(f"**Checks passed**: {quality['passed']}/{quality['total']} ({quality_percentage:.1f}%)")

elif page == "Predictions":
    st.title("🔮 Traffic and Mobility Predictions")
    st.subheader(f"Forecasts for {current_date}")

    # Enhanced predictions with new transport data
    pred_tab1, pred_tab2, pred_tab3, pred_tab4 = st.tabs([
        "🚗 Traffic Predictions",
        "🌤️ Weather Impact",
        "🚊 Transport Reliability",
        "🗺️ Congestion Zones"
    ])

    with pred_tab1:
        st.write("Predicted traffic conditions for key routes in La Défense")

        # Time selection
        prediction_time = st.slider(
            "Select hour of day",
            min_value=0,
            max_value=23,
            value=datetime.now().hour
        )

        # Enhanced prediction data
        prediction_data = pd.DataFrame({
            "road_name": [
                "A14 (Paris → La Défense)",
                "N13",
                "Boulevard Circulaire",
                "Avenue de la Division Leclerc",
                "Pont de Neuilly",
                "A86 (Inner Ring)",
                "A86 (Outer Ring)"
            ],
            "normal_travel_time": [12, 8, 5, 4, 6, 15, 18],
            "predicted_travel_time": [15, 10, 8, 5, 9, 22, 25],
            "congestion_level": [3, 2, 3, 1, 4, 4, 5],
            "confidence": [0.85, 0.78, 0.92, 0.88, 0.75, 0.82, 0.79]
        })

        # Enhanced visualization
        colors = ['#28a745', '#6f42c1', '#ffc107', '#fd7e14', '#dc3545', '#6c757d']

        fig = go.Figure()

        for i, row in prediction_data.iterrows():
            congestion = row['congestion_level']
            confidence = row['confidence']

            fig.add_trace(go.Bar(
                x=[row['road_name']],
                y=[row['predicted_travel_time']],
                name=row['road_name'],
                marker_color=colors[congestion] if congestion < len(colors) else colors[-1],
                text=f"{row['predicted_travel_time']} min<br>({confidence:.0%} confidence)",
                textposition='auto',
                showlegend=False
            ))

        fig.update_layout(
            title="Predicted Travel Times with Confidence Levels",
            xaxis_title="Road",
            yaxis_title="Travel Time (minutes)",
            height=400
        )

        st.plotly_chart(fig, use_container_width=True)

    with pred_tab2:
        st.write("Predicted impact of weather conditions on mobility")

        if not all_data["hourly_weather"].empty:
            current_weather = all_data["current_weather"].iloc[0] if not all_data["current_weather"].empty else None

            # Enhanced weather impact analysis
            weather_impact = pd.DataFrame({
                "condition": ["Clear", "Light Rain", "Heavy Rain", "Snow", "High Winds", "Fog"],
                "metro_impact": [1.0, 1.05, 1.1, 1.2, 1.0, 1.05],
                "rer_impact": [1.0, 1.1, 1.3, 1.8, 1.2, 1.4],
                "bus_impact": [1.0, 1.2, 1.5, 2.0, 1.4, 1.6],
                "walking_impact": [1.0, 1.3, 2.0, 3.0, 1.8, 2.2]
            })

            # Current conditions impact
            st.subheader("Current Weather Impact on Transport")

            current_condition = "Clear"
            if current_weather is not None:
                precip = current_weather.get('precipitation', 0)
                wind_speed = current_weather.get('wind_speed', 0)
                visibility = current_weather.get('visibility', 10)

                if precip > 10:
                    current_condition = "Heavy Rain"
                elif precip > 2:
                    current_condition = "Light Rain"
                elif wind_speed > 40:
                    current_condition = "High Winds"
                elif visibility < 1:
                    current_condition = "Fog"

            # Display impact matrix
            impact_row = weather_impact[weather_impact['condition'] == current_condition]
            if not impact_row.empty:
                impact_data = impact_row.iloc[0]

                transport_impacts = {
                    "🚇 Metro": impact_data['metro_impact'],
                    "🚄 RER": impact_data['rer_impact'],
                    "🚌 Bus": impact_data['bus_impact'],
                    "🚶 Walking": impact_data['walking_impact']
                }

                cols = st.columns(len(transport_impacts))
                for i, (transport, impact) in enumerate(transport_impacts.items()):
                    with cols[i]:
                        delay_pct = (impact - 1) * 100
                        st.metric(
                            transport,
                            f"{impact:.1f}x",
                            f"{delay_pct:+.0f}%" if delay_pct != 0 else "No impact"
                        )

    with pred_tab3:
        st.write("Predicted reliability for each transport line")

        if not all_data["traffic_status"].empty:
            # Create reliability predictions based on current status
            reliability_data = []

            for _, line in all_data["traffic_status"].iterrows():
                transport_type = line["transport_type"]
                line_id = line["line"]
                status = line["status"]

                # Base reliability scores by transport type
                base_reliability = {
                    "metro": 0.95,
                    "rers": 0.92,
                    "transilien": 0.88,
                    "buses": 0.85,
                    "idfm": 0.90
                }.get(transport_type, 0.85)

                # Adjust based on current status
                status_adjustments = {
                    "normal": 0.0,
                    "minor": -0.05,
                    "major": -0.15,
                    "critical": -0.30
                }

                adjusted_reliability = base_reliability + status_adjustments.get(status, 0)
                adjusted_reliability = max(0.4, min(1.0, adjusted_reliability))

                # Generate hourly predictions
                hourly_predictions = []
                for hour in range(24):
                    # Add time-based variations
                    if 7 <= hour <= 9 or 17 <= hour <= 19:  # Rush hours
                        hour_reliability = adjusted_reliability - 0.05
                    elif 22 <= hour or hour <= 5:  # Night hours
                        hour_reliability = adjusted_reliability + 0.03
                    else:
                        hour_reliability = adjusted_reliability

                    hourly_predictions.append(max(0.4, min(1.0, hour_reliability)))

                reliability_data.append({
                    "transport_type": transport_type,
                    "line": line_id,
                    "current_reliability": adjusted_reliability,
                    "hourly_predictions": hourly_predictions
                })

            # Display reliability metrics
            st.subheader("Current Reliability Scores")

            rel_cols = st.columns(min(4, len(reliability_data)))
            for i, line_data in enumerate(reliability_data[:4]):  # Show first 4 lines
                with rel_cols[i % 4]:
                    reliability_score = line_data["current_reliability"] * 100
                    line_name = f"{line_data['transport_type'].title()} {line_data['line']}"

                    if reliability_score >= 90:
                        color = "#28a745"
                        status_text = "Excellent"
                    elif reliability_score >= 80:
                        color = "#ffc107"
                        status_text = "Good"
                    elif reliability_score >= 70:
                        color = "#fd7e14"
                        status_text = "Fair"
                    else:
                        color = "#dc3545"
                        status_text = "Poor"

                    st.markdown(f"""
                    <div style="
                        border: 2px solid {color};
                        border-radius: 8px;
                        padding: 15px;
                        text-align: center;
                        margin: 5px 0;
                    ">
                        <h4 style="margin: 0; color: {color};">{line_name}</h4>
                        <div style="font-size: 2em; font-weight: bold; color: {color};">
                            {reliability_score:.0f}%
                        </div>
                        <div style="color: #666;">{status_text}</div>
                    </div>
                    """, unsafe_allow_html=True)

            # 24-hour reliability forecast
            if reliability_data:
                st.subheader("24-Hour Reliability Forecast")

                # Create DataFrame for plotting
                forecast_df = pd.DataFrame({
                    'hour': list(range(24))
                })

                for line_data in reliability_data[:5]:  # Show first 5 lines
                    line_name = f"{line_data['transport_type'].title()} {line_data['line']}"
                    forecast_df[line_name] = [r * 100 for r in line_data['hourly_predictions']]

                fig_rel = px.line(
                    forecast_df,
                    x='hour',
                    y=[col for col in forecast_df.columns if col != 'hour'],
                    title='Predicted Reliability by Hour (%)',
                    labels={'value': 'Reliability (%)', 'hour': 'Hour of Day'},
                    range_y=[60, 100]
                )

                fig_rel.update_layout(
                    xaxis=dict(tickmode='linear', dtick=2),
                    hovermode="x unified",
                    height=400
                )

                st.plotly_chart(fig_rel, use_container_width=True)
        else:
            st.info("No transport status data available for reliability predictions")

    with pred_tab4:
        st.write("Predicted congestion zones in La Défense")
        # render_traffic_heatmap()

        # Additional congestion insights
        # st.subheader("🚨 Congestion Hotspots")
        st.subheader("🚨 Known Congestion Areas")

        congestion_zones = [
            {
                "zone": "Grande Arche Area",
                "peak_hours": "8:00-9:30, 18:00-19:30",
                "congestion_level": "High",
                "recommendation": "Use RER E instead of RER A during peak"
            },
            {
                "zone": "CNIT Complex",
                "peak_hours": "12:00-14:00, 18:00-20:00",
                "congestion_level": "Moderate",
                "recommendation": "Bus 144 provides good alternative"
            },
            {
                "zone": "Quatre Temps",
                "peak_hours": "11:00-13:00, 17:00-19:00",
                "congestion_level": "Moderate",
                "recommendation": "Walking connections available"
            },
            {
                "zone": "Pont de Neuilly",
                "peak_hours": "7:30-9:00, 17:30-19:00",
                "congestion_level": "Very High",
                "recommendation": "Avoid during rush hours"
            }
        ]

        # Display in columns
        cols = st.columns(2)
        for i, zone in enumerate(congestion_zones):
            with cols[i % 2]:
                level_colors = {
                    "Low": "#28a745",
                    "Moderate": "#ffc107",
                    "High": "#fd7e14",
                    "Very High": "#dc3545"
                }

                color = level_colors.get(zone["congestion_level"], "#6c757d")

                st.markdown(f"""
                    <div style="
                        border: 2px solid {color};
                        border-radius: 8px;
                        padding: 15px;
                        margin: 10px 0;
                        background-color: #f8f9fa;
                    ">
                        <h4 style="color: {color}; margin-top: 0;">
                            📍 {zone['zone']}
                        </h4>
                        <p><strong>Level:</strong> {zone['congestion_level']}</p>
                        <p><strong>Peak:</strong> {zone['peak_hours']}</p>
                        <p><strong>Tip:</strong> {zone['recommendation']}</p>
                    </div>
                    """, unsafe_allow_html=True)

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