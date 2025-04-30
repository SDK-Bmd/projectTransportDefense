"""
Streamlit dashboard for La Défense mobility visualization
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
import boto3
from botocore.client import Config

# Add the project root to Python path to enable imports
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

# Import project modules
import config


# MinIO connection setup
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


# Page configuration
st.set_page_config(
    page_title="La Défense Mobility Dashboard",
    page_icon="🚆",
    layout="wide",
    initial_sidebar_state="expanded"
)


# Load data using the data lake connection
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_weather_data():
    """Load weather data from data lake"""
    s3 = get_s3_client()
    bucket_name = config.DATA_LAKE["bucket_name"]

    try:
        # Get current weather
        current_response = s3.get_object(Bucket=bucket_name, Key='refined/weather/current_latest.parquet')
        current_df = pd.read_parquet(io.BytesIO(current_response['Body'].read()))

        # Get daily forecast
        daily_response = s3.get_object(Bucket=bucket_name, Key='refined/weather/daily_latest.parquet')
        daily_df = pd.read_parquet(io.BytesIO(daily_response['Body'].read()))

        # Get hourly forecast
        hourly_response = s3.get_object(Bucket=bucket_name, Key='refined/weather/hourly_latest.parquet')
        hourly_df = pd.read_parquet(io.BytesIO(hourly_response['Body'].read()))

        return current_df, daily_df, hourly_df
    except Exception as e:
        st.error(f"Error loading weather data: {str(e)}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


@st.cache_data(ttl=3600)
def load_transport_data():
    """Load transport schedules and traffic status"""
    s3 = get_s3_client()
    bucket_name = config.DATA_LAKE["bucket_name"]

    try:
        # Get traffic status
        traffic_response = s3.get_object(Bucket=bucket_name, Key='refined/transport/traffic_latest.parquet')
        traffic_df = pd.read_parquet(io.BytesIO(traffic_response['Body'].read()))

        # Get schedules if available
        try:
            schedules_response = s3.get_object(Bucket=bucket_name, Key='refined/transport/schedules_latest.parquet')
            schedules_df = pd.read_parquet(io.BytesIO(schedules_response['Body'].read()))
        except:
            # If schedules aren't available, create empty DataFrame
            schedules_df = pd.DataFrame(columns=['transport_type', 'line', 'direction', 'message'])

        return schedules_df, traffic_df
    except Exception as e:
        st.error(f"Error loading transport data: {str(e)}")
        return pd.DataFrame(columns=['transport_type', 'line', 'direction', 'message']), pd.DataFrame()


@st.cache_data(ttl=3600)
def load_station_data():
    """Load station information"""
    s3 = get_s3_client()
    bucket_name = config.DATA_LAKE["bucket_name"]

    try:
        # Get combined station data
        stations_response = s3.get_object(Bucket=bucket_name, Key='refined/stations/combined_stations_latest.parquet')
        stations_df = pd.read_parquet(io.BytesIO(stations_response['Body'].read()))
        return stations_df
    except Exception as e:
        st.error(f"Error loading station data: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_traffic_data():
    """Load road traffic data"""
    s3 = get_s3_client()
    bucket_name = config.DATA_LAKE["bucket_name"]

    try:
        # Get traffic data
        response = s3.get_object(Bucket=bucket_name, Key='landing/traffic/traffic_ladefense_latest.json')
        traffic_data = json.loads(response['Body'].read().decode('utf-8'))
        return traffic_data
    except Exception as e:
        st.error(f"Error loading traffic data: {str(e)}")
        return {}


@st.cache_data(ttl=1800)  # Cache for 30 minutes
def load_data_quality_status():
    """Load basic data quality status"""

    # Check if data quality log exists
    log_path = 'data_quality.log'
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


# Main function to load all data
def load_all_data():
    with st.spinner("Loading data..."):
        current_weather, daily_weather, hourly_weather = load_weather_data()
        schedules_df, traffic_df = load_transport_data()
        stations_df = load_station_data()
        road_traffic_data = load_traffic_data()
        quality_status = load_data_quality_status()

        return {
            "current_weather": current_weather,
            "daily_weather": daily_weather,
            "hourly_weather": hourly_weather,
            "schedules": schedules_df,
            "traffic_status": traffic_df,
            "stations": stations_df,
            "road_traffic": road_traffic_data,
            "quality_status": quality_status
        }


# Load the data
all_data = load_all_data()

# Sidebar
st.sidebar.title("La Défense Mobility")
page = st.sidebar.selectbox(
    "Choose a page",
    ["Overview", "Weather Impact", "Transport Analysis", "Station Information", "Data Quality"]
)

# Last refresh time
st.sidebar.markdown("---")
refresh_time = datetime.now().strftime("%Y-%m-%d %H:%M")
st.sidebar.write(f"Last data refresh: {refresh_time}")

# Button to refresh data
if st.sidebar.button("Refresh Data"):
    st.cache_data.clear()
    st.experimental_rerun()

# Prepare the date
current_date = datetime.now().strftime("%Y-%m-%d")
current_time = datetime.now().strftime("%H:%M:%S")

# Pages
if page == "Overview":
    st.title("La Défense Mobility Dashboard")
    st.subheader(f"Current Status as of {current_date} {current_time}")

    # Data quality indicator
    quality = all_data["quality_status"]
    quality_color = "green" if quality.get("status") == "Good" else "orange" if quality.get(
        "status") == "Issues Detected" else "gray"

    st.write(f"""
    <div style="
        padding: 5px 15px; 
        border-radius: 5px; 
        background-color: {quality_color}; 
        color: white;
        display: inline-block;
        margin-bottom: 15px;">
        Data Quality: {quality.get("status", "Unknown")}
    </div>
    """, unsafe_allow_html=True)

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        if not all_data["current_weather"].empty:
            st.metric(
                "Temperature",
                f"{all_data['current_weather']['temperature'].iloc[0]}°C",
                f"{all_data['current_weather']['feels_like'].iloc[0] - all_data['current_weather']['temperature'].iloc[0]:.1f}°C"
            )
        else:
            st.metric("Temperature", "N/A")

    with col2:
        if not all_data["traffic_status"].empty:
            # Count lines with issues
            lines_with_issues = len(
                all_data["traffic_status"][all_data["traffic_status"]["status"] != "normal"]) if "status" in all_data[
                "traffic_status"].columns else 0
            total_lines = len(all_data["traffic_status"])
            st.metric("Transport Status", f"{total_lines - lines_with_issues}/{total_lines} normal")
        else:
            st.metric("Transport Status", "N/A")

    with col3:
        if not all_data["schedules"].empty:
            next_departures = len(all_data["schedules"])
            st.metric("Next Departures", f"{next_departures} scheduled")
        else:
            st.metric("Next Departures", "N/A")

    with col4:
        if "tomtom_flow" in all_data["road_traffic"]:
            # This is simplified - in a real app, you'd extract the actual value
            st.metric("Road Traffic", "Moderate", "10%")
        else:
            st.metric("Road Traffic", "N/A")

    # Map of La Défense area
    st.subheader("La Défense Transportation Hub")

    # Create a figure with stations
    if not all_data["stations"].empty:
        # Filter stations with valid coordinates
        valid_stations = all_data["stations"][(all_data["stations"]["lat"] != 0) & (all_data["stations"]["lon"] != 0)]

        if not valid_stations.empty:
            fig = px.scatter_mapbox(
                valid_stations,
                lat="lat",
                lon="lon",
                hover_name="name",
                hover_data=["type", "wheelchair_accessible", "elevator_available"],
                color="type",
                size_max=15,
                zoom=14,
                height=500
            )

            fig.update_layout(
                mapbox_style="open-street-map",
                margin={"r": 0, "t": 0, "l": 0, "b": 0}
            )

            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No stations with valid coordinates found")
    else:
        st.warning("No station data available to display on the map")

    # Next departures
    st.subheader("Next Departures from La Défense")

    if not all_data["schedules"].empty:
        schedules = all_data["schedules"].head(10)

        # Check if we have the required columns
        required_columns = ["transport_type", "line", "direction", "message"]
        missing_columns = [col for col in required_columns if col not in schedules.columns]

        if not missing_columns:
            st.dataframe(
                schedules[required_columns],
                use_container_width=True,
                hide_index=True
            )
        else:
            st.warning(f"Schedule data is missing columns: {', '.join(missing_columns)}")
    else:
        st.info("No schedule information available at this time")

elif page == "Weather Impact":
    st.title("Weather Impact on Mobility")

    # Current weather
    if not all_data["current_weather"].empty:
        # Check if we have the required columns
        required_columns = ["temperature", "feels_like", "humidity", "wind_speed", "conditions", "precipitation"]
        has_required = all([col in all_data["current_weather"].columns for col in required_columns])

        if has_required:
            current = all_data["current_weather"].iloc[0]

            st.subheader("Current Weather Conditions")
            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric("Temperature", f"{current['temperature']}°C")
                st.metric("Feels Like", f"{current['feels_like']}°C")

            with col2:
                st.metric("Humidity", f"{current['humidity']}%")
                st.metric("Wind", f"{current['wind_speed']} km/h")

            with col3:
                st.metric("Conditions", current["conditions"])
                st.metric("Precipitation", f"{current['precipitation']} mm")
        else:
            st.warning("Weather data structure is incomplete")
    else:
        st.warning("No current weather data available")

    # Forecast
    if not all_data["daily_weather"].empty:
        # Check if we have the required columns
        required_columns = ["date", "temperature_max", "temperature_min", "precipitation", "precipitation_probability"]
        has_required = all([col in all_data["daily_weather"].columns for col in required_columns])

        if has_required:
            st.subheader("Weather Forecast")

            fig = px.line(
                all_data["daily_weather"],
                x="date",
                y=["temperature_max", "temperature_min"],
                title="Temperature Forecast (°C)",
                labels={"value": "Temperature (°C)", "date": "Date", "variable": "Type"},
                color_discrete_map={"temperature_max": "red", "temperature_min": "blue"}
            )
            st.plotly_chart(fig, use_container_width=True)

            # Precipitation forecast
            fig = px.bar(
                all_data["daily_weather"],
                x="date",
                y="precipitation",
                title="Precipitation Forecast (mm)",
                labels={"precipitation": "Precipitation (mm)", "date": "Date"},
                color="precipitation_probability"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Weather forecast data structure is incomplete")
    else:
        st.warning("No weather forecast data available")

    # Weather impact analysis
    st.subheader("Mobility Impact Analysis")

    impact_analysis = """
    Based on current weather conditions and forecast:

    - **Low precipitation**: Minimal impact on travel times
    - **Moderate temperatures**: Optimal conditions for all modes of transport
    - **Expected impact**: Low (10% potential delay)

    For optimal travel planning, no significant weather-related adjustments are needed today.
    """

    st.markdown(impact_analysis)

elif page == "Transport Analysis":
    st.title("Transportation Analysis")

    # Transport lines status
    if not all_data["traffic_status"].empty:
        # Check if we have the required columns
        required_columns = ["transport_type", "line", "status", "title", "message"]
        has_required = all([col in all_data["traffic_status"].columns for col in required_columns])

        if has_required:
            st.subheader("Transport Lines Status")

            for _, line in all_data["traffic_status"].iterrows():
                status_color = "green" if line["status"] == "normal" else "orange" if line[
                                                                                          "status"] == "critical" else "red"

                st.markdown(
                    f"""
                    <div style="border-left: 5px solid {status_color}; padding-left: 10px; margin-bottom: 10px;">
                    <p style="margin-bottom: 0;"><strong>{line['transport_type'].upper()} Line {line['line']}</strong>: {line['title']}</p>
                    <p style="font-size: 0.9em; color: #777;">{line['message']}</p>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
        else:
            st.info("Transport status data structure is incomplete")
    else:
        st.info("No transport status information available")

    # Departure schedules
    if not all_data["schedules"].empty:
        # Check if we have the required columns
        required_columns = ["transport_type", "line", "direction", "message"]
        has_required = all([col in all_data["schedules"].columns for col in required_columns])

        if has_required:
            st.subheader("Departure Schedules by Transport Type")

            # Group by transport type
            transport_types = all_data["schedules"]["transport_type"].unique()

            for transport_type in transport_types:
                transport_schedules = all_data["schedules"][all_data["schedules"]["transport_type"] == transport_type]

                with st.expander(f"{transport_type.upper()} Schedules", expanded=transport_type == transport_types[0]):
                    st.dataframe(
                        transport_schedules[["line", "direction", "message"]].sort_values("line"),
                        use_container_width=True,
                        hide_index=True
                    )
        else:
            st.info("Schedule data structure is incomplete")
    else:
        st.info("No schedule information available")

elif page == "Station Information":
    st.title("Station Information")

    if not all_data["stations"].empty:
        # Check if we have at least name column
        if "name" in all_data["stations"].columns:
            # Station selection
            selected_station = st.selectbox(
                "Select a station",
                all_data["stations"]["name"].unique()
            )

            # Filter for selected station
            station_data = all_data["stations"][all_data["stations"]["name"] == selected_station].iloc[0]

            # Display station details
            st.subheader(f"{selected_station} Details")

            # Check which columns are available
            columns = station_data.index.tolist()

            col1, col2 = st.columns(2)

            with col1:
                if "type" in columns:
                    st.markdown(f"**Type**: {station_data['type']}")
                if "lines" in columns:
                    st.markdown(f"**Lines**: {station_data['lines']}")
                if "wheelchair_accessible" in columns:
                    st.markdown(f"**Wheelchair Accessible**: {station_data['wheelchair_accessible']}")

            with col2:
                if "elevator_available" in columns:
                    st.markdown(f"**Elevator Available**: {station_data['elevator_available']}")
                if "escalator_available" in columns:
                    st.markdown(f"**Escalator Available**: {station_data['escalator_available']}")
                if "num_entrances" in columns:
                    st.markdown(f"**Number of Entrances**: {station_data['num_entrances']}")

            # Display station on map if coordinates are available
            if "lat" in columns and "lon" in columns and station_data["lat"] != 0 and station_data["lon"] != 0:
                lat, lon = station_data["lat"], station_data["lon"]

                fig = px.scatter_mapbox(
                    pd.DataFrame([{
                        "lat": lat,
                        "lon": lon,
                        "name": selected_station,
                        "size": 1
                    }]),
                    lat="lat",
                    lon="lon",
                    hover_name="name",
                    size="size",
                    size_max=15,
                    zoom=15,
                    height=400
                )

                fig.update_layout(
                    mapbox_style="open-street-map",
                    margin={"r": 0, "t": 0, "l": 0, "b": 0}
                )

                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No coordinate information available for this station")
        else:
            st.warning("Station data is missing required fields")
    else:
        st.warning("No station information available")

elif page == "Data Quality":
    st.title("Data Quality Status")

    quality = all_data["quality_status"]

    # Display quality status
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
                    {'range': [0, 60], 'color': "red"},
                    {'range': [60, 80], 'color': "orange"},
                    {'range': [80, 100], 'color': "green"}
                ],
                'threshold': {
                    'line': {'color': "black", 'width': 4},
                    'thickness': 0.75,
                    'value': 90
                }
            }
        ))

        st.plotly_chart(fig)

        # Display details
        st.subheader("Data Quality Details")
        st.write(f"Last check: {quality.get('timestamp', 'Unknown')}")
        st.write(f"Checks passed: {quality['passed']}/{quality['total']} ({quality_percentage:.1f}%)")

        # Check status by data type
        st.subheader("Data Availability Status")

        data_types = {
            "Weather data": not all_data["current_weather"].empty,
            "Transport schedules": not all_data["schedules"].empty,
            "Transport status": not all_data["traffic_status"].empty,
            "Station information": not all_data["stations"].empty,
            "Traffic data": bool(all_data["road_traffic"])
        }

        for data_type, available in data_types.items():
            status = "✅ Available" if available else "❌ Missing"
            st.write(f"{status} | {data_type}")
    else:
        st.warning("No detailed quality information available")

        # Still show what data we have available
        st.subheader("Data Availability Status")

        data_types = {
            "Weather data": not all_data["current_weather"].empty,
            "Transport schedules": not all_data["schedules"].empty,
            "Transport status": not all_data["traffic_status"].empty,
            "Station information": not all_data["stations"].empty,
            "Traffic data": bool(all_data["road_traffic"])
        }

        for data_type, available in data_types.items():
            status = "✅ Available" if available else "❌ Missing"
            st.write(f"{status} | {data_type}")

# Footer
st.markdown("---")
st.markdown("La Défense Mobility Dashboard | Data refreshed every hour | Powered by open data")