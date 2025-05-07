"""
Streamlit dashboard for La Défense mobility visualization with predictive features
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
from dash_app.components.maps import render_station_map, render_traffic_heatmap
from dash_app.components.weather import render_weather_section
from dash_app.components.transport import render_transport_status, render_schedules, render_schedule_summary
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


@st.cache_data(ttl=3600)
def load_transport_data():
    """Load transport schedules and traffic status"""
    bucket_name = DATA_LAKE["bucket_name"]

    try:
        # Get traffic status - first try IDFM data, then fallback to RATP data
        try:
            traffic_df = read_parquet_from_data_lake(bucket_name, 'refined/transport/traffic_latest.parquet')
        except Exception:
            traffic_df = pd.DataFrame()

        # If IDFM traffic data failed, try loading from RATP source as fallback
        if traffic_df.empty:
            try:
                st.warning("Primary traffic data source unavailable, using fallback source")
                traffic_df = read_parquet_from_data_lake(bucket_name, 'refined/transport/ratp_traffic_latest.parquet')
            except Exception:
                traffic_df = pd.DataFrame()

        # Get schedules if available - first try IDFM data, then fallback to RATP data
        try:
            schedules_df = read_parquet_from_data_lake(bucket_name, 'refined/transport/schedules_latest.parquet')
        except Exception:
            schedules_df = pd.DataFrame()

        # If IDFM schedules failed, try loading from RATP source as fallback
        if schedules_df.empty:
            try:
                st.warning("Primary schedules data source unavailable, using fallback source")
                schedules_df = read_parquet_from_data_lake(bucket_name,
                                                           'refined/transport/ratp_schedules_latest.parquet')
            except Exception:
                schedules_df = pd.DataFrame(columns=['transport_type', 'line', 'direction', 'message'])

        # If still empty, create a template DataFrame
        if schedules_df.empty:
            schedules_df = pd.DataFrame(columns=['transport_type', 'line', 'direction', 'message'])

        return schedules_df, traffic_df
    except Exception as e:
        st.error(f"Error loading transport data: {str(e)}")
        return pd.DataFrame(columns=['transport_type', 'line', 'direction', 'message']), pd.DataFrame()


@st.cache_data(ttl=3600)
def load_station_data():
    """Load station information"""
    bucket_name = DATA_LAKE["bucket_name"]

    try:
        # First try to get combined stations data from IDFM
        try:
            stations_df = read_parquet_from_data_lake(bucket_name, 'refined/stations/combined_stations_latest.parquet')
        except Exception:
            stations_df = pd.DataFrame()

        # If IDFM stations data failed, try loading from original RATP+OSM source as fallback
        if stations_df.empty:
            try:
                st.warning("Primary stations data source unavailable, using fallback source")
                stations_df = read_parquet_from_data_lake(bucket_name,
                                                          'refined/stations/ratp_osm_combined_latest.parquet')
            except Exception:
                stations_df = pd.DataFrame()

        return stations_df
    except Exception as e:
        st.error(f"Error loading station data: {str(e)}")
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
    with st.spinner("Loading data..."):
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
st.sidebar.title("La Défense Mobility")
page = st.sidebar.selectbox(
    "Choose a page",
    ["Overview", "Route Planner", "Weather Impact", "Transport Analysis", "Station Information", "Data Quality",
     "Predictions"]
)

# Data source indicator
data_source = st.sidebar.empty()

# Last refresh time
st.sidebar.markdown("---")
refresh_time = datetime.now().strftime("%Y-%m-%d %H:%M")
st.sidebar.write(f"Last data refresh: {refresh_time}")

# Button to refresh data
if st.sidebar.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# Load the data
all_data = load_all_data()

# Determine and display data source
if all_data["idfm_raw"]:
    data_source.success("Using IDFM data")
else:
    data_source.warning("Using RATP data (fallback)")

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
    render_station_map(all_data["stations"])

    # Next departures
    st.subheader("Next Departures from La Défense")
    render_schedule_summary(all_data["schedules"])

elif page == "Route Planner":
    st.title("La Défense Route Planner")
    st.subheader("Find the best route based on real-time conditions")

    col1, col2 = st.columns(2)

    with col1:
        origin = st.selectbox(
            "Origin",
            all_data["stations"]["name"].unique() if not all_data["stations"].empty else []
        )

        # Accessibility preferences
        st.subheader("Preferences")
        time_pref = st.slider("Prioritize time", 0.0, 1.0, 1.0)
        transfer_pref = st.slider("Minimize transfers", 0.0, 1.0, 0.3)
        access_pref = st.checkbox("Require wheelchair accessibility")
        access_val = 1.0 if access_pref else 0.0

    with col2:
        destination = st.selectbox(
            "Destination",
            all_data["stations"]["name"].unique() if not all_data["stations"].empty else []
        )

        departure_time = st.time_input("Departure time", datetime.now().time())

        # Carbon footprint preference
        eco_friendly = st.checkbox("Prefer eco-friendly routes")

    if st.button("Find Routes"):
        st.subheader("Recommended Routes")

        # Placeholder for route optimization results
        # In a real implementation, this would use a route optimizer to find paths

        # Example routes for demonstration
        routes = {
            "Fastest Route": {
                "route_details": [
                    {"transport_type": "metro", "line": "1", "from_station": origin, "to_station": "Esplanade de La Défense", "travel_time": 5, "congestion_factor": 1.0},
                    {"transport_type": "walking", "line": "", "from_station": "Esplanade de La Défense", "to_station": destination, "travel_time": 7, "congestion_factor": 1.0}
                ],
                "total_time": 12,
                "num_transfers": 1
            },
            "Alternative Route": {
                "route_details": [
                    {"transport_type": "bus", "line": "73", "from_station": origin, "to_station": "Grande Arche", "travel_time": 8, "congestion_factor": 1.2},
                    {"transport_type": "walking", "line": "", "from_station": "Grande Arche", "to_station": destination, "travel_time": 5, "congestion_factor": 1.0}
                ],
                "total_time": 13,
                "num_transfers": 1
            }
        }

        # If eco-friendly is selected, calculate and compare emissions
        if eco_friendly:
            # Simple emissions calculation for demonstration
            emissions = {
                "Fastest Route": 120,  # g CO2
                "Alternative Route": 80   # g CO2
            }

            # Show emissions comparison
            emissions_df = pd.DataFrame({
                "route_name": emissions.keys(),
                "emissions_g": emissions.values()
            })

            fig = px.bar(
                emissions_df,
                x="route_name",
                y="emissions_g",
                title="CO2 Emissions by Route (grams)",
                color="route_name"
            )
            st.plotly_chart(fig)

        # Display routes
        for route_name, route_data in routes.items():
            with st.expander(f"{route_name} - {route_data['total_time']} min"):
                for step in route_data["route_details"]:
                    st.write(f"{step['transport_type'].capitalize()} {step['line']} from {step['from_station']} to {step['to_station']} - {step['travel_time']} min")

                st.write(f"Total travel time: {route_data['total_time']} minutes")
elif page == "Weather Impact":
    st.title("Weather Impact on Mobility")

    # Current weather
    if not all_data["current_weather"].empty:
        # Render weather section
        render_weather_section(
            all_data["current_weather"],
            all_data["daily_weather"],
            all_data["hourly_weather"]
        )

        # Add specific analysis of weather impact on mobility
        st.subheader("Mobility Recommendations Based on Weather")

        # Get current weather conditions
        current = all_data["current_weather"].iloc[0]
        precip = current.get('precipitation', 0)
        wind_speed = current.get('wind_speed', 0)
        temp = current.get('temperature', 15)

        # Generate recommendations based on weather conditions
        recommendations = []

        if precip > 5:
            recommendations.append("🚇 Consider using Metro Line 1 instead of surface transport during heavy rain.")
            recommendations.append("⏱️ Allow extra travel time due to possible slow traffic in rainy conditions.")

        if wind_speed > 30:
            recommendations.append("🚌 Bus services may experience delays due to high winds.")
            recommendations.append(
                "🚶‍♀️ Take care when walking in the open areas around Grande Arche due to strong winds.")

        if temp < 5:
            recommendations.append("❄️ Platforms may be slippery due to cold conditions.")
            recommendations.append(
                "🧣 Indoor routes through Les Quatre Temps shopping center recommended for pedestrians.")

        if temp > 28:
            recommendations.append("🔆 Metro Line 1 may be crowded and warm during peak hours.")
            recommendations.append("💧 Stay hydrated while using public transport during hot weather.")

        # Display recommendations
        if recommendations:
            for rec in recommendations:
                st.info(rec)
        else:
            st.success(
                "Current weather conditions have minimal impact on mobility. All transport modes are recommended.")
    else:
        st.warning("No weather data available")

elif page == "Transport Analysis":
    st.title("Transportation Analysis")

    # Transport lines status
    render_transport_status(all_data["traffic_status"])

    # Departure schedules
    render_schedules(all_data["schedules"])

    # Transport usage patterns (mock data for demonstration)
    st.subheader("Transport Usage Patterns")

    # Create mock hourly usage data
    hours = list(range(5, 24))
    metro_usage = [10, 35, 80, 95, 70, 60, 55, 50, 55, 60, 65, 75, 90, 85, 75, 70, 60, 40, 20]
    rer_usage = [5, 25, 70, 90, 65, 50, 45, 40, 45, 50, 55, 65, 85, 80, 70, 65, 55, 35, 15]
    bus_usage = [8, 20, 50, 60, 55, 45, 40, 35, 40, 45, 50, 55, 65, 60, 55, 50, 40, 30, 15]

    # Create DataFrame
    usage_df = pd.DataFrame({
        'hour': hours,
        'Metro': metro_usage,
        'RER': rer_usage,
        'Bus': bus_usage
    })

    # Create line chart
    fig = px.line(
        usage_df,
        x='hour',
        y=['Metro', 'RER', 'Bus'],
        title='Transport Usage by Hour',
        labels={'value': 'Passenger Load (% of capacity)', 'hour': 'Hour of Day', 'variable': 'Transport Type'}
    )

    fig.update_layout(
        xaxis=dict(tickmode='linear', dtick=1),
        hovermode="x unified"
    )

    # Add vertical lines for peak hours
    fig.add_vline(x=8.5, line_width=1, line_dash="dash", line_color="red")
    fig.add_vline(x=18.5, line_width=1, line_dash="dash", line_color="red")

    st.plotly_chart(fig, use_container_width=True)

    # Add insights about transport patterns
    st.info(
        "🕗 Peak hours are observed at 8-9 AM and 6-7 PM on weekdays. Consider traveling outside these hours for a more comfortable journey.")

elif page == "Station Information":
    st.title("Station Information")

    # Station tab selection
    tab1, tab2 = st.tabs(["Station Details", "Accessibility Overview"])

    with tab1:
        render_station_details(all_data["stations"])

    with tab2:
        render_accessibility_overview(all_data["stations"])

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

elif page == "Predictions":
    st.title("Traffic and Mobility Predictions")
    st.subheader(f"Forecasts for {current_date}")

    # Tabs for different prediction types
    pred_tab1, pred_tab2, pred_tab3 = st.tabs(["Traffic Predictions", "Weather Impact", "Congestion Zones"])

    with pred_tab1:
        st.write("Predicted traffic conditions for key routes in La Défense")

        # Time selection
        prediction_time = st.slider(
            "Select hour of day",
            min_value=0,
            max_value=23,
            value=datetime.now().hour
        )

        # Display traffic predictions for main roads
        # In a real implementation, this would use a traffic predictor model
        prediction_data = pd.DataFrame({
            "road_name": ["A14 (Paris → La Défense)", "N13", "Boulevard Circulaire", "Avenue de la Division Leclerc",
                          "Pont de Neuilly"],
            "normal_travel_time": [12, 8, 5, 4, 6],
            "predicted_travel_time": [15, 10, 8, 5, 9],
            "congestion_level": [3, 2, 3, 1, 4]
        })

        # Color scale for congestion
        colors = ['green', 'lightgreen', 'yellow', 'orange', 'red', 'darkred']

        # Create visualization
        fig = go.Figure()

        for i, row in prediction_data.iterrows():
            congestion = row['congestion_level']
            fig.add_trace(go.Bar(
                x=[row['road_name']],
                y=[row['predicted_travel_time']],
                name=row['road_name'],
                marker_color=colors[congestion],
                text=f"{row['predicted_travel_time']} min (+{row['predicted_travel_time'] - row['normal_travel_time']} min)",
                textposition='auto'
            ))

        fig.update_layout(
            title="Predicted Travel Times at Selected Hour",
            xaxis_title="Road",
            yaxis_title="Travel Time (minutes)",
            showlegend=False,
            height=400
        )

        st.plotly_chart(fig, use_container_width=True)

        # Legend for congestion levels
        st.write("Congestion levels:")
        legend_cols = st.columns(6)
        for i, (color, label) in enumerate(
                zip(colors, ["Free flow", "Light", "Moderate", "Heavy", "Very heavy", "Gridlock"])):
            legend_cols[i].markdown(
                f"<div style='background-color: {color}; padding: 10px; text-align: center; border-radius: 5px;'>{label}</div>",
                unsafe_allow_html=True)

    with pred_tab2:
        st.write("Predicted impact of weather conditions on mobility")

        if not all_data["hourly_weather"].empty:
            # Get current and forecasted weather
            current_weather = all_data["current_weather"].iloc[0] if not all_data["current_weather"].empty else None
            hourly_forecast = all_data["hourly_weather"]

            # Weather conditions that affect traffic
            weather_impact = pd.DataFrame({
                "condition": ["Rain", "Snow", "High winds", "Fog", "Normal"],
                "impact_factor": [1.3, 1.8, 1.4, 1.5, 1.0],
                "description": [
                    "Increases travel time by 30%",
                    "Increases travel time by 80%",
                    "Increases travel time by 40%",
                    "Increases travel time by 50%",
                    "No impact on travel time"
                ]
            })

            # Display current weather impact
            st.subheader("Current Weather Impact")

            # Determine current condition
            current_condition = "Normal"
            impact_value = 1.0
            impact_desc = "No significant impact on travel times"

            if current_weather is not None:
                precip = current_weather.get('precipitation', 0)
                wind_speed = current_weather.get('wind_speed', 0)
                visibility = current_weather.get('visibility', 10)

                if precip > 5:  # Heavy rain
                    current_condition = "Rain"
                elif wind_speed > 40:  # High winds
                    current_condition = "High winds"
                elif visibility < 3:  # Fog
                    current_condition = "Fog"

                # Get impact details
                current_impact = weather_impact[weather_impact['condition'] == current_condition]
                if not current_impact.empty:
                    impact_value = current_impact['impact_factor'].values[0]
                    impact_desc = current_impact['description'].values[0]

            st.metric(
                "Travel Time Multiplier",
                f"{impact_value:.1f}x",
                delta=f"{(impact_value - 1) * 100:.0f}%" if impact_value > 1 else "0%",
                delta_color="inverse"
            )

            st.info(f"Current conditions: {current_condition} - {impact_desc}")

            # Create a simplified forecast impact visualization
            st.subheader("Forecast Weather Impact")

            # Generate simulated forecast impact for demonstration
            hours = [f"{(datetime.now() + timedelta(hours=i)).hour}:00" for i in range(12)]
            impact_values = [1.0, 1.0, 1.1, 1.3, 1.3, 1.2, 1.1, 1.0, 1.0, 1.0, 1.0, 1.0]

            forecast_df = pd.DataFrame({
                "hour": hours,
                "impact_factor": impact_values
            })

            fig = px.line(
                forecast_df,
                x="hour",
                y="impact_factor",
                title="Predicted Weather Impact on Travel Times",
                labels={"impact_factor": "Travel Time Multiplier", "hour": "Hour"},
                markers=True
            )

            # Add reference line for normal conditions
            fig.add_hline(y=1.0, line_dash="dash", line_color="green", annotation_text="Normal conditions")

            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No weather forecast data available")

    with pred_tab3:
        st.write("Predicted congestion zones in La Défense")

        # Display a heatmap of predicted congestion
        render_traffic_heatmap()

# Footer
st.markdown("---")
st.markdown("La Défense Mobility Dashboard | Data refreshed every hour | Powered by open data")