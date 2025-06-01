"""
Map visualization components for the La Défense mobility dashboard
"""
import streamlit as st
import plotly.express as px
import pandas as pd
import numpy as np
import sys
import os

# Add the parent directory to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(os.path.dirname(current_dir))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from configuration.config import LADEFENSE_COORDINATES

def render_station_map(stations_df):
    """Render a map of stations in La Défense"""
    st.subheader("La Défense Transportation Hub")

    # Create a figure with stations
    if not stations_df.empty:
        # Filter stations with valid coordinates
        valid_stations = stations_df[(stations_df["lat"] != 0) & (stations_df["lon"] != 0)]

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


# def render_traffic_heatmap(traffic_data=None):
#     """Render a heatmap of traffic conditions in La Défense"""
#     st.subheader("Current Traffic Conditions")
#
#     # If no traffic data is provided, generate mock data for demonstration
#     if traffic_data is None or not isinstance(traffic_data, pd.DataFrame):
#         # Generate mock data around La Défense
#         la_defense_lat, la_defense_lon = LADEFENSE_COORDINATES["lat"], LADEFENSE_COORDINATES["lon"]
#
#         # Create a grid of points around La Défense
#         grid_size = 20
#         lat_range = np.linspace(la_defense_lat - 0.01, la_defense_lat + 0.01, grid_size)
#         lon_range = np.linspace(la_defense_lon - 0.015, la_defense_lon + 0.015, grid_size)
#
#         # Generate congestion levels (mock data)
#         np.random.seed(42)  # For reproducibility
#         congestion_pattern = np.random.normal(2, 1, size=(grid_size, grid_size))
#
#         # Add some hotspots
#         for hotspot in [(10, 10), (5, 15)]:
#             i, j = hotspot
#             congestion_pattern[max(0, i - 2):min(grid_size, i + 3), max(0, j - 2):min(grid_size, j + 3)] += 2
#
#         # Clip values to 0-5 range
#         congestion_pattern = np.clip(congestion_pattern, 0, 5)
#
#         # Create points for the heatmap
#         heatmap_data = []
#         for i, lat in enumerate(lat_range):
#             for j, lon in enumerate(lon_range):
#                 heatmap_data.append({
#                     'lat': lat,
#                     'lon': lon,
#                     'congestion': congestion_pattern[i, j]
#                 })
#
#         traffic_data = pd.DataFrame(heatmap_data)
#
#     # Create the heatmap
#     fig = px.density_mapbox(
#         traffic_data,
#         lat='lat',
#         lon='lon',
#         z='congestion',
#         radius=15,
#         center=dict(lat=LADEFENSE_COORDINATES["lat"], lon=LADEFENSE_COORDINATES["lon"]),
#         zoom=14,
#         mapbox_style="open-street-map",
#         color_continuous_scale=[
#             [0, 'green'],
#             [0.2, 'lime'],
#             [0.4, 'yellow'],
#             [0.6, 'orange'],
#             [0.8, 'red'],
#             [1, 'darkred']
#         ],
#         range_color=[0, 5],
#         title="Traffic Congestion Level"
#     )
#
#     fig.update_layout(height=500)
#
#     st.plotly_chart(fig, use_container_width=True)
#
#     # Add legend
#     st.write("Congestion levels:")
#     legend_cols = st.columns(6)
#     colors = ['green', 'lime', 'yellow', 'orange', 'red', 'darkred']
#     labels = ["Free flow", "Light", "Moderate", "Heavy", "Very heavy", "Gridlock"]
#
#     for i, (color, label) in enumerate(zip(colors, labels)):
#         legend_cols[i].markdown(
#             f"<div style='background-color: {color}; padding: 10px; text-align: center; border-radius: 5px;'>{label}</div>",
#             unsafe_allow_html=True)