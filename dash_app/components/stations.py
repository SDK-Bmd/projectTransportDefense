"""
Station information components for the La Défense mobility dashboard
"""
import streamlit as st
import pandas as pd
import plotly.express as px


def render_station_details(stations_df, selected_station=None):
    """Render detailed station information"""
    st.subheader("Station Information")

    if not stations_df.empty:
        # Check if we have at least name column
        if "name" in stations_df.columns:
            # Station selection
            if selected_station is None:
                selected_station = st.selectbox(
                    "Select a station",
                    stations_df["name"].unique()
                )

            # Filter for selected station
            station_data = stations_df[stations_df["name"] == selected_station].iloc[0]

            # Display station details
            st.subheader(f"{selected_station} Details")

            # Check which columns are available
            columns = station_data.index.tolist()

            col1, col2 = st.columns(2)

            with col1:
                if "wheelchair_accessible" in columns:
                    st.markdown(f"**Wheelchair Accessible**: {station_data['wheelchair_accessible']}")
                if "escalator_available" in columns:
                    st.markdown(f"**Escalator Available**: {station_data['escalator_available']}")
            with col2:
                if "elevator_available" in columns:
                    st.markdown(f"**Elevator Available**: {station_data['elevator_available']}")


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


def render_accessibility_overview(stations_df):
    """Render an overview of accessibility information for stations"""
    st.subheader("Station Accessibility Overview")

    if not stations_df.empty:
        # Check if we have the required columns
        if "wheelchair_accessible" in stations_df.columns:
            # Calculate accessibility statistics
            total_stations = len(stations_df)
            accessible_stations = len(stations_df[stations_df["wheelchair_accessible"] == "yes"])
            accessibility_percentage = (accessible_stations / total_stations) * 100 if total_stations > 0 else 0

            # Create a simple pie chart
            accessibility_df = pd.DataFrame({
                "status": ["Accessible", "Not Accessible", "Unknown"],
                "count": [
                    len(stations_df[stations_df["wheelchair_accessible"] == "yes"]),
                    len(stations_df[stations_df["wheelchair_accessible"] == "no"]),
                    len(stations_df[stations_df["wheelchair_accessible"] == "unknown"])
                ]
            })

            fig = px.pie(
                accessibility_df,
                values="count",
                names="status",
                title="Station Wheelchair Accessibility",
                color="status",
                color_discrete_map={
                    "Accessible": "green",
                    "Not Accessible": "red",
                    "Unknown": "gray"
                }
            )

            st.plotly_chart(fig, use_container_width=True)

            # Display elevator availability if available
            if "elevator_available" in stations_df.columns:
                elevator_df = pd.DataFrame({
                    "status": ["Available", "Not Available", "Unknown"],
                    "count": [
                        len(stations_df[stations_df["elevator_available"] == "yes"]),
                        len(stations_df[stations_df["elevator_available"] == "no"]),
                        len(stations_df[stations_df["elevator_available"] == "unknown"])
                    ]
                })

                fig = px.pie(
                    elevator_df,
                    values="count",
                    names="status",
                    title="Station Elevator Availability",
                    color="status",
                    color_discrete_map={
                        "Available": "green",
                        "Not Available": "red",
                        "Unknown": "gray"
                    }
                )

                st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Accessibility data is not available")
    else:
        st.warning("No station data available")