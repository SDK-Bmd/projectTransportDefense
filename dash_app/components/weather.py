"""
Weather visualization components for the La Défense mobility dashboard
"""
import streamlit as st
import plotly.express as px
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def render_weather_section(current_weather, daily_weather, hourly_weather):
    """Render weather information section for the dashboard"""
    st.subheader("Weather Conditions")

    # Check if we have valid data
    has_current = not current_weather.empty
    has_daily = not daily_weather.empty
    has_hourly = not hourly_weather.empty

    if has_current:
        # Current weather display
        current = current_weather.iloc[0]

        cols = st.columns(3)

        with cols[0]:
            temp = current.get('temperature', 'N/A')
            feels_like = current.get('feels_like', 'N/A')
            st.metric("Temperature", f"{temp}°C",
                      delta=f"{feels_like - temp:.1f}°C" if isinstance(temp, (int, float)) and isinstance(feels_like, (
                      int, float)) else None)

            humidity = current.get('humidity', 'N/A')
            st.metric("Humidity", f"{humidity}%" if isinstance(humidity, (int, float)) else "N/A")

        with cols[1]:
            wind_speed = current.get('wind_speed', 'N/A')
            st.metric("Wind", f"{wind_speed} km/h" if isinstance(wind_speed, (int, float)) else "N/A")

            conditions = current.get('conditions', 'Unknown')
            st.metric("Conditions", conditions)

        with cols[2]:
            precip = current.get('precipitation', 0)
            st.metric("Precipitation", f"{precip} mm" if isinstance(precip, (int, float)) else "N/A")

            precip_prob = current.get('precipitation_probability', 0)
            st.metric("Precipitation Probability",
                      f"{precip_prob}%" if isinstance(precip_prob, (int, float)) else "N/A")

        # Display weather impact on mobility
        st.subheader("Weather Impact on Mobility")

        # Calculate weather impact
        impact_factor = 1.0  # Default: no impact
        impact_description = "Current weather conditions have minimal impact on transport."

        # Determine impact based on weather conditions
        precip = current.get('precipitation', 0)
        wind_speed = current.get('wind_speed', 0)
        visibility = current.get('visibility', 10)

        if isinstance(precip, (int, float)) and precip > 5:  # Heavy rain
            impact_factor = 1.3
            impact_description = "Heavy rain may cause reduced visibility and increased travel times."
        elif isinstance(wind_speed, (int, float)) and wind_speed > 40:  # High winds
            impact_factor = 1.4
            impact_description = "High winds may affect high-sided vehicles and outdoor waiting conditions."
        elif isinstance(visibility, (int, float)) and visibility < 3:  # Fog
            impact_factor = 1.5
            impact_description = "Low visibility conditions may significantly slow traffic."

        # Display impact
        st.info(impact_description)
        st.metric("Transport Time Multiplier", f"{impact_factor:.1f}x",
                  delta=f"+{(impact_factor - 1) * 100:.0f}%" if impact_factor > 1 else "No impact",
                  delta_color="inverse")
    else:
        st.warning("No current weather data available")

    # Display forecast
    if has_daily:
        with st.expander("Weather Forecast", expanded=False):
            # Temperature forecast
            fig = px.line(
                daily_weather,
                x="date",
                y=["temperature_max", "temperature_min"],
                title="Temperature Forecast (°C)",
                labels={"value": "Temperature (°C)", "date": "Date", "variable": "Type"},
                color_discrete_map={"temperature_max": "red", "temperature_min": "blue"}
            )
            st.plotly_chart(fig, use_container_width=True)

            # Precipitation forecast
            fig = px.bar(
                daily_weather,
                x="date",
                y="precipitation",
                title="Precipitation Forecast (mm)",
                labels={"precipitation": "Precipitation (mm)", "date": "Date"},
                color="precipitation_probability"
            )
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No weather forecast data available")