import streamlit as st
import plotly.express as px
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def safe_get_weather_value(current_data, key, default=0):
    """Safely extract weather values with None handling"""
    value = current_data.get(key, default)

    # Handle None, NaN, or empty string values
    if value is None or value == "" or str(value).lower() in ['none', 'nan', 'null', 'n/a']:
        return default

    # Convert to numeric if possible
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def render_weather_section(current_weather, daily_weather, hourly_weather):
    """Enhanced weather section with debugging and safe data handling"""
    st.subheader("Weather Conditions")

    # Debug information (expandable)
    with st.expander("🔍 Debug Weather Data", expanded=False):
        st.write("**Data Availability:**")
        st.write(f"- Current weather: {len(current_weather)} records")
        st.write(f"- Daily weather: {len(daily_weather)} records")
        st.write(f"- Hourly weather: {len(hourly_weather)} records")

        if not daily_weather.empty:
            st.write("**Daily Weather Info:**")
            st.write(f"- Columns: {list(daily_weather.columns)}")
            st.write(f"- Data types: {daily_weather.dtypes.to_dict()}")

            # Check temperature data specifically
            temp_cols = [col for col in daily_weather.columns if 'temp' in col.lower()]
            st.write(f"- Temperature columns found: {temp_cols}")

            if temp_cols:
                for col in temp_cols:
                    values = daily_weather[col].dropna()
                    if len(values) > 0:
                        st.write(f"  - {col}: min={values.min():.1f}, max={values.max():.1f}, mean={values.mean():.1f}")
                    else:
                        st.write(f"  - {col}: NO DATA")

            st.write("**Sample Daily Data:**")
            st.dataframe(daily_weather.head(3))

    # Check if we have valid data
    has_current = not current_weather.empty
    has_daily = not daily_weather.empty
    has_hourly = not hourly_weather.empty

    if has_current:
        # Current weather display with SAFE value extraction
        current = current_weather.iloc[0]

        cols = st.columns(3)

        with cols[0]:
            temp = safe_get_weather_value(current, 'temperature', 15)
            feels_like = safe_get_weather_value(current, 'feels_like', temp)
            st.metric("Temperature", f"{temp}°C",
                      delta=f"{feels_like - temp:+.1f}°C feels like" if feels_like != temp else None)

            humidity = safe_get_weather_value(current, 'humidity', 50)
            st.metric("Humidity", f"{humidity}%")

        with cols[1]:
            wind_speed = safe_get_weather_value(current, 'wind_speed', 0)
            st.metric("Wind", f"{wind_speed} km/h")

            conditions = current.get('conditions', 'Unknown')
            if not conditions or conditions in ['None', 'null', '']:
                conditions = 'Unknown'
            st.metric("Conditions", conditions)

        with cols[2]:
            precip = safe_get_weather_value(current, 'precipitation', 0)
            st.metric("Precipitation", f"{precip} mm")

            precip_prob = safe_get_weather_value(current, 'precipitation_probability', 0)
            st.metric("Precipitation Probability", f"{precip_prob}%")

        # Display weather impact on mobility with SAFE calculations
        st.subheader("Weather Impact on Mobility")

        # Calculate weather impact with safe values
        impact_factor = 1.0  # Default: no impact
        impact_description = "Current weather conditions have minimal impact on transport."

        # All comparisons are now safe
        if precip > 5:  # Heavy rain
            impact_factor = 1.3
            impact_description = "Heavy rain may cause reduced visibility and increased travel times."
        elif wind_speed > 40:  # High winds
            impact_factor = 1.4
            impact_description = "High winds may affect high-sided vehicles and outdoor waiting conditions."
        elif safe_get_weather_value(current, 'visibility', 10) < 3:  # Fog
            impact_factor = 1.5
            impact_description = "Low visibility conditions may significantly slow traffic."

        # Display impact
        st.info(impact_description)
        st.metric("Transport Time Multiplier", f"{impact_factor:.1f}x",
                  delta=f"+{(impact_factor - 1) * 100:.0f}%" if impact_factor > 1 else "No impact",
                  delta_color="inverse")
    else:
        st.warning("No current weather data available")

    # ENHANCED forecast display with robust error handling
    if has_daily and len(daily_weather) > 0:
        with st.expander("Weather Forecast", expanded=True):  # Expanded for debugging
            st.write(f"📊 Displaying forecast for {len(daily_weather)} days")

            # Column detection
            temp_avg_col = None
            date_col = None

            # Find temperature columns with multiple naming patterns
            for col in daily_weather.columns:
                col_lower = col.lower()
                if any(x in col_lower for x in ['tempavg', 'temperature_avg', 'temp_avg', 'avg_temp']):
                    temp_avg_col = col

            # Find date column with multiple naming patterns
            for col in ['date', 'datetime', 'Date', 'DATE']:
                if col in daily_weather.columns:
                    date_col = col
                    break

            st.write(f"🌡️ Temperature columns detected:")
            st.write(f"  - Avg: {temp_avg_col}")
            st.write(f"📅 Date column detected: {date_col}")

            if date_col and (temp_avg_col):
                # Prepare data for plotting
                plot_data = daily_weather.copy()

                # Ensure date is datetime
                if not pd.api.types.is_datetime64_any_dtype(plot_data[date_col]):
                    try:
                        plot_data[date_col] = pd.to_datetime(plot_data[date_col])
                        st.success(f"✅ Successfully converted {date_col} to datetime")
                    except Exception as e:
                        st.error(f"❌ Could not convert {date_col} to datetime: {str(e)}")
                        st.write("Sample date values:", plot_data[date_col].head().tolist())
                        # Try alternative date parsing
                        try:
                            plot_data[date_col] = pd.to_datetime(plot_data[date_col], errors='coerce')
                            if plot_data[date_col].isna().all():
                                st.error("All dates failed to parse")
                                return
                            else:
                                st.warning("Some dates failed to parse but continuing with valid ones")
                        except:
                            st.error("Complete failure in date parsing")
                            return

                # Build temperature columns list with data validation
                temp_columns_to_plot = []

                if temp_avg_col and temp_avg_col in plot_data.columns:
                    # Check if column has non-null data
                    non_null_data = plot_data[temp_avg_col].dropna()
                    if len(non_null_data) > 0:
                        temp_columns_to_plot.append(temp_avg_col)
                        st.write(f"  ✅ {temp_avg_col}: {len(non_null_data)} valid values")
                    else:
                        st.write(f"  ❌ {temp_avg_col}: No valid data")

                if temp_columns_to_plot:
                    try:
                        # Filter out rows with invalid dates
                        plot_data = plot_data.dropna(subset=[date_col])

                        if len(plot_data) == 0:
                            st.error("❌ No valid data rows after filtering")
                            return

                        # Create the temperature forecast chart
                        fig = px.line(
                            plot_data,
                            x=date_col,
                            y=temp_columns_to_plot,
                            title="Temperature Forecast (°C)",
                            labels={"value": "Temperature (°C)", date_col: "Date", "variable": "Type"},
                            color_discrete_map={
                                temp_avg_col: "yellow" if temp_avg_col in temp_columns_to_plot else None
                            }
                        )

                        fig.update_layout(
                            xaxis_title="Date",
                            yaxis_title="Temperature (°C)",
                            hovermode="x unified",
                            height=400
                        )

                        st.plotly_chart(fig, use_container_width=True)
                        st.success("✅ Temperature forecast chart created successfully!")

                    except Exception as e:
                        st.error(f"❌ Error creating temperature chart: {str(e)}")
                        st.write("Debug info:")
                        st.write(f"- plot_data shape: {plot_data.shape}")
                        st.write(f"- columns to plot: {temp_columns_to_plot}")
                        st.write(f"- date column type: {plot_data[date_col].dtype}")
                        st.write("Sample of plot data:")
                        try:
                            debug_cols = [date_col] + temp_columns_to_plot
                            st.dataframe(plot_data[debug_cols].head())
                        except:
                            st.write("Could not display sample data")
                else:
                    st.error("❌ No valid temperature data found for plotting")
                    st.write("Available columns with 'temp':", [col for col in daily_weather.columns if 'temp' in col.lower()])

                    # Show data sample for debugging
                    st.write("Sample of available data:")
                    st.dataframe(daily_weather.head())

                # Precipitation forecast (if available and has data)
                if 'precipitation' in daily_weather.columns:
                    precip_data = daily_weather['precipitation'].fillna(0)
                    if precip_data.sum() > 0:
                        try:
                            fig_precip = px.bar(
                                plot_data,
                                x=date_col,
                                y="precipitation",
                                title="Precipitation Forecast (mm)",
                                labels={"precipitation": "Precipitation (mm)", date_col: "Date"},
                                color="precipitation_probability" if "precipitation_probability" in plot_data.columns else "precipitation"
                            )
                            fig_precip.update_layout(height=300)
                            st.plotly_chart(fig_precip, use_container_width=True)
                        except Exception as e:
                            st.warning(f"Could not create precipitation chart: {str(e)}")
                    else:
                        st.info("ℹ️ No significant precipitation expected in the forecast period")
                else:
                    st.info("ℹ️ Precipitation data not available in forecast")
            else:
                st.error("❌ Missing required columns for temperature forecast")
                st.write("**Available columns:**", list(daily_weather.columns))
                st.write("**Need:** a date column and at least one temperature column")

                # Show what we found
                date_candidates = [col for col in daily_weather.columns if any(x in col.lower() for x in ['date', 'time'])]
                temp_candidates = [col for col in daily_weather.columns if 'temp' in col.lower()]

                st.write(f"**Date candidates found:** {date_candidates}")
                st.write(f"**Temperature candidates found:** {temp_candidates}")

                if daily_weather.shape[0] > 0:
                    st.write("**Sample of raw data:**")
                    st.dataframe(daily_weather.head())
    else:
        st.warning("No weather forecast data available")
        if not has_daily:
            st.error("Daily weather DataFrame is empty - check data extraction and processing")
        else:
            st.error(f"Daily weather DataFrame exists but has {len(daily_weather)} rows")