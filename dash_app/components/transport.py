"""
Transport visualization components for the La Défense mobility dashboard
Updated to support Metro, RER A/E, Transilien L, and multiple bus lines
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


def get_transport_display_name(transport_type, line):
    """
    Get the proper display name for transport type and line

    Args:
        transport_type: Type of transport
        line: Line identifier

    Returns:
        str: Formatted display name
    """
    display_names = {
        "metro": f"Métro {line}",
        "rers": f"RER {line}",
        "transilien": f"Transilien {line}",
        "buses": f"Bus {line}",
        "idfm": f"IDFM {line}"  # For IDFM data
    }

    return display_names.get(transport_type, f"{transport_type} {line}")


def get_transport_color(transport_type, line):
    """
    Get the official color for each transport line

    Args:
        transport_type: Type of transport
        line: Line identifier

    Returns:
        str: Hex color code
    """
    colors = {
        # Metro colors
        ("metro", "1"): "#FFCD00",  # Yellow

        # RER colors
        ("rers", "A"): "#E2231A",   # Red
        ("rers", "E"): "#6E1E78",   # Purple

        # Transilien colors
        ("transilien", "L"): "#8D653A",  # Brown

        # Bus colors (generic blue for all buses)
        ("buses", "73"): "#0055C8",
        ("buses", "144"): "#0055C8",
        ("buses", "158"): "#0055C8",
        ("buses", "163"): "#0055C8",
        ("buses", "174"): "#0055C8",
        ("buses", "178"): "#0055C8",
        ("buses", "258"): "#0055C8",
        ("buses", "262"): "#0055C8",
        ("buses", "272"): "#0055C8",
        ("buses", "275"): "#0055C8",

        # IDFM generic
        ("idfm", ""): "#4A90E2"
    }

    return colors.get((transport_type, line), "#666666")  # Default gray


def render_transport_status(traffic_status_df):
    """Render transport lines status information with improved organization"""
    st.subheader("🚊 Transport Lines Status")

    if not traffic_status_df.empty:
        # Group by transport type for better organization
        transport_groups = {
            "🚇 Metro": traffic_status_df[traffic_status_df["transport_type"] == "metro"],
            "🚄 RER": traffic_status_df[traffic_status_df["transport_type"] == "rers"],
            "🚂 Transilien": traffic_status_df[traffic_status_df["transport_type"] == "transilien"],
            "🚌 Bus": traffic_status_df[traffic_status_df["transport_type"] == "buses"],
            "🚈 IDFM": traffic_status_df[traffic_status_df["transport_type"] == "idfm"]
        }

        # Count total lines with issues
        total_lines = len(traffic_status_df)
        normal_lines = len(traffic_status_df[traffic_status_df["status"] == "normal"])

        # Summary metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Lines", total_lines)
        with col2:
            st.metric("Normal Service", normal_lines, delta=f"{normal_lines}/{total_lines}")
        with col3:
            issues = total_lines - normal_lines
            st.metric("Lines with Issues", issues, delta=f"-{issues}" if issues > 0 else "0")

        # Display by transport type
        for transport_name, group_df in transport_groups.items():
            if not group_df.empty:
                with st.expander(f"{transport_name} ({len(group_df)} lines)", expanded=len(group_df) <= 3):
                    for _, line in group_df.iterrows():
                        # Determine status color and icon
                        if line["status"] == "normal":
                            status_color = "#28a745"  # Green
                            status_icon = "✅"
                        elif line["status"] == "minor":
                            status_color = "#ffc107"  # Yellow
                            status_icon = "⚠️"
                        elif line["status"] == "major":
                            status_color = "#fd7e14"  # Orange
                            status_icon = "🚨"
                        else:  # critical
                            status_color = "#dc3545"  # Red
                            status_icon = "❌"

                        # Get transport color for line badge
                        line_color = get_transport_color(line["transport_type"], line["line"])
                        display_name = get_transport_display_name(line["transport_type"], line["line"])

                        st.markdown(
                            f"""
                            <div style="
                                border-left: 5px solid {status_color}; 
                                padding: 10px; 
                                margin-bottom: 10px; 
                                background-color: #f8f9fa;
                                border-radius: 0 5px 5px 0;
                            ">
                                <div style="display: flex; align-items: center; margin-bottom: 5px;">
                                    <span style="
                                        background-color: {line_color}; 
                                        color: white; 
                                        padding: 2px 8px; 
                                        border-radius: 15px; 
                                        font-weight: bold; 
                                        font-size: 0.8em;
                                        margin-right: 10px;
                                    ">{display_name}</span>
                                    <span style="font-size: 1.1em;">{status_icon}</span>
                                    <strong style="margin-left: 5px;">{line.get('title', 'Service Information')}</strong>
                                </div>
                                <p style="margin: 0; color: #666; font-size: 0.9em;">{line.get('message', 'No additional information')}</p>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )
    else:
        st.info("No transport status information available at this time")


def render_schedules(schedules_df):
    """Render departure schedules information with improved filtering"""
    st.subheader("📅 Departure Schedules")

    if not schedules_df.empty:
        # Add transport type filter
        transport_types = schedules_df["transport_type"].unique()

        # Create filter columns
        filter_col1, filter_col2 = st.columns(2)

        with filter_col1:
            selected_types = st.multiselect(
                "Filter by Transport Type",
                options=transport_types,
                default=transport_types,
                format_func=lambda x: {
                    "metro": "🚇 Metro",
                    "rers": "🚄 RER",
                    "transilien": "🚂 Transilien",
                    "buses": "🚌 Bus",
                    "idfm": "🚈 IDFM"
                }.get(x, x)
            )

        with filter_col2:
            # Get lines for selected transport types
            filtered_df = schedules_df[schedules_df["transport_type"].isin(selected_types)]
            available_lines = sorted(filtered_df["line"].unique()) if not filtered_df.empty else []

            selected_lines = st.multiselect(
                "Filter by Line",
                options=available_lines,
                default=available_lines[:5] if len(available_lines) > 5 else available_lines
            )

        # Apply filters
        if selected_types and selected_lines:
            filtered_schedules = schedules_df[
                (schedules_df["transport_type"].isin(selected_types)) &
                (schedules_df["line"].isin(selected_lines))
            ]
        else:
            filtered_schedules = pd.DataFrame()

        if not filtered_schedules.empty:
            # Group by transport type for organized display
            transport_groups = filtered_schedules.groupby("transport_type")

            for transport_type, group in transport_groups:
                transport_display = {
                    "metro": "🚇 Metro",
                    "rers": "🚄 RER",
                    "transilien": "🚂 Transilien",
                    "buses": "🚌 Bus",
                    "idfm": "🚈 IDFM"
                }.get(transport_type, transport_type)

                with st.expander(f"{transport_display} Schedules", expanded=True):
                    # Create a clean display dataframe
                    display_df = group.copy()
                    display_df["Transport"] = display_df.apply(
                        lambda row: get_transport_display_name(row["transport_type"], row["line"]),
                        axis=1
                    )

                    # Select and rename columns for display
                    columns_to_show = ["Transport", "direction", "destination", "message"]
                    available_columns = [col for col in columns_to_show if col in display_df.columns]

                    if "destination" not in display_df.columns and "message" in display_df.columns:
                        # Use message as destination if destination not available
                        display_df["destination"] = display_df["message"]
                        available_columns = ["Transport", "direction", "destination"]

                    display_df_clean = display_df[available_columns].copy()

                    # Rename columns for better display
                    column_renames = {
                        "Transport": "Line",
                        "direction": "Direction",
                        "destination": "Destination",
                        "message": "Information"
                    }

                    display_df_clean = display_df_clean.rename(columns={
                        col: column_renames.get(col, col) for col in display_df_clean.columns
                    })

                    st.dataframe(
                        display_df_clean,
                        use_container_width=True,
                        hide_index=True
                    )
        else:
            st.info("No schedules available for the selected filters")
    else:
        st.info("No schedule information available at this time")


def render_schedule_summary(schedules_df):
    """Render a summary of next departures for the overview page"""
    if not schedules_df.empty:
        # Limit to next 10 departures and add transport colors
        summary_df = schedules_df.head(10).copy()

        # Add formatted transport line
        summary_df["Line"] = summary_df.apply(
            lambda row: get_transport_display_name(row["transport_type"], row["line"]),
            axis=1
        )

        # Select columns for summary display
        if "destination" in summary_df.columns:
            summary_columns = ["Line", "direction", "destination"]
            display_names = {"direction": "Direction", "destination": "Destination"}
        else:
            summary_columns = ["Line", "direction", "message"]
            display_names = {"direction": "Direction", "message": "Information"}

        summary_display = summary_df[summary_columns].copy()
        summary_display = summary_display.rename(columns=display_names)

        st.dataframe(
            summary_display,
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No departure information available")


def render_transport_usage_chart():
    """Render transport usage patterns chart with all transport types"""
    st.subheader("📊 Transport Usage Patterns")

    # Create enhanced mock hourly usage data for all transport types
    hours = list(range(5, 24))

    # Usage patterns (% of capacity)
    usage_data = {
        'hour': hours,
        'Métro 1': [10, 35, 80, 95, 70, 60, 55, 50, 55, 60, 65, 75, 90, 85, 75, 70, 60, 40, 20],
        'RER A': [5, 25, 70, 90, 65, 50, 45, 40, 45, 50, 55, 65, 85, 80, 70, 65, 55, 35, 15],
        'RER E': [3, 20, 60, 80, 55, 45, 40, 35, 40, 45, 50, 60, 75, 70, 60, 55, 45, 30, 12],
        'Transilien L': [4, 18, 55, 75, 50, 40, 35, 30, 35, 40, 45, 55, 70, 65, 55, 50, 40, 25, 10],
        'Bus Network': [8, 20, 50, 60, 55, 45, 40, 35, 40, 45, 50, 55, 65, 60, 55, 50, 40, 30, 15]
    }

    usage_df = pd.DataFrame(usage_data)

    # Create line chart
    fig = px.line(
        usage_df,
        x='hour',
        y=['Métro 1', 'RER A', 'RER E', 'Transilien L', 'Bus Network'],
        title='Transport Usage by Hour (% of Capacity)',
        labels={
            'value': 'Passenger Load (% of capacity)',
            'hour': 'Hour of Day',
            'variable': 'Transport Line'
        },
        color_discrete_map={
            'Métro 1': '#FFCD00',
            'RER A': '#E2231A',
            'RER E': '#6E1E78',
            'Transilien L': '#8D653A',
            'Bus Network': '#0055C8'
        }
    )

    fig.update_layout(
        xaxis=dict(tickmode='linear', dtick=1),
        hovermode="x unified",
        height=400
    )

    # Add vertical lines for peak hours
    fig.add_vline(x=8.5, line_width=2, line_dash="dash", line_color="red",
                  annotation_text="Morning Peak")
    fig.add_vline(x=18.5, line_width=2, line_dash="dash", line_color="red",
                  annotation_text="Evening Peak")

    st.plotly_chart(fig, use_container_width=True)

    # Add insights
    col1, col2 = st.columns(2)
    with col1:
        st.info("🕗 **Morning Peak**: 8:00-9:30 AM - Highest capacity usage across all lines")
    with col2:
        st.info("🕕 **Evening Peak**: 6:00-7:30 PM - Second highest capacity period")

    # Usage recommendations
    st.markdown("### 💡 Travel Recommendations")
    recommendations = [
        "**Best Times**: Travel before 7:30 AM or after 8:00 PM for maximum comfort",
        "**RER E**: Less crowded alternative to RER A during peak hours",
        "**Bus Network**: Good option for short distances during off-peak hours",
        "**Transilien L**: Excellent for connections to western suburbs"
    ]

    for rec in recommendations:
        st.markdown(f"- {rec}")


def render_line_performance_metrics(schedules_df, traffic_df):
    """Render performance metrics for each transport line"""
    if not schedules_df.empty and not traffic_df.empty:
        st.subheader("🎯 Line Performance Metrics")

        # Calculate metrics by line
        performance_data = []

        # Get unique transport lines
        if not schedules_df.empty:
            lines = schedules_df.groupby(['transport_type', 'line']).size().reset_index()

            for _, row in lines.iterrows():
                transport_type, line = row['transport_type'], row['line']

                # Count departures
                line_schedules = schedules_df[
                    (schedules_df['transport_type'] == transport_type) &
                    (schedules_df['line'] == line)
                ]
                departure_count = len(line_schedules)

                # Check traffic status
                line_traffic = traffic_df[
                    (traffic_df['transport_type'] == transport_type) &
                    (traffic_df['line'] == line)
                ]

                if not line_traffic.empty:
                    status = line_traffic.iloc[0]['status']
                    on_time_rate = {
                        'normal': 95,
                        'minor': 85,
                        'major': 70,
                        'critical': 40
                    }.get(status, 90)
                else:
                    status = 'normal'
                    on_time_rate = 90

                performance_data.append({
                    'Line': get_transport_display_name(transport_type, line),
                    'Transport Type': transport_type,
                    'Departures': departure_count,
                    'On-Time Rate (%)': on_time_rate,
                    'Status': status,
                    'Color': get_transport_color(transport_type, line)
                })

        if performance_data:
            perf_df = pd.DataFrame(performance_data)

            # Create metrics visualization
            col1, col2 = st.columns(2)

            with col1:
                # On-time performance chart
                fig_performance = px.bar(
                    perf_df,
                    x='Line',
                    y='On-Time Rate (%)',
                    title='On-Time Performance by Line',
                    color='On-Time Rate (%)',
                    color_continuous_scale=['red', 'yellow', 'green'],
                    range_color=[50, 100]
                )
                fig_performance.update_layout(xaxis_tickangle=-45, height=400)
                st.plotly_chart(fig_performance, use_container_width=True)

            with col2:
                # Departure frequency chart
                fig_frequency = px.bar(
                    perf_df,
                    x='Line',
                    y='Departures',
                    title='Number of Scheduled Departures',
                    color='Transport Type',
                    color_discrete_map={
                        'metro': '#FFCD00',
                        'rers': '#E2231A',
                        'transilien': '#8D653A',
                        'buses': '#0055C8',
                        'idfm': '#4A90E2'
                    }
                )
                fig_frequency.update_layout(xaxis_tickangle=-45, height=400)
                st.plotly_chart(fig_frequency, use_container_width=True)

            # Performance summary table
            st.markdown("### 📋 Performance Summary")
            summary_df = perf_df[['Line', 'Departures', 'On-Time Rate (%)', 'Status']].copy()
            summary_df['Status'] = summary_df['Status'].map({
                'normal': '✅ Normal',
                'minor': '⚠️ Minor Issues',
                'major': '🚨 Major Issues',
                'critical': '❌ Critical Issues'
            })

            st.dataframe(summary_df, use_container_width=True, hide_index=True)