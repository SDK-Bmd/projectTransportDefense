"""
Transport visualization components for the La Défense mobility dashboard
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


def render_transport_status(traffic_status_df):
    """Render transport lines status information"""
    st.subheader("Transport Lines Status")

    if not traffic_status_df.empty:
        # Check if we have the required columns
        required_columns = ["transport_type", "line", "status", "title", "message"]
        has_required = all([col in traffic_status_df.columns for col in required_columns])

        if has_required:
            for _, line in traffic_status_df.iterrows():
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


def render_schedules(schedules_df):
    """Render departure schedules information"""
    st.subheader("Next Departures from La Défense")

    if not schedules_df.empty:
        # Check if we have the required columns
        required_columns = ["transport_type", "line", "direction", "message"]
        has_required = all([col in schedules_df.columns for col in required_columns])

        if has_required:
            # Group by transport type
            transport_types = schedules_df["transport_type"].unique()

            for transport_type in transport_types:
                transport_schedules = schedules_df[schedules_df["transport_type"] == transport_type]

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


def render_schedule_summary(schedules_df):
    """Render a summary of next departures"""
    if not schedules_df.empty:
        schedules = schedules_df.head(10)

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