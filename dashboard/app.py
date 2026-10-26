"""
Streamlit Live Analytics & Anomaly Detection Dashboard.

Real-Time Big Data Stream Analytics and Anomaly Detection Using Apache Kafka and Spark Structured Streaming.
Displays live telemetry, windowed aggregations, multi-method anomaly detection comparisons,
system performance latency, and interactive simulation controls.
"""

import sys
import time
import subprocess
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from sklearn.metrics import precision_score, recall_score, f1_score, confusion_matrix

sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.config import config
from streaming.storage import storage
from producer.event_generator import EventGenerator

# Page setup
st.set_page_config(
    page_title="Real-Time Big Data Stream Analytics",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for modern glassmorphic look
st.markdown("""
<style>
    /* Global styles */
    .stApp {
        background: linear-gradient(135deg, #0e1117 0%, #161b22 100%);
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    /* Metric Card Styling */
    div[data-testid="stMetric"] {
        background: rgba(26, 32, 44, 0.7);
        backdrop-filter: blur(12px);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 12px;
        padding: 16px;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.25);
        transition: transform 0.2s ease, border-color 0.2s ease;
    }
    div[data-testid="stMetric"]:hover {
        transform: translateY(-2px);
        border-color: rgba(59, 130, 246, 0.5);
    }
    /* Severity Badges */
    .badge-high {
        background-color: #ef4444;
        color: white;
        padding: 3px 8px;
        border-radius: 6px;
        font-weight: 600;
        font-size: 0.8rem;
    }
    .badge-medium {
        background-color: #f59e0b;
        color: white;
        padding: 3px 8px;
        border-radius: 6px;
        font-weight: 600;
        font-size: 0.8rem;
    }
    .badge-low {
        background-color: #3b82f6;
        color: white;
        padding: 3px 8px;
        border-radius: 6px;
        font-weight: 600;
        font-size: 0.8rem;
    }
</style>
""", unsafe_allow_html=True)


# --- Sidebar Navigation & Simulation Controls ---
st.sidebar.title("⚡ Stream Analytics")
st.sidebar.caption("Kafka + Spark Structured Streaming")

# Auto-refresh control
auto_refresh = st.sidebar.toggle("Auto-Refresh Live Stream", value=True)
refresh_interval = st.sidebar.slider("Refresh Interval (seconds)", min_value=1, max_value=10, value=2)

st.sidebar.markdown("---")
st.sidebar.subheader("🎛️ Simulation Generator")

gen_rate = st.sidebar.slider("Event Rate (Hz)", min_value=1, max_value=100, value=10)
gen_anomaly_prob = st.sidebar.slider("Anomaly Probability", min_value=0.0, max_value=0.5, value=0.1, step=0.01)

col_sim1, col_sim2 = st.sidebar.columns(2)
inject_anomaly = col_sim1.button("Inject Anomaly 🚨")
clear_db = col_sim2.button("Reset DB 🗑️")

if clear_db:
    storage.clear_database()
    st.sidebar.success("Database reset successfully!")
    st.rerun()

if inject_anomaly:
    from producer.kafka_producer import SensorKafkaProducer
    p = SensorKafkaProducer(rate_hz=10)
    if p.connect(max_retries=2):
        # Generate an extreme spike
        spike_evt = {
            "event_id": f"inject_{int(time.time()*1000)}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "sensor_id": "sensor_01",
            "temperature": 96.8,
            "vibration": 1.15,
            "pressure": 138.4,
            "ground_truth_label": "anomaly",
            "anomaly_type": "point",
            "anomaly_severity": "high"
        }
        p.send_single_event(spike_evt)
        p.producer.flush()
        p.stop()
        st.sidebar.success("Controlled Anomaly Injected!")
    else:
        st.sidebar.error("Could not reach Kafka broker.")

st.sidebar.markdown("---")
st.sidebar.info("""
**Architecture Pipeline:**
1. Event Generator (Python)
2. Apache Kafka Broker (KRaft :9092)
3. Spark Structured Streaming (Micro-Batch)
4. Detection Engine (Threshold, Z-score, IF)
5. SQLite (WAL Mode)
6. Streamlit Dashboard
""")

# --- Main Dashboard Header ---
st.title("Real-Time Big Data Stream Analytics & Anomaly Detection")
st.markdown("Live distributed telemetry processing using **Apache Kafka**, **Apache Spark Structured Streaming**, and **Machine Learning**.")

# Fetch KPIs from SQLite
kpis = storage.get_summary_kpis()
total_events = kpis["total_events"]
total_alerts = kpis["total_alerts"]
active_sensors = kpis["active_sensors"]
throughput = kpis["avg_throughput"]
latency_ms = kpis["avg_latency_ms"]

# --- Section 1: Top KPI Cards ---
kpi_cols = st.columns(5)
kpi_cols[0].metric("Total Events Ingested", f"{total_events:,}")
kpi_cols[1].metric("Active Anomaly Alerts", f"{total_alerts:,}", delta=f"{round(total_alerts/max(1, total_events)*100, 1)}% rate", delta_color="inverse")
kpi_cols[2].metric("Active Sensors", f"{active_sensors}")
kpi_cols[3].metric("Processing Throughput", f"{throughput} evt/s")
kpi_cols[4].metric("Processing Latency", f"{latency_ms} ms")

st.markdown("---")

# Tabbed Layout
tab_live, tab_telemetry, tab_windows, tab_alerts, tab_comparison, tab_performance = st.tabs([
    "📊 Live Overview",
    "📈 Sensor Telemetry",
    "⏱️ Windowed Aggregations",
    "🚨 Anomaly Alerts Feed",
    "⚖️ Method Comparison",
    "⚡ System Performance"
])

# --- Tab 1: Live Overview ---
with tab_live:
    col_l1, col_l2 = st.columns([2, 1])

    with col_l1:
        st.subheader("Live Telemetry Snapshot")
        recent_df = storage.get_recent_events(limit=60)
        if not recent_df.empty:
            # Multi-line temperature trend
            fig_temp = px.line(
                recent_df,
                x="timestamp",
                y="temperature",
                color="sensor_id",
                title="Real-Time Temperature Stream (°C) per Sensor",
                labels={"temperature": "Temperature (°C)", "timestamp": "Timestamp"},
                template="plotly_dark"
            )
            fig_temp.add_hline(y=config.thresholds.temp_max, line_dash="dash", line_color="red", annotation_text="Max Limit (75°C)")
            fig_temp.update_layout(height=350, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig_temp, use_container_width=True)
        else:
            st.info("Awaiting telemetry events from Kafka... Run the producer to stream events.")

    with col_l2:
        st.subheader("Anomaly Breakdown")
        alerts_df = storage.get_recent_alerts(limit=500)
        if not alerts_df.empty:
            # Donut chart of severity
            sev_counts = alerts_df["severity"].value_counts().reset_index()
            sev_counts.columns = ["Severity", "Count"]
            fig_pie = px.pie(
                sev_counts,
                values="Count",
                names="Severity",
                hole=0.5,
                color="Severity",
                color_discrete_map={"high": "#ef4444", "medium": "#f59e0b", "low": "#3b82f6"},
                template="plotly_dark",
                title="Alert Severity Distribution"
            )
            fig_pie.update_layout(height=350, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.info("No anomalies detected yet.")

# --- Tab 2: Sensor Telemetry ---
with tab_telemetry:
    st.subheader("Multi-Parameter Sensor Diagnostics")
    sensor_filter = st.selectbox("Select Sensor ID", options=["All"] + config.generator.sensors)
    telemetry_limit = st.slider("Historical Event Window", min_value=50, max_value=500, value=150)

    events_df = storage.get_recent_events(limit=telemetry_limit, sensor_id=sensor_filter)

    if not events_df.empty:
        c1, c2 = st.columns(2)
        with c1:
            fig_vib = px.line(
                events_df,
                x="timestamp",
                y="vibration",
                color="sensor_id",
                title="Vibration Sensor Readings (g)",
                template="plotly_dark"
            )
            fig_vib.add_hline(y=config.thresholds.vib_max, line_dash="dash", line_color="orange", annotation_text="Vibration Limit (0.65g)")
            fig_vib.update_layout(height=320, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig_vib, use_container_width=True)

        with c2:
            fig_press = px.line(
                events_df,
                x="timestamp",
                y="pressure",
                color="sensor_id",
                title="Pressure Sensor Readings (kPa)",
                template="plotly_dark"
            )
            fig_press.add_hline(y=config.thresholds.press_max, line_dash="dash", line_color="yellow", annotation_text="High Limit (115 kPa)")
            fig_press.add_hline(y=config.thresholds.press_min, line_dash="dash", line_color="yellow", annotation_text="Low Limit (85 kPa)")
            fig_press.update_layout(height=320, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig_press, use_container_width=True)

        # Telemetry Table
        st.dataframe(
            events_df[["event_id", "timestamp", "sensor_id", "temperature", "vibration", "pressure", "ground_truth_label", "anomaly_type"]].head(20),
            use_container_width=True
        )
    else:
        st.info("No telemetry records available.")

# --- Tab 3: Windowed Aggregations ---
with tab_windows:
    st.subheader("Spark Event-Time Window Aggregations")
    st.markdown("Aggregated summary statistics computed across micro-batches and time windows.")

    aggs_df = storage.get_window_aggregations(limit=50)
    if not aggs_df.empty:
        w1, w2 = st.columns([3, 2])
        with w1:
            fig_bar = px.bar(
                aggs_df.head(20),
                x="sensor_id",
                y="avg_temp",
                color="anomaly_count",
                title="Window Average Temperature & Anomaly Frequency per Sensor",
                color_continuous_scale="Viridis",
                template="plotly_dark"
            )
            fig_bar.update_layout(height=350)
            st.plotly_chart(fig_bar, use_container_width=True)

        with w2:
            st.dataframe(
                aggs_df[["window_start", "sensor_id", "event_count", "avg_temp", "avg_vib", "avg_press", "anomaly_count"]].head(15),
                use_container_width=True
            )
    else:
        st.info("No window aggregations computed yet. Ensure Spark processor is active.")

# --- Tab 4: Anomaly Alerts Feed ---
with tab_alerts:
    st.subheader("🚨 Real-Time Anomaly Alert Feed")
    col_f1, col_f2 = st.columns(2)
    sev_filter = col_f1.selectbox("Filter by Severity", ["All", "high", "medium", "low"])
    alert_search = col_f2.text_input("Search Sensor or Reason", "")

    alerts_df = storage.get_recent_alerts(limit=100, severity=sev_filter)
    if not alerts_df.empty:
        if alert_search:
            alerts_df = alerts_df[
                alerts_df["explanation"].str.contains(alert_search, case=False, na=False) |
                alerts_df["sensor_id"].str.contains(alert_search, case=False, na=False)
            ]

        # Display table with styled columns
        st.dataframe(
            alerts_df[[
                "event_id", "timestamp", "sensor_id", "temperature", "vibration",
                "pressure", "detected_by", "severity", "explanation", "ground_truth_label"
            ]],
            use_container_width=True
        )
    else:
        st.info("No alerts currently matching filter.")

# --- Tab 5: Analytical Method Comparison ---
with tab_comparison:
    st.subheader("⚖️ Anomaly Detection Analytical Comparison")
    st.markdown("""
    Evaluating the performance trade-offs of the three analytical methods against known ground-truth labels:
    - **Method A:** Threshold-Based Detection (Static Operating Limits)
    - **Method B:** Rolling Z-Score (Adaptive Dynamic History)
    - **Method C:** Isolation Forest (Unsupervised Multivariate Tree Isolation)
    """)

    eval_df = storage.get_recent_events(limit=500)
    alerts_all = storage.get_recent_alerts(limit=500)

    if not eval_df.empty and not alerts_all.empty:
        # Merge event ground truth with detection flags
        merged = eval_df.merge(
            alerts_all[["event_id", "flag_threshold", "flag_zscore", "flag_isolation_forest"]],
            on="event_id",
            how="left"
        ).fillna(0)

        y_true = (merged["ground_truth_label"] == "anomaly").astype(int)

        methods = ["Threshold", "Rolling Z-Score", "Isolation Forest"]
        flag_cols = ["flag_threshold", "flag_zscore", "flag_isolation_forest"]

        metrics_list = []
        for name, col in zip(methods, flag_cols):
            y_pred = merged[col].astype(int)
            prec = precision_score(y_true, y_pred, zero_division=0)
            rec = recall_score(y_true, y_pred, zero_division=0)
            f1 = f1_score(y_true, y_pred, zero_division=0)

            # Confusion matrix
            tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[0, 1]).ravel()
            fpr = fp / (fp + tn) if (fp + tn) > 0 else 0.0

            metrics_list.append({
                "Method": name,
                "Precision": round(prec, 3),
                "Recall": round(rec, 3),
                "F1-Score": round(f1, 3),
                "False Positive Rate": round(fpr, 3),
                "True Positives (TP)": int(tp),
                "False Positives (FP)": int(fp),
                "False Negatives (FN)": int(fn),
                "True Negatives (TN)": int(tn)
            })

        m_df = pd.DataFrame(metrics_list)
        st.table(m_df[["Method", "Precision", "Recall", "F1-Score", "False Positive Rate"]])

        # Comparative Bar Chart
        fig_comp = go.Figure()
        fig_comp.add_trace(go.Bar(name="Precision", x=m_df["Method"], y=m_df["Precision"], marker_color="#3b82f6"))
        fig_comp.add_trace(go.Bar(name="Recall", x=m_df["Method"], y=m_df["Recall"], marker_color="#10b981"))
        fig_comp.add_trace(go.Bar(name="F1-Score", x=m_df["Method"], y=m_df["F1-Score"], marker_color="#8b5cf6"))
        fig_comp.update_layout(barmode="group", title="Detection Quality Metrics Comparison", template="plotly_dark", height=350)
        st.plotly_chart(fig_comp, use_container_width=True)

        # Confusion Matrices Breakdown
        cm_cols = st.columns(3)
        for idx, row in m_df.iterrows():
            with cm_cols[idx]:
                st.write(f"**{row['Method']} Confusion Matrix**")
                cm_matrix = pd.DataFrame(
                    [[row["True Negatives (TN)"], row["False Positives (FP)"]],
                     [row["False Negatives (FN)"], row["True Positives (TP)"]]],
                    index=["Actual Normal", "Actual Anomaly"],
                    columns=["Pred Normal", "Pred Anomaly"]
                )
                st.dataframe(cm_matrix)
    else:
        st.info("Insufficient events for method comparison. Generate streaming events to populate benchmark data.")

# --- Tab 6: System Performance ---
with tab_performance:
    st.subheader("⚡ Streaming System Performance & Latency")
    st.markdown("Real-time measurements of Spark micro-batch execution latency, throughput, and processing efficiency.")

    perf_df = storage.get_recent_metrics(limit=50)
    if not perf_df.empty:
        p1, p2 = st.columns(2)
        with p1:
            fig_lat = px.line(
                perf_df,
                x="batch_id",
                y="batch_duration_ms",
                title="Micro-Batch Processing Duration (ms)",
                labels={"batch_id": "Micro-Batch ID", "batch_duration_ms": "Duration (ms)"},
                template="plotly_dark"
            )
            fig_lat.update_layout(height=320, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig_lat, use_container_width=True)

        with p2:
            fig_thru = px.line(
                perf_df,
                x="batch_id",
                y="process_rows_per_second",
                title="Processing Throughput (events/sec)",
                labels={"batch_id": "Micro-Batch ID", "process_rows_per_second": "Throughput (evt/s)"},
                template="plotly_dark"
            )
            fig_thru.update_layout(height=320, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig_thru, use_container_width=True)

        st.dataframe(perf_df.head(20), use_container_width=True)
    else:
        st.info("No system performance metrics captured yet. Run Spark processor to track batch durations.")


# Auto-refresh loop
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()
