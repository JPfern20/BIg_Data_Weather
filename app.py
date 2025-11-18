"""
Streaming Data Dashboard Template
STUDENT PROJECT: Big Data Streaming Dashboard

This is a template for students to build a real-time streaming data dashboard.
Students will need to implement the actual data processing, Kafka consumption,
and storage integration.

IMPLEMENT THE TODO SECTIONS
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import time
import json
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
from streamlit_autorefresh import st_autorefresh
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Page configuration
st.set_page_config(
    page_title="Streaming Data Dashboard by Albano, Calipusan, and Fernandez",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

def setup_sidebar():
    st.sidebar.title("Dashboard Controls")
    st.sidebar.subheader("Data Source Configuration")

    kafka_broker = st.sidebar.text_input("Kafka Broker", value="localhost:9092")
    kafka_topic = st.sidebar.text_input("Kafka Topic", value="streaming-data")

    st.sidebar.subheader("Storage Configuration")
    storage_type = st.sidebar.selectbox("Storage Type", ["HDFS", "MongoDB"])

    return {
        "kafka_broker": kafka_broker,
        "kafka_topic": kafka_topic,
        "storage_type": storage_type
    }
    

def generate_sample_data(metric_type="temperature"):
    current_time = datetime.now()
    times = [current_time - timedelta(minutes=i) for i in range(100, 0, -1)]

    # Define ranges for each metric type
    ranges = {
        "temperature": (20, 35),   # °C
        "humidity": (30, 90),      # %
        "pressure": (980, 1040)    # hPa
    }
    low, high = ranges.get(metric_type, (0, 100))

    values = [low + (i % (high - low)) for i in range(100)]

    return pd.DataFrame({
        "timestamp": times,
        "value": values,
        "metric_type": [metric_type] * 100,
        "sensor_id": ["sensor_1"] * 100
    })
    

def consume_kafka_data(config):
    kafka_broker = config.get("kafka_broker", "localhost:9092")
    kafka_topic = config.get("kafka_topic", "streaming-data")
    cache_key = f"kafka_consumer_{kafka_broker}_{kafka_topic}"

    if cache_key not in st.session_state:
        try:
            st.session_state[cache_key] = KafkaConsumer(
                kafka_topic,
                bootstrap_servers=[kafka_broker],
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=5000
            )
        except Exception as e:
            st.error(f"Kafka connection failed: {e}")
            st.session_state[cache_key] = None

    consumer = st.session_state[cache_key]
    if not consumer:
        return generate_sample_data()

    messages = []
    for msg_pack in consumer.poll(timeout_ms=2000).values():
        for message in msg_pack:
            data = message.value
            try:
                timestamp = datetime.fromisoformat(data['timestamp'].replace("Z", "+00:00"))
                messages.append({
                    'timestamp': timestamp,
                    'value': float(data['value']),
                    'metric_type': data['metric_type'],
                    'sensor_id': data['sensor_id']
                })
            except Exception:
                continue

    return pd.DataFrame(messages) if messages else generate_sample_data()

def query_historical_data(time_range="1h", metrics=None):
    try:
        spark = SparkSession.builder.appName("HistoricalDataQuery").getOrCreate()
        hdfs_path = "hdfs://localhost:9000/data/weather/"
        df = spark.read.parquet(hdfs_path)

        now = datetime.now()
        if time_range.endswith("h"):
            delta = timedelta(hours=int(time_range[:-1]))
        elif time_range.endswith("d"):
            delta = timedelta(days=int(time_range[:-1]))
        else:
            delta = timedelta(hours=1)
        start_time = now - delta

        df = df.filter(col("timestamp") >= start_time)

        if metrics:
            df = df.filter(col("metric_type").isin(metrics))

        pdf = df.toPandas()

        # If empty, generate sample data with the requested metric type
        if pdf.empty:
            st.warning(f"No historical data found for {metrics or 'all'}, using sample data instead.")
            sample = generate_sample_data(metrics[0] if metrics else "temperature")
            return sample

        return pdf

    except Exception as e:
        st.error(f"HDFS query failed: {e}")
        # Fallback: sample data with requested metric type
        return generate_sample_data(metrics[0] if metrics else "temperature")

def display_real_time_view(config, refresh_interval):
    """
    Page 1: Real-time Streaming View
    STUDENT TODO: Implement real-time data visualization from Kafka
    """
    st.header("📈 Real-time Streaming Dashboard")
    
    # Refresh status
    refresh_state = st.session_state.refresh_state
    st.info(f"**Auto-refresh:** {'🟢 Enabled' if refresh_state['auto_refresh'] else '🔴 Disabled'} - Updates every {refresh_interval} seconds")
    
    # Loading indicator for data consumption
    with st.spinner("Fetching real-time data from Kafka..."):
        real_time_data = consume_kafka_data(config)
    
    if real_time_data is not None:
        # Data freshness indicator
        data_freshness = datetime.now() - refresh_state['last_refresh']
        freshness_color = "🟢" if data_freshness.total_seconds() < 10 else "🟡" if data_freshness.total_seconds() < 30 else "🔴"
        
        st.success(f"{freshness_color} Data updated {data_freshness.total_seconds():.0f} seconds ago")
        
        # Real-time data metrics
        st.subheader("📊 Live Data Metrics")
        if not real_time_data.empty:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Records Received", len(real_time_data))
            with col2:
                st.metric("Latest Value", f"{real_time_data['value'].iloc[-1]:.2f}")
            with col3:
                st.metric("Data Range", f"{real_time_data['timestamp'].min().strftime('%H:%M')} - {real_time_data['timestamp'].max().strftime('%H:%M')}")
        
        # Real-time chart
        st.subheader("📈 Real-time Trend")
        
        if not real_time_data.empty:
            # STUDENT TODO: Customize this chart for your specific data
            fig = px.line(
                real_time_data,
                x='timestamp',
                y='value',
                title=f"Real-time Data Stream (Last {len(real_time_data)} records)",
                labels={'value': 'Sensor Value', 'timestamp': 'Time'},
                template='plotly_white'
            )
            fig.update_layout(
                xaxis_title="Time",
                yaxis_title="Value",
                hovermode='x unified'
            )
            st.plotly_chart(fig, width='stretch')
            
            # Raw data table with auto-refresh
            with st.expander("📋 View Raw Data"):
                st.dataframe(
                    real_time_data.sort_values('timestamp', ascending=False),
                    width='stretch',
                    height=300
                )
        else:
            st.warning("No real-time data available. STUDENT TODO: Implement Kafka consumer.")
    
    else:
        st.error("STUDENT TODO: Kafka data consumption not implemented")


def display_historical_view(config):
    st.header("📊 Historical Data Analysis")

    col1, col2, col3 = st.columns(3)
    time_range = col1.selectbox("Time Range", ["1h", "24h", "7d", "30d"])
    metric_type = col2.selectbox("Metric Type", ["temperature", "humidity", "pressure", "all"])
    aggregation = col3.selectbox("Aggregation", ["raw", "hourly", "daily", "weekly"])

    metrics = [metric_type] if metric_type != "all" else None
    historical_data = query_historical_data(time_range, metrics)

    if historical_data.empty:
        st.error("No historical data available.")
        return

    st.subheader("Historical Data Table")
    st.dataframe(historical_data.sort_values("timestamp"), width='stretch', hide_index=True)

    if aggregation != "raw":
        if aggregation == "hourly":
            historical_data["bucket"] = historical_data["timestamp"].dt.floor("H")
        elif aggregation == "daily":
            historical_data["bucket"] = historical_data["timestamp"].dt.floor("D")
        elif aggregation == "weekly":
            historical_data["bucket"] = historical_data["timestamp"].dt.to_period("W").dt.start_time

        agg_df = historical_data.groupby("bucket")["value"].agg(["mean", "min", "max"]).reset_index()
        st.subheader(f"Aggregated {aggregation.capitalize()} Trends")
        fig = px.line(agg_df, x="bucket", y="mean", title=f"{aggregation.capitalize()} Average Values")
        st.plotly_chart(fig, use_container_width=True)

        col1, col2, col3 = st.columns(3)
        col1.metric("Max Value", f"{agg_df['max'].max():.2f}")
        col2.metric("Min Value", f"{agg_df['min'].min():.2f}")
        col3.metric("Average Value", f"{agg_df['mean'].mean():.2f}")
    else:
        st.subheader("Raw Historical Trend")
        fig = px.line(historical_data, x="timestamp", y="value", color="metric_type", title="Raw Historical Data")
        st.plotly_chart(fig, use_container_width=True)

def main():
    st.title("🚀 Streaming Data Dashboard")
    
    with st.expander("Names of Team Members"):
        st.markdown("""
        Albano, Reagan \n
        Calipusan, Althea and \n
        Fernandez, John Patrick

""")
    
    # Initialize session state for refresh management
    if 'refresh_state' not in st.session_state:
        st.session_state.refresh_state = {
            'last_refresh': datetime.now(),
            'auto_refresh': True
        }
    
    # Setup configuration
    config = setup_sidebar()
    
    # Refresh controls in sidebar
    st.sidebar.subheader("Refresh Settings")
    st.session_state.refresh_state['auto_refresh'] = st.sidebar.checkbox(
        "Enable Auto Refresh",
        value=st.session_state.refresh_state['auto_refresh'],
        help="Automatically refresh real-time data"
    )
    
    if st.session_state.refresh_state['auto_refresh']:
        refresh_interval = st.sidebar.slider(
            "Refresh Interval (seconds)",
            min_value=5,
            max_value=60,
            value=15,
            help="Set how often real-time data refreshes"
        )
        
        # Auto-refresh using streamlit-autorefresh package
        st_autorefresh(interval=refresh_interval * 1000, key="auto_refresh")
    
    # Manual refresh button
    if st.sidebar.button("🔄 Manual Refresh"):
        st.session_state.refresh_state['last_refresh'] = datetime.now()
        st.rerun()
    
    # Display refresh status
    st.sidebar.markdown("---")
    st.sidebar.metric("Last Refresh", st.session_state.refresh_state['last_refresh'].strftime("%H:%M:%S"),"\nby JPFern20@github (_patcutie)")
    
    # Create tabs for different views
    tab1, tab2 = st.tabs(["📈 Real Time Mode", "📊 Historical Mode"])
    
    with tab1:
        display_real_time_view(config, refresh_interval)
    
    with tab2:
        display_historical_view(config)
    

if __name__ == "__main__":
    main()