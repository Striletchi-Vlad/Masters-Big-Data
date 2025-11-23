import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from kafka import KafkaConsumer
import json
import os
import time
from hdfs import InsecureClient

# Config
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:9092')
TOPIC = 'sensors.anomalies'
HDFS_NAMENODE = os.environ.get('HDFS_NAMENODE', 'http://namenode:9870')
HDFS_USER = os.environ.get('HDFS_USER', 'root')

st.set_page_config(page_title="IoT Anomaly Detection", layout="wide")

st.title("IoT Sensor Anomaly Detection System")

# Sidebar
st.sidebar.header("Settings")
refresh_rate = st.sidebar.slider("Refresh Rate (s)", 0.5, 5.0, 1.0)

tab1, tab2 = st.tabs(["Real-Time Monitoring", "Batch Analysis (HDFS)"])

# Global variable to store data for the session (simple cache)
if 'data_buffer' not in st.session_state:
    st.session_state.data_buffer = pd.DataFrame(columns=['timestamp', 'sensor_id', 'sensor_type', 'value', 'is_anomaly'])

def get_kafka_consumer():
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            group_id=f'dashboard-group-{time.time()}', # Unique group to get all latest messages
            consumer_timeout_ms=1000 # Don't block indefinitely
        )
        return consumer
    except Exception as e:
        st.error(f"Connection to Kafka failed: {e}")
        return None

def read_hdfs_data():
    try:
        client = InsecureClient(HDFS_NAMENODE, user=HDFS_USER)
        files = client.list('/')
        all_data = []
        sensor_files = [f for f in files if f.startswith('sensors_data_')]
        
        # Limit to last 5 files to avoid reading too much in demo
        for f in sorted(sensor_files)[-5:]:
            with client.read(f'/{f}', encoding='utf-8') as reader:
                data = json.load(reader)
                all_data.extend(data)
        
        return pd.DataFrame(all_data)
    except Exception as e:
        # st.warning(f"Could not read from HDFS (might be empty or connecting): {e}")
        return pd.DataFrame()

with tab1:
    st.subheader("Live Sensor Streams")
    
    col1, col2 = st.columns(2)
    with col1:
        placeholder_temp = st.empty()
    with col2:
        placeholder_vib = st.empty()
        
    consumer = get_kafka_consumer()
    
    if consumer:
        run_monitoring = st.checkbox("Run Live Monitoring")
        
        if run_monitoring:
            while run_monitoring:
                # Poll Kafka
                messages = consumer.poll(timeout_ms=500)
                
                new_rows = []
                for tp, msgs in messages.items():
                    for msg in msgs:
                        new_rows.append(msg.value)
                
                if new_rows:
                    new_df = pd.DataFrame(new_rows)
                    # handle timestamp
                    new_df['timestamp'] = pd.to_datetime(new_df['timestamp'], unit='s')
                    
                    st.session_state.data_buffer = pd.concat([st.session_state.data_buffer, new_df], ignore_index=True)
                    
                    # Keep buffer size reasonable (e.g., last 500 points)
                    if len(st.session_state.data_buffer) > 500:
                         st.session_state.data_buffer = st.session_state.data_buffer.iloc[-500:]

                # Visualization
                df = st.session_state.data_buffer
                if not df.empty:
                     # Temp Sensor
                    temp_df = df[df['sensor_type'] == 'temperature']
                    if not temp_df.empty:
                        fig_temp = px.line(temp_df, x='timestamp', y='value', color='sensor_id', title="Temperature Sensor", markers=True)
                        anomalies = temp_df[temp_df['is_anomaly'] == 1]
                        if not anomalies.empty:
                            fig_temp.add_trace(go.Scatter(x=anomalies['timestamp'], y=anomalies['value'], mode='markers', marker=dict(color='red', size=10), name='Anomaly'))
                        
                        with placeholder_temp.container():
                             st.plotly_chart(fig_temp, use_container_width=True, key=f"temp_{time.time()}")
                    
                    # Vibration Sensor
                    vib_df = df[df['sensor_type'] == 'vibration']
                    if not vib_df.empty:
                        fig_vib = px.line(vib_df, x='timestamp', y='value', color='sensor_id', title="Vibration Sensor", markers=True)
                        anomalies = vib_df[vib_df['is_anomaly'] == 1]
                        if not anomalies.empty:
                            fig_vib.add_trace(go.Scatter(x=anomalies['timestamp'], y=anomalies['value'], mode='markers', marker=dict(color='red', size=10), name='Anomaly'))
                            
                        with placeholder_vib.container():
                             st.plotly_chart(fig_vib, use_container_width=True, key=f"vib_{time.time()}")

                time.sleep(refresh_rate)

with tab2:
    st.subheader("Historical Data Analysis (HDFS)")
    
    if st.button("Load Data from HDFS"):
        with st.spinner("Reading from HDFS..."):
            hdfs_df = read_hdfs_data()
            
        if not hdfs_df.empty:
            st.success(f"Loaded {len(hdfs_df)} records.")
            
            st.write("### Data Preview")
            st.dataframe(hdfs_df.head())
            
            st.write("### Distributions")
            
            col_h1, col_h2 = st.columns(2)
            
            with col_h1:
                # Histogram for Temperature
                temp_data = hdfs_df[hdfs_df['sensor_type'] == 'temperature']
                if not temp_data.empty:
                    fig_hist_temp = px.histogram(temp_data, x="value", nbins=30, title="Temperature Distribution", color="is_anomaly")
                    st.plotly_chart(fig_hist_temp, use_container_width=True)
                    
            with col_h2:
                # Histogram for Vibration
                vib_data = hdfs_df[hdfs_df['sensor_type'] == 'vibration']
                if not vib_data.empty:
                    fig_hist_vib = px.histogram(vib_data, x="value", nbins=30, title="Vibration Distribution", color="is_anomaly")
                    st.plotly_chart(fig_hist_vib, use_container_width=True)
                    
            st.write("### Anomaly Statistics")
            st.write(hdfs_df.groupby(['sensor_type', 'is_anomaly']).size().unstack(fill_value=0))
            
        else:
            st.info("No data found in HDFS or connection failed.")
