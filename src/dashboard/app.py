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
show_features = st.sidebar.checkbox("Show Advanced Features", value=True)

tab1, tab2 = st.tabs(["Real-Time Monitoring", "Batch Analysis (HDFS)"])

# Global variable to store data for the session (simple cache)
if 'data_buffer' not in st.session_state:
    st.session_state.data_buffer = pd.DataFrame()

def get_kafka_consumer():
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            group_id=f'dashboard-group-{time.time()}', 
            consumer_timeout_ms=1000 
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
        
        for f in sorted(sensor_files)[-5:]:
            with client.read(f'/{f}', encoding='utf-8') as reader:
                data = json.load(reader)
                all_data.extend(data)
        
        return pd.DataFrame(all_data)
    except Exception as e:
        return pd.DataFrame()

def plot_sensor_metrics(df, sensor_type, title, show_features=False):
    sensor_df = df[df['sensor_type'] == sensor_type]
    if sensor_df.empty:
        return

    # Main Chart: Value and Anomalies
    fig = px.line(sensor_df, x='timestamp', y='value', color='sensor_id', title=f"{title} - Raw Signal")
    
    anomalies = sensor_df[sensor_df['is_anomaly'] == 1]
    if not anomalies.empty:
        fig.add_trace(go.Scatter(
            x=anomalies['timestamp'], 
            y=anomalies['value'], 
            mode='markers', 
            marker=dict(color='red', size=10, symbol='x'), 
            name='Anomaly'
        ))
    
    st.plotly_chart(fig, use_container_width=True, key=f"{sensor_type}_main_{time.time()}")

    # Feature Charts
    if show_features and 'rolling_mean' in sensor_df.columns:
        with st.expander(f"Advanced Features: {title}"):
            col1, col2 = st.columns(2)
            
            with col1:
                # Volatility and Trend
                fig_trend = go.Figure()
                fig_trend.add_trace(go.Scatter(x=sensor_df['timestamp'], y=sensor_df['rolling_mean'], name='Trend (Mean)'))
                fig_trend.add_trace(go.Scatter(x=sensor_df['timestamp'], y=sensor_df['rolling_mean'] + 2*sensor_df['rolling_std'], name='Upper Bound', line=dict(dash='dot', width=1)))
                fig_trend.add_trace(go.Scatter(x=sensor_df['timestamp'], y=sensor_df['rolling_mean'] - 2*sensor_df['rolling_std'], name='Lower Bound', line=dict(dash='dot', width=1)))
                fig_trend.update_layout(title="Trend & Volatility (Bollinger Bands)")
                st.plotly_chart(fig_trend, use_container_width=True, key=f"{sensor_type}_trend_{time.time()}")
                
            with col2:
                # Derivatives
                fig_deriv = go.Figure()
                fig_deriv.add_trace(go.Scatter(x=sensor_df['timestamp'], y=sensor_df['velocity'], name='Velocity'))
                fig_deriv.add_trace(go.Scatter(x=sensor_df['timestamp'], y=sensor_df['acceleration'], name='Acceleration'))
                fig_deriv.update_layout(title="Rate of Change (Derivatives)")
                st.plotly_chart(fig_deriv, use_container_width=True, key=f"{sensor_type}_deriv_{time.time()}")
                
            # Local Z-Score
            fig_z = px.bar(sensor_df, x='timestamp', y='local_z', title="Local Z-Score (Deviation Strength)")
            fig_z.add_hline(y=3, line_dash="dash", line_color="red")
            fig_z.add_hline(y=-3, line_dash="dash", line_color="red")
            st.plotly_chart(fig_z, use_container_width=True, key=f"{sensor_type}_z_{time.time()}")


with tab1:
    st.subheader("Live Sensor Streams")
    
    col_t, col_v = st.columns(2)
    with col_t:
        ph_temp = st.empty()
    with col_v:
        ph_vib = st.empty()
        
    consumer = get_kafka_consumer()
    
    if consumer:
        run_monitoring = st.checkbox("Run Live Monitoring")
        
        if run_monitoring:
            while run_monitoring:
                messages = consumer.poll(timeout_ms=500)
                
                new_rows = []
                for tp, msgs in messages.items():
                    for msg in msgs:
                        new_rows.append(msg.value)
                
                if new_rows:
                    new_df = pd.DataFrame(new_rows)
                    new_df['timestamp'] = pd.to_datetime(new_df['timestamp'], unit='s')
                    
                    st.session_state.data_buffer = pd.concat([st.session_state.data_buffer, new_df], ignore_index=True)
                    
                    if len(st.session_state.data_buffer) > 300: # Smaller buffer for smoother viz
                         st.session_state.data_buffer = st.session_state.data_buffer.iloc[-300:]

                df = st.session_state.data_buffer
                if not df.empty:
                    with ph_temp.container():
                        plot_sensor_metrics(df, 'temperature', "Temperature Sensor", show_features)
                    
                    with ph_vib.container():
                        plot_sensor_metrics(df, 'vibration', "Vibration Sensor", show_features)

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
                temp_data = hdfs_df[hdfs_df['sensor_type'] == 'temperature']
                if not temp_data.empty:
                    fig_hist_temp = px.histogram(temp_data, x="value", nbins=30, title="Temperature Distribution", color="is_anomaly")
                    st.plotly_chart(fig_hist_temp, use_container_width=True)
                    
            with col_h2:
                vib_data = hdfs_df[hdfs_df['sensor_type'] == 'vibration']
                if not vib_data.empty:
                    fig_hist_vib = px.histogram(vib_data, x="value", nbins=30, title="Vibration Distribution", color="is_anomaly")
                    st.plotly_chart(fig_hist_vib, use_container_width=True)
            
            # Feature Correlation (New)
            if 'velocity' in hdfs_df.columns:
                 st.write("### Feature Correlations (Anomaly vs Normal)")
                 fig_corr = px.scatter_matrix(hdfs_df, dimensions=['value', 'velocity', 'acceleration', 'local_z'], color='is_anomaly')
                 st.plotly_chart(fig_corr, use_container_width=True)

            st.write("### Anomaly Statistics")
            st.write(hdfs_df.groupby(['sensor_type', 'is_anomaly']).size().unstack(fill_value=0))
            
        else:
            st.info("No data found in HDFS or connection failed.")
