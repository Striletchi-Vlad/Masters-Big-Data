# IoT Anomaly Detection System

This project implements a real-time anomaly detection pipeline for IoT sensor data using Kafka, HDFS, and Python (Streamlit, Scikit-learn).

## Architecture

1.  **Data Source**: `src/producers/main.py` simulates Temperature and Vibration sensors.
2.  **Ingestion**: Apache Kafka (`sensors.raw` topic).
3.  **Processing**: `src/detector/main.py` consumes raw data, detects anomalies using Isolation Forest, and produces to `sensors.anomalies`. It also writes raw data to HDFS in batches.
4.  **Storage**: HDFS (Hadoop Distributed File System) for historical data.
5.  **Visualization**: Streamlit Dashboard (`src/dashboard/app.py`) for real-time monitoring and batch analysis.

## Prerequisites

- Docker and Docker Compose

## How to Run

1.  Start the environment:
    ```bash
    docker-compose up --build -d
    ```

2.  Wait for services to initialize (Kafka and HDFS take a minute).

3.  Access the Dashboard:
    Open [http://localhost:8501](http://localhost:8501) in your browser.

4.  In the Dashboard:
    - Click "Run Live Monitoring" checkbox to see real-time data and anomalies.
    - Go to "Batch Analysis" tab and click "Load Data from HDFS" to see historical stats (after some data has been generated).

5.  Stop the environment:
    ```bash
    docker-compose down
    ```

## Components

- **Producer**: Generates synthetic sensor data with occasional anomalies.
- **Detector**: Anomaly detection service.
- **Dashboard**: Visualization interface.
- **Kafka/Zookeeper**: Message broker.
- **Namenode/Datanode**: HDFS storage.
