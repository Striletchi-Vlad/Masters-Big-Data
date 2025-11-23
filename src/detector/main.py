import json
import time
import os
import logging
import numpy as np
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from sklearn.ensemble import IsolationForest
from hdfs import InsecureClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:9092')
INPUT_TOPIC = 'sensors.raw'
OUTPUT_TOPIC = 'sensors.anomalies'
HDFS_NAMENODE = os.environ.get('HDFS_NAMENODE', 'http://namenode:9870')
HDFS_USER = os.environ.get('HDFS_USER', 'root')
HDFS_PATH = '/sensors/data'

def create_kafka_client():
    consumer = None
    producer = None
    while not consumer or not producer:
        try:
            consumer = KafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='latest',
                group_id='detector-group'
            )
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            logger.info("Connected to Kafka")
        except Exception as e:
            logger.error(f"Waiting for Kafka: {e}")
            time.sleep(5)
    return consumer, producer

def get_hdfs_client():
    client = None
    while not client:
        try:
            # Wait for Namenode to be ready (simple retry)
            client = InsecureClient(HDFS_NAMENODE, user=HDFS_USER)
            # check connection by listing root
            client.list('/')
            logger.info("Connected to HDFS")
        except Exception as e:
            logger.error(f"Waiting for HDFS: {e}")
            time.sleep(10)
    return client

class AnomalyDetector:
    def __init__(self):
        self.models = {} # one per sensor_type
        self.data_buffers = {} # one per sensor_type
        self.min_train_size = 100
        self.max_buffer_size = 1000
        
    def train(self, sensor_type, data):
        if len(data) < self.min_train_size:
            return None
            
        # Reshape for sklearn
        X = np.array(data).reshape(-1, 1)
        model = IsolationForest(contamination=0.05, random_state=42)
        model.fit(X)
        self.models[sensor_type] = model
        logger.info(f"Retrained model for {sensor_type} with {len(data)} samples")
        
    def predict(self, sensor_type, value):
        if sensor_type not in self.models:
            return 0 # Unknown status (could treat as normal or pending)
            
        X = np.array([[value]])
        # Isolation Forest returns -1 for anomaly, 1 for normal
        pred = self.models[sensor_type].predict(X)[0]
        return 1 if pred == -1 else 0 # Return 1 if anomaly, 0 if normal

    def update(self, sensor_type, value):
        if sensor_type not in self.data_buffers:
            self.data_buffers[sensor_type] = []
            
        self.data_buffers[sensor_type].append(value)
        
        # Keep buffer size managed
        if len(self.data_buffers[sensor_type]) > self.max_buffer_size:
            self.data_buffers[sensor_type].pop(0)
            
        # Retrain periodically or if no model exists
        if sensor_type not in self.models or len(self.data_buffers[sensor_type]) % 100 == 0:
            self.train(sensor_type, self.data_buffers[sensor_type])
            
        return self.predict(sensor_type, value)

def main():
    consumer, producer = create_kafka_client()
    hdfs_client = get_hdfs_client()
    detector = AnomalyDetector()
    
    # HDFS batching
    batch_data = []
    BATCH_SIZE = 100
    
    logger.info("Starting Detector Service...")
    
    for message in consumer:
        record = message.value
        sensor_type = record.get('sensor_type')
        value = record.get('value')
        
        if sensor_type and value is not None:
            is_anomaly = detector.update(sensor_type, value)
            
            record['is_anomaly'] = int(is_anomaly)
            
            # Send to anomalies topic
            producer.send(OUTPUT_TOPIC, value=record)
            
            # Add to HDFS batch
            batch_data.append(record)
            
            if len(batch_data) >= BATCH_SIZE:
                try:
                    # Write to HDFS
                    # Create a filename with timestamp
                    filename = f"/sensors_data_{int(time.time())}.json"
                    with hdfs_client.write(filename, encoding='utf-8') as writer:
                        json.dump(batch_data, writer)
                    logger.info(f"Flushed {len(batch_data)} records to HDFS: {filename}")
                    batch_data = []
                except Exception as e:
                    logger.error(f"Failed to write to HDFS: {e}")
                    # Keep buffer? Or drop to avoid memory leak? 
                    # For now, we keep and retry next loop or drop if too big.
                    if len(batch_data) > 1000:
                        batch_data = [] # Drop if stuck

if __name__ == '__main__':
    main()
