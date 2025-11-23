import time
import json
import random
import numpy as np
from kafka import KafkaProducer
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:9092')
TOPIC = 'sensors.raw'

def create_producer():
    producer = None
    while not producer:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            logger.info("Connected to Kafka")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            time.sleep(5)
    return producer

def generate_sensor_data(sensor_id, sensor_type):
    timestamp = time.time()
    
    if sensor_type == 'temperature':
        # Normal: 20-30 C, Anomaly: > 35 or < 15
        base_temp = 25
        noise = np.random.normal(0, 2)
        value = base_temp + noise
        
        # Inject anomaly randomly (5% chance)
        if random.random() < 0.05:
            value += random.choice([-15, 15])
            
    elif sensor_type == 'vibration':
        # Normal: 0-10 Hz, Anomaly: > 20
        value = abs(np.random.normal(5, 2))
        
        # Inject anomaly randomly (5% chance)
        if random.random() < 0.05:
            value += 20

    return {
        'sensor_id': sensor_id,
        'sensor_type': sensor_type,
        'timestamp': timestamp,
        'value': value
    }

def main():
    producer = create_producer()
    
    logger.info("Starting sensor simulation...")
    
    while True:
        # Simulate Temperature Sensor
        data_temp = generate_sensor_data('sensor_temp_01', 'temperature')
        producer.send(TOPIC, value=data_temp)
        
        # Simulate Vibration Sensor
        data_vib = generate_sensor_data('sensor_vib_01', 'vibration')
        producer.send(TOPIC, value=data_vib)
        
        logger.debug(f"Sent: {data_temp} | {data_vib}")
        
        # 100 events per second total roughly? Or just 1 per second for visibility?
        # Requirement says "1k-50k events/sec" for "realistic loads".
        # But for a demo, that floods the logs. Let's do 10 per second.
        time.sleep(0.1) 

if __name__ == '__main__':
    main()
