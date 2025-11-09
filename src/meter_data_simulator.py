import json
import random
import time
import paho.mqtt.client as mqtt
from faker import Faker
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo  # For IST timezone
import logging

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Faker with Indian locale for realistic Tamil Nadu flavor
fake = Faker('en_IN')  # 'en_IN' for Indian English, adds realism

# Configurations
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "meter/data"

def generate_meter_data():
    # Define lists for variety
    substations = [f"SUB_TN_{fake.random_int(min=1, max=50):03d}" for _ in range(3)]  # e.g., SUB_TN_001, SUB_TN_015
    meters = [f"MTR_TN_{fake.random_int(min=100, max=999):03d}" for _ in range(10)]  # e.g., MTR_TN_123
    
    # Generate data
    data = {
        "meter_id": random.choice(meters),
        "substation_id": random.choice(substations),
        "timestamp": fake.date_time_this_year(before_now=True, after_now=False, tzinfo=ZoneInfo('Asia/Kolkata')).isoformat(),  # Realistic IST timestamp within this year
        "voltage": round(random.uniform(220.0, 240.0), 1),  # Realistic voltage range
        "current": round(random.uniform(5.0, 15.0), 1),     # Realistic current
        "energy_kwh": round(random.uniform(1.0, 10.0), 1)   # Cumulative energy per push
    }
    return json.dumps(data)

# Set up MQTT client
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.connect(MQTT_BROKER, MQTT_PORT, 60)  # Connect with 60s keepalive

# MQTT Callbacks
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logger.info("Connected to MQTT broker")
    else:
        logger.error(f"MQTT connect failed with code {rc}")

def on_publish(client, userdata, mid, rc=None, properties=None):
    if rc == mqtt.MQTT_ERR_SUCCESS:
        logger.info(f"Published successfully: mid={mid}")
    else:
        logger.error(f"Publish failed with code {rc}")

client.on_connect = on_connect
client.on_publish = on_publish

try:
    logger.info("Simulator started. Publishing data every 15 seconds. Press Ctrl+C to stop.")
    while True:
        payload = generate_meter_data()
        result = client.publish(MQTT_TOPIC, payload)
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            logger.info(f"Published successfully: {payload}")
        else:
            logger.error(f"Publish failed with code {result.rc}")
        time.sleep(15)  # 15 seconds for testing; in real, could be 900 for 15 mins
except KeyboardInterrupt:
    logger.info("Simulator stopping...")
finally:
    client.disconnect()
    logger.info("Disconnected from MQTT broker.")