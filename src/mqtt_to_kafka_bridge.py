# src/mqtt_to_kafka_bridge.py
import asyncio
import json
import hashlib
import logging
from tenacity import retry, stop_after_attempt, wait_exponential
from aiokafka import AIOKafkaProducer
import paho.mqtt.client as mqtt
import time
import os

# ==================== CONFIG ====================
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = 1883
MQTT_TOPIC = "meter/data"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "iot_meters"
MAX_RETRIES = 5
# ===============================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Kafka Producer with retry
@retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_exponential(multiplier=1, min=2, max=10))
async def create_producer():
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        acks='all',
    )
    await producer.start()
    return producer

# Retryable send with exponential backoff
@retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_exponential(multiplier=1, min=2, max=10))
async def send_to_kafka(payload: dict, producer: AIOKafkaProducer):
    try:
        await producer.send_and_wait(
            KAFKA_TOPIC,
            value=json.dumps(payload).encode('utf-8')
        )
        logger.info(f"✅ Kafka SUCCESS | meter_id={payload['data']['meter_id']} | hash={payload['message_hash'][:8]}")
    except Exception as e:
        logger.error(f"❌ Kafka FAILED | {e}")
        raise

# Validate and enrich message
def enrich_and_validate(raw_payload: str) -> dict:
    try:
        data = json.loads(raw_payload)  # Parse JSON
        required = ["meter_id", "substation_id", "timestamp", "voltage", "current", "energy_kwh"]
        if not all(k in data for k in required):
            raise ValueError("Missing fields")
        
        # Add metadata
        message_hash = hashlib.md5(raw_payload.encode()).hexdigest()
        enriched = {
            "ingest_time": time.time(),
            "data": data,
            "message_hash": message_hash,
            "source": "mqtt_simulator"
        }
        return enriched
    except json.JSONDecodeError:
        logger.warning(f"⚠️ Invalid JSON skipped: {raw_payload[:60]}...")
        return None
    except Exception as e:
        logger.warning(f"⚠️ Validation failed: {e} | payload: {raw_payload[:60]}...")
        return None

# MQTT Callbacks
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logger.info("✅ MQTT Connected & Subscribed to meter/data")
        client.subscribe(MQTT_TOPIC)
    else:
        logger.error(f"❌ MQTT Connect failed: {rc}")

def on_message(client, userdata, msg):
    payload = msg.payload.decode('utf-8')
    logger.info(f"📥 MQTT Received | {payload[:70]}...")
    enriched = enrich_and_validate(payload)
    if enriched:
        loop = userdata['loop']   # ✅ use the captured loop
        asyncio.run_coroutine_threadsafe(
            userdata['send_task'](enriched, userdata['producer']),
            loop
        )

# Main runner
async def run_bridge():
    producer = await create_producer()
    loop = asyncio.get_running_loop()
    
    client = mqtt.Client(userdata={
        'producer': producer,
        'send_task': send_to_kafka,
        'loop': loop    
    })
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    
    def start_mqtt():
        client.loop_forever()
    
    await asyncio.to_thread(start_mqtt)

if __name__ == "__main__":
    try:
        asyncio.run(run_bridge())
    except KeyboardInterrupt:
        logger.info("🛑 Bridge stopped by user")