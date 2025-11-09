import asyncio
import json
import hashlib
import logging
import time
from tenacity import retry, stop_after_attempt, wait_fixed
from aiokafka import AIOKafkaProducer
import paho.mqtt.client as mqtt

# Configurations
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "meter/data"
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "iot_meters"
MAX_RETRIES = 5
RETRY_WAIT_TIME = 5

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka Producer
async def create_kafka_producer():
    producer = AIOKafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        acks='all'
    )
    await producer.start()
    return producer

# Retryable Kafka send
@retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_fixed(RETRY_WAIT_TIME))
async def send_to_kafka(data, producer):
    try:
        await producer.send_and_wait(KAFKA_TOPIC, value=json.dumps(data).encode('utf-8'))
        logger.info(f"Successfully sent to Kafka: {data['message_hash']}")
    except Exception as e:
        logger.error(f"Kafka send failed for message {data['message_hash']}: {e}")
        raise

# Process incoming MQTT message
async def process_message(payload: str, producer: AIOKafkaProducer):
    try:
        message_hash = hashlib.md5(payload.encode('utf-8')).hexdigest()
        data = {
            "timestamp": time.time(),
            "data": payload,
            "message_hash": message_hash
        }
        await send_to_kafka(data, producer)
    except Exception as e:
        logger.error(f"Failed to process message: {e}")

# MQTT Callbacks
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logger.info("Connected to MQTT broker")
        client.subscribe(MQTT_TOPIC)
    else:
        logger.error(f"MQTT connect failed with code {rc}")

def on_message(client, userdata, msg):
    payload = msg.payload.decode('utf-8')
    logger.info(f"Received MQTT message on topic {msg.topic}: {payload}")
    # Schedule coroutine in the event loop from the MQTT thread
    asyncio.run_coroutine_threadsafe(
        userdata['task_queue'].put((payload, userdata['producer'])),
        userdata['loop']
    )

# MQTT + Asyncio Integration
async def run_mqtt_client(producer):
    loop = asyncio.get_running_loop()
    task_queue = asyncio.Queue()
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        userdata={'producer': producer, 'task_queue': task_queue, 'loop': loop}
    )
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    client.loop_start()
    try:
        while True:
            try:
                payload, prod = await task_queue.get()
                await process_message(payload, prod)
            except Exception as e:
                logger.error(f"Error processing message: {e}")
            finally:
                task_queue.task_done()
    except asyncio.CancelledError:
        logger.info("Shutting down MQTT processor...")
        client.loop_stop()
        client.disconnect()
        raise

# Main
async def main():
    producer = await create_kafka_producer()
    try:
        await run_mqtt_client(producer)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await producer.stop()
        logger.info("Kafka producer stopped.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass