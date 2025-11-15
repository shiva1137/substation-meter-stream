import json
import logging
import asyncio
import os
from datetime import datetime
from typing import Dict, Any

from kafka import KafkaConsumer
from kafka.errors import KafkaError
import pymongo
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure, DuplicateKeyError

# ==================== CONFIG ====================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "iot_meters"
KAFKA_GROUP = "mongodb_consumer_group"

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DB = "substation_meters"
MONGODB_COLLECTION = "meter_readings"

# ===============================================

# Setup logging 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

class KafkaMongoDBConsumer:
    """
    Consumes messages from Kafka topic and writes to MongoDB
    Handles batching, error handling, and data validation
    """

    def __init__(self):
        self.kafka_consumer = None
        self.mongo_client = None
        self.db = None
        self.collection = None
        self.batch_size = 100
        self.message_count = 0
        self.error_count = 0

    def connect_mongodb(self):
        """Establish MongoDB connection and create collections"""
        try:
            logger.info(f"🔗 Connecting to MongoDB at {MONGODB_URI}...")
            self.mongo_client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
            
            # Verify connection
            self.mongo_client.admin.command('ping')
            logger.info("✅ MongoDB connection successful")
            
            # Get database and collection
            self.db = self.mongo_client[MONGODB_DB]
            self.collection = self.db[MONGODB_COLLECTION]
            
            # Create indexes for performance
            self._create_indexes()
            
        except ConnectionFailure as e:
            logger.error(f"❌ MongoDB connection failed: {e}")
            raise

    def _create_indexes(self):
        """Create MongoDB indexes for query optimization"""
        try:
            logger.info("📑 Creating MongoDB indexes...")
            
            # Composite index on meter_id and timestamp for time-range queries
            self.collection.create_index([
                ("data.meter_id", ASCENDING),
                ("ingest_time", DESCENDING)
            ], name="meter_timestamp_idx", background=True)
            
            # Index on substation_id for aggregations
            self.collection.create_index(
                [("data.substation_id", ASCENDING)],
                name="substation_idx",
                background=True
            )
            
            # TTL index: auto-delete records older than 90 days
            self.collection.create_index(
                [("ingest_time", ASCENDING)],
                expireAfterSeconds=7776000,  # 90 days
                name="ttl_idx"
            )
            
            logger.info("✅ Indexes created successfully")
            
        except Exception as e:
            logger.warning(f"⚠️  Index creation warning: {e}")

    def connect_kafka(self):
        """Establish Kafka consumer connection"""
        try:
            logger.info(f"🔗 Connecting to Kafka at {KAFKA_BROKER}...")
            
            self.kafka_consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                group_id=KAFKA_GROUP,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',  # Read from beginning if group is new
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,
                session_timeout_ms=30000,
                max_poll_records=100
            )
            
            logger.info(f"✅ Kafka consumer connected to topic '{KAFKA_TOPIC}'")
            logger.info(f"📊 Consumer group: {KAFKA_GROUP}")
            
        except Exception as e:
            logger.error(f"❌ Kafka connection failed: {e}")
            raise

    def validate_message(self, message: Dict[str, Any]) -> bool:
        """
        Validate message structure before inserting to MongoDB
        Returns: True if valid, False otherwise
        """
        required_keys = ['data', 'ingest_time', 'message_hash']
        required_data_keys = ['meter_id', 'voltage', 'current', 'energy_kwh']
        
        # Check top-level keys
        if not all(key in message for key in required_keys):
            logger.warning(f"⚠️  Invalid message structure: missing required keys")
            return False
        
        # Check nested data keys
        if 'data' in message:
            if not all(key in message['data'] for key in required_data_keys):
                logger.warning(f"⚠️  Invalid data structure: missing meter data fields")
                return False
        
        return True

    def write_to_mongodb(self, messages: list) -> int:
        """
        Batch write messages to MongoDB
        Returns: Number of successfully inserted records
        """
        if not messages:
            return 0
        
        inserted_count = 0
        
        try:
            # Insert multiple documents (unordered to skip duplicates)
            result = self.collection.insert_many(messages, ordered=False)
            inserted_count = len(result.inserted_ids)
            
            logger.info(f"✅ Inserted {inserted_count} records into MongoDB")
            
        except DuplicateKeyError:
            # Some records might be duplicates - this is ok
            logger.warning(f"⚠️  Some duplicate records skipped (already in DB)")
            
        except Exception as e:
            logger.error(f"❌ MongoDB insert error: {e}")
            self.error_count += len(messages)
            return 0
        
        return inserted_count

    def consume_messages(self):
        """
        Main consumer loop: read from Kafka, validate, and write to MongoDB
        """
        batch = []
        logger.info("🚀 Starting Kafka → MongoDB consumer...")
        logger.info(f"📥 Listening on topic '{KAFKA_TOPIC}'\n")
        
        try:
            for message in self.kafka_consumer:
                try:
                    # Extract message value
                    data = message.value
                    
                    # ✅ CONVERT UNIX TIMESTAMP TO MONGODB DATETIME
                    if 'ingest_time' in data:
                        data['ingest_time'] = datetime.fromtimestamp(data['ingest_time'])
                    
                    # Validate message
                    if not self.validate_message(data):
                        self.error_count += 1
                        continue
                    
                    # Add to batch
                    batch.append(data)
                    self.message_count += 1
                    
                    # Log progress every 10 messages
                    if self.message_count % 10 == 0:
                        logger.info(f"📊 Received {self.message_count} messages | Batch: {len(batch)}")
                    
                    # Write batch to MongoDB when threshold reached
                    if len(batch) >= self.batch_size:
                        inserted = self.write_to_mongodb(batch)
                        batch = []
                        
                        # Show statistics every batch
                        logger.info(f"📈 Total messages: {self.message_count} | "
                                  f"Errors: {self.error_count} | Success rate: "
                                  f"{((self.message_count - self.error_count) / self.message_count * 100):.1f}%\n")
                    
                except Exception as e:
                    logger.error(f"❌ Error processing message: {e}")
                    self.error_count += 1
                    continue
        
        except KeyboardInterrupt:
            logger.info("\n⏸️  Shutting down gracefully...")
            
            # Flush remaining batch
            if batch:
                logger.info(f"🔄 Flushing final batch of {len(batch)} messages...")
                self.write_to_mongodb(batch)
            
            # Print final statistics
            self._print_final_stats()
            
        except Exception as e:
            logger.error(f"❌ Fatal error: {e}")
            self.error_count += 1
        
        finally:
            self.disconnect()

    def _print_final_stats(self):
        """Print final statistics"""
        logger.info("\n" + "="*60)
        logger.info("📊 FINAL STATISTICS")
        logger.info("="*60)
        logger.info(f"Total messages received: {self.message_count}")
        logger.info(f"Successful inserts: {self.message_count - self.error_count}")
        logger.info(f"Failed inserts: {self.error_count}")
        if self.message_count > 0:
            success_rate = ((self.message_count - self.error_count) / self.message_count) * 100
            logger.info(f"Success rate: {success_rate:.1f}%")
        logger.info("="*60 + "\n")

    def disconnect(self):
        """Clean up connections"""
        if self.kafka_consumer:
            logger.info("🔌 Closing Kafka consumer...")
            self.kafka_consumer.close()
        
        if self.mongo_client:
            logger.info("🔌 Closing MongoDB connection...")
            self.mongo_client.close()
        
        logger.info("✅ All connections closed")


def main():
    """Main entry point"""
    consumer = KafkaMongoDBConsumer()
    
    try:
        # Connect to both services
        consumer.connect_mongodb()
        consumer.connect_kafka()
        
        # Start consuming and writing
        consumer.consume_messages()
        
    except Exception as e:
        logger.error(f"❌ Fatal error in main: {e}")
        consumer.disconnect()
        exit(1)


if __name__ == "__main__":
    main()
