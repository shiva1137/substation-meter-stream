from typing import Any


import json
from pymongo import MongoClient, ASCENDING, DESCENDING
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

MONGODB_URI = "mongodb://localhost:27017"
MONGODB_DB = "substation_meters"


def setup_mongodb():
    """Initialize MongoDB with collections and indexes"""
    
    try:
        # Connect to MongoDB
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        logger.info("✅ Connected to MongoDB")
        
        db = client[MONGODB_DB]
        collection = db["meter_readings"]
        
        # Drop existing collection (for fresh start in development)
        logger.info("🗑️  Dropping existing collection...")
        collection.drop()
        
        # Create collection
        logger.info("📝 Creating 'meter_readings' collection...")
        db.create_collection(
            "meter_readings",
            validator={
                "$jsonSchema": {
                    "bsonType": "object",
                    "required": ["data", "ingest_time", "message_hash"],
                    "properties": {
                        "data": {
                            "bsonType": "object",
                            "required": ["meter_id", 'timestamp',"voltage", "current",'energy_kwh'],
                            "properties": {
                                "meter_id": {"bsonType": "string"},
                                "substation_id": {"bsonType": "string"},
                                "timestamp": {"bsonType": "string"},
                                "voltage": {"bsonType": "double"},
                                "current": {"bsonType": "double"},
                                "energy_kwh": {"bsonType": "double"}
                            }
                        },
                        "ingest_time": {"bsonType": "date"},
                        "message_hash": {"bsonType": "string"},
                        "source": {"bsonType": "string"}
                    }
                }
            }
        )
        logger.info("✅ Collection created with schema validation")
        
        # Create indexes
        logger.info("📑 Creating indexes...")
        collection.create_index([
            ("data.meter_id", ASCENDING),
            ("ingest_time", DESCENDING)
        ], name="meter_timestamp_idx", background=True)
        
        collection.create_index(
            [("data.substation_id", ASCENDING)],
            name="substation_idx",
            background=True
        )
        
        collection.create_index(
            [("ingest_time", ASCENDING)],
            name="ttl_idx",
            expireAfterSeconds=7776000  # 90 days TTL
        )
        
        logger.info("✅ Indexes created:")
        logger.info("   - meter_timestamp_idx: For time-range queries")
        logger.info("   - substation_idx: For substation aggregations")
        logger.info("   - ttl_idx: Auto-delete records older than 90 days")
        
        # Show collection info
        logger.info("\n📊 Collection Info:")
        logger.info(f"   Database: {MONGODB_DB}")
        logger.info(f"   Collection: meter_readings")
        logger.info(f"   URI: {MONGODB_URI}")
        
        client.close()
        logger.info("\n✅ MongoDB setup complete!")
        
    except Exception as e:
        logger.error(f"❌ Setup failed: {e}")
        raise


if __name__ == "__main__":
    setup_mongodb()