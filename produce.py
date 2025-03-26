# Creates the json data to push to kafka
import json
import os
import random
from datetime import datetime
from typing import Dict, Any, List
from faker import Faker
from confluent_kafka import Producer
from pydantic import BaseModel, Field
import logging
from flask import Flask, jsonify

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
app = Flask(__name__)

# Configuration
BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'broker:9092')
TOPIC_NAME = 'locations'
NUM_MESSAGES = 10
BATCH_SIZE = 10

# Data model for location events
class LocationEvent(BaseModel):
    id: str
    timestamp: str
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)
    elevation: float
    speed: float = Field(..., ge=0)
    direction: float = Field(..., ge=0, lt=360)
    device_id: str
    vehicle_type: str

# Initialize Faker for generating realistic data
fake = Faker()

def delivery_report(err, msg):
    """Callback for message delivery reports."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def create_producer() -> Producer:
    """Create and return a Kafka producer instance."""
    conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'client.id': 'location-producer'
    }
    return Producer(conf)

def generate_location_event() -> Dict[str, Any]:
    """Generate a fake location event."""
    vehicle_types = ["car", "truck", "motorcycle", "bicycle", "bus"]
    
    event = LocationEvent(
        id=fake.uuid4(),
        timestamp=datetime.now().isoformat(),
        latitude=fake.latitude(),
        longitude=fake.longitude(),
        elevation=random.uniform(0, 2000),
        speed=random.uniform(0, 120),
        direction=random.uniform(0, 359.99),
        device_id=f"device-{fake.uuid4()[:8]}",
        vehicle_type=random.choice(vehicle_types)
    )
    
    return event.model_dump()

def generate_batch(num_records: int = 10) -> List[Dict[str, Any]]:
    """Generate a batch of location events."""
    return [generate_location_event() for _ in range(num_records)]

def main():
    """Main function to produce messages to Kafka."""
    logger.info("Creating Kafka topic if it doesn't exist")
    create_topic()
    logger.info("Creating Kafka producer")
    producer = create_producer()
    
    try:
        logger.info(f"Starting to produce {NUM_MESSAGES} messages to topic '{TOPIC_NAME}'")
        
        for i in range(NUM_MESSAGES):
            # Generate event data
            event = generate_location_event()
            
            # Convert to JSON string
            event_json = json.dumps(event)
            
            # Produce message
            producer.produce(
                TOPIC_NAME,
                key=event['id'],
                value=event_json,
                callback=delivery_report
            )
            
            # Trigger any available delivery callbacks
            producer.poll(0)
            
            logger.debug(f"Produced message {i+1}/{NUM_MESSAGES}")
            
            # Every BATCH_SIZE messages, print batch completion
            if (i + 1) % BATCH_SIZE == 0:
                logger.info(f"Completed batch {(i+1)//BATCH_SIZE}/{NUM_MESSAGES//BATCH_SIZE}")
                
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user")
    except Exception as e:
        logger.error(f"Error producing messages: {str(e)}")
    finally:
        # Wait for any outstanding messages to be delivered
        logger.info("Flushing producer...")
        producer.flush()
        logger.info("Producer stopped")

def create_topic():
    """Create the topic if it doesn't exist."""
    try:
        from confluent_kafka.admin import AdminClient, NewTopic
        admin = AdminClient({'bootstrap.servers': BOOTSTRAP_SERVERS})
        topics = admin.list_topics().topics
        
        if TOPIC_NAME not in topics:
            print(f"Creating topic '{TOPIC_NAME}'")
            new_topic = NewTopic(
                TOPIC_NAME,
                num_partitions=3,
                replication_factor=1
            )
            admin.create_topics([new_topic])
            print(f"Topic '{TOPIC_NAME}' created")
        else:
            print(f"Topic '{TOPIC_NAME}' already exists")
    except Exception as e:
        print(f"Error creating topic: {e}")
        

# Server 
@app.route('/', methods=['POST'])
def produce():
    """Endpoint to trigger data production to Kafka"""
    try:
        # Run producer in a thread so it doesn't block
        # main()
        logger.info("Received request to produce messages")
        main()  
        return jsonify({"status": "success", "message": "Producer completed"}), 200
    except Exception as e:
        logger.error(f"Error in producer: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    # Create the topic if it doesn't exist
    app.run(host='0.0.0.0', port=5000)
    