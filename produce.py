# Creates the json data to push to kafka
import json
import os
import random
from datetime import datetime
from typing import Dict, Any, List
from faker import Faker
from confluent_kafka import Producer
from pydantic import BaseModel, Field, PositiveInt
import logging
from flask import Flask, jsonify, request

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
app = Flask(__name__)
fake = Faker()

# Configuration
BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'broker:9092')
TOPIC_NAME = 'locations'
DEFAULT_NUM_MESSAGES = 10  # Default value if not specified in request

# Data models
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

class ProduceRequest(BaseModel):
    num_messages: PositiveInt = Field(default=DEFAULT_NUM_MESSAGES, description="Number of messages to produce")

# Initialize Faker for generating realistic data

def delivery_report(err, msg):
    """Callback for message delivery reports."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")

        
_producer = None
def get_or_create_producer() -> Producer:
    """Create and return a Kafka producer instance."""
    global _producer
    if _producer is None:
        conf = {
            'bootstrap.servers': BOOTSTRAP_SERVERS,
            'client.id': 'location-producer',
            'queue.buffering.max.messages': 1000000,  # Increase from default 100000
            'queue.buffering.max.kbytes': 2097151,    # Default is 1048576 (1GB), increasing to ~2GB
        }
        _producer = Producer(conf)
    return _producer

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

def create_topic():
    """Create the topic if it doesn't exist."""
    try:
        from confluent_kafka.admin import AdminClient, NewTopic
        admin = AdminClient({'bootstrap.servers': BOOTSTRAP_SERVERS})
        topics = admin.list_topics().topics
        
        if TOPIC_NAME not in topics:
            logger.info(f"Creating topic '{TOPIC_NAME}'")
            new_topic = NewTopic(
                TOPIC_NAME,
                num_partitions=3,
                replication_factor=1
            )
            admin.create_topics([new_topic])
            logger.info(f"Topic '{TOPIC_NAME}' created")
        else:
            logger.info(f"Topic '{TOPIC_NAME}' already exists")
    except Exception as e:
        logger.error(f"Error creating topic: {e}")
        

def main(num_messages=DEFAULT_NUM_MESSAGES):
    """Main function to produce messages to Kafka."""
    create_topic()
    producer = get_or_create_producer()
    
    try:
        start_time = datetime.now()
        logger.info(f"Starting to produce {num_messages} messages to topic '{TOPIC_NAME}'")
        
        for i in range(num_messages):
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
                        
        producer.poll(0)
        
        # Calculate messages per second
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        msg_per_second = num_messages / duration
        
        logger.info(f"Sent {num_messages} messages in {duration:.2f} seconds")
        logger.info(f"Average throughput: {msg_per_second:.2f} messages/second")
                
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user")
    except Exception as e:
        logger.error(f"Error producing messages: {str(e)}")
    finally:
        # Wait for any outstanding messages to be delivered
        logger.info("Flushing producer...")
        producer.flush()
        logger.info("Producer stopped")

# Server 
@app.route('/', methods=['POST'])
def produce():
    """Endpoint to trigger data production to Kafka"""
    try:
        logger.info("Received request to produce messages")
        data = request.get_json() or {}
        request_model = ProduceRequest(**data)
        
        main(request_model.num_messages)
        return jsonify({
            "status": "success", 
            "message": f"Producer completed - sent {request_model.num_messages} messages"
        }), 200
    except Exception as e:
        error_message = str(e)
        status_code = 400 if isinstance(e, (ValueError, TypeError)) else 500
        logger.error(f"Error in producer: {error_message}")
        return jsonify({
            "status": "error", 
            "message": "Validation error" if status_code == 400 else "Server error",
            "details": error_message
        }), status_code

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
    