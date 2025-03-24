# Creates the json data to push to kafka
import json
import os
import time
import random
from datetime import datetime
from typing import Dict, Any, List
from faker import Faker
from confluent_kafka import Producer
from pydantic import BaseModel, Field

# Configuration
BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'broker:9092')
TOPIC_NAME = 'locations'
NUM_MESSAGES = 10
SLEEP_TIME = 1  # seconds between batches

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
    producer = create_producer()
    
    try:
        print(f"Starting to produce {NUM_MESSAGES} messages to topic '{TOPIC_NAME}'")
        
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
            
            print(f"Produced message {i+1}/{NUM_MESSAGES}")
            
            # Sleep between messages
            time.sleep(0.1)
            
            # Every 10 messages, sleep a bit longer to simulate batches
            if (i + 1) % 10 == 0:
                print(f"Completed batch {(i+1)//10}/{NUM_MESSAGES//10}")
                time.sleep(SLEEP_TIME)
                
    except KeyboardInterrupt:
        print("Producer interrupted by user")
    finally:
        # Wait for any outstanding messages to be delivered
        print("Flushing producer...")
        producer.flush()
        print("Producer stopped")

if __name__ == "__main__":
    # Wait for Kafka to be ready
    time.sleep(5)
    
    # Create the topic if it doesn't exist
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
    
    # Start producing
    main()