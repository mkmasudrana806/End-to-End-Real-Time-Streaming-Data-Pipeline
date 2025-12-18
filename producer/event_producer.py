from kafka import KafkaProducer
from datetime import datetime, timezone
from kafka.errors import NoBrokersAvailable
import json
import random
import uuid
import time



KAFKA_BROKER = "broker:9092"
TOPIC_NAME = "user_events"

event_types = ["add_to_cart", "purchase", "product_view"]
product_category = ["books", "electronics", "fashion", "technology"]

def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                retries=5,
                acks="all"
            )
            print("Kafka producer connected successfully")
            return producer
        except NoBrokersAvailable:
            print("Kafka not ready yet. Retrying in 5 seconds...")
            time.sleep(5)

def generate_event():
    event_type = random.choice(event_types)
    category = random.choice(product_category)
    price = round(random.uniform(10, 10000), 2)
    
    message = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "user_id": f"user_{random.randint(1, 1000)}",
        "product_id": f"product_{random.randint(1, 500)}",
        "category": category,
        "price": price,
        "event_time": datetime.now(timezone.utc).isoformat()
    }
    return message

producer = create_producer()

while True:
    message = generate_event()
    producer.send(topic=TOPIC_NAME, value=message, key=message["product_id"])
    
    print(f"Sent event: ", message)
    time.sleep(1)


