from kafka import KafkaProducer
from datetime import datetime, timezone
import json
import random
import uuid
import time



kafa_broker = "broker:9092"
topic_name = "user_events"

event_types = ["add_to_cart", "purchase", "product_view"]
product_category = ["books", "electronics", "fasion", "technology"]

# initialize kafka producer
producer = KafkaProducer( bootstrap_servers=kafa_broker, value_serializer=lambda v: json.dumps(v).encode("utf-8"), key_serializer=lambda k: k.encode("utf-8"), retries=5, acks="all" )


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



while True:
    message = generate_event()
    producer.send(topic=topic_name, value=message, key=message["product_id"])
    
    print(f"Sent event: ", message)
    time.sleep(1)