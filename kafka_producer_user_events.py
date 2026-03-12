from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=["127.0.0.1:29092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    api_version=(0,10,1),
    request_timeout_ms=20000,
    retries=5
)

usernames = ["alice", "bob", "charlie", "david"]

actions = ["view", "add_to_cart", "purchase"]

products = [
    {
        "product_id": "P1001",
        "product_name": "Hydrating Face Cream",
        "product_category": "Skincare",
        "product_subcategory1": "Moisturizers",
        "product_subcategory2": "Face Cream",
        "price": 28.99
    },
    {
        "product_id": "P1002",
        "product_name": "Matte Lipstick",
        "product_category": "Makeup",
        "product_subcategory1": "Lips",
        "product_subcategory2": "Lipstick",
        "price": 19.50
    },
    {
        "product_id": "P1003",
        "product_name": "Vitamin C Serum",
        "product_category": "Skincare",
        "product_subcategory1": "Serums",
        "product_subcategory2": "Vitamin C",
        "price": 35.75
    }
]

addresses = [
    {"street": "101 Market St", "city": "San Francisco", "state": "CA"},
    {"street": "22 Broadway", "city": "New York", "state": "NY"},
    {"street": "77 Michigan Ave", "city": "Chicago", "state": "IL"}
]

print("Producing beauty product events...")

while True:

    product = random.choice(products)
    address = random.choice(addresses)

    quantity = random.randint(1, 3)

    event = {
        "username": random.choice(usernames),
        "action": random.choice(actions),
        "timestamp": time.time(),
        "event_date": datetime.now().strftime("%Y%m%d"),

        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "product_category": product["product_category"],
        "product_subcategory1": product["product_subcategory1"],
        "product_subcategory2": product["product_subcategory2"],

        "price": product["price"],
        "quantity": quantity,
        "total_price": round(product["price"] * quantity, 2),

        "delivery_address": {
            "street": address["street"],
            "city": address["city"],
            "state": address["state"]
        }
    }

    producer.send("beauty_events", event)

    print("sent:", event)

    producer.flush()

    time.sleep(2)