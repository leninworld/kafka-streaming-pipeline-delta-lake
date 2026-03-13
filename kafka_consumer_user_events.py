import json

from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = ["127.0.0.1:29092"]
KAFKA_TOPIC = "beauty_events"


consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="beauty-events-debug-consumer",
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

print(f"Consuming messages from topic '{KAFKA_TOPIC}'...")

for message in consumer:
    print(message.value)
