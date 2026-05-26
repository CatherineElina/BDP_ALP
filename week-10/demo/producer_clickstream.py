"""Generate simple clickstream events and publish them to Kafka."""

import json
import os
import time
import uuid
from datetime import datetime
from random import choice

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "clickstream-events")
MESSAGE_INTERVAL_SECONDS = float(os.getenv("MESSAGE_INTERVAL_SECONDS", "1"))

PAGES = ["home", "search", "product", "cart", "checkout", "payment"]
EVENT_TYPES = ["page_view", "click", "add_to_cart", "purchase"]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
SOURCES = ["organic", "ads", "social", "email"]


def build_event() -> dict[str, str]:
    user_id = f"user-{choice(range(1, 21)):03d}"

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "session_id": str(uuid.uuid4()),
        "page": choice(PAGES),
        "event_type": choice(EVENT_TYPES),
        "device_type": choice(DEVICE_TYPES),
        "source": choice(SOURCES),
        "event_time": datetime.utcnow().isoformat(timespec="seconds"),
    }


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
        acks="all",
    )

    print(f"Producing clickstream events to topic '{TOPIC}' on {BOOTSTRAP_SERVERS}")

    try:
        while True:
            event = build_event()
            producer.send(TOPIC, key=event["user_id"], value=event)
            producer.flush()
            print(json.dumps(event))
            time.sleep(MESSAGE_INTERVAL_SECONDS)
            print(f"\nWait for {MESSAGE_INTERVAL_SECONDS}s.")
    except KeyboardInterrupt:
        print("\nProducer stopped by user.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
