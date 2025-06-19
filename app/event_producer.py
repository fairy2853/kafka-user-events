import json
import time
import random
from faker import Faker
from confluent_kafka import Producer

fake = Faker()

p = Producer({"bootstrap.servers": "kafka:9092"})

events = ["page_view", "click", "scroll", "login", "logout", "signup"]


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}]")


while True:
    event = {
        "user_id": fake.uuid4(),
        "event_type": random.choice(events),
        "timestamp": int(time.time()),
        "ip": fake.ipv4_public(),
        "user_agent": fake.user_agent(),
        "page": fake.uri_path(),
        "region": fake.country_code(),
    }

    p.produce(
        "user_events",
        key=event["user_id"],
        value=json.dumps(event),
        callback=delivery_report,
    )
    p.poll(0)

    time.sleep(random.uniform(0.1, 1.0))  # від 0.1 до 1 сек на подію
