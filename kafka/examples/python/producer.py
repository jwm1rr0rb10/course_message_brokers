"""Idempotent producer with keys and headers (module 4.8).

    python producer.py
"""
import json
import os

from confluent_kafka import Producer

BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092,localhost:9192,localhost:9292")
TOPIC = os.environ.get("TOPIC", "shop.orders.events")

producer = Producer({
    "bootstrap.servers": BROKERS,
    "client.id": "order-service",
    "acks": "all",
    "enable.idempotence": True,
    "linger.ms": 10,
    "compression.type": "zstd",
})

failed = []


def on_delivery(err, msg):
    if err is not None:
        failed.append(err)
        print("not written:", err)
    else:
        print(f"{msg.key().decode()} -> partition={msg.partition()} offset={msg.offset()}")


for i in range(1, 4):
    for event in ("OrderCreated", "OrderPaid"):
        producer.produce(
            TOPIC,
            key=f"order-{i}",
            value=json.dumps({"event": event, "order_id": f"order-{i}"}),
            headers={"event-type": event},
            on_delivery=on_delivery,
        )
        producer.poll(0)  # serve delivery callbacks

remaining = producer.flush(15)
if remaining or failed:
    raise SystemExit(f"{remaining} undelivered, {len(failed)} failed")
