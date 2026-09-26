"""Consumer with manual commit after processing (module 5.6).

    python consumer.py
"""
import json
import os

from confluent_kafka import Consumer, KafkaException

BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092,localhost:9192,localhost:9292")
TOPIC = os.environ.get("TOPIC", "shop.orders.events")
IDLE_SECONDS = float(os.environ.get("IDLE_SECONDS", "0"))  # 0 = run forever

consumer = Consumer({
    "bootstrap.servers": BROKERS,
    "group.id": os.environ.get("GROUP", "billing-py"),
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})
consumer.subscribe([TOPIC])

idle = 0.0
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            idle += 1
            if IDLE_SECONDS and idle >= IDLE_SECONDS:
                break
            continue
        idle = 0
        if msg.error():
            raise KafkaException(msg.error())
        event = json.loads(msg.value())            # processing must be idempotent
        print(f"p={msg.partition()} off={msg.offset()} {event['order_id']} {event['event']}")
        consumer.commit(message=msg, asynchronous=False)  # commit AFTER processing
finally:
    consumer.close()  # leave the group right away, not after the session timeout
