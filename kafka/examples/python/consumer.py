"""Consumer with manual commit after processing (module 5.6) and a DLQ for
messages that cannot be parsed (module 13.4), like the Go consumer.

    python consumer.py
"""
import json
import os
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaException, Producer

BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092,localhost:9192,localhost:9292")
TOPIC = os.environ.get("TOPIC", "shop.orders.events")
GROUP = os.environ.get("GROUP", "billing-py")
IDLE_SECONDS = float(os.environ.get("IDLE_SECONDS", "0"))  # 0 = run forever

consumer = Consumer({
    "bootstrap.servers": BROKERS,
    "group.id": GROUP,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})
dlq_producer = Producer({"bootstrap.servers": BROKERS, "acks": "all", "enable.idempotence": True})


def send_to_dlq(msg, error, attempts=1):
    """Write the original key and value unchanged, plus context headers, and
    wait for the broker to confirm. Raises if the DLQ write fails: then the
    offset must NOT be committed, or the message is lost."""
    headers = list(msg.headers() or [])  # keep the original headers
    headers += [
        ("dlq-original-topic", msg.topic()),
        ("dlq-original-partition", str(msg.partition())),
        ("dlq-original-offset", str(msg.offset())),
        ("dlq-error-class", type(error).__name__),
        ("dlq-error-message", str(error)),
        ("dlq-attempts", str(attempts)),
        ("dlq-consumer-group", GROUP),
        ("dlq-failed-at", datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")),
    ]
    result = []
    dlq_producer.produce(msg.topic() + ".dlq", key=msg.key(), value=msg.value(), headers=headers,
                         on_delivery=lambda err, _msg: result.append(err))
    dlq_producer.flush(30)
    if not result or result[0] is not None:
        raise KafkaException(result[0] if result else "DLQ write timed out")


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
        try:
            event = json.loads(msg.value())        # processing must be idempotent
            print(f"p={msg.partition()} off={msg.offset()} {event['order_id']} {event['event']}")
        except (ValueError, KeyError, TypeError) as e:  # permanent error: no retries
            send_to_dlq(msg, e)                    # DLQ first; if it fails we exit uncommitted
            print(f"p={msg.partition()} off={msg.offset()} moved to DLQ: {e!r}")
        consumer.commit(message=msg, asynchronous=False)  # commit AFTER processing / DLQ
finally:
    consumer.close()  # leave the group right away, not after the session timeout
    dlq_producer.flush(10)
