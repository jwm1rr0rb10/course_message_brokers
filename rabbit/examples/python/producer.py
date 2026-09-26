"""Publisher with confirms and mandatory (module 5.5).

    python producer.py
"""
import json
import os
import uuid

import pika
from pika.exceptions import NackError, UnroutableError

URL = os.environ.get("RABBITMQ_URL", "amqp://admin:admin@localhost:5672/%2F")

conn = pika.BlockingConnection(pika.URLParameters(URL))
ch = conn.channel()
ch.exchange_declare("shop.events", exchange_type="topic", durable=True)
ch.confirm_delivery()  # every basic_publish waits for the broker's ack

failed = 0
for i in range(1, 4):
    try:
        ch.basic_publish(
            exchange="shop.events",
            routing_key="order.created",
            body=json.dumps({"order_id": f"order-{i}", "amount": 1000 * i}),
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json",
                message_id=str(uuid.uuid4()),
                headers={"event-type": "OrderCreated"},
            ),
            mandatory=True,
        )
        print(f"order-{i}: confirmed")
    except UnroutableError:
        failed += 1
        print(f"order-{i}: no queue is bound for order.created (start consumer.py first)")
    except NackError:
        failed += 1
        print(f"order-{i}: nacked by the broker")
conn.close()
raise SystemExit(1 if failed else 0)
