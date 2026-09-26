"""Consumer with manual ack, prefetch and DLX for invalid messages (module 6.5).

    python consumer.py
"""
import json
import os

import pika

URL = os.environ.get("RABBITMQ_URL", "amqp://admin:admin@localhost:5672/%2F")
IDLE_SECONDS = float(os.environ.get("IDLE_SECONDS", "0"))  # 0 = run forever

conn = pika.BlockingConnection(pika.URLParameters(URL))
ch = conn.channel()
# topology from modules 3.4 and 8.2
ch.exchange_declare("shop.events", exchange_type="topic", durable=True)
ch.exchange_declare("shop.dlx", exchange_type="topic", durable=True)
ch.queue_declare("shop.dead", durable=True, arguments={"x-queue-type": "quorum"})
ch.queue_bind("shop.dead", "shop.dlx", "#")
ch.queue_declare("payments", durable=True, arguments={
    "x-queue-type": "quorum", "x-dead-letter-exchange": "shop.dlx", "x-delivery-limit": 5})
ch.queue_bind("payments", "shop.events", "order.created")
ch.basic_qos(prefetch_count=20)

for method, props, body in ch.consume("payments", inactivity_timeout=IDLE_SECONDS or None):
    if method is None:          # idle for IDLE_SECONDS
        break
    try:
        event = json.loads(body)
        print("charged", event["order_id"], "redelivered:", method.redelivered)
        ch.basic_ack(method.delivery_tag)                      # after processing
    except (json.JSONDecodeError, KeyError):
        print("invalid message -> DLX")
        ch.basic_reject(method.delivery_tag, requeue=False)
ch.cancel()
conn.close()
