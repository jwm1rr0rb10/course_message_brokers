"""Core NATS pub/sub with headers (module 4.3).

    python pubsub.py
"""
import asyncio
import json
import os

import nats

SERVERS = os.environ.get(
    "NATS_URL", "nats://localhost:4222,nats://localhost:4223,nats://localhost:4224"
).split(",")


async def main() -> None:
    nc = await nats.connect(
        servers=SERVERS,
        name="order-service",
        max_reconnect_attempts=-1,
        reconnect_time_wait=0.5,
    )

    async def handler(msg):
        print(msg.subject, msg.headers, msg.data.decode())

    await nc.subscribe("shop.orders.>", cb=handler)

    await nc.publish(
        "shop.orders.created",
        json.dumps({"order_id": "order-1", "amount": 4990}).encode(),
        headers={"Event-Type": "OrderCreated"},
    )
    await nc.flush()
    await asyncio.sleep(1)
    await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())
