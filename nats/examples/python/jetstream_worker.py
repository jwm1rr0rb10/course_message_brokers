"""JetStream publish with deduplication (module 5.6) and a pull consumer
with Ack / Nak / Term (module 6.6).

    python jetstream_worker.py
"""
import asyncio
import json
import os

import nats
import nats.errors
import nats.js.errors
from nats.js.api import AckPolicy, ConsumerConfig, StreamConfig

SERVERS = os.environ.get(
    "NATS_URL", "nats://localhost:4222,nats://localhost:4223,nats://localhost:4224"
).split(",")
REPLICAS = int(os.environ.get("NATS_REPLICAS", "3"))


async def publish_with_retry(js, subject, data, headers=None, attempts=5):
    """Module 5.7: on "no response from stream" (e.g. a freshly created R3
    stream still electing its leader) retry with backoff. The Nats-Msg-Id
    header makes the retries safe: they cannot create duplicates."""
    for attempt in range(attempts):
        try:
            return await js.publish(subject, data, headers=headers)
        except (nats.js.errors.NoStreamResponseError, nats.errors.TimeoutError):
            if attempt == attempts - 1:
                raise
            await asyncio.sleep(0.2 * 2 ** attempt)


class TemporaryError(Exception):
    pass


async def process(data: bytes) -> None:
    order = json.loads(data)  # raises on invalid JSON -> Term
    print("processed", order["order_id"])


async def main() -> None:
    nc = await nats.connect(servers=SERVERS, name="billing-worker-py")
    js = nc.jetstream()

    await js.add_stream(
        StreamConfig(
            name="ORDERS_PY",
            subjects=["py.orders.>"],
            num_replicas=REPLICAS,
            duplicate_window=120,  # seconds
        )
    )

    for i in range(1, 4):
        ack = await publish_with_retry(
        js,
            "py.orders.created",
            json.dumps({"order_id": f"order-{i}"}).encode(),
            headers={"Nats-Msg-Id": f"order-{i}-created"},
        )
        print("published", ack.stream, ack.seq, "duplicate" if ack.duplicate else "")

    sub = await js.pull_subscribe(
        "py.orders.created",
        durable="BILLING_PY",
        stream="ORDERS_PY",
        config=ConsumerConfig(ack_policy=AckPolicy.EXPLICIT, max_deliver=5, ack_wait=30),
    )

    try:
        msgs = await sub.fetch(batch=50, timeout=5)
    except asyncio.TimeoutError:  # no messages; also catches nats.errors.TimeoutError (a subclass)
        msgs = []

    for msg in msgs:
        try:
            await process(msg.data)
            await msg.ack()
        except TemporaryError:
            await msg.nak(delay=5)
        except Exception:
            await msg.term()

    await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())
