"""Checks used by CI: the Python snippets from the course behave as described.

    python check.py
"""
import asyncio
import json
import os
import uuid

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


async def main() -> None:
    nc = await nats.connect(servers=SERVERS, name="course-check-py")
    js = nc.jetstream()
    suffix = uuid.uuid4().hex[:8].upper()
    stream, subject = f"T_PY_{suffix}", f"tpy.{suffix}"

    # 4.3: pub/sub with headers
    got = asyncio.get_running_loop().create_future()

    async def handler(msg):
        if not got.done():
            got.set_result(msg)

    await nc.subscribe(f"{subject}.core.>", cb=handler)
    await nc.publish(f"{subject}.core.created", b"{}", headers={"Event-Type": "OrderCreated"})
    msg = await asyncio.wait_for(got, 5)
    assert msg.headers["Event-Type"] == "OrderCreated", msg.headers
    print("ok  core pub/sub with headers")

    # 4.8: no responders
    try:
        await nc.request(f"{subject}.nobody", b"hi", timeout=2)
        raise AssertionError("request without responders succeeded")
    except nats.errors.NoRespondersError:
        print("ok  no responders")

    # 5.6: deduplication
    await js.add_stream(StreamConfig(name=stream, subjects=[f"{subject}.js.>"],
                                     num_replicas=REPLICAS, duplicate_window=120))
    try:
        acks = [await publish_with_retry(js, f"{subject}.js.created", b'{"order_id":"a"}',
                                 headers={"Nats-Msg-Id": "evt-1"}) for _ in range(3)]
        dups = [bool(a.duplicate) for a in acks]  # nats-py reports None instead of False
        assert dups == [False, True, True], dups
        info = await js.stream_info(stream)
        assert info.state.messages == 1, info.state.messages
        print("ok  deduplication")

        # 6.6: pull consumer, Term on invalid data, fetch timeout exception type
        await publish_with_retry(js, f"{subject}.js.created", b"{not json")
        sub = await js.pull_subscribe(f"{subject}.js.created", durable="W", stream=stream,
                                      config=ConsumerConfig(ack_policy=AckPolicy.EXPLICIT, max_deliver=5))
        outcomes = []
        for m in await sub.fetch(batch=10, timeout=5):
            try:
                json.loads(m.data)
                await m.ack()
                outcomes.append("ack")
            except ValueError:
                await m.term()
                outcomes.append("term")
        assert outcomes == ["ack", "term"], outcomes
        print("ok  pull consumer ack/term")

        try:
            await sub.fetch(batch=10, timeout=1)
            raise AssertionError("fetch on an empty consumer returned messages")
        except asyncio.TimeoutError:
            # nats-py raises either asyncio.TimeoutError or its subclass
            # nats.errors.TimeoutError depending on the code path, so catch the base
            print("ok  empty fetch raises asyncio.TimeoutError")
    finally:
        await js.delete_stream(stream)
        await nc.close()

    print("All Python checks passed.")


if __name__ == "__main__":
    asyncio.run(main())
