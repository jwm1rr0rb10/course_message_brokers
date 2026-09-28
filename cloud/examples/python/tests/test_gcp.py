"""Google Cloud Pub/Sub claims from module 8, checked against the Pub/Sub emulator."""
import os
import time

import pytest
from google.api_core import exceptions as gexc
from google.cloud import pubsub_v1

from conftest import PUBSUB_HOST, require, unique

pytestmark = pytest.mark.gcp
PROJECT = os.environ.get("PUBSUB_PROJECT_ID", "course-project")


@pytest.fixture(scope="module")
def clients():
    require(PUBSUB_HOST, "Pub/Sub")
    os.environ["PUBSUB_EMULATOR_HOST"] = PUBSUB_HOST
    return pubsub_v1.PublisherClient(), pubsub_v1.SubscriberClient()


def topic(pub):
    path = pub.topic_path(PROJECT, unique("orders"))
    pub.create_topic(name=path)
    return path


def subscription(sub, topic_path, **extra):
    path = sub.subscription_path(PROJECT, unique("sub"))
    sub.create_subscription(request={"name": path, "topic": topic_path, "ack_deadline_seconds": 10, **extra})
    return path


def pull(sub, path, want, wait=10, ack=True):
    got, deadline = [], time.time() + wait
    while len(got) < want and time.time() < deadline:
        try:
            resp = sub.pull(subscription=path, max_messages=want - len(got), timeout=5)
        except gexc.DeadlineExceeded:
            continue
        if ack and resp.received_messages:
            sub.acknowledge(subscription=path, ack_ids=[m.ack_id for m in resp.received_messages])
        got.extend(resp.received_messages)
    return got


# 8.1: every subscription of a topic gets its own copy (fan-out)
def test_fanout(clients):
    pub, sub = clients
    t = topic(pub)
    a, b = subscription(sub, t), subscription(sub, t)
    pub.publish(t, b"order-1").result()
    assert [m.message.data for m in pull(sub, a, 1)] == [b"order-1"]
    assert [m.message.data for m in pull(sub, b, 1)] == [b"order-1"]


# 8.1: inside one subscription messages are shared, each delivered once when acked
def test_one_subscription_is_a_queue(clients):
    pub, sub = clients
    t = topic(pub)
    s = subscription(sub, t)
    for i in range(10):
        pub.publish(t, str(i).encode()).result()
    got = pull(sub, s, 10)
    assert sorted(int(m.message.data) for m in got) == list(range(10))
    assert pull(sub, s, 1, wait=3) == [], "acked messages must not come back"


# 8.3: a nack (ack deadline set to 0) makes the message available again
def test_nack_redelivers(clients):
    pub, sub = clients
    t = topic(pub)
    s = subscription(sub, t)
    pub.publish(t, b"job").result()
    first = pull(sub, s, 1, ack=False)
    assert len(first) == 1
    sub.modify_ack_deadline(subscription=s, ack_ids=[first[0].ack_id], ack_deadline_seconds=0)
    again = pull(sub, s, 1)
    assert [m.message.data for m in again] == [b"job"]


# 8.6: messages with one ordering key arrive in publish order
def test_ordering_key(clients):
    _, sub = clients
    pub = pubsub_v1.PublisherClient(
        publisher_options=pubsub_v1.types.PublisherOptions(enable_message_ordering=True))
    t = topic(pub)
    s = subscription(sub, t, enable_message_ordering=True)
    events = [b"created", b"paid", b"shipped", b"delivered"]
    for e in events:
        pub.publish(t, e, ordering_key="order-1")
    pub.stop()   # flush
    got = []
    for m in pull(sub, s, len(events), wait=15):
        got.append(m.message.data)
    assert got == events, got


# 8.8: a subscription filter on attributes
def test_subscription_filter(clients):
    pub, sub = clients
    t = topic(pub)
    try:
        s = subscription(sub, t, filter='attributes.event_type = "OrderCreated"')
    except (gexc.InvalidArgument, gexc.MethodNotImplemented) as e:
        pytest.skip(f"emulator does not support filters: {e}")
    pub.publish(t, b"cancelled", event_type="OrderCancelled").result()
    pub.publish(t, b"created", event_type="OrderCreated").result()
    got = [m.message.data for m in pull(sub, s, 2, wait=6)]
    assert got == [b"created"], got


# 8.5: after max_delivery_attempts the message goes to the dead letter topic
def test_dead_letter_topic(clients):
    pub, sub = clients
    t, dlq_topic = topic(pub), topic(pub)
    dlq_sub = subscription(sub, dlq_topic)
    try:
        s = subscription(sub, t, dead_letter_policy={"dead_letter_topic": dlq_topic, "max_delivery_attempts": 5})
    except (gexc.InvalidArgument, gexc.MethodNotImplemented) as e:
        pytest.skip(f"emulator does not support dead letter policies: {e}")
    pub.publish(t, b"poison").result()
    attempts = 0
    for _ in range(10):
        got = pull(sub, s, 1, wait=3, ack=False)
        if not got:
            break
        attempts += 1
        sub.modify_ack_deadline(subscription=s, ack_ids=[got[0].ack_id], ack_deadline_seconds=0)
    dead = pull(sub, dlq_sub, 1, wait=10)
    if not dead and attempts >= 10:
        pytest.skip("emulator accepted the policy but does not dead-letter messages")
    assert attempts == 5, f"delivered {attempts} times with max_delivery_attempts 5"
    assert [m.message.data for m in dead] == [b"poison"]
