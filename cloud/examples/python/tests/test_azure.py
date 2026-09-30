"""Azure Service Bus claims from module 6, checked against the Service Bus emulator.
Entities come from examples/emulators/servicebus-config.json."""
import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from azure.servicebus import (ServiceBusClient, ServiceBusMessage, ServiceBusReceiveMode,
                              ServiceBusSubQueue)

from conftest import SERVICEBUS_CONN, require

pytestmark = pytest.mark.azure


@pytest.fixture(scope="module")
def client():
    require("localhost:5672", "Azure Service Bus")
    with ServiceBusClient.from_connection_string(SERVICEBUS_CONN) as c:
        yield c


def props(msg):
    # application_properties keys may come back as bytes
    return {(k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
            for k, v in (msg.application_properties or {}).items()}


def drain(client, **target):
    with _receiver(client, receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE, max_wait_time=1, **target) as r:
        while r.receive_messages(max_message_count=50, max_wait_time=1):
            pass


def _receiver(client, topic=None, subscription=None, queue=None, **kw):
    if topic:
        return client.get_subscription_receiver(topic, subscription, **kw)
    return client.get_queue_receiver(queue, **kw)


def find(receiver, marker, wait=10):
    """Receive until the message with this marker shows up; other messages are completed."""
    deadline = time.time() + wait
    while time.time() < deadline:
        for m in receiver.receive_messages(max_message_count=10, max_wait_time=2):
            if props(m).get("marker") == marker:
                return m
            receiver.complete_message(m)
    return None


def send(client, queue, marker, **kw):
    with client.get_queue_sender(queue) as s:
        s.send_messages(ServiceBusMessage("job", application_properties={"marker": marker}, **kw))


# 6.3: complete removes the message
def test_complete(client):
    marker = uuid.uuid4().hex
    send(client, "tasks", marker)
    with client.get_queue_receiver("tasks") as r:
        m = find(r, marker)
        assert m is not None
        r.complete_message(m)
        assert find(r, marker, wait=3) is None


# 6.3: abandon returns the message and increments delivery_count
def test_abandon_increments_delivery_count(client):
    marker = uuid.uuid4().hex
    send(client, "tasks", marker)
    with client.get_queue_receiver("tasks") as r:
        first = find(r, marker)
        assert first is not None
        r.abandon_message(first)
        again = find(r, marker)
        assert again is not None
        assert again.delivery_count == first.delivery_count + 1
        r.complete_message(again)


# 6.3 / 6.4: explicit dead-lettering keeps the reason and description
def test_explicit_dead_letter(client):
    drain(client, queue="tasks", sub_queue=ServiceBusSubQueue.DEAD_LETTER)
    marker = uuid.uuid4().hex
    send(client, "tasks", marker)
    with client.get_queue_receiver("tasks") as r:
        m = find(r, marker)
        assert m is not None
        r.dead_letter_message(m, reason="InvalidPayload", error_description="bad json")
    with client.get_queue_receiver("tasks", sub_queue=ServiceBusSubQueue.DEAD_LETTER) as dlq:
        m = find(dlq, marker)
        assert m is not None
        assert m.dead_letter_reason == "InvalidPayload"
        assert m.dead_letter_error_description == "bad json"
        dlq.complete_message(m)


# 6.3: after MaxDeliveryCount (3 in the config) the message goes to $DeadLetterQueue by itself
def test_max_delivery_count_dead_letters(client):
    drain(client, queue="tasks", sub_queue=ServiceBusSubQueue.DEAD_LETTER)
    marker = uuid.uuid4().hex
    send(client, "tasks", marker)
    deliveries = 0
    with client.get_queue_receiver("tasks") as r:
        while True:
            m = find(r, marker, wait=5)
            if m is None:
                break
            deliveries += 1
            r.abandon_message(m)
    assert deliveries == 3, f"delivered {deliveries} times with MaxDeliveryCount 3"
    with client.get_queue_receiver("tasks", sub_queue=ServiceBusSubQueue.DEAD_LETTER) as dlq:
        m = find(dlq, marker)
        assert m is not None
        assert m.dead_letter_reason == "MaxDeliveryCountExceeded"
        dlq.complete_message(m)


# 6.5: messages of one session arrive in order
def test_session_order(client):
    session = f"order-{uuid.uuid4().hex[:8]}"
    with client.get_queue_sender("orders-ordered") as s:
        for event in ["OrderCreated", "OrderPaid", "OrderShipped"]:
            s.send_messages(ServiceBusMessage(event, session_id=session))
    got = []
    with client.get_queue_receiver("orders-ordered", session_id=session, max_wait_time=5) as r:
        for m in r:
            got.append(str(m))
            r.complete_message(m)
            if len(got) == 3:
                break
    assert got == ["OrderCreated", "OrderPaid", "OrderShipped"]


# 6.6: duplicate detection drops a resend with the same message_id
def test_duplicate_detection(client):
    drain(client, queue="dedup")
    marker = uuid.uuid4().hex
    for _ in range(2):
        send(client, "dedup", marker, message_id=f"evt-{marker}")
    with client.get_queue_receiver("dedup") as r:
        assert find(r, marker) is not None
        assert find(r, marker, wait=3) is None, "the duplicate must be dropped"


# 6.6: a scheduled message is not delivered before its time
def test_scheduled_message(client):
    marker = uuid.uuid4().hex
    with client.get_queue_sender("tasks") as s:
        s.schedule_messages(ServiceBusMessage("later", application_properties={"marker": marker}),
                            datetime.now(timezone.utc) + timedelta(seconds=5))
    with client.get_queue_receiver("tasks") as r:
        assert find(r, marker, wait=2) is None
        m = find(r, marker, wait=15)
        assert m is not None
        r.complete_message(m)


# 6.7: a SQL filter on a subscription; the default rule passes everything
def test_topic_subscription_filter(client):
    drain(client, topic="orders", subscription="billing")
    drain(client, topic="orders", subscription="audit")
    marker = uuid.uuid4().hex
    with client.get_topic_sender("orders") as s:
        for event in ["OrderCreated", "OrderCancelled"]:
            s.send_messages(ServiceBusMessage(event, application_properties={"EventType": event, "marker": marker}))

    def collect(sub):
        got = []
        with client.get_subscription_receiver("orders", sub, max_wait_time=5) as r:
            for m in r.receive_messages(max_message_count=10, max_wait_time=5):
                if props(m).get("marker") == marker:
                    got.append(str(m))
                r.complete_message(m)
        return sorted(got)

    assert collect("billing") == ["OrderCreated"]
    assert collect("audit") == ["OrderCancelled", "OrderCreated"]
