"""AWS claims from modules 3–5, checked against moto (or LocalStack / real AWS)."""
import json
import os
import time

import boto3
import pytest

from conftest import AWS_ENDPOINT, require, unique

pytestmark = pytest.mark.aws


@pytest.fixture(scope="module")
def session():
    require(AWS_ENDPOINT, "AWS (moto/LocalStack)")
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
    return boto3.session.Session(region_name=os.environ.get("AWS_DEFAULT_REGION", "eu-central-1"))


@pytest.fixture(scope="module")
def sqs(session):
    return session.client("sqs", endpoint_url=AWS_ENDPOINT)


def make_queue(sqs, name, **attrs):
    url = sqs.create_queue(QueueName=name, Attributes={k: str(v) for k, v in attrs.items()})["QueueUrl"]
    arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    return url, arn


def receive(sqs, url, wait=1, **kw):
    return sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=wait,
                               MessageSystemAttributeNames=["ApproximateReceiveCount"], **kw).get("Messages", [])


# 3.1: a received message is invisible until the visibility timeout expires, then it comes back
def test_visibility_timeout_redelivers(sqs):
    url, _ = make_queue(sqs, unique("vis"), VisibilityTimeout=2)
    sqs.send_message(QueueUrl=url, MessageBody="job")
    first = receive(sqs, url)
    assert len(first) == 1
    assert receive(sqs, url, wait=0) == [], "message must be invisible while in flight"
    time.sleep(2.5)
    again = receive(sqs, url)
    assert len(again) == 1
    assert again[0]["Attributes"]["ApproximateReceiveCount"] == "2"
    assert again[0]["ReceiptHandle"] != first[0]["ReceiptHandle"], "each receive has its own receipt handle"
    sqs.delete_message(QueueUrl=url, ReceiptHandle=again[0]["ReceiptHandle"])
    time.sleep(2.5)
    assert receive(sqs, url, wait=0) == [], "deleted message must not return"


# 3.8 / 10.1: ChangeMessageVisibility(0) returns a message immediately
def test_change_visibility_zero_returns_now(sqs):
    url, _ = make_queue(sqs, unique("chg"), VisibilityTimeout=60)
    sqs.send_message(QueueUrl=url, MessageBody="job")
    m = receive(sqs, url)[0]
    sqs.change_message_visibility(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"], VisibilityTimeout=0)
    assert len(receive(sqs, url)) == 1


# 3.9: after maxReceiveCount receives without delete the message moves to the DLQ
def test_redrive_to_dlq(sqs):
    dlq_url, dlq_arn = make_queue(sqs, unique("dlq"), MessageRetentionPeriod=1209600)
    url, _ = make_queue(sqs, unique("work"), VisibilityTimeout=1,
                        RedrivePolicy=json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "2"}))
    sqs.send_message(QueueUrl=url, MessageBody="poison")
    def dlq_depth():
        attrs = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["ApproximateNumberOfMessages"])
        return int(attrs["Attributes"]["ApproximateNumberOfMessages"])

    receives = 0
    for _ in range(10):
        receives += len(receive(sqs, url))   # "fail": never delete
        if dlq_depth():
            break
        time.sleep(1.2)
    dead = receive(sqs, dlq_url)
    assert receives == 2, f"received {receives} times with maxReceiveCount 2"
    assert len(dead) == 1 and dead[0]["Body"] == "poison"


# 3.2 / 3.6: FIFO deduplicates by MessageDeduplicationId and keeps order within a group
def test_fifo_dedup_and_order(sqs):
    url, _ = make_queue(sqs, unique("orders") + ".fifo", FifoQueue="true")
    for body in ["created", "paid", "shipped"]:
        sqs.send_message(QueueUrl=url, MessageBody=body, MessageGroupId="order-1",
                         MessageDeduplicationId=f"order-1-{body}")
    # a retry with the same deduplication id within 5 minutes is dropped
    sqs.send_message(QueueUrl=url, MessageBody="paid", MessageGroupId="order-1",
                     MessageDeduplicationId="order-1-paid")
    bodies = []
    for _ in range(5):
        msgs = receive(sqs, url)
        for m in msgs:
            bodies.append(m["Body"])
            sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
        if len(bodies) >= 3 and not msgs:
            break
    assert bodies == ["created", "paid", "shipped"], bodies


# 11.4: a batch call can partially fail while the HTTP request succeeds
def test_batch_partial_failure(sqs):
    url, _ = make_queue(sqs, unique("batch"))
    sqs.send_message(QueueUrl=url, MessageBody="job")
    m = receive(sqs, url)[0]
    resp = sqs.delete_message_batch(QueueUrl=url, Entries=[
        {"Id": "good", "ReceiptHandle": m["ReceiptHandle"]},
        {"Id": "bad", "ReceiptHandle": "not-a-valid-receipt-handle"},
    ])
    assert [e["Id"] for e in resp.get("Successful", [])] == ["good"]
    assert [e["Id"] for e in resp.get("Failed", [])] == ["bad"], resp


# 4.2–4.4: SNS fan-out to SQS with raw delivery and a filter policy
def test_sns_fanout_raw_and_filter(session, sqs):
    sns = session.client("sns", endpoint_url=AWS_ENDPOINT)
    topic = sns.create_topic(Name=unique("orders"))["TopicArn"]
    all_url, all_arn = make_queue(sqs, unique("audit"))
    created_url, created_arn = make_queue(sqs, unique("billing"))
    sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=all_arn,
                  Attributes={"RawMessageDelivery": "true"})
    sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=created_arn,
                  Attributes={"RawMessageDelivery": "true",
                              "FilterPolicy": json.dumps({"event_type": ["OrderCreated"]})})

    for event in ["OrderCreated", "OrderCancelled"]:
        sns.publish(TopicArn=topic, Message=json.dumps({"event": event}),
                    MessageAttributes={"event_type": {"DataType": "String", "StringValue": event}})

    audit = receive(sqs, all_url, wait=2)
    billing = receive(sqs, created_url, wait=2)
    assert len(audit) == 2, "the unfiltered subscription gets every message"
    assert [json.loads(m["Body"])["event"] for m in billing] == ["OrderCreated"]
    assert "Type" not in json.loads(billing[0]["Body"]), "raw delivery must not wrap the body"


# 5.2: an EventBridge rule routes by content (numeric comparison) to an SQS target
def test_eventbridge_rule_numeric_match(session, sqs):
    events = session.client("events", endpoint_url=AWS_ENDPOINT)
    bus = unique("shop")
    events.create_event_bus(Name=bus)
    url, arn = make_queue(sqs, unique("fraud"))
    rule = unique("big-orders")
    events.put_rule(Name=rule, EventBusName=bus, EventPattern=json.dumps({
        "source": ["shop.orders"], "detail-type": ["OrderCreated"],
        "detail": {"amount": [{"numeric": [">", 10000]}]}}))
    events.put_targets(Rule=rule, EventBusName=bus, Targets=[{"Id": "fraud", "Arn": arn}])

    resp = events.put_events(Entries=[
        {"Source": "shop.orders", "DetailType": "OrderCreated", "EventBusName": bus,
         "Detail": json.dumps({"order_id": "big", "amount": 14990})},
        {"Source": "shop.orders", "DetailType": "OrderCreated", "EventBusName": bus,
         "Detail": json.dumps({"order_id": "small", "amount": 500})},
    ])
    assert resp["FailedEntryCount"] == 0
    got = receive(sqs, url, wait=2)
    assert [json.loads(m["Body"])["detail"]["order_id"] for m in got] == ["big"]
