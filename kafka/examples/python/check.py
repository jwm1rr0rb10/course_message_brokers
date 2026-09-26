"""Checks used by CI: the Python snippets from the course behave as described.

    python check.py
"""
import os
import uuid

from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092,localhost:9192,localhost:9292")
RF = int(os.environ.get("KAFKA_RF", "3"))


def create_topic(admin, partitions=3):
    name = f"course-py.{uuid.uuid4().hex[:8]}"
    fut = admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=RF)])[name]
    fut.result(30)
    return name


def consume(group, topic, n, timeout=20.0, isolation=None, commit_at=None):
    conf = {"bootstrap.servers": BROKERS, "group.id": group,
            "auto.offset.reset": "earliest", "enable.auto.commit": False}
    if isolation:
        conf["isolation.level"] = isolation
    c = Consumer(conf)
    c.subscribe([topic])
    out, waited = [], 0.0
    try:
        while len(out) < n and waited < timeout:
            m = c.poll(1.0)
            if m is None:
                waited += 1
                continue
            if m.error():
                raise KafkaException(m.error())
            out.append(m)
            if commit_at is not None and len(out) == commit_at:
                c.commit(message=m, asynchronous=False)
    finally:
        c.close()
    return out


def main():
    admin = AdminClient({"bootstrap.servers": BROKERS})
    topics = []
    try:
        # 1.4: same key -> same partition, in order
        t = create_topic(admin, 6)
        topics.append(t)
        p = Producer({"bootstrap.servers": BROKERS, "enable.idempotence": True, "acks": "all"})
        results = []
        for i in range(10):
            p.produce(t, key="order-1", value=str(i),
                      on_delivery=lambda err, msg: results.append((err, msg.partition(), msg.offset())))
        p.flush(15)
        assert all(err is None for err, _, _ in results), results
        assert len({part for _, part, _ in results}) == 1, results
        assert [o for _, _, o in results] == sorted(o for _, _, o in results), results
        print("ok  same key -> one partition, increasing offsets")

        # 5.2: no commit -> the group starts over; after a commit it continues
        t = create_topic(admin, 1)
        topics.append(t)
        for i in range(5):
            p.produce(t, value=str(i))
        p.flush(15)
        group = f"course-py-group.{uuid.uuid4().hex[:8]}"
        first = consume(group, t, 5)
        assert len(first) == 5, len(first)
        again = consume(group, t, 5, commit_at=3)            # re-reads, commits after the 3rd
        assert [m.offset() for m in again] == [m.offset() for m in first], "group did not start over"
        rest = consume(group, t, 2)
        assert rest and rest[0].offset() == again[2].offset() + 1, [m.offset() for m in rest]
        print("ok  consumer group resumes from the committed offset")

        # 6.3: read_committed hides aborted transactions
        t = create_topic(admin, 1)
        topics.append(t)
        tp = Producer({"bootstrap.servers": BROKERS, "transactional.id": f"course-py-txn.{uuid.uuid4().hex[:8]}"})
        tp.init_transactions(30)
        tp.begin_transaction()
        tp.produce(t, value="aborted")
        tp.flush(15)
        tp.abort_transaction(30)
        tp.begin_transaction()
        tp.produce(t, value="committed")
        tp.commit_transaction(30)
        committed = consume(f"g.{uuid.uuid4().hex[:8]}", t, 2, timeout=8, isolation="read_committed")
        assert [m.value() for m in committed] == [b"committed"], [m.value() for m in committed]
        print("ok  read_committed hides the aborted transaction")
    finally:
        if topics:
            for f in admin.delete_topics(topics).values():
                try:
                    f.result(15)
                except Exception:
                    pass

    print("All Python checks passed.")


if __name__ == "__main__":
    main()
