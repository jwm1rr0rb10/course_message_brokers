"""Checks used by CI: the Python snippets from the course behave as described.

    python check.py
"""
import os
import uuid

import pika
from pika.exceptions import UnroutableError

URL = os.environ.get("RABBITMQ_URL", "amqp://admin:admin@localhost:5672/%2F")


def name(prefix):
    return f"{prefix}.{uuid.uuid4().hex[:8]}"


def main():
    conn = pika.BlockingConnection(pika.URLParameters(URL))
    ch = conn.channel()
    cleanup_q, cleanup_x = [], []
    try:
        # 5.4 / 5.5: mandatory + confirms raise UnroutableError when nothing is bound
        ex = name("course-py-ex")
        ch.exchange_declare(ex, exchange_type="direct", durable=True)
        cleanup_x.append(ex)
        ch.confirm_delivery()
        try:
            ch.basic_publish(ex, "nobody", b"x", mandatory=True)
            raise AssertionError("unroutable mandatory publish did not raise")
        except UnroutableError:
            print("ok  mandatory publish without a route raises UnroutableError")

        # 8.1 / 8.3: reject(requeue=False) -> DLX with x-death reason 'rejected'
        dlx, dead, work = name("course-py-dlx"), name("course-py-dead"), name("course-py-work")
        ch.exchange_declare(dlx, exchange_type="fanout", durable=True)
        cleanup_x.append(dlx)
        ch.queue_declare(dead, durable=True, arguments={"x-queue-type": "quorum"})
        ch.queue_bind(dead, dlx)
        ch.queue_declare(work, durable=True, arguments={"x-queue-type": "quorum", "x-dead-letter-exchange": dlx})
        cleanup_q += [dead, work]
        ch.basic_publish("", work, b"{not json", pika.BasicProperties(delivery_mode=2))
        method, props, body = next(ch.consume(work, inactivity_timeout=5))
        ch.basic_reject(method.delivery_tag, requeue=False)
        ch.cancel()
        method, props, body = next(ch.consume(dead, inactivity_timeout=5))
        assert method is not None, "rejected message did not reach the DLX"
        ch.basic_ack(method.delivery_tag)
        ch.cancel()
        reason = (props.headers or {}).get("x-first-death-reason")
        assert reason == "rejected", props.headers
        print("ok  reject(requeue=False) dead-letters with reason 'rejected'")

        # 11.3: RPC with direct reply-to
        rpc = name("course-py-rpc")
        ch.queue_declare(rpc, durable=True, arguments={"x-queue-type": "quorum"})
        cleanup_q.append(rpc)
        server = conn.channel()

        def on_request(c, m, p, b):
            c.basic_publish("", p.reply_to, b.upper(), pika.BasicProperties(correlation_id=p.correlation_id))
            c.basic_ack(m.delivery_tag)

        server.basic_consume(rpc, on_request)
        client = conn.channel()
        replies = {}
        client.basic_consume("amq.rabbitmq.reply-to",
                             lambda c, m, p, b: replies.__setitem__(p.correlation_id, b), auto_ack=True)
        corr = str(uuid.uuid4())
        client.basic_publish("", rpc, b"ping", pika.BasicProperties(
            reply_to="amq.rabbitmq.reply-to", correlation_id=corr, expiration="5000"))
        for _ in range(50):
            conn.process_data_events(time_limit=0.1)
            if corr in replies:
                break
        assert replies.get(corr) == b"PING", replies
        print("ok  RPC over direct reply-to")

        # 10.3: a stream can be replayed from 'first' by every consumer
        stream = name("course-py-stream")
        ch.queue_declare(stream, durable=True, arguments={"x-queue-type": "stream"})
        cleanup_q.append(stream)
        for b in (b"a", b"b", b"c"):
            ch.basic_publish("", stream, b)
        for _ in range(2):
            reader = conn.channel()
            reader.basic_qos(prefetch_count=100)
            got = []
            for m, p, b in reader.consume(stream, arguments={"x-stream-offset": "first"}, inactivity_timeout=3):
                if m is None:
                    break
                got.append(b)
                reader.basic_ack(m.delivery_tag)
                if len(got) == 3:
                    break
            reader.close()
            assert got == [b"a", b"b", b"c"], got
        print("ok  stream replay from 'first'")
    finally:
        c = conn.channel()
        for q in cleanup_q:
            c.queue_delete(q)
        for x in cleanup_x:
            c.exchange_delete(x)
        conn.close()
    print("All Python checks passed.")


if __name__ == "__main__":
    main()
