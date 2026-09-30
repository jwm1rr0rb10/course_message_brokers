# Message brokers: a free course series

[![examples](https://github.com/jwm1rr0rb10/course_message_brokers/actions/workflows/examples.yml/badge.svg)](https://github.com/jwm1rr0rb10/course_message_brokers/actions/workflows/examples.yml)
![Free](https://img.shields.io/badge/price-free-brightgreen)

Four courses from zero to production: NATS, Apache Kafka, RabbitMQ and the cloud brokers of AWS, Azure and Google Cloud. Each has theory, diagrams, commands you can run locally, common mistakes, self-check questions, a capstone project and interview questions.

🇷🇺 Русская версия: [READMEru.md](READMEru.md)

## Courses

| Course | Version | Topics |
| --- | --- | --- |
| [NATS and JetStream](nats/README.md) | NATS Server 2.15 | Core NATS, subjects, request-reply, streams and consumers, KV and Object Store, clusters, superclusters and leaf nodes, security |
| [Apache Kafka](kafka/README.md) | Kafka 4.3, KRaft | Topics and partitions, consumer groups and the new protocol, transactions, compaction, replication and ISR, Connect, Streams, share groups |
| [RabbitMQ](rabbit/README.md) | RabbitMQ 4.3 | Exchanges and routing, quorum queues, streams, confirms and acks, DLX and retries, clustering, AMQP 1.0, Federation and Shovel |
| [Cloud brokers](cloud/README.md) | 2026 | SQS, SNS, EventBridge, Kinesis, Service Bus, Event Grid, Event Hubs, Pub/Sub, Eventarc, Cloud Tasks — the same tasks in three clouds side by side |

## Where to start

The courses are independent. If you're not sure which one to take:

1. **A task queue or routing** — [RabbitMQ](rabbit/README.md).
2. **An event log, replay, high throughput** — [Kafka](kafka/README.md).
3. **A lightweight bus for microservices, request-reply, edge** — [NATS](nats/README.md).
4. **You work in a cloud** — [cloud brokers](cloud/README.md), best after one of the above: it links to them wherever a cloud service repeats their ideas.

If you're choosing a broker for a project, start with the comparison sections: [NATS 0.7](nats/README.md#07-nats-vs-kafka-vs-rabbitmq-vs-grpc), [Kafka 0.6](kafka/README.md#06-kafka-vs-rabbitmq-nats-and-pulsar), [RabbitMQ 0.6](rabbit/README.md#06-rabbitmq-vs-kafka-nats-and-redis), and for the cloud, [15.3 "Cloud or your own cluster"](cloud/README.md#153-cloud-or-your-own-cluster-a-rough-estimate).

## Examples and checked claims

Each course has an `examples/` folder: a Docker Compose cluster, a smoke test of the commands in the text, Go and Python code, and integration tests that check the course's claims against a live broker.

[CI](.github/workflows/examples.yml) runs on every push and weekly:

- Go code (`go vet`, unit tests) and tests on in-process brokers — embedded nats-server and kfake for Kafka, no Docker;
- clusters and emulators with the smoke and integration tests, twice: on the version pinned in the course and on the newest image.

If a new broker release changes behaviour the course describes, the build turns red.

Integration tests skip when no broker is running; with `COURSE_REQUIRE_BROKER=1` they fail instead, which is how CI runs them.

## License

[MIT](LICENSE)
