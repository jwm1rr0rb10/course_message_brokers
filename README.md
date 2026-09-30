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

If you already know the task (a saga, a task queue, an outbox, event sourcing), see [Patterns: which broker for what](#patterns-which-broker-for-what) below.

## Patterns: which broker for what

Almost any pattern can be built on any broker. The question is where it comes **for free** and where you have to build it on top: write code, add topics and handle the edge cases. Below, for each major pattern: which broker fits best and why, the alternatives, the downsides and the traps. Links lead to the course chapters where the pattern is covered in depth.

First, the main distinction that almost everything else follows from:

- **A queue** (RabbitMQ, SQS, Azure Service Bus): a message goes to one of the consumers and is deleted once acknowledged. Acks, retries and DLQs work per message. You can't re-read history.
- **A log** (Kafka, Kinesis, Event Hubs, RabbitMQ streams, NATS JetStream): messages are kept by time or size, and each reader moves its own position. You can re-read history, add new readers, and order is kept within a partition. There is no per-message ack, only an offset.
- **A subject-addressed bus** (Core NATS): delivery to whoever is subscribed right now, no storage, sub-millisecond latency. JetStream adds storage.
- **An event router** (EventBridge, Event Grid, Eventarc): receives events and forwards them to other services by rules. Neither a queue nor a log by itself.

### Cheat sheet

✅ built in and convenient, ⚠️ possible, with caveats or your own code, ❌ not what it's for.

| Pattern | Kafka | RabbitMQ | NATS | Cloud |
| --- | :---: | :---: | :---: | --- |
| [Task queue](#1-task-queue-competing-consumers) | ⚠️ consumer groups, ✅ share groups | ✅ | ✅ JetStream | ✅ SQS, Service Bus |
| [Pub/sub and fan-out](#2-pubsub-and-fan-out) | ✅ | ✅ | ✅ | ✅ SNS + SQS, Service Bus topics, Pub/Sub |
| [Content-based routing](#3-content-based-routing) | ❌ | ✅ | ⚠️ by subject | ✅ EventBridge, SNS, Service Bus |
| [Request-reply](#4-request-reply-rpc) | ❌ | ✅ | ✅ best | ❌ |
| [Scatter-gather](#5-scatter-gather) | ❌ | ⚠️ | ✅ | ❌ |
| [Per-key ordering](#6-per-key-ordering) | ✅ best | ⚠️ | ⚠️ | ✅ FIFO, sessions, ordering keys |
| [Delayed messages and retries](#7-delayed-messages-and-retries-with-a-delay) | ❌ | ✅ | ✅ retries | ✅ Service Bus, SQS, Cloud Tasks |
| [DLQ](#8-dlq-and-poison-messages) | ⚠️ own code | ✅ | ⚠️ own code | ✅ |
| [Priorities](#9-priorities) | ❌ | ✅ | ❌ | ❌ |
| [Transactional outbox](#10-transactional-outbox) | ✅ with Debezium | ✅ | ✅ | ✅ |
| [Idempotent consumer](#11-idempotent-consumer-inbox) | ⚠️ | ⚠️ | ⚠️ | ⚠️ |
| [Saga: choreography](#12-sagas) | ✅ best | ✅ | ✅ | ✅ EventBridge, SNS, Pub/Sub |
| [Saga: orchestration](#12-sagas) | ⚠️ | ✅ | ✅ | ✅ Step Functions, Durable Functions, Workflows |
| [Event sourcing](#13-event-sourcing) | ⚠️ as transport | ❌ | ✅ JetStream | ❌ |
| [CQRS and projections](#14-cqrs-and-projections) | ✅ | ⚠️ | ✅ KV | ⚠️ |
| [CDC](#15-change-data-capture-cdc) | ✅ best | ⚠️ | ⚠️ | ✅ DynamoDB Streams, Change Feed |
| [Replay](#16-replay) | ✅ best | ⚠️ streams | ✅ | ⚠️ Kinesis, Event Hubs, Pub/Sub seek |
| [Stream processing](#17-stream-processing) | ✅ best | ❌ | ❌ | ✅ Kinesis + Flink, Dataflow |
| [Claim check](#18-claim-check-large-messages) | ⚠️ | ⚠️ | ✅ Object Store | ⚠️ |
| [Multiple regions](#19-multiple-regions-and-data-centers) | ⚠️ MirrorMaker 2 | ⚠️ Federation, Shovel | ✅ | ✅ Pub/Sub, Service Bus Premium |
| [Edge and IoT](#20-edge-and-iot) | ❌ | ✅ MQTT | ✅ best | ⚠️ |
| [KV, configuration, locks](#21-kv-configuration-and-locks) | ⚠️ | ❌ | ✅ | ❌ |

### 1. Task queue (competing consumers)

Several workers take jobs from a shared queue, each job runs once, and if a worker fails another one picks the job up. **Load leveling** belongs here too: the queue absorbs a spike and workers drain it at their own pace.

**Best: RabbitMQ (quorum queues); in the cloud, SQS or Service Bus.** A queue is built for exactly this: ack and retry per message, `prefetch` limits how many jobs a worker holds at once, and a slow job doesn't hold up the others. The number of workers isn't tied to partitions: add a worker and it takes jobs right away. Failures go to a DLX by `delivery-limit`. SQS gives the same with no servers and scales very well for serverless.

- **NATS:** a JetStream stream with `workqueue` retention and a pull consumer works as a queue with acks. Core NATS queue groups balance load without storage: if there are no workers, the message is lost.
- **Kafka:** a consumer group is limited by the number of partitions, and one slow message holds up its whole partition (head-of-line blocking). **Share groups** (GA since Kafka 4.2) remove both limits: per-record acks and more consumers than partitions. A good choice if you already run Kafka and don't want a second broker just for a queue.
- **Downsides of a queue:** you can't re-read history; a long queue in RabbitMQ is a symptom of a problem, not a normal mode of operation.

In depth: [RabbitMQ 11.1](rabbit/README.md#111-work-queue-task-queue), [RabbitMQ 6.8](rabbit/README.md#68-ordering-single-active-consumer-and-consumer-priorities), [Kafka 12.1](kafka/README.md#121-the-problem-share-groups-solve), [NATS 4.5](nats/README.md#45-queue-groups-load-balancing-with-zero-configuration), [Cloud 10.1](cloud/README.md#101-task-queue).

### 2. Pub/sub and fan-out

One event reaches every interested service, each independently.

**Best: Kafka, for high volume and when subscribers need history.** Each consumer group reads the log from its own position, data isn't copied per subscriber, and a new service can read events for the whole retention period. Subscribers don't affect each other: a slow one just falls behind.

- **RabbitMQ:** a fanout or topic exchange with a queue per service. Convenient and flexible, but the message is copied into each queue, and a new subscriber sees only new events. Streams give you a log inside RabbitMQ.
- **NATS:** Core subscribes to subjects with wildcards, but an offline subscriber misses messages. With JetStream each service has its own durable consumer.
- **Cloud:** in AWS the classic combination is SNS + SQS (each subscriber has its own queue). Also Service Bus topics with subscriptions and Pub/Sub topics with subscriptions.
- **Trap:** in RabbitMQ, name the queue after the consumer (`billing.orders`), not the event; otherwise two services will share messages instead of each getting its own copy.

In depth: [RabbitMQ 0.4](rabbit/README.md#04-queue-vs-log-the-most-important-thing-to-understand-about-rabbitmq), [Kafka 0.4](kafka/README.md#04-log-vs-queue-the-most-important-thing-to-understand-about-kafka), [NATS 0.4](nats/README.md#04-core-nats-and-jetstream-two-layers-one-system), [Cloud 10.2](cloud/README.md#102-fan-out).

### 3. Content-based routing

A message reaches only those who need it, by type, region, amount and so on.

**Best: RabbitMQ; in the cloud, EventBridge.** In RabbitMQ routing happens on the broker: a topic exchange by routing key with wildcards, a headers exchange by headers, and rules change through bindings without touching consumer code. EventBridge filters on fields of the JSON body (numbers, prefixes, lists) and delivers straight to Lambda, SQS, Step Functions or HTTP.

- **NATS:** routing by subject hierarchy (`orders.eu.created`, subscription `orders.*.created`) is very fast but works on the name only. The subject hierarchy has to be designed up front.
- **Cloud:** SNS filter policies (on attributes or the body), SQL filters on Service Bus subscriptions, Pub/Sub subscription filters on attributes.
- **Kafka:** no broker-side filtering: the consumer reads everything and drops what it doesn't need, or Kafka Streams splits events into separate topics.

In depth: [RabbitMQ 3.1](rabbit/README.md#31-four-exchange-types), [NATS 3.3](nats/README.md#33-designing-the-subject-space), [Cloud 4.4](cloud/README.md#44-subscription-filtering), [Cloud 5.2](cloud/README.md#52-event-structure-and-rules).

### 4. Request-reply (RPC)

A service sends a request and waits for the answer.

**Best: NATS.** Request-reply is part of the protocol: the client publishes a request with a unique inbox, and the first reply comes back in a fraction of a millisecond. Load balancing across service instances is a queue group, with no configuration. If the service isn't running, the client gets `no responders` right away instead of waiting for a timeout.

- **RabbitMQ:** works through direct reply-to and `correlation_id`. A reasonable option if you already run RabbitMQ, but it needs more code and has higher latency.
- **Kafka and cloud brokers:** a poor fit. There is no reply mechanism, so you need reply topics and matching by correlation id, with latency of tens of milliseconds or more. HTTP or gRPC is the honest choice here.
- **When you don't need a broker at all:** for a synchronous call between two services without load balancing or discovery, gRPC is usually enough.

In depth: [NATS 4.6](nats/README.md#46-request-reply), [RabbitMQ 11.3](rabbit/README.md#113-rpc-request-and-reply), [Cloud 10.9](cloud/README.md#109-request-reply).

### 5. Scatter-gather

One request goes to many services, and answers are collected until a timeout: for example, asking several suppliers for a price.

**Best: NATS.** The request is published on a subject, every subscriber receives it, and the client collects replies until time runs out or it has enough. It's a couple of lines of code.

- **RabbitMQ:** a fanout exchange, a reply queue and your own aggregator with a timeout.
- **Kafka and cloud:** not suited to synchronous scatter-gather. The asynchronous version is replies into a topic and aggregation in Kafka Streams.

In depth: [NATS 4.7](nats/README.md#47-scatter-gather-collecting-answers-from-many-services).

### 6. Per-key ordering

Events of one entity (an order, an account) are processed strictly in order, while different entities are processed in parallel.

**Best: Kafka.** Order is guaranteed within a partition, and the producer picks the partition by key, so all events of order `42` land in the same partition. Parallelism scales with the number of partitions. The idempotent producer (on by default) keeps order across retries.

- **Cloud:** good built-in options: message groups in SQS FIFO, sessions in Service Bus, ordering keys in Pub/Sub, partition keys in Kinesis.
- **RabbitMQ:** order holds in one queue with one consumer (single active consumer). To scale, use a consistent hash exchange or super streams, which are partitions in all but name.
- **NATS:** a stream keeps order, but parallel consumers break it. For per-key order: a subject per entity with `MaxAckPending=1`, or partitioning by subject.
- **Traps:** adding partitions in Kafka changes which partition a key maps to, so order breaks at that boundary. A hot key overloads one partition. A retry with a pause inside a group blocks the whole group.

In depth: [Kafka 4.4](kafka/README.md#44-message-ordering), [RabbitMQ 11.5](rabbit/README.md#115-competing-consumers-and-ordering), [NATS 6.8](nats/README.md#68-message-ordering-in-nats), [Cloud 10.3](cloud/README.md#103-per-entity-ordering).

### 7. Delayed messages and retries with a delay

Retry processing in 10 seconds, a minute, an hour; send a reminder in a day.

**Best for retries: RabbitMQ 4.3 and NATS. For delivery at a specific time: Service Bus and Cloud Tasks.**

- **RabbitMQ:** in 4.3, quorum queues have a built-in delayed retry with a growing delay (`x-delayed-retry-*`). Before 4.3, the classic scheme is TTL + DLX with wait queues. The delayed message exchange plugin doesn't work on 4.3.
- **NATS:** `NakWithDelay` with a delay based on the attempt number, or `BackOff` on the consumer (it replaces `AckWait`). Don't combine them: the delays add up.
- **Cloud:** Service Bus has scheduled messages at any time; SQS has `DelaySeconds` up to 15 minutes and visibility timeout extension for retries; Pub/Sub has a retry policy with backoff up to 600 seconds, and Cloud Tasks for delayed jobs.
- **Kafka:** no delays. Non-blocking retries are built with retry topics and a pause before reading. It works, but it's your own code and extra topics.

In depth: [RabbitMQ 8.6](rabbit/README.md#86-delayed-retries-with-ttl--dlx), [RabbitMQ 8.7](rabbit/README.md#87-delayed-retry-in-quorum-queues-43), [NATS 6.4](nats/README.md#64-key-consumer-parameters), [Kafka 13.3](kafka/README.md#133-non-blocking-retries-retry-topics), [Cloud 10.6](cloud/README.md#106-delays-retries-and-schedules).

### 8. DLQ and poison messages

A message that can't be processed is set aside in a separate queue so it doesn't block the rest and doesn't get lost.

**Best: cloud queues and RabbitMQ, where the DLQ is built in.** SQS: a redrive policy by `maxReceiveCount` and redrive back with one click. Service Bus: every queue and subscription has its own dead-letter subqueue. Pub/Sub: a dead letter topic after a set number of attempts. RabbitMQ: a DLX, a delivery limit on quorum queues (20 by default) and `x-death` headers with the reason.

- **RabbitMQ 4.3:** `basic.reject` counts toward the delivery limit, while `basic.nack(requeue=true)` doesn't. A message returned with `nack` will cycle forever.
- **NATS:** no built-in DLQ. It's built from `max_deliver` and advisory messages about exceeded attempts. On workqueue streams `Term` deletes the message before it reaches the DLQ, so publish it to the DLQ first.
- **Kafka:** a DLQ is an ordinary topic the application writes to. The rule: write to the DLQ first, then commit the offset, or the message is lost.
- **General:** put the reason, the attempt count and the source topic or queue into the DLQ message. You need a process to review and resend, or the DLQ becomes a dump.

In depth: [RabbitMQ 8.2](rabbit/README.md#82-setting-up-a-dlx), [NATS 14.3](nats/README.md#143-a-dlq-built-on-advisories), [Kafka 13.4](kafka/README.md#134-what-to-put-in-a-dlq-message), [Cloud 10.5](cloud/README.md#105-dlq-and-redrive).

### 9. Priorities

Urgent jobs are processed before ordinary ones.

**Best: RabbitMQ, the only broker here with message priority at the queue level.** Both classic and quorum queues support it; the details and limits are in section 11.4.

- **Everywhere else:** a separate queue or topic per priority, with more workers on the urgent one. This is often more reliable than built-in priorities: urgent jobs don't wait behind a long tail of ordinary ones.
- **Trap:** priority only matters when messages are actually waiting. If workers keep up, everything goes in arrival order. With a large `prefetch` a worker has already taken ordinary jobs and won't see the urgent one immediately.

In depth: [RabbitMQ 11.4](rabbit/README.md#114-priorities).

### 10. Transactional outbox

Save a change to the database and publish an event with neither lost events nor events without a matching change. You can't write to the database and the broker atomically, so the event is written to an outbox table in the same transaction and a separate process publishes it.

**Best: Kafka + Debezium.** Debezium reads the database log (WAL, binlog) instead of polling the table, and its Outbox Event Router turns outbox rows into events in the right topics, keyed by aggregate. No poller of your own, low latency, and order per aggregate is kept.

- **NATS:** a poller plus `Nats-Msg-Id`. If the poller crashes and publishes again, JetStream drops the duplicate within the deduplication window.
- **RabbitMQ:** a poller with publisher confirms. A row is marked as sent only after the broker confirms it.
- **Cloud:** DynamoDB Streams + EventBridge Pipes, Cosmos DB Change Feed, Spanner change streams: an outbox without your own poller.
- **Traps:** the poller must publish in write order if order matters. The outbox table has to be cleaned up. Debezium holds a replication slot: if the connector stops, the WAL grows until the disk is full.

In depth: [Kafka 6.6](kafka/README.md#66-transactional-outbox), [Kafka 10.6](kafka/README.md#106-outbox-with-debezium), [NATS 7.7](nats/README.md#77-transactional-outbox), [RabbitMQ 7.5](rabbit/README.md#75-transactional-outbox), [Cloud 11.3](cloud/README.md#113-transactional-outbox-in-the-cloud).

### 11. Idempotent consumer (inbox)

All the brokers covered deliver at least once. So a consumer has to survive duplicates: record the ID of a processed message in the same transaction as the result, and skip ones it has already seen.

**No broker wins here: idempotency is the application's job.** Brokers only reduce the number of duplicates:

- **Kafka:** the idempotent producer removes duplicates on resend, and transactions give exactly-once for a read-from-Kafka → write-to-Kafka chain. As soon as a side effect leaves Kafka (a database, a payment, an email), you need an idempotent consumer.
- **NATS:** deduplication by `Nats-Msg-Id` on publish; `DoubleAck` confirms the ack reached the server.
- **Cloud:** deduplication in SQS FIFO (5 minutes), duplicate detection in Service Bus (configurable window), exactly-once delivery in Pub/Sub (on the subscription side).
- **RabbitMQ:** publisher confirms and `message_id`; deduplication is on the consumer side.

In depth: [Kafka 6.5](kafka/README.md#65-the-idempotent-consumer), [NATS 7.5](nats/README.md#75-the-idempotent-consumer), [RabbitMQ 7.3](rabbit/README.md#73-the-idempotent-consumer), [Cloud 11.2](cloud/README.md#112-an-idempotent-consumer-in-the-cloud).

### 12. Sagas

A business process that spans several services and databases can't be one transaction. A saga splits it into local transactions with **compensating actions**: if payment succeeded but the stock reservation failed, the money is refunded.

**Choreography: services react to each other's events, with no central coordinator.**

- **Best: Kafka.** Saga events are facts read by several services. The log keeps them, so the process can be rebuilt, an incident investigated and a new service added. The saga key (the order ID) as the message key keeps one saga's events in order.
- **Also a good fit:** RabbitMQ (topic exchange), NATS JetStream; in the cloud, EventBridge, SNS + SQS, Pub/Sub.
- **Pros:** loose coupling, no single point of failure, easy to add a participant.
- **Cons:** the process isn't described anywhere as a whole and has to be pieced together from the services' code. Cyclic dependencies are easy to create. Timeouts ("no payment within 15 minutes") are hard. Debugging needs an end-to-end correlation id and tracing.

**Orchestration: an orchestrator keeps the saga's state, sends commands to the steps and waits for replies.**

- **Best: a broker with command-queue semantics, RabbitMQ or NATS; in the cloud, a managed workflow engine.** A command is addressed to one executor and must be handled by exactly that executor: that's a queue, not a log. RabbitMQ gives command queues, replies with `correlation_id`, a DLX and delays for step timeouts. NATS gives request-reply for short steps, JetStream for durability and KV for the saga's state.
- **In the cloud**, instead of your own orchestrator: AWS Step Functions, Azure Durable Functions, Google Workflows; state, retries, timeouts and compensations are declared. Service Bus sessions with session state are a convenient place to keep one saga's state.
- **On Kafka** orchestration works too (command and reply topics), but you have to write timeouts and step scheduling yourself.
- **Pros:** the process is described in one place; timeouts, branching and monitoring are easy to add.
- **Cons:** the orchestrator is a separate stateful service that has to be made reliable, and there's a risk it collects every service's business logic.

**For both:**
- every step is idempotent, because commands and events are redelivered;
- events and commands are published through an outbox;
- every step has a compensation, and it is idempotent too;
- the saga ID travels through every message.

The choice is simple: 2–3 steps with no timeouts, choreography; more steps, timeouts, branching, or you need to answer "which step is order 42 on", orchestration.

In depth: [RabbitMQ 11.6](rabbit/README.md#116-sagas-and-choreography), [Kafka 6.6](kafka/README.md#66-transactional-outbox), [Cloud 5.2](cloud/README.md#52-event-structure-and-rules).

### 13. Event sourcing

An entity's state is stored as a sequence of events, and the current state is obtained by replaying them. This needs appending an aggregate's events with a version check (optimistic concurrency) and reading all events of one aggregate quickly.

**Closest to native: NATS JetStream.** A subject per aggregate (`orders.42`); a publish with the `Nats-Expected-Last-Subject-Sequence` header is rejected if someone appended an event between the read and the write. Reading an aggregate's events is a consumer filtered by subject.

- **Kafka** is excellent at **distributing** events but awkward as an event store: there is no per-key version check, and reading one aggregate means scanning a partition. A common architecture: events are stored in a database (or a dedicated event store) and published to Kafka through an outbox or CDC. Compacted topics work well for snapshots.
- **RabbitMQ and cloud queues** don't fit event sourcing: a message is deleted once read.
- **Downsides of the approach in general:** event schema evolution, snapshots for long histories, complex projections. Event sourcing pays off where change history is part of the domain (finance, audit), not everywhere.

In depth: [NATS 7.4](nats/README.md#74-optimistic-concurrency-on-publish), [Kafka 7.3](kafka/README.md#73-log-compaction-the-latest-value-per-key), [Kafka 9.1](kafka/README.md#91-why-schemas).

### 14. CQRS and projections

The write model is separated from the read models, and read models (projections) are built from the event stream.

**Best: Kafka.** A projection can be rebuilt from scratch by re-reading the topic. Compacted topics keep the latest value per key, and Kafka Streams builds tables (KTables) and aggregates straight from the stream.

- **NATS:** KV (built on streams) holds small read models and pushes changes through watch; the stream can be re-read to rebuild.
- **RabbitMQ:** can update projections but not rebuild them (except with streams).
- **Cloud:** rebuilding is possible from Kinesis, Event Hubs or Pub/Sub with seek, but only within the retention period.
- **Trap:** read models lag behind writes (eventual consistency). The UI has to account for it, for example by showing "order received" rather than the final state right away.

In depth: [Kafka 7.3](kafka/README.md#73-log-compaction-the-latest-value-per-key), [Kafka 11.1](kafka/README.md#111-what-it-is), [NATS 9.1](nats/README.md#91-what-it-is-and-why).

### 15. Change Data Capture (CDC)

Database changes become a stream of events without changing application code.

**Best: Kafka + Debezium (through Kafka Connect).** It's the industry standard: connectors for PostgreSQL, MySQL, SQL Server, MongoDB and more, an initial snapshot, schemas, and reprocessing from the log.

- **Other brokers:** Debezium Server can also send changes to NATS JetStream, RabbitMQ, Kinesis, Pub/Sub and Event Hubs, but without the Connect ecosystem.
- **Cloud:** DynamoDB Streams, Cosmos DB Change Feed, Spanner change streams and Datastream give CDC for their own databases without Debezium.
- **Traps:** CDC publishes table changes, which is a service's internal schema. For events between services, run CDC over an outbox table rather than raw tables. Watch the replication slot.

In depth: [Kafka 10.2](kafka/README.md#102-cdc-database-changes-as-a-stream-of-events), [Kafka 10.5](kafka/README.md#105-the-main-danger-of-cdc-the-replication-slot), [Kafka 10.6](kafka/README.md#106-outbox-with-debezium).

### 16. Replay

Re-read events from a past period: rebuild a projection, fix a handler bug, attach a new service to history.

**Best: Kafka.** Retention by time or size, tiered storage for keeping data for years, and resetting a group's offset to a point in time with one command.

- **NATS JetStream:** retention by limits, and a consumer that starts at a time or sequence.
- **RabbitMQ:** streams and super streams; ordinary queues can't be re-read.
- **Cloud:** Kinesis and Event Hubs (limited retention), Pub/Sub seek to a time or snapshot with acked-message retention on, EventBridge archive and replay. SQS and Service Bus keep no history.

In depth: [Kafka 7.2](kafka/README.md#72-retention-how-long-to-keep-data), [Kafka 7.6](kafka/README.md#76-tiered-storage-hot-data-locally-old-data-in-object-storage), [Kafka 5.9](kafka/README.md#59-managing-groups-and-offsets-from-the-cli), [RabbitMQ 10.1](rabbit/README.md#101-why-streams-if-there-are-queues), [NATS 6.9](nats/README.md#69-ordered-consumers), [Cloud 10.7](cloud/README.md#107-replay).

### 17. Stream processing

Aggregations, windows, stream joins, real-time enrichment.

**Best: Kafka.** Kafka Streams is a library, not a cluster. Flink reads Kafka natively. Transactions give exactly-once for a read → compute → write chain.

- **Cloud:** Kinesis + Managed Service for Apache Flink, Event Hubs + Stream Analytics, Pub/Sub + Dataflow.
- **RabbitMQ and NATS:** no stream processing frameworks, only your own code.

In depth: [Kafka 11.1](kafka/README.md#111-what-it-is), [Kafka 6.3](kafka/README.md#63-transactions).

### 18. Claim check (large messages)

A large payload goes into object storage (S3, Blob Storage, GCS), and the broker carries a reference.

**Best: NATS, which has an Object Store**, storage for large objects on top of JetStream, so a claim check needs no external storage.

- **Default limits:** Kafka, about 1 MB per message (can be raised, but it hurts the broker); RabbitMQ, 16 MiB maximum; NATS, `max_payload` 1 MB; SQS, 1 MiB; Pub/Sub, 10 MB; Service Bus, 256 KB on Standard and up to 100 MB on Premium.
- **Trap:** the consumer must be able to access the reference (permissions, signed URL lifetime), and objects have to be deleted once the message is processed.

In depth: [NATS 10.1](nats/README.md#101-why), [Cloud 3.3](cloud/README.md#33-queue-parameters).

### 19. Multiple regions and data centers

**Best: NATS; in the cloud, Pub/Sub.** NATS joins clusters into a supercluster through gateways, and streams are replicated between regions with mirrors and sources, all built in. Pub/Sub is global by design, with storage regions set by policy.

- **Kafka:** MirrorMaker 2, asynchronous replication with translation of consumer group offsets. It works, but it's separate infrastructure, and failover has to be rehearsed.
- **RabbitMQ:** Federation (subscribing to a remote exchange or queue) and Shovel (moving messages).
- **Cloud:** Service Bus Premium has geo-disaster recovery and geo-replication; SQS is regional, and cross-region delivery goes through SNS or EventBridge.
- **General:** cross-region replication is almost always asynchronous. On failover some messages may be lost or delivered twice, so consumers must be idempotent.

In depth: [NATS 12.3](nats/README.md#123-superclusters-and-gateways), [NATS 13.2](nats/README.md#132-mirrors-for-geo-replication), [Kafka 14.2](kafka/README.md#142-mirrormaker-2), [RabbitMQ 13.4](rabbit/README.md#134-federation-or-shovel).

### 20. Edge and IoT

Devices, shops and factories with unreliable connectivity.

**Best: NATS.** A small binary; leaf nodes keep working locally when the link drops and sync with the center; JetStream streams move data through sources and mirrors. A built-in MQTT listener connects devices without a separate broker.

- **RabbitMQ:** the MQTT plugin handles many device connections and maps them to ordinary exchanges and queues.
- **Kafka:** devices don't connect to it directly. The usual setup: an MQTT broker at the edge, Kafka in the center.
- **Cloud:** dedicated services (AWS IoT Core, Azure IoT Hub), outside the scope of the course.

In depth: [NATS 12.4](nats/README.md#124-leaf-nodes), [NATS 12.6](nats/README.md#126-mqtt-and-websocket-built-in-listeners), [RabbitMQ 12.3](rabbit/README.md#123-mqtt).

### 21. KV, configuration and locks

**Best: NATS KV.** Watches on changes, version history, TTL, and conditional writes by revision (compare-and-set): enough for configuration, feature flags, a service registry and simple locks.

- **Kafka:** a compacted topic distributes configuration, but without conditional writes.
- **Trap with locks:** a lock with a TTL can expire while its holder is still working. Correctness needs a fencing token (the revision number) checked by the protected resource.

In depth: [NATS 9.1](nats/README.md#91-what-it-is-and-why), [NATS 9.5](nats/README.md#95-a-distributed-lock-on-kv).

### When you don't need a broker

- **A synchronous call to one service:** HTTP or gRPC.
- **Everything happens in one database:** a transaction, not a saga.
- **A background job once an hour:** a scheduler (cron, Cloud Scheduler, EventBridge Scheduler).
- **A few messages a minute between two services of one team:** a jobs table in the database (`SELECT … FOR UPDATE SKIP LOCKED`) is simpler than one more system to operate.

## Examples and checked claims

Each course has an `examples/` folder: a Docker Compose cluster, a smoke test of the commands in the text, Go and Python code, and integration tests that check the course's claims against a live broker.

[CI](.github/workflows/examples.yml) runs on every push and weekly:

- Go code (`go vet`, unit tests) and tests on in-process brokers — embedded nats-server and kfake for Kafka, no Docker;
- clusters and emulators with the smoke and integration tests, twice: on the version pinned in the course and on the newest image.

If a new broker release changes behaviour the course describes, the build turns red.

Integration tests skip when no broker is running; with `COURSE_REQUIRE_BROKER=1` they fail instead, which is how CI runs them.

## License

[MIT](LICENSE)
