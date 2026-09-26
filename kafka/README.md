# Kafka Course 2026: a free Apache Kafka course from zero to pro

![Kafka 4.3](https://img.shields.io/badge/Apache%20Kafka-4.3-231F20?logo=apachekafka&logoColor=white)
![KRaft](https://img.shields.io/badge/KRaft-no%20ZooKeeper-blue)
![Language English](https://img.shields.io/badge/language-english-red)
![Free course](https://img.shields.io/badge/price-free-brightgreen)
![junior to senior](https://img.shields.io/badge/level-junior%20→%20senior-orange)

> **A complete free Apache Kafka course.** Theory, practice, Docker, Java, Go and Python, topics and partitions, keys and ordering, producers and consumer groups, offsets and rebalancing, delivery guarantees and transactions, retention and log compaction, replication and ISR, Schema Registry, Kafka Connect and Debezium, Kafka Streams, share groups (queues for Kafka), DLQs, MirrorMaker 2, monitoring, security and production architecture. All in a single README, current for **Apache Kafka 4.3 (2026)**, KRaft only, no ZooKeeper.

**Kafka without the fluff:** every module gives you clear theory, diagrams, commands you can actually run, the mistakes people make, and self-check questions. Use it to learn Kafka from scratch, to prepare for a backend, data or DevOps interview, and to design a reliable Kafka-based system in production.

⭐ If the course helps, star the repository so other developers can find it.

🇷🇺 Русская версия: [READMEru.md](READMEru.md)

---

## Who this Kafka course is for

| Who you are | What you get |
|---|---|
| **New to messaging** | What Kafka is, how a log differs from a queue, how to run a cluster in five minutes |
| **Backend developer** (examples in Java, Go and Python) | Producers and consumers without losses or duplicates, keys and ordering, idempotence, transactions |
| **Microservices developer** | Event-driven architecture, outbox, sagas, event contracts |
| **Data engineer** | Retention and compaction, Schema Registry, Kafka Connect, CDC with Debezium, Kafka Streams |
| **DevOps / SRE** | KRaft, replication and ISR, monitoring lag and under-replicated partitions, upgrades, Kubernetes |
| **Architect / Tech Lead** | Topic and key design, choosing partition counts, multi-DC, anti-patterns |
| **Interview prep** | 35 Kafka questions with answers at junior, middle and senior level |

## What you'll be able to do after the course

- explain the Kafka architecture: broker, controller, topic, partition, offset, replica, leader, ISR, consumer group;
- run a three-node Kafka cluster in KRaft mode in Docker and break it while watching leader elections;
- choose partition counts and message keys that preserve ordering and still scale;
- write producers and consumers in Java, Go and Python without losing or duplicating messages;
- understand `acks=0`, `acks=1` and `acks=all` and how they relate to `min.insync.replicas`;
- manage offsets deliberately: auto-commit, manual commit, resetting a group;
- explain rebalancing and the new consumer group protocol (KIP-848);
- get exactly-once with the idempotent producer, transactions and `read_committed`;
- configure retention, log compaction and tiered storage;
- evolve schemas with Schema Registry without breaking consumers;
- stream CDC out of PostgreSQL with Kafka Connect and Debezium;
- write simple Kafka Streams applications;
- use share groups as a task queue on top of Kafka;
- build retry topics and DLQs;
- monitor consumer lag, under-replicated partitions and disk usage;
- enable TLS, SASL and ACLs;
- size disks, memory and broker count for production.

---

## Table of contents

- [Who this Kafka course is for](#who-this-kafka-course-is-for)
- [What you'll be able to do after the course](#what-youll-be-able-to-do-after-the-course)
- [How to take this course](#how-to-take-this-course)
- [Module 0. What Kafka is and why you need it](#module-0-what-kafka-is-and-why-you-need-it)
- [Module 1. Kafka architecture: broker, topic, partition, offset](#module-1-kafka-architecture-broker-topic-partition-offset)
- [Module 2. Installing Kafka in Docker and first commands](#module-2-installing-kafka-in-docker-and-first-commands)
- [Module 3. Topics, partitions and keys: design](#module-3-topics-partitions-and-keys-design)
- [Module 4. Producer: acks, idempotence, batching and ordering](#module-4-producer-acks-idempotence-batching-and-ordering)
- [Module 5. Consumers and consumer groups: offsets and rebalancing](#module-5-consumers-and-consumer-groups-offsets-and-rebalancing)
- [Module 6. Delivery guarantees and exactly-once](#module-6-delivery-guarantees-and-exactly-once)
- [Module 7. Storage: retention, segments, log compaction, tiered storage](#module-7-storage-retention-segments-log-compaction-tiered-storage)
- [Module 8. Replication and fault tolerance: ISR and min.insync.replicas](#module-8-replication-and-fault-tolerance-isr-and-mininsyncreplicas)
- [Module 9. Schema Registry and schema evolution](#module-9-schema-registry-and-schema-evolution)
- [Module 10. Kafka Connect and CDC with Debezium](#module-10-kafka-connect-and-cdc-with-debezium)
- [Module 11. Kafka Streams](#module-11-kafka-streams)
- [Module 12. Share groups: queues in Kafka](#module-12-share-groups-queues-in-kafka)
- [Module 13. Error handling: retry topics, DLQs and poison pills](#module-13-error-handling-retry-topics-dlqs-and-poison-pills)
- [Module 14. Multiple data centres: MirrorMaker 2](#module-14-multiple-data-centres-mirrormaker-2)
- [Module 15. Performance and tuning](#module-15-performance-and-tuning)
- [Module 16. Monitoring: lag, under-replicated partitions, alerts](#module-16-monitoring-lag-under-replicated-partitions-alerts)
- [Module 17. Security: TLS, SASL, ACLs](#module-17-security-tls-sasl-acls)
- [Module 18. Kafka in production: Kubernetes, operations, upgrades](#module-18-kafka-in-production-kubernetes-operations-upgrades)
- [Module 19. Capstone project: an event-driven online shop](#module-19-capstone-project-an-event-driven-online-shop)
- [Kafka CLI cheat sheet](#kafka-cli-cheat-sheet)
- [Configuration cheat sheet](#configuration-cheat-sheet)
- [Kafka interview questions with answers](#kafka-interview-questions-with-answers)
- [FAQ](#faq)
- [Kafka glossary](#kafka-glossary)
- [Official sources and what to read next](#official-sources-and-what-to-read-next)

---

## How to take this course

1. **Go in order.** Modules 0–6 are the foundation. Without partitions, offsets and consumer groups everything else looks like magic.
2. **Run every command.** Kafka is learned by hand. Reading about rebalancing and watching partitions move between consumers in `kafka-consumer-groups.sh --describe` are different levels of understanding.
3. **Break the cluster.** Stop brokers, kill consumers mid-processing, fill the disk. That is how production experience appears.
4. **Answer the questions at the end of each module** out loud, as if you were in an interview.
5. **Do the capstone project.** It pulls every topic into one system.

**What to install:** Docker and Docker Compose, Git, any IDE. Java 17+ for the Java examples, Go 1.26+ for Go (the current franz-go requires it), Python 3.10+ for Python.

**Version:** all examples target Apache Kafka 4.3, image `apache/kafka:4.3.1`. Since 4.0 Kafka runs **only in KRaft mode**: ZooKeeper has been removed entirely, not merely deprecated.

**Runnable examples:** the cluster, a smoke test of the CLI commands, Go and Python code and integration tests for the course's claims live in [`examples/`](examples/). CI runs them every week against 4.3.1 and the newest Kafka image, so if a release changes behaviour described here, the build goes red.

---

# Module 0. What Kafka is and why you need it

## 0.1 Kafka in plain words

**Apache Kafka** is a distributed **event log** (commit log). Services append events to the end of the log, and other services read them at their own pace, each from its own position.

Kafka does three things:

1. **Accepts a stream of events** from producers and writes it durably to the disks of several servers.
2. **Keeps events** for as long as you configure: hours, days, years, or forever (the latest value per key).
3. **Serves events** to any number of consumers; each reads independently and can re-read history from any point.

The core idea of Kafka is **a log, not a queue**. In a classic queue a message disappears once it is processed. In Kafka the message stays in the log, and the consumer simply remembers how far it has read (its **offset**). That is why ten different systems can read the same stream without interfering, and why a new system can read the whole history from the start.

Kafka was created at LinkedIn around 2010 (Jay Kreps, Neha Narkhede, Jun Rao) and donated to the Apache Software Foundation in 2011. It is written in Java and Scala. Today it is the de facto standard for streaming data: event buses, CDC, analytics pipelines and system integration are built on it.

## 0.2 The problem Kafka solves

Picture a company where order data is needed by many systems:

```
                   +--> Billing
                   |
Order Service -----+--> Warehouse
                   |
                   +--> Analytics (data warehouse)
                   |
                   +--> Search (Elasticsearch)
                   |
                   +--> Fraud detection
                   |
                   +--> Notifications
```

Without a shared bus every pair of systems integrates separately: REST calls, cron exports, table copies. You end up with N×M "spaghetti" connections:

| Question | The problem with point-to-point integration |
|---|---|
| Analytics wants last year's data | Someone writes yet another export from the database |
| Search fell an hour behind after an outage | Nobody knows where to resume from |
| A new system appears | The order service has to change |
| The notification service is down | The order fails or the event is lost |
| A traffic spike | Every consumer falls over in a cascade |

## 0.3 The same system with Kafka

```
Order Service
     |
     | produce -> topic "orders"
     v
+-----------------------------------------------+
|  topic orders   [0][1][2][3][4][5][6][7] ...  |   <- a log on disk,
+-----------------------------------------------+      kept for 7 days
     |          |           |            |
     v          v           v            v
  Billing   Warehouse   Analytics      Search
 offset=7   offset=7    offset=3      offset=0
                        (catching up) (re-reading
                                       all history)
```

What you gain:

- **Decoupling in time.** A consumer can be down for an hour: when it comes back, it resumes from the offset where it stopped.
- **Independent consumers.** Each system reads at its own pace and doesn't affect the others.
- **History.** A new system or a fixed bug? Re-read the events from the start.
- **Scaling.** A topic is split into partitions, and several instances of a service read them in parallel.
- **A buffer for spikes.** Kafka accepts the stream faster than it can be processed and keeps it while consumers catch up.

## 0.4 Log vs queue: the most important thing to understand about Kafka

This is **the key idea of the whole course**.

```
QUEUE (RabbitMQ, SQS)                  LOG (Kafka)
---------------------------            ---------------------------------
message deleted after ack              message stays until retention
one consumer per message               any number of independent readers
broker tracks what was delivered       consumer tracks its own offset
no re-reading                          re-read from any position
ordering blurs with N workers          strict ordering within a partition
parallelism = number of workers        parallelism = number of partitions
```

| Question | Queue | Kafka |
|---|---|---|
| What happens after processing | The message is deleted | The message stays, the offset moves |
| How many systems can read the stream | Each needs its own copy of the queue | As many as you like, one consumer group per system |
| Can you re-read yesterday | No | Yes, by resetting the offset |
| Ordering | Usually not guaranteed with several workers | Guaranteed within a partition |
| Acknowledging an individual message | Yes (ack/nack) | No, you commit a position (since 4.2 there are share groups, module 12) |
| Scaling processing | Add workers | Add partitions and consumers |

**Rule of thumb:** Kafka when the event stream matters as history and several systems read it. A classic queue when you need to hand tasks to workers and forget them. Share groups (module 12) partly cover the second case inside Kafka.

## 0.5 What an event (record) is

```
Topic:     orders
Partition: 2
Offset:    18734
Timestamp: 2026-09-15T10:00:00.123Z
Key:       "order-123"
Headers:   event-type=OrderCreated
           trace-id=4bf92f3577b34da6
           schema-version=2
Value:     {"order_id":"order-123","user_id":"user-42","amount":4990}
```

- **Key** is optional. It decides which partition the message lands in, and therefore ordering (module 3). Usually it is the id of an entity: an order, a user, an account.
- **Value** is just bytes. Kafka neither knows nor checks the format: JSON, Avro, Protobuf, your choice (module 9).
- **Headers** carry metadata: event type, trace id, schema version.
- **Timestamp** is either the creation time (set by the producer by default) or the time the broker appended it, depending on the topic setting.
- **Offset** is the sequence number of the message within its partition. The broker assigns it and it never changes.

The default maximum message size is about **1 MB** (`message.max.bytes` on the broker, `max.request.size` on the producer). You can raise it, but you shouldn't: large files go to object storage and the event carries a reference.

## 0.6 Kafka vs RabbitMQ, NATS and Pulsar

| Criterion | Apache Kafka | RabbitMQ | NATS JetStream | Apache Pulsar |
|---|---|---|---|---|
| Model | Distributed log | Queue broker (AMQP) | Subjects + streams | Log with separate storage (BookKeeper) |
| What you deploy | JVM cluster (KRaft) | Erlang cluster | One Go binary | Brokers + BookKeeper + metadata store |
| Storage and replay | Yes, it's the core product | Limited (Streams) | Yes | Yes, plus tiered storage |
| Ordering | Within a partition | Within a queue | Within a stream and subject | Within a partition |
| Scaling reads | Partitions | Workers on a queue | Workers on a consumer | Partitions + shared subscriptions |
| Ecosystem | Connect, Streams, Debezium, Schema Registry, hundreds of connectors | Plugins, shovel, federation | Smaller but growing | Pulsar IO, Functions |
| Strength | High throughput, history, data integration | Flexible routing, priorities | Simplicity, low latency, edge | Multi-tenancy, geo-replication |

**An honest word on choosing:** if you need event history, CDC, analytics pipelines and ready-made connectors to dozens of systems, Kafka's ecosystem is almost unrivalled. If you need complex routing, priorities and per-message TTL, RabbitMQ is more flexible. For lightweight RPC and edge deployments, look at NATS. Many companies run two systems: Kafka for data streams and something lighter for commands and RPC.

### Kafka and HTTP together

Kafka doesn't replace HTTP:

```
Client --HTTP--> Order API --(saves the order, returns 201)--> Client
                     |
                     +--OrderCreated event--> Kafka --> everyone else
```

HTTP is for when the user is waiting for an answer. Kafka is for asynchronous event distribution and data integration. Kafka is **not designed for request-reply**: you can build RPC on top of it, but it is awkward and slow.

## 0.7 Where Kafka is used

| Scenario | How Kafka is applied |
|---|---|
| **Event-driven microservices** | Domain events, sagas, outbox |
| **CDC (Change Data Capture)** | Debezium reads the database WAL and publishes changes to Kafka |
| **Analytics pipelines** | Events → Kafka → ClickHouse, Snowflake, S3, data lake |
| **Logs and metrics** | Logs from every server into one bus |
| **Stream processing** | Kafka Streams, Flink: aggregates, windows, enrichment in real time |
| **System integration** | Kafka Connect: hundreds of ready connectors to databases, queues, clouds |
| **Event sourcing** | The event log as the source of truth, compacted topics as snapshots |
| **Task queues** | Share groups (since 4.2) or classic consumer groups |

## 0.8 When you don't need Kafka

- You have one monolith and a couple of background jobs: a PostgreSQL or Redis queue is enough.
- You need a synchronous answer for the user: use HTTP or gRPC.
- You need low-latency RPC between services: Kafka is awkward for that.
- You need header-based routing and message priorities: RabbitMQ is more flexible.
- You want transactions between a database and the broker out of the box: nobody offers that; design an outbox and idempotency.
- The team isn't ready to operate a distributed JVM system: consider a managed service.

## 0.9 What Kafka will NOT do for you

Kafka gives infrastructure guarantees. Correctness is still yours:

- idempotent processing in the application;
- event schemas and their evolution (Schema Registry helps, but the contracts are yours);
- retry strategy and handling "poison" messages;
- choosing keys and partition counts (mistakes are expensive to fix later);
- monitoring lag and disks;
- access control and quotas.

### Self-check questions

1. How does a log differ from a queue, and what follows for consumers?
2. What is an offset and who stores it?
3. Why can two systems read one topic without interfering?
4. When would you pick RabbitMQ or NATS over Kafka?

---

# Module 1. Kafka architecture: broker, topic, partition, offset

## 1.1 The main hierarchy

```
Kafka cluster
  ├── Controllers (KRaft quorum: hold the cluster metadata)
  └── Brokers (hold the data and serve clients)
        └── Topic (a logical stream name, e.g. orders)
              └── Partition 0, 1, 2 ... (an ordered log)
                    ├── Leader (one replica; writes and reads go through it)
                    └── Followers (copies on other brokers)
                          └── Segments on disk
                                └── Records (key, value, headers, offset)

and separately, on the client side:
  Consumer group (e.g. billing)
    └── Consumers, each assigned its own partitions
          └── A committed offset for each partition
```

Memorise this picture. The whole course hangs on it.

## 1.2 Core components

| Component | What it is | Analogy |
|---|---|---|
| **Record** | An event: key, value, headers, timestamp | A line in a journal |
| **Topic** | A named stream of events | A table |
| **Partition** | An ordered, immutable log inside a topic | A table shard |
| **Offset** | The position of a record in a partition | A row number |
| **Broker** | A server that stores partitions | A database server |
| **Controller** | A KRaft quorum node that manages metadata | The cluster master |
| **Replica** | A copy of a partition on another broker | A database replica |
| **Leader** | The replica that accepts writes | Primary |
| **ISR** | In-Sync Replicas: replicas that are not behind the leader | Synchronous replicas |
| **Producer** | A client that writes events | Writer |
| **Consumer** | A client that reads events | Reader |
| **Consumer group** | Consumers sharing partitions between them | A worker pool |

## 1.3 The partition is the unit of everything

A topic is just a name. The real work happens in **partitions**:

```
topic orders (3 partitions)

partition 0:  [0][1][2][3][4][5][6] ->  always appended at the end
partition 1:  [0][1][2][3]          ->
partition 2:  [0][1][2][3][4]       ->
```

A partition is the unit of:

- **ordering**: order is guaranteed only within one partition;
- **parallelism**: in a group, one partition is read by exactly one consumer;
- **replication**: every partition has its own leader and followers;
- **storage**: a partition is a set of segment files in a broker directory.

An offset is unique **only within a partition**. "Offset 3" exists in each of the three partitions, and those are three different messages.

## 1.4 How a message picks a partition

```
has a key        -> partition = hash(key) % number_of_partitions   (murmur2 in the Java client)
no key           -> sticky partitioner: a batch goes to one partition,
                    the next batch to another (even and efficient)
partition given  -> that partition
```

This is Kafka's key rule: **all events with the same key land in the same partition and are read in order**. If the key is `order_id`, then `OrderCreated`, `OrderPaid`, `OrderShipped` for one order reach the consumer strictly in that order.

The flip side: **if you add partitions, keys move to other partitions**, and ordering between "old" and "new" events of the same key breaks for the transition period (module 3).

## 1.5 Replication: leader, followers, ISR

```
topic orders, replication.factor=3

             broker-1          broker-2          broker-3
partition 0  [LEADER]  ----->  [follower]  ---> [follower]
partition 1  [follower]        [LEADER]         [follower]
partition 2  [follower]        [follower]       [LEADER]
```

- The producer writes **only to the leader** of a partition. Followers fetch data from the leader themselves.
- **ISR** (In-Sync Replicas) are replicas that haven't fallen behind the leader for longer than `replica.lag.time.max.ms` (30 seconds by default).
- With `acks=all` the leader confirms a write only when **every replica in the ISR** has it.
- `min.insync.replicas=2` means: if fewer than two replicas are left in the ISR, `acks=all` writes are rejected. Kafka refuses data it might lose.
- If the leader dies, the controller elects a new leader **from the ISR**.

Partition leaders are spread across brokers, so the load is shared by all nodes. Details in module 8.

## 1.6 KRaft: a cluster without ZooKeeper

Up to 3.x the cluster metadata (which topics exist, where leaders are, which settings apply) lived in ZooKeeper. Since Kafka 4.0 ZooKeeper is **gone** and Kafka stores metadata itself in **KRaft** (Kafka Raft) mode:

```
+---------------- KRaft controller quorum -----------------+
|  controller-1 (active)   controller-2    controller-3    |
|  metadata log __cluster_metadata replicated via Raft     |
+----------------------------------------------------------+
          |  brokers receive metadata changes
          v
  broker-1        broker-2        broker-3        ...
```

A node's roles come from `process.roles`:

| Mode | `process.roles` | When |
|---|---|---|
| Combined | `broker,controller` | Development, small clusters, this course |
| Separate | `broker` or `controller` | Production: 3 (or 5) dedicated controllers + N brokers |

The controller quorum needs a majority: 3 controllers survive one failure, 5 survive two.

## 1.7 Consumer groups: how several instances share the work

```
topic orders: 4 partitions

consumer group "billing" (2 instances)      consumer group "analytics" (1 instance)
  consumer A <- p0, p1                         consumer X <- p0, p1, p2, p3
  consumer B <- p2, p3

- every group receives ALL messages of the topic;
- within a group each partition is assigned to exactly one consumer;
- every group has its own offsets.
```

Consequences:

- **a group's maximum parallelism equals the number of partitions**. A fifth consumer with four partitions sits idle;
- when a consumer joins or leaves, partitions are redistributed: that's a **rebalance** (module 5);
- a group's committed offsets live in the internal topic `__consumer_offsets`.

## 1.8 The numbers that describe a partition and a group

| Term | Meaning |
|---|---|
| **Log start offset** | The oldest offset still stored (retention deleted the older ones) |
| **Log end offset (LEO)** | The offset the next record will get |
| **High watermark (HW)** | Up to which offset data is in every ISR replica; consumers only see up to HW |
| **Committed offset** | How far the group has reported processing |
| **Lag** | `LEO − committed offset`: how many messages the group hasn't processed yet |

**Lag is the key consumer metric.** Growing lag means the consumer can't keep up or has stopped.

## 1.9 Your first mental model

```
Writing:
  producer -> computes the partition from the key -> sends a batch to the partition leader ->
  leader appends to its log -> ISR followers fetch a copy ->
  with acks=all the leader answers the producer -> the high watermark moves

Reading:
  a consumer in a group gets its assigned partitions ->
  reads from the committed offset -> processes -> commits the new offset ->
  the message stays in the log until retention deletes it
```

### Self-check questions

1. Why is ordering in Kafka guaranteed only within a partition?
2. What happens to the ordering of one key's events if you add partitions?
3. What is the ISR and how does it relate to `acks=all`?
4. Why does a fifth consumer in a group sit idle with four partitions?
5. Why doesn't Kafka 4.x need ZooKeeper?

---

# Module 2. Installing Kafka in Docker and first commands

## 2.1 The fastest way to run Kafka

A single node in combined mode (broker and controller in one process):

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:4.3.1
```

The official `apache/kafka` image generates a single-node KRaft config on its own. Check it:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker logs kafka | grep -i "Kafka Server started"
```

All CLI tools live in `/opt/kafka/bin` inside the container. To avoid typing the long path, define a function:

```bash
kt() { docker exec -i kafka-1 /opt/kafka/bin/"$@"; }
# example: kt kafka-topics.sh --bootstrap-server kafka-1:19092 --list
```

For the single node from 2.1, replace `kafka-1` with `kafka`.

## 2.2 A three-node cluster in Docker Compose

For learning you need a real cluster: three nodes, each both broker and controller. Then you can stop nodes and watch leaders being re-elected.

Create a directory:

```bash
mkdir kafka-course && cd kafka-course
```

`docker-compose.yml`:

```yaml
x-kafka-common: &kafka-common
  image: apache/kafka:4.3.1
  restart: unless-stopped
  environment: &kafka-env
    # the same cluster ID on every node (any base64 string of 16 bytes)
    CLUSTER_ID: "4L6g3nShT-eMCtK--X86sw"
    KAFKA_PROCESS_ROLES: "broker,controller"
    KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093"
    KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
    KAFKA_INTER_BROKER_LISTENER_NAME: "INTERNAL"
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT"
    # safe defaults for a three-node cluster
    KAFKA_DEFAULT_REPLICATION_FACTOR: 3
    KAFKA_MIN_INSYNC_REPLICAS: 2
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
    KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
    KAFKA_NUM_PARTITIONS: 3
    # without this the data ends up in /tmp inside the container, not on the volume
    KAFKA_LOG_DIRS: "/var/lib/kafka/data"

services:
  kafka-1:
    <<: *kafka-common
    container_name: kafka-1
    ports: ["9092:9092"]
    environment:
      <<: *kafka-env
      KAFKA_NODE_ID: 1
      KAFKA_LISTENERS: "INTERNAL://:19092,EXTERNAL://:9092,CONTROLLER://:9093"
      KAFKA_ADVERTISED_LISTENERS: "INTERNAL://kafka-1:19092,EXTERNAL://localhost:9092"
    volumes: [kafka1-data:/var/lib/kafka/data]

  kafka-2:
    <<: *kafka-common
    container_name: kafka-2
    ports: ["9192:9192"]
    environment:
      <<: *kafka-env
      KAFKA_NODE_ID: 2
      KAFKA_LISTENERS: "INTERNAL://:19092,EXTERNAL://:9192,CONTROLLER://:9093"
      KAFKA_ADVERTISED_LISTENERS: "INTERNAL://kafka-2:19092,EXTERNAL://localhost:9192"
    volumes: [kafka2-data:/var/lib/kafka/data]

  kafka-3:
    <<: *kafka-common
    container_name: kafka-3
    ports: ["9292:9292"]
    environment:
      <<: *kafka-env
      KAFKA_NODE_ID: 3
      KAFKA_LISTENERS: "INTERNAL://:19092,EXTERNAL://:9292,CONTROLLER://:9093"
      KAFKA_ADVERTISED_LISTENERS: "INTERNAL://kafka-3:19092,EXTERNAL://localhost:9292"
    volumes: [kafka3-data:/var/lib/kafka/data]

volumes:
  kafka1-data:
  kafka2-data:
  kafka3-data:
```

**Why three listeners.** This is the most confusing part of Kafka configuration, so let's go through it:

| Listener | Who connects | Address the broker advertises |
|---|---|---|
| `CONTROLLER` | Controllers among themselves (Raft) | not advertised to clients |
| `INTERNAL` | Brokers among themselves and clients inside the Docker network | `kafka-N:19092` |
| `EXTERNAL` | Your applications on the host | `localhost:9092/9192/9292` |

A Kafka client connects to any address from `bootstrap.servers`, receives **metadata** listing the brokers and their `advertised.listeners`, and from then on talks **directly to partition leaders** at those addresses. If a broker advertises an address the client can't reach (for example, a container name for an app on the host), the first connection succeeds, but produce and consume time out. This is mistake number one when running Kafka in Docker.

Start the cluster:

```bash
docker compose up -d
docker compose ps
```

Check the controller quorum:

```bash
kt kafka-metadata-quorum.sh --bootstrap-server kafka-1:19092 describe --status
kt kafka-metadata-quorum.sh --bootstrap-server kafka-1:19092 describe --replication
```

You'll see `LeaderId` (the active controller), `CurrentVoters` with three nodes, and how far each node lags behind the metadata leader.

## 2.3 Your first topic

```bash
kt kafka-topics.sh --bootstrap-server kafka-1:19092 \
  --create --topic orders \
  --partitions 3 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=604800000

kt kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --topic orders
```

The `--describe` output:

```
Topic: orders  PartitionCount: 3  ReplicationFactor: 3  Configs: min.insync.replicas=2,retention.ms=604800000
  Topic: orders  Partition: 0  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
  Topic: orders  Partition: 1  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
  Topic: orders  Partition: 2  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
```

- **Leader** is the broker currently accepting writes for this partition;
- **Replicas** lists where the copies live (the first is the "preferred" leader);
- **Isr** lists which replicas are in sync right now.

## 2.4 Your first message

Terminal 1, a consumer:

```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:19092 --topic orders \
  --from-beginning \
  --property print.key=true --property print.partition=true --property print.offset=true
```

Terminal 2, a producer with keys:

```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka-1:19092 --topic orders \
  --property parse.key=true --property key.separator=:
```

Type lines of the form `key:value`:

```
order-1:{"event":"OrderCreated","order_id":"order-1"}
order-2:{"event":"OrderCreated","order_id":"order-2"}
order-1:{"event":"OrderPaid","order_id":"order-1"}
order-1:{"event":"OrderShipped","order_id":"order-1"}
```

Look at the partition number in the consumer output: **all `order-1` events ended up in one partition and arrived in order**.

**The experiment that separates understanding from "I tried it".** Stop the consumer, send two more messages and start the consumer again, this time without `--from-beginning`. A consumer without a group starts at the end and won't see what it missed. Now run it with `--group test` twice: the second run continues where the first stopped. The read position belongs to the group, not to the topic.

## 2.5 Consumer groups from the command line

```bash
# run the same consumer in the billing group in two terminals
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:19092 --topic orders --group billing

# in a third terminal, see how the group split the partitions
kt kafka-consumer-groups.sh --bootstrap-server kafka-1:19092 --describe --group billing
```

```
GROUP    TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID       HOST
billing  orders  0          3               3               0    console-xxxx-1    /172.18.0.5
billing  orders  1          1               1               0    console-xxxx-1    /172.18.0.5
billing  orders  2          2               2               0    console-yyyy-2    /172.18.0.5
```

Stop one of the consumers and run `--describe` again: its partitions moved to the other one. That is a rebalance.

## 2.6 Where Kafka stores data

```bash
docker exec kafka-1 ls /var/lib/kafka/data
docker exec kafka-1 ls /var/lib/kafka/data/orders-0
```

```
/var/lib/kafka/data/
├── __cluster_metadata-0/         # KRaft metadata log
├── __consumer_offsets-0 ... -49/ # consumer group offsets
├── meta.properties               # node.id and cluster.id of this node
└── orders-0/                     # partition directory: <topic>-<number>
    ├── 00000000000000000000.log        # segment with data
    ├── 00000000000000000000.index      # offset -> file position
    ├── 00000000000000000000.timeindex  # time -> offset
    └── leader-epoch-checkpoint
```

The data directory must live on a persistent volume. A Kubernetes pod without a persistent volume comes back empty after a restart and starts re-replicating from other brokers, or loses data if there are no other replicas.

## 2.7 The most common startup mistakes

| Symptom | Cause | Fix |
|---|---|---|
| The app connects, but produce times out | `advertised.listeners` advertises an address the client can't reach | A separate listener for external clients with the right host and port |
| `NOT_ENOUGH_REPLICAS` on write | Fewer replicas in the ISR than `min.insync.replicas` | Check the brokers are alive; for a single node use `min.insync.replicas=1` |
| A topic appears by itself with one partition | `auto.create.topics.enable` is on | Turn it off and create topics explicitly |
| A consumer reads nothing | The group already committed an offset at the end, or `auto.offset.reset=latest` | Check `kafka-consumer-groups.sh --describe`, reset the offset |
| A node won't join the quorum | Different `CLUSTER_ID` or wrong `controller.quorum.voters` | Same ID and voter list on every node |
| `The Cluster ID ... doesn't match stored clusterId` | The volume belongs to another cluster | Delete the volume or restore the old `CLUSTER_ID` |

### Practice

1. Bring up the three-node cluster and check the quorum with `kafka-metadata-quorum.sh`.
2. Create the `orders` topic with three partitions and `replication-factor 3`, send 10 keyed messages and confirm messages with one key share a partition.
3. Stop the broker that leads partition 0 (`docker stop kafka-N`) and look at `--describe`: who became leader, how did the ISR change? Keep writing and reading.
4. Run three consumers in one group on a three-partition topic, then a fourth. What does it get?

---

# Module 3. Topics, partitions and keys: design

## 3.1 Naming topics

Topic names are your system's API. A topic can't be renamed: you can only create a new one and move every producer and consumer to it.

**A template that works:**

```
<domain>.<entity>.<kind>[.<version>]

shop.orders.events
shop.payments.events
shop.orders.commands
crm.customers.cdc
shop.orders.events.v2
```

| Rule | Why |
|---|---|
| Dots or dashes, but pick one | Kafka warns that `.` and `_` collide in metric names |
| Domain first | ACLs and quotas are easy to grant by prefix (`shop.`) |
| Separate events from commands | Different guarantees and different consumers |
| Version in the name only for incompatible schema changes | Compatible changes go through Schema Registry (module 9) |
| Don't create a topic per customer or tenant | Thousands of topics = thousands of partitions = load on the controller |

## 3.2 One topic or several

A frequent question: should `OrderCreated`, `OrderPaid`, `OrderShipped` go into one topic or three?

| | One topic per entity | A topic per event type |
|---|---|---|
| Ordering of one entity's events | **Preserved** (one key, one partition) | Lost across topics |
| A consumer needs only one type | Reads everything and filters | Reads only its own |
| Schemas | Several types per topic (union in Avro/Protobuf) | One schema per topic |

**Rule:** if a consumer cares about the order of one entity's events (order created → paid → shipped), put them **in one topic with one key**. Split by type when events are independent.

## 3.3 Choosing a key

The key determines the partition, and the partition determines ordering and load distribution.

| Key | Ordering | Distribution | When |
|---|---|---|---|
| `order_id` | Per order | Good, many orders | Order lifecycle events |
| `user_id` | Per user | Good, unless there are "super users" | User actions |
| `tenant_id` | Per customer | **Poor** with unequal customers | Dangerous: a big customer = a hot partition |
| `country` | Per country | **Very poor**: few values | Almost never |
| no key | None | Even | Logs, metrics, independent events |

A **hot partition** is the main harm of a bad key. If 40% of traffic belongs to one customer, 40% of messages land in one partition and its consumer becomes the bottleneck, no matter how many partitions the topic has.

If you need ordering per large customer but one partition can't cope, use a composite key (`tenant_id + order_id`): ordering is kept per order, not per whole customer.

## 3.4 How many partitions

The partition count is one of the most expensive decisions: you can increase it but never decrease it, and increasing it breaks the "key → partition" mapping.

An estimate:

```
partitions >= max( target_throughput / throughput_of_one_consumer,
                   target_throughput / write_throughput_of_one_partition,
                   expected_max_consumer_instances )

example: you need 30,000 msg/s, one consumer handles 2,000 msg/s
         -> at least 15 partitions; take 24 to leave room for growth
```

Practical starting points:

| Situation | Reasonable start |
|---|---|
| Low traffic, ordering matters | 3–6 |
| A typical service topic | 6–24 |
| A high-throughput stream | 24–100+ |
| Hundreds of thousands of partitions per cluster | Only deliberately: every partition costs memory, files and leader election time |

Too many partitions hurt too: more open files, longer recovery after failures, more metadata, worse producer batching (messages spread thin).

## 3.5 What happens when you add partitions

```bash
kt kafka-topics.sh --bootstrap-server kafka-1:19092 --alter --topic orders --partitions 6
```

```
before: hash("order-1") % 3 = 1  -> partition 1
after:  hash("order-1") % 6 = 4  -> partition 4

old order-1 events sit in partition 1,
new ones go to partition 4 and may be processed BEFORE the old ones
```

No data is moved. So:

- if per-key ordering matters, provision partitions with headroom from the start;
- if you must add partitions, do it at a quiet time and let consumers drain the old partitions first;
- the alternative is a new topic with the right partition count and a migration.

## 3.6 Topic configuration

```bash
kt kafka-topics.sh --bootstrap-server kafka-1:19092 --create --topic shop.orders.events \
  --partitions 12 --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete \
  --config max.message.bytes=1048588 \
  --config compression.type=producer

# change the settings of an existing topic
kt kafka-configs.sh --bootstrap-server kafka-1:19092 \
  --alter --entity-type topics --entity-name shop.orders.events \
  --add-config retention.ms=1209600000

kt kafka-configs.sh --bootstrap-server kafka-1:19092 \
  --describe --entity-type topics --entity-name shop.orders.events
```

| Parameter | Default | Meaning |
|---|---|---|
| `replication.factor` | from the broker | How many copies of each partition; 3 in production |
| `min.insync.replicas` | 1 | Minimum in-sync replicas for `acks=all` writes; 2 in production |
| `retention.ms` | 7 days | How long to keep messages; `-1` means forever |
| `retention.bytes` | -1 | Size limit **per partition** |
| `cleanup.policy` | `delete` | `delete`, `compact` or `compact,delete` (module 7) |
| `max.message.bytes` | ~1 MB | Maximum size of a record batch |
| `compression.type` | `producer` | Keep the producer's compression or recompress |
| `segment.bytes` | 1 GB | Segment size on disk |

## 3.7 Internal topics

| Topic | Purpose |
|---|---|
| `__consumer_offsets` | Committed offsets of consumer groups (50 partitions) |
| `__transaction_state` | State of producer transactions |
| `__share_group_state` | State of share groups (module 12) |
| `__cluster_metadata` | The KRaft metadata log (not visible as a normal topic) |
| `_schemas` | Schema Registry schemas (if you run it) |
| `connect-configs`, `connect-offsets`, `connect-status` | Kafka Connect state |

Never write to them by hand and never delete them.

### Practice

1. Design topics and keys for a delivery service: orders, couriers, statuses, courier GPS positions. Where does ordering matter, and per which entity?
2. Send 100 messages with keys `tenant-1` (90 of them) and `tenant-2..10` (one each). Look at the distribution with `kafka-get-offsets.sh --topic orders`.
3. Add partitions to a topic and send new events for old keys. Which partitions did they land in?

---

# Module 4. Producer: acks, idempotence, batching and ordering

## 4.1 How a producer sends a message

```
send(record)
   |
   v
serialize key/value -> choose a partition -> per-partition buffer (RecordAccumulator)
                                                 |
                                   the batch is full (batch.size)
                                   or linger.ms expired
                                                 v
                           the Sender thread ships batches to partition leaders
                                                 |
                              the leader writes, waits for the ISR (with acks=all)
                                                 v
                              response -> callback / Future completes
```

The key point: `send()` is **asynchronous**. It puts the message in a buffer and returns immediately. A write error arrives later, in the callback or when you call `get()` on the Future. An application that doesn't check the send result loses messages silently.

## 4.2 acks: what "written" means

| `acks` | The leader answers when… | What can be lost |
|---|---|---|
| `0` | Never; the producer doesn't wait | Anything, even with a healthy cluster |
| `1` | It has written locally | Messages not yet copied to followers if the leader dies |
| `all` (`-1`) | Every ISR replica has written | Nothing, as long as ISR ≥ `min.insync.replicas` |

`acks=all` without `min.insync.replicas` is a trap. If the ISR has shrunk to just the leader, `acks=all` means "one leader wrote it", which is effectively `acks=1`. The reliable combination:

```
replication.factor = 3
min.insync.replicas = 2
acks = all
```

Such a topic survives one broker failure with no data loss and no write outage. With two brokers down, writes stop with `NOT_ENOUGH_REPLICAS`, and that is correct: better to refuse than to lose.

## 4.3 The idempotent producer

**Problem:** the producer sent a batch, the leader wrote it, but the response got lost in the network. The producer retries and the log now contains a duplicate.

**Solution:** idempotence. The broker gives the producer a `producer.id`, the producer numbers batches per partition (`sequence number`), and the broker drops a retry with a sequence it has already seen.

```
producer (pid=42)  --batch seq=7-->  leader: written, response lost
producer (pid=42)  --batch seq=7-->  leader: seq 7 already there -> ack without writing again
```

Since Kafka 3.0 this is **on by default**: `enable.idempotence=true`, `acks=all`, `retries=Integer.MAX_VALUE`. Idempotence also preserves ordering with `max.in.flight.requests.per.connection` up to 5.

The limits of idempotence:

- it works **within one producer session**: after a restart the application gets a new `producer.id`, and re-sending "the same" message creates a new message;
- it protects against duplicates from **network retries**, not against your code calling `send()` twice for one event;
- protection across restarts and atomic writes to several partitions need transactions (module 6).

## 4.4 Message ordering

Ordering is guaranteed **within a partition** and **for one producer**, provided:

- the messages share a key (so one partition);
- idempotence is on (the default), or `max.in.flight.requests.per.connection=1`.

Without idempotence and with several requests in flight, reordering is possible: batch 1 fails and is retried, batch 2 is written meanwhile, then the retried batch 1 is written.

Ordering is **not** guaranteed:

- across partitions;
- between different producers writing the same key;
- if the application sends from several threads without synchronising per key.

## 4.5 Batching, compression and throughput

| Parameter | Default | Meaning |
|---|---|---|
| `batch.size` | 16 KB | Maximum batch size per partition |
| `linger.ms` | 5 ms (since Kafka 4.0, previously 0) | How long to wait to fill a batch |
| `compression.type` | `none` | `lz4`, `zstd`, `snappy`, `gzip` |
| `buffer.memory` | 32 MB | The producer's total buffer |
| `max.block.ms` | 60 s | How long `send()` may block when the buffer is full |
| `delivery.timeout.ms` | 120 s | Upper bound for the whole delivery, retries included |
| `request.timeout.ms` | 30 s | Timeout of a single request to a broker |

A typical high-throughput setup:

```
linger.ms=20
batch.size=131072
compression.type=zstd   (or lz4 for less CPU)
```

Compression applies to the whole batch, so bigger batches compress better. The broker stores the batch compressed and the consumer decompresses it: compression saves network, disk and replication.

## 4.6 Producer in Java

```xml
<dependency>
  <groupId>org.apache.kafka</groupId>
  <artifactId>kafka-clients</artifactId>
  <version>4.3.1</version>
</dependency>
```

```java
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class OrderProducer {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9192,localhost:9292");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "order-service");
        // these are the defaults, but spell them out: it documents intent
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    "shop.orders.events", "order-1", "{\"event\":\"OrderCreated\",\"order_id\":\"order-1\"}");
            record.headers().add("event-type", "OrderCreated".getBytes(StandardCharsets.UTF_8));

            // asynchronous: the error MUST be handled in the callback
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("not written: " + exception); // alert, outbox, retry
                } else {
                    System.out.printf("partition=%d offset=%d%n", metadata.partition(), metadata.offset());
                }
            });

            // synchronous: get() throws if the write failed
            RecordMetadata md = producer.send(new ProducerRecord<>(
                    "shop.orders.events", "order-1", "{\"event\":\"OrderPaid\",\"order_id\":\"order-1\"}")).get();
            System.out.printf("paid: partition=%d offset=%d%n", md.partition(), md.offset());

            producer.flush(); // wait until the whole buffer is sent
        }
    }
}
```

**Rules:**

- one `KafkaProducer` per application: it is thread-safe and batches messages from all threads efficiently;
- always check the result: callback or `get()`;
- close the producer on shutdown (`close()` waits for the buffer to drain);
- a synchronous `get()` on every message kills batching: use it only where you really must wait for the write.

## 4.7 Producer in Go (franz-go)

```bash
go get github.com/twmb/franz-go/pkg/kgo
```

```go
package main

import (
	"context"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092", "localhost:9192", "localhost:9292"),
		kgo.ClientID("order-service"),
		kgo.RequiredAcks(kgo.AllISRAcks()), // the default; idempotence is on too
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer cl.Close()

	ctx := context.Background()
	rec := &kgo.Record{
		Topic:   "shop.orders.events",
		Key:     []byte("order-1"),
		Value:   []byte(`{"event":"OrderCreated","order_id":"order-1"}`),
		Headers: []kgo.RecordHeader{{Key: "event-type", Value: []byte("OrderCreated")}},
	}

	// synchronous
	if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		log.Fatal("not written: ", err)
	}
	log.Printf("partition=%d offset=%d", rec.Partition, rec.Offset)

	// asynchronous
	cl.Produce(ctx, &kgo.Record{Topic: "shop.orders.events", Key: []byte("order-1"),
		Value: []byte(`{"event":"OrderPaid","order_id":"order-1"}`)},
		func(r *kgo.Record, err error) {
			if err != nil {
				log.Println("not written:", err)
			}
		})
	if err := cl.Flush(ctx); err != nil {
		log.Fatal(err)
	}
}
```

## 4.8 Producer in Python (confluent-kafka)

```bash
pip install confluent-kafka
```

```python
import json
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "localhost:9092,localhost:9192,localhost:9292",
    "client.id": "order-service",
    "acks": "all",
    "enable.idempotence": True,
    "linger.ms": 10,
    "compression.type": "zstd",
})

def on_delivery(err, msg):
    if err is not None:
        print("not written:", err)          # alert, outbox, retry
    else:
        print(f"partition={msg.partition()} offset={msg.offset()}")

producer.produce(
    "shop.orders.events",
    key="order-1",
    value=json.dumps({"event": "OrderCreated", "order_id": "order-1"}),
    headers={"event-type": "OrderCreated"},
    on_delivery=on_delivery,
)
producer.poll(0)      # serve callbacks
producer.flush(10)    # wait for delivery before exiting
```

In `confluent-kafka`, callbacks are only invoked inside `poll()` or `flush()`. An application that sends in a loop and never calls `poll()` won't learn about errors and will eventually hit a full buffer.

## 4.9 What to do when a write fails

```
send() completed with an error
   |
   +-- TimeoutException (delivery.timeout.ms expired) -> cluster unavailable or overloaded
   +-- NotEnoughReplicasException                     -> ISR < min.insync.replicas: alert
   +-- RecordTooLargeException                        -> message over the limit: don't retry
   +-- SerializationException                         -> a bug in the code: don't retry
   +-- AuthorizationException                         -> no permission: don't retry
```

The client retries transient errors by itself until `delivery.timeout.ms`. If an error reaches your code, retrying it in a loop is usually pointless. The right options: write the event to an outbox table and send it later (module 6), raise an alert, fail the incoming request.

### Self-check questions

1. Why doesn't `acks=all` without `min.insync.replicas=2` protect against data loss?
2. Which duplicates does the idempotent producer prevent, and which not?
3. Why does a synchronous `get()` on every message reduce throughput?
4. What changed about the default `linger.ms` in Kafka 4.0, and why?

---

# Module 5. Consumers and consumer groups: offsets and rebalancing

## 5.1 The consumer loop

```
subscribe(topics) -> poll() -> [records] -> process -> commit offset -> poll() -> ...
                        |
                        +-- inside poll() the client takes part in the group,
                            receives assigned partitions and updates
```

The consumer **pulls** data itself. If it doesn't call `poll()` for longer than `max.poll.interval.ms` (5 minutes by default), the group considers it stuck and gives its partitions to others.

## 5.2 Offsets and commits

A committed offset is **the number of the next message** the group should read. If the group committed 42, after a restart reading resumes at 42.

| Method | How | Risk |
|---|---|---|
| Auto-commit (default) | Every `auto.commit.interval.ms` (5 s) during `poll()` | The commit may happen before processing finishes |
| Manual synchronous | `commitSync()` after processing | Latency on every commit |
| Manual asynchronous | `commitAsync()` | Errors must be handled; a final `commitSync()` on shutdown |

**The order of operations defines the guarantee:**

```
process -> commit   = at-least-once: crash in between -> reprocessing
commit -> process   = at-most-once:  crash in between -> message lost
```

You almost always want the first, plus idempotent processing.

## 5.3 auto.offset.reset: where to start without a commit

| Value | Behaviour |
|---|---|
| `latest` (default) | From the end: only new messages |
| `earliest` | From the oldest retained message |
| `none` | Error if there is no commit |
| `by_duration:PT1H` | From messages of the last hour (KIP-1106, Kafka 4.0+) |

The trap: a new service with `latest` starts, the group has no commits yet, and everything published before its first start is never seen. Most business consumers need `earliest`.

The setting applies **only when there is no commit** (a new group, or the offset was removed by retention). To re-read data for an existing group you have to reset its offsets (5.9).

## 5.4 Consumer in Java

```java
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class BillingConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9192,localhost:9292");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "billing");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // the new consumer group protocol (KIP-848), see 5.7
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));
            consumer.subscribe(List.of("shop.orders.events"));
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, String> r : records) {
                        process(r); // must be idempotent
                    }
                    if (!records.isEmpty()) {
                        consumer.commitSync(); // commit AFTER the whole batch is processed
                    }
                }
            } catch (org.apache.kafka.common.errors.WakeupException e) {
                // normal shutdown
            }
        }
    }

    static void process(ConsumerRecord<String, String> r) {
        System.out.printf("p=%d off=%d key=%s value=%s%n", r.partition(), r.offset(), r.key(), r.value());
    }
}
```

`KafkaConsumer` is **not thread-safe**: one instance, one thread. The only method you may call from another thread is `wakeup()`, which interrupts `poll()` for a clean shutdown.

## 5.5 Consumer in Go (franz-go)

```go
cl, err := kgo.NewClient(
	kgo.SeedBrokers("localhost:9092", "localhost:9192", "localhost:9292"),
	kgo.ConsumerGroup("billing"),
	kgo.ConsumeTopics("shop.orders.events"),
	kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), // same as earliest
	kgo.DisableAutoCommit(),
	kgo.BlockRebalanceOnPoll(), // no rebalance in the middle of a batch
)
if err != nil {
	log.Fatal(err)
}
defer cl.Close()

for {
	fetches := cl.PollFetches(ctx)
	if fetches.IsClientClosed() || ctx.Err() != nil {
		return
	}
	fetches.EachError(func(topic string, p int32, err error) {
		log.Printf("fetch error %s/%d: %v", topic, p, err)
	})
	fetches.EachRecord(func(r *kgo.Record) {
		process(r) // idempotent
	})
	if err := cl.CommitUncommittedOffsets(ctx); err != nil {
		log.Println("commit:", err)
	}
	cl.AllowRebalance() // batch processed and committed, now it may happen
}
```

## 5.6 Consumer in Python (confluent-kafka)

```python
from confluent_kafka import Consumer, KafkaException

consumer = Consumer({
    "bootstrap.servers": "localhost:9092,localhost:9192,localhost:9292",
    "group.id": "billing",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})
consumer.subscribe(["shop.orders.events"])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        process(msg)                                   # idempotent
        consumer.commit(message=msg, asynchronous=False)
finally:
    consumer.close()   # leave the group right away, not after a timeout
```

Committing after every message is simple but slow. On large streams commit in batches: every N messages or once a second.

## 5.7 Rebalancing and consumer group protocols

A **rebalance** redistributes partitions among a group's consumers. It happens when a consumer joins, leaves, crashes or misses a `poll()` deadline, and when the partition count changes.

### The classic protocol

Historically the **clients** managed the group: one consumer (the "group leader") computed the assignment and the broker coordinator merely handed it out.

```
eager (old):          everyone gives up all partitions -> the whole group pauses -> new assignment
cooperative sticky:   only the partitions that move are given up, the rest keep working
```

With the classic protocol use `partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor`: it avoids stopping the whole group on every change.

### The new protocol (KIP-848)

Since Kafka 4.0 the **new consumer group protocol** is generally available: the **broker coordinator** computes assignments and applies changes incrementally, without a global synchronisation of all members.

```properties
group.protocol=consumer
# the assignor now runs on the server: uniform (default) or range
group.remote.assignor=uniform
```

What changes for you:

- rebalances are faster and don't stop the whole group;
- `session.timeout.ms` and `heartbeat.interval.ms` are set on the broker (`group.consumer.session.timeout.ms`, `group.consumer.heartbeat.interval.ms`), not in the client;
- `partition.assignment.strategy` on the client is no longer used.

In Kafka 4.3 the Java consumer logs a warning when it uses the classic protocol and recommends switching: `classic` is being prepared for removal. If you don't use Java, check KIP-848 support in your client library.

## 5.8 Avoiding unnecessary rebalances

| Cause | Fix |
|---|---|
| Processing a batch takes longer than `max.poll.interval.ms` | Lower `max.poll.records` or speed up processing; move heavy work out of the loop carefully, without losing control of commits |
| A deployment restarts every pod in turn | **Static membership**: a unique, stable `group.instance.id` per instance (e.g. the pod name in a StatefulSet); a restart within the session timeout doesn't trigger a rebalance |
| A consumer dies without closing | Always `close()` on shutdown: the consumer leaves the group immediately |
| Long GC pauses | JVM tuning, less memory per batch |

## 5.9 Managing groups and offsets from the CLI

```bash
# list groups and their state
kt kafka-consumer-groups.sh --bootstrap-server kafka-1:19092 --list
kt kafka-consumer-groups.sh --bootstrap-server kafka-1:19092 --describe --group billing
kt kafka-consumer-groups.sh --bootstrap-server kafka-1:19092 --describe --group billing --members

# reset offsets: the group must be inactive (all consumers stopped)
kt kafka-consumer-groups.sh --bootstrap-server kafka-1:19092 --group billing \
  --topic shop.orders.events --reset-offsets --to-earliest --dry-run
kt kafka-consumer-groups.sh --bootstrap-server kafka-1:19092 --group billing \
  --topic shop.orders.events --reset-offsets --to-datetime 2026-09-15T00:00:00.000 --execute

# delete the group
kt kafka-consumer-groups.sh --bootstrap-server kafka-1:19092 --delete --group billing
```

Reset options: `--to-earliest`, `--to-latest`, `--to-offset N`, `--shift-by -100`, `--to-datetime`, `--by-duration PT1H`. Always run with `--dry-run` first.

## 5.10 Scaling consumption

```
topic: 12 partitions

1 instance    -> reads all 12
4 instances   -> 3 partitions each
12 instances  -> one each
16 instances  -> 12 work, 4 sit idle
```

If processing a single message is slow (an external API, heavy logic) and partitions are few, there are two ways out:

1. **More partitions**, but that changes the key mapping (module 3).
2. **Parallel processing inside the consumer** while keeping per-key order: split the batch into sub-streams by key and commit only when every record up to an offset is done. Libraries such as Confluent Parallel Consumer do this. For work where order doesn't matter, use share groups (module 12).

### Self-check questions

1. What exactly does a committed offset store: the last processed message or the next one?
2. Why can auto-commit lose messages?
3. When does `auto.offset.reset` apply, and why doesn't it help re-read data for an existing group?
4. How does the new consumer group protocol differ from the classic one?
5. What is `group.instance.id` for?

---

# Module 6. Delivery guarantees and exactly-once

## 6.1 Where duplicates and losses come from

| Scenario | Result | Protection |
|---|---|---|
| `acks=0` or `acks=1` and the leader died | Loss | `acks=all` + `min.insync.replicas=2` |
| A network retry of a send | Duplicate in the log | Idempotent producer (default) |
| The application restarted and re-sent | Duplicate in the log | Transactional producer or consumer-side idempotency |
| Crash between the DB write and the Kafka send | Lost event | Transactional outbox |
| Sent to Kafka, then the DB transaction rolled back | Phantom event | Transactional outbox |
| The consumer processed but crashed before committing | Reprocessing | Idempotent consumer |
| The consumer committed before processing and crashed | Loss | Commit after processing |
| Processing took longer than `max.poll.interval.ms` | Rebalance and reprocessing | Smaller batches, faster processing |
| `unclean.leader.election.enable=true` | Acknowledged writes lost | Keep it `false` (module 8) |

## 6.2 The three semantics

```
AT-MOST-ONCE
  commit offset -> process
  crash in between -> message lost

AT-LEAST-ONCE (the standard)
  process -> commit offset
  crash in between -> reprocessing

EXACTLY-ONCE
  inside Kafka: transactions (read -> process -> write -> commit offset atomically)
  with an external system: at-least-once + an idempotent write
```

**Important:** exactly-once in Kafka honestly covers the **Kafka → processing → Kafka** case. As soon as the result goes to an external system (a database, an API, an email), that system has to be idempotent.

## 6.3 Transactions

A transactional producer can atomically write messages to several partitions and topics **together with the consumer's offset commit**:

```
consume from orders -> compute -> produce to invoices + produce to audit + commit offset of orders
                       \________________ one transaction: all or nothing ________________/
```

Key settings:

| Where | Parameter | Meaning |
|---|---|---|
| Producer | `transactional.id` | A stable id per instance: survives restarts and fences off zombie instances with the same id |
| Consumer | `isolation.level=read_committed` | See only messages of committed transactions |
| Broker | `transaction.state.log.replication.factor=3`, `transaction.state.log.min.isr=2` | Durable storage of transaction state |

By default `isolation.level=read_uncommitted`: the consumer also sees messages from open or aborted transactions. If you write transactionally, every consumer must read with `read_committed`, or the point is lost.

## 6.4 Consume-transform-produce in Java

```java
Properties pp = new Properties();
pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
pp.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "invoicer-" + instanceId); // stable per instance
pp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
pp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

Properties cp = new Properties();
cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
cp.put(ConsumerConfig.GROUP_ID_CONFIG, "invoicer");
cp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
cp.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
cp.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");
cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

KafkaProducer<String, String> producer = new KafkaProducer<>(pp);
KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cp);
producer.initTransactions();
consumer.subscribe(List.of("shop.orders.events"));

while (running) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
    if (records.isEmpty()) continue;

    producer.beginTransaction();
    try {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (ConsumerRecord<String, String> r : records) {
            producer.send(new ProducerRecord<>("shop.invoices.events", r.key(), toInvoice(r.value())));
            offsets.put(new TopicPartition(r.topic(), r.partition()), new OffsetAndMetadata(r.offset() + 1));
        }
        // the consumer offsets are committed in the same transaction
        producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
        producer.commitTransaction();
    } catch (ProducerFencedException e) {
        // another instance with the same transactional.id: this one is a zombie
        producer.close();
        throw e;
    } catch (KafkaException e) {
        producer.abortTransaction();
        // rewind the consumer to the last commit to re-read the batch
        for (TopicPartition tp : consumer.assignment()) {
            OffsetAndMetadata committed = consumer.committed(Set.of(tp)).get(tp);
            consumer.seek(tp, committed == null ? 0 : committed.offset());
        }
    }
}
```

`OffsetAndMetadata(r.offset() + 1)` is not a typo: you commit the **next** offset to read.

If you need exactly this read-transform-write shape, Kafka Streams with `processing.guarantee=exactly_once_v2` (module 11) is often simpler: it does all of this for you.

## 6.5 The idempotent consumer

When the result goes to an external database, Kafka transactions won't help. You need idempotency on the write side:

```sql
CREATE TABLE processed_events (
    event_id     UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

```sql
BEGIN;
INSERT INTO processed_events (event_id) VALUES ($1)
  ON CONFLICT (event_id) DO NOTHING;
-- 0 rows inserted -> already processed: COMMIT and move on
UPDATE accounts SET balance = balance - $2 WHERE id = $3;
COMMIT;
-- the Kafka offset is committed only after a successful COMMIT in the database
```

Alternatives:

- an `UPSERT` on a natural key;
- a conditional update on a version column (`WHERE version = $expected`);
- **store the offset in the same database** as the result, in one transaction (`topic, partition, offset`), and `seek()` to the stored position when the consumer starts. Then the Kafka commit isn't needed for correctness at all.

## 6.6 Transactional outbox

**The dual-write problem:**

```
1. INSERT INTO orders ...          OK
2. producer.send(OrderCreated)     -> the service crashed
   -> the order exists, the event doesn't, nobody downstream knows
```

**The fix: an outbox table in the same transaction.**

```
+--------------- one database transaction -------------+
| INSERT INTO orders (...)                             |
| INSERT INTO outbox (id, topic, key, payload)         |
+------------------------------------------------------+
                |
                v
   outbox relay: a separate process
   or Debezium (CDC) reads the outbox and publishes to Kafka
                |
                v
         topic shop.orders.events
```

```sql
CREATE TABLE outbox (
    id           UUID PRIMARY KEY,
    aggregate_id TEXT NOT NULL,          -- becomes the message key
    topic        TEXT NOT NULL,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Two kinds of relay:

| Option | How | Pros and cons |
|---|---|---|
| Polling | A process selects unsent rows, sends them, marks them | Simple; latency and load on the database |
| CDC (Debezium) | Reads the database WAL, publishes new outbox rows | Almost no latency or load; needs Kafka Connect (module 10) |

The relay can send an event twice (crash between sending and marking), so the message carries `outbox.id` (an `event-id` header), and consumers deduplicate by it (6.5).

## 6.7 Choosing a guarantee

| Task | Recommendation |
|---|---|
| Logs, metrics, clicks | `acks=1` is acceptable, at-most-once or at-least-once |
| User notifications | At-least-once; a duplicate is harmless or filtered by id |
| Domain business events | `acks=all` + `min.insync.replicas=2` + outbox + idempotent consumer |
| Kafka → processing → Kafka | Transactions or Kafka Streams with `exactly_once_v2` |
| Moving money | All of the above + `processed_events` + reconciliation |

### Self-check questions

1. Why doesn't exactly-once in Kafka extend to writes into an external database?
2. Why must `transactional.id` be stable per application instance?
3. What does a `read_uncommitted` consumer see in a topic written transactionally?
4. What problem does the transactional outbox solve, and why do consumers still need deduplication?
5. Why store the offset in the same database as the processing result?

---

# Module 7. Storage: retention, segments, log compaction, tiered storage

## 7.1 How a partition lies on disk

```
/var/lib/kafka/data/shop.orders.events-0/
├── 00000000000000000000.log        # segment: offsets 0 .. 1,048,575
├── 00000000000000000000.index
├── 00000000000000000000.timeindex
├── 00000000000001048576.log        # the next segment
├── 00000000000001048576.index
├── 00000000000001048576.timeindex
└── 00000000000002097152.log        # the ACTIVE segment: writes go here
```

- A partition is a set of **segments**. The file name is the offset of the first record in the segment.
- Writes always go to the end of the **active** segment. When it reaches `segment.bytes` (1 GB) or the age `segment.ms` (7 days), a new one is opened.
- Deletion and compaction work on **whole segments** and **never touch the active segment**.
- `.index` and `.timeindex` are sparse indexes: the broker uses them to find the file position for a given offset or time quickly.

Kafka writes and reads sequentially and serves data from the OS page cache. That's why it is fast even on ordinary disks and keeps almost no data on the JVM heap.

## 7.2 Retention: how long to keep data

`cleanup.policy=delete` (the default) removes old segments by time or size:

| Parameter | Default | Meaning |
|---|---|---|
| `retention.ms` | 604800000 (7 days) | Delete segments whose **newest** record is older than this |
| `retention.bytes` | -1 | Size limit **per partition** (not per topic!) |
| `segment.bytes` | 1 GB | Segment size |
| `segment.ms` | 7 days | Maximum age of a segment before it rolls |
| `log.retention.check.interval.ms` | 5 minutes | How often the broker checks what to delete |

Consequences that surprise people:

- a message lives **longer** than `retention.ms`: a segment is deleted only as a whole, once its newest record has expired, and the active segment is never deleted;
- on a quiet topic with `retention.ms=1h` and `segment.ms=7d`, data can stay for a week. If you need a short lifetime, lower `segment.ms` too;
- `retention.bytes=10GB` on a topic with 12 partitions and `replication.factor=3` means up to 360 GB of disk across the cluster.

```bash
kt kafka-configs.sh --bootstrap-server kafka-1:19092 --alter \
  --entity-type topics --entity-name shop.orders.events \
  --add-config retention.ms=259200000,segment.ms=86400000
```

## 7.3 Log compaction: the latest value per key

`cleanup.policy=compact` keeps **the latest value for each key** and a background process (the log cleaner) removes older versions:

```
before compaction:
offset: 0        1        2        3        4        5
key:    user-1   user-2   user-1   user-3   user-2   user-1
value:  Ann      Bob      Anna     Carl     Bobby    Anya

after:
offset:                            3        4        5
key:                               user-3   user-2   user-1
value:                             Carl     Bobby    Anya
```

- Order and offsets are preserved: the log simply gets "holes".
- A consumer reading the topic from the start gets **the current state of every key**, and then receives changes.
- Messages without a key can't be written to a compacted topic: the broker rejects them.

This is the basis for:

- state tables (profiles, settings, prices) as a topic;
- `__consumer_offsets`, which is compacted itself;
- KTables in Kafka Streams (module 11);
- Debezium CDC topics when you need the current state of a row;
- event sourcing with snapshots.

## 7.4 Tombstones: how to delete a key

A message with a key and an **empty (null) value** is a tombstone:

```
key: user-2   value: null   -> after compaction user-2 disappears completely
```

The tombstone itself is kept for `delete.retention.ms` (1 day by default) so that lagging consumers can see the deletion. A consumer more than `delete.retention.ms` behind may never learn that the key was deleted and will keep a stale value in its copy. This matters for GDPR deletion and caches.

## 7.5 When compaction runs

| Parameter | Default | Meaning |
|---|---|---|
| `min.cleanable.dirty.ratio` | 0.5 | Compact when the "dirty" (uncompacted) share of the log exceeds this |
| `min.compaction.lag.ms` | 0 | Minimum record age before compaction (a guarantee consumers see intermediate values) |
| `max.compaction.lag.ms` | infinity | Maximum delay: compact no later than this (important for deleting personal data) |
| `delete.retention.ms` | 1 day | How long to keep tombstones |
| `segment.ms` | 7 days | The active segment isn't compacted: lower it for frequent compaction |

Compaction is not instant: a compacted topic **may contain several values for one key at the same time**. Consumers must cope with that and take the last one.

`cleanup.policy=compact,delete` combines both: the latest value per key, but no older than `retention.ms`.

## 7.6 Tiered storage: hot data locally, old data in object storage

Since Kafka 3.9 tiered storage (KIP-405) is production-ready. Old segments are offloaded to remote storage (S3, GCS, Azure Blob, HDFS), and only the "hot" tail stays on the broker's local disks:

```
broker local disk:  the last 12 hours   (local.retention.ms)
remote storage:     30 days             (retention.ms)
a consumer reading old data transparently gets it from remote storage
```

```properties
# broker
remote.log.storage.system.enable=true
remote.log.storage.manager.class.name=<storage plugin class>
remote.log.metadata.manager.class.name=org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager
```

```bash
# topic
kt kafka-topics.sh --bootstrap-server kafka-1:19092 --create --topic shop.clicks \
  --partitions 12 --replication-factor 3 \
  --config remote.storage.enable=true \
  --config local.retention.ms=43200000 \
  --config retention.ms=2592000000
```

What to know:

- Apache Kafka provides the **interface**; the storage implementation is a plugin (for example the open-source Aiven plugin for S3/GCS/Azure);
- tiered storage doesn't support compacted topics;
- reading old data is slower: the first bytes come from object storage;
- the gains are cheap long retention and faster broker operations: less local data, faster node replacement and rebalancing.

## 7.7 Sizing disks

```
disk = avg_message_size × messages_per_day × retention_days × replication.factor × 1.3
                                                                             headroom ^

example: 1 KB × 50,000,000 × 7 × 3 × 1.3 ≈ 1.4 TB for the cluster
         on 3 brokers ≈ 470 GB each + headroom for rebalancing
```

Account for compression (zstd often gives 3–5× on JSON) and for the fact that when a broker fails its partitions must be restored onto the remaining ones. Keep disk usage **below 60–70%**: a disk at 100% stops the broker.

### Self-check questions

1. Why can a message live longer than `retention.ms`?
2. Why is `retention.bytes` on a topic more dangerous than it looks?
3. What is a tombstone and why can a lagging consumer miss it?
4. Why can a consumer of a compacted topic see several values for one key?
5. What problem does tiered storage solve besides storage cost?

---

# Module 8. Replication and fault tolerance: ISR and min.insync.replicas

## 8.1 How replication works

```
producer --(acks=all)--> partition leader (broker-1)
                            |  followers fetch from the leader themselves,
                            |  just like ordinary consumers
               +------------+------------+
               v                         v
        follower (broker-2)       follower (broker-3)

the leader answers the producer when ALL ISR replicas have the write;
then the high watermark moves and consumers can see it
```

- **ISR** are replicas that have caught up with the leader and haven't lagged for longer than `replica.lag.time.max.ms` (30 s). A lagging replica drops out of the ISR and comes back once it catches up.
- **High watermark** is the last offset present in every ISR replica. Consumers don't see records past the HW: otherwise, after a leader change, they could read something that later disappears.
- **Leader epoch** is the number of a leadership "era". A replica uses it after a failure to work out which part of its log to truncate so it doesn't diverge from the new leader.

## 8.2 min.insync.replicas and acks together

| RF | `min.insync.replicas` | 1 broker down | 2 brokers down |
|---|---|---|---|
| 3 | 1 | Writes continue | Writes go to a single copy: **risk of loss** |
| 3 | 2 | Writes continue | **Writes stop** (`NOT_ENOUGH_REPLICAS`), reads work |
| 3 | 3 | **Writes stop** | Stopped |
| 5 | 3 | Writes continue | Writes continue |

The table describes a cluster with dedicated controllers. In combined mode with three nodes (as in module 2), losing two nodes also loses the KRaft quorum, and then the cluster can't even shrink the ISR, so writes don't get an error but hang until they time out.

The production standard is **RF=3, min.insync.replicas=2, acks=all**: survive the loss of one broker with no outage and no data loss.

`min.insync.replicas` only affects producers with `acks=all`. A producer with `acks=1` keeps writing even with one live replica.

## 8.3 Electing a new leader

When a partition leader dies, the controller appoints a new leader **from the ISR**. This takes from a fraction of a second to a few seconds; clients get `NOT_LEADER_OR_FOLLOWER`, refresh metadata and switch over by themselves.

If nobody is left in the ISR (all in-sync replicas are dead), there are two options:

| `unclean.leader.election.enable` | Behaviour |
|---|---|
| `false` (default) | The partition is **unavailable** until an ISR replica returns. No data is lost |
| `true` | A lagging replica becomes leader. The partition is available again, but **acknowledged writes are lost** |

Keep `false` for anything that matters. `true` is acceptable only where availability beats completeness (some logs and metrics).

Kafka 4.x is developing Eligible Leader Replicas (KIP-966), a safer way to choose leaders when the ISR shrinks. Check the documentation for your version to see whether it is enabled and how it's configured.

## 8.4 Preferred leader and balancing

The first replica in the `Replicas` list is the **preferred leader**. After a broker restart, leadership stays with whoever took it over and load becomes skewed. Kafka moves leadership back by itself (`auto.leader.rebalance.enable=true`, checked every 5 minutes), or you can do it manually:

```bash
kt kafka-leader-election.sh --bootstrap-server kafka-1:19092 \
  --election-type preferred --all-topic-partitions
```

## 8.5 Rack awareness: surviving a zone failure

If all three replicas are in one availability zone, a zone outage kills the partition. Set each broker's zone:

```properties
broker.rack=eu-central-1a
```

Kafka then spreads replicas across racks when creating topics. A bonus is reading from the nearest replica (KIP-392), which saves cross-zone traffic:

```properties
# broker
replica.selector.class=org.apache.kafka.common.replica.RackAwareReplicaSelector
# consumer
client.rack=eu-central-1a
```

## 8.6 Moving partitions between brokers

A newly added broker is empty: Kafka does **not** move existing partitions automatically. You move them manually or with Cruise Control:

```bash
# 1. generate a plan for the topics
cat > topics.json <<'JSON'
{"version":1,"topics":[{"topic":"shop.orders.events"}]}
JSON
kt kafka-reassign-partitions.sh --bootstrap-server kafka-1:19092 \
  --topics-to-move-json-file /tmp/topics.json --broker-list "1,2,3,4" --generate

# 2. save the proposed plan as plan.json and execute it with a throttle
kt kafka-reassign-partitions.sh --bootstrap-server kafka-1:19092 \
  --reassignment-json-file /tmp/plan.json --execute --throttle 50000000

# 3. verify and remove the throttle
kt kafka-reassign-partitions.sh --bootstrap-server kafka-1:19092 \
  --reassignment-json-file /tmp/plan.json --verify
```

(Copy the files into the container first: `docker cp topics.json kafka-1:/tmp/`.)

`--throttle` is mandatory on a live cluster: without it the move saturates network and disks, and producers and consumers slow down.

## 8.7 Diagnostics

```bash
# partitions with fewer replicas in ISR than in Replicas
kt kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --under-replicated-partitions
# partitions with ISR below min.insync.replicas: acks=all writes are blocked
kt kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --under-min-isr-partitions
# partitions without a leader
kt kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --unavailable-partitions
```

In a healthy cluster all three lists are **empty**.

## 8.8 Failure scenarios

| What happened | What happens | What to do |
|---|---|---|
| 1 of 3 brokers down (RF=3, minISR=2) | Leaders move, writes and reads continue, URP > 0 | Bring the broker back; it catches up |
| 2 brokers down (dedicated controllers) | `acks=all` writes stop, reads from live leaders work | Bring brokers back; don't enable unclean election |
| 2 of 3 nodes down in combined mode | The KRaft quorum is lost too: leaders can't change and the ISR can't shrink, writes hang | Bring at least one node back; use dedicated controllers in production |
| A broker lost its disk | It comes back empty and re-replicates everything from leaders | Watch network and throttles |
| The active controller died | The quorum elects a new one within seconds | Nothing; keep the quorum at ≥ 2 of 3 |
| An availability zone is lost | Survivable if replicas are spread across racks | Configure `broker.rack` in advance |
| A disk is 100% full | The broker stops | Alert at 75%, retention, tiered storage |

### Practice

1. Create a topic with RF=3 and `min.insync.replicas=3`. With all three brokers up, `acks=all` writes succeed. Stop one broker and wait for the ISR to shrink to two replicas: a producer with `acks=all` gets `NOT_ENOUGH_REPLICAS`, while one with `acks=1` keeps writing. Explain the result. (Why not "stop two brokers out of three"? In the module 2 cluster every node is also a controller, and without two nodes the KRaft quorum is lost. Then the cluster can neither shrink the ISR nor elect new leaders, and writes just hang until a timeout instead of getting a clear error. The smoke test in `examples/` checks exactly the `min.insync.replicas=3` variant.)
2. Bring the brokers back and watch the ISR recover and `--under-replicated-partitions` become empty.
3. After a broker restart, check where the leaders are and run a preferred leader election.

---

# Module 9. Schema Registry and schema evolution

## 9.1 Why schemas

Kafka stores bytes and doesn't check them. Six months in, a topic contains events in five formats from three teams, and every new consumer has to guess what's inside. Typical breakages:

- a producer renamed `amount` to `total`, and every consumer silently got `null`;
- someone started writing the amount as a string instead of a number, and deserialisation fails in the middle of the topic;
- nobody knows which fields are required and what they mean.

**Schema Registry** is a separate service that stores event schemas, assigns them ids and **refuses incompatible changes**.

## 9.2 How it works

```
producer                              Schema Registry                consumer
   |  1. registers the schema ------>  subject shop.orders.events-value
   |  <-------- id = 17                versions 1, 2, 3 ...
   |
   |  2. writes to Kafka:  [0][00 00 00 11][avro bytes]
   |                        |   \_ schema id (4 bytes)
   |                        \_ magic byte
   |
   |                                                    3. reads id=17
   |                                  <------------------ fetches the schema (cached)
   |                                                    4. deserialises
```

Each message carries only 5 bytes of overhead, not the whole schema. Clients cache schemas, so the Registry doesn't become a bottleneck.

Implementations:

| Implementation | Licence | Notes |
|---|---|---|
| Confluent Schema Registry | Confluent Community License | The most widespread, de facto standard API |
| Apicurio Registry | Apache 2.0 | Compatible API, storage in Kafka or a database |
| Karapace | Apache 2.0 | Open implementation compatible with the Confluent API |

## 9.3 Formats

| Format | Pros | Cons |
|---|---|---|
| **Avro** | Compact, excellent evolution support, the standard in the Kafka world | Needs the schema to read, less familiar outside the JVM |
| **Protobuf** | Compact, great code generators for every language | Its own compatibility rules (field numbers) |
| **JSON Schema** | Readable, easy to debug | Large, weaker type control |
| Plain JSON without a schema | Quick to start | No guarantees: fine only for prototypes |

## 9.4 Subjects and naming strategies

Schemas are grouped into **subjects**. By default (`TopicNameStrategy`) the subject is `<topic>-key` and `<topic>-value`: each topic has one evolving value schema.

| Strategy | Subject | When |
|---|---|---|
| `TopicNameStrategy` | `shop.orders.events-value` | One event type per topic |
| `RecordNameStrategy` | `com.shop.OrderCreated` | Several types per topic, schema shared across topics |
| `TopicRecordNameStrategy` | `shop.orders.events-com.shop.OrderCreated` | Several types per topic, each topic has its own history |

If you put `OrderCreated`, `OrderPaid`, `OrderShipped` in one topic (module 3.2), use `TopicRecordNameStrategy` or a union schema.

## 9.5 Compatibility modes

The Registry's main value is checking that a new schema version won't break consumers or producers.

| Mode | Guarantee | Allowed | Upgrade first |
|---|---|---|---|
| `BACKWARD` (default) | The new schema can read data written with the previous one | Delete fields; add fields **with a default** | Consumers |
| `FORWARD` | The old schema can read new data | Add fields; delete fields that have a default | Producers |
| `FULL` | Both | Only add and delete fields **with defaults** | Either |
| `*_TRANSITIVE` | Same, but against **all** past versions, not just the last | | |
| `NONE` | No checks | Anything | Never in production |

For events that are kept long and re-read from the start, choose `BACKWARD_TRANSITIVE` or `FULL_TRANSITIVE`: a new consumer must be able to read an event written a year ago.

## 9.6 Evolving an Avro schema, by example

Version 1:

```json
{
  "type": "record", "name": "OrderCreated", "namespace": "com.shop",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "user_id",  "type": "string"},
    {"name": "amount",   "type": "long"}
  ]
}
```

Version 2, compatible:

```json
{
  "type": "record", "name": "OrderCreated", "namespace": "com.shop",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "user_id",  "type": "string"},
    {"name": "amount",   "type": "long"},
    {"name": "currency", "type": "string", "default": "EUR"},
    {"name": "coupon",   "type": ["null", "string"], "default": null}
  ]
}
```

Incompatible changes the Registry rejects in `BACKWARD` mode:

- adding a field **without** a default;
- renaming a field (for Avro it's deleting the old one and adding a new one; `aliases` help partly);
- changing a type from `long` to `string`.

If you really need an incompatible change: a new topic (`shop.orders.events.v2`) and a period when the producer writes to both.

## 9.7 Schema Registry in Docker Compose

Add a service to the module 2 cluster:

```yaml
  schema-registry:
    image: confluentinc/cp-schema-registry:8.0.0   # use the current version
    container_name: schema-registry
    depends_on: [kafka-1, kafka-2, kafka-3]
    ports: ["8081:8081"]
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka-1:19092,kafka-2:19092,kafka-3:19092
```

Schemas are stored in the compacted topic `_schemas` in Kafka itself.

## 9.8 REST API

```bash
# register a schema
curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "{\"type\":\"record\",\"name\":\"OrderCreated\",\"namespace\":\"com.shop\",\"fields\":[{\"name\":\"order_id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"long\"}]}"}' \
  http://localhost:8081/subjects/shop.orders.events-value/versions

# list subjects and versions
curl -s http://localhost:8081/subjects
curl -s http://localhost:8081/subjects/shop.orders.events-value/versions/latest

# compatibility mode for a subject
curl -s -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility": "BACKWARD_TRANSITIVE"}' \
  http://localhost:8081/config/shop.orders.events-value

# check a new schema BEFORE registering it (handy in CI)
curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @new-schema.json \
  http://localhost:8081/compatibility/subjects/shop.orders.events-value/versions/latest
```

## 9.9 An Avro producer in Java

```xml
<dependency>
  <groupId>io.confluent</groupId>
  <artifactId>kafka-avro-serializer</artifactId>
  <version><!-- the version matching your Confluent Platform --></version>
</dependency>
```

```java
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
props.put("schema.registry.url", "http://localhost:8081");
// in production CI registers schemas, not the application
props.put("auto.register.schemas", false);
props.put("use.latest.version", true);

// OrderCreated is generated from the .avsc by avro-maven-plugin
OrderCreated event = OrderCreated.newBuilder()
        .setOrderId("order-1").setUserId("user-42").setAmount(4990L).build();
producer.send(new ProducerRecord<>("shop.orders.events", event.getOrderId(), event));
```

The consumer uses `KafkaAvroDeserializer` and `specific.avro.reader=true` to get generated classes instead of `GenericRecord`.

## 9.10 Schemas as code

A practice that works:

1. Schemas live in a separate repository (or directory) next to the code.
2. On every PR, CI calls `/compatibility/...` and **fails** on an incompatible change.
3. After merge, CI registers the schema.
4. Applications run with `auto.register.schemas=false`: an accidental class change can't sneak a new schema into production.

### Self-check questions

1. What is in the first 5 bytes of a message serialised via Schema Registry?
2. How does `BACKWARD` differ from `FORWARD`, and who is upgraded first in each?
3. Why do long-lived topics need a `*_TRANSITIVE` mode?
4. Why is `auto.register.schemas` turned off in production?

---

# Module 10. Kafka Connect and CDC with Debezium

## 10.1 What Kafka Connect is

Kafka Connect is a framework for moving data between Kafka and external systems **without writing code**: you describe a connector in JSON, and Connect reads, writes, retries, tracks its position and scales by itself.

```
  Source connectors                                  Sink connectors
  PostgreSQL (Debezium) --+                     +--> ClickHouse
  MySQL (Debezium) -------+--> Kafka Connect ---+--> S3 / data lake
  MongoDB ----------------+       |   ^         +--> Elasticsearch / OpenSearch
  files, HTTP, MQTT ------+       v   |         +--> PostgreSQL (JDBC sink)
                                 Kafka
```

| Concept | What it is |
|---|---|
| **Worker** | A Connect process. In distributed mode several workers form a cluster |
| **Connector** | A job description: from where or to where, with which settings |
| **Task** | The unit of parallelism; a connector is split into `tasks.max` tasks |
| **Converter** | The data format in Kafka: JSON, Avro, Protobuf, strings, bytes |
| **SMT** | Single Message Transform: a light on-the-fly change to each message |

Connect's state lives in three internal topics: connector configs, offsets (source connector positions) and statuses. Workers are stateless: a failed worker is replaced and its tasks move to others.

## 10.2 CDC: database changes as a stream of events

**Change Data Capture** captures changes from the database transaction log (WAL in PostgreSQL, binlog in MySQL). Debezium reads the log and publishes every `INSERT/UPDATE/DELETE` as an event in Kafka.

```
PostgreSQL WAL --(logical replication slot)--> Debezium (Kafka Connect) --> topic shop.public.orders
```

Why CDC beats "poll the table from cron":

- it sees every change, including deletes and intermediate states;
- it barely loads the database (it reads the log, not the tables);
- latency is a fraction of a second;
- no `updated_at` column needed, no application changes.

A Debezium event (simplified):

```json
{
  "before": {"id": 1, "status": "NEW"},
  "after":  {"id": 1, "status": "PAID"},
  "op": "u",
  "source": {"db": "shop", "table": "orders", "lsn": 23859120, "ts_ms": 1789466400000},
  "ts_ms": 1789466400123
}
```

`op`: `c` insert, `u` update, `d` delete, `r` read during the initial snapshot.

## 10.3 PostgreSQL and Debezium in Docker Compose

Add to the cluster:

```yaml
  postgres:
    image: postgres:17
    container_name: postgres
    command: ["postgres", "-c", "wal_level=logical"]
    environment:
      POSTGRES_USER: shop
      POSTGRES_PASSWORD: shop
      POSTGRES_DB: shop
    ports: ["5432:5432"]

  connect:
    image: quay.io/debezium/connect:3.6
    container_name: connect
    depends_on: [kafka-1, kafka-2, kafka-3, postgres]
    ports: ["8083:8083"]
    environment:
      BOOTSTRAP_SERVERS: kafka-1:19092,kafka-2:19092,kafka-3:19092
      GROUP_ID: connect-cluster
      CONFIG_STORAGE_TOPIC: connect-configs
      OFFSET_STORAGE_TOPIC: connect-offsets
      STATUS_STORAGE_TOPIC: connect-status
      CONFIG_STORAGE_REPLICATION_FACTOR: 3
      OFFSET_STORAGE_REPLICATION_FACTOR: 3
      STATUS_STORAGE_REPLICATION_FACTOR: 3
```

`wal_level=logical` is mandatory: without it PostgreSQL doesn't expose changes via logical replication.

## 10.4 A PostgreSQL connector

```bash
curl -s -X POST -H "Content-Type: application/json" http://localhost:8083/connectors --data '{
  "name": "shop-postgres",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "shop",
    "database.password": "shop",
    "database.dbname": "shop",
    "topic.prefix": "shop",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_shop",
    "table.include.list": "public.orders,public.customers",
    "heartbeat.interval.ms": "10000",
    "tasks.max": "1"
  }
}'
```

Events from `public.orders` go to the topic `shop.public.orders`, keyed by the row's primary key. Debezium first takes a **snapshot** of existing data (`op: r`), then switches to streaming changes.

Managing connectors:

```bash
curl -s http://localhost:8083/connectors
curl -s http://localhost:8083/connectors/shop-postgres/status
curl -s -X PUT  -H "Content-Type: application/json" http://localhost:8083/connectors/shop-postgres/config --data @config.json
curl -s -X POST http://localhost:8083/connectors/shop-postgres/restart?includeTasks=true
curl -s -X PUT  http://localhost:8083/connectors/shop-postgres/pause
curl -s -X PUT  http://localhost:8083/connectors/shop-postgres/resume
curl -s -X DELETE http://localhost:8083/connectors/shop-postgres
```

## 10.5 The main danger of CDC: the replication slot

A logical replication slot makes PostgreSQL **keep WAL until Debezium has read it**. If the connector is stopped, failed or stuck:

```
Debezium stopped -> the slot doesn't advance -> WAL piles up -> the database disk fills -> the database stops
```

Protection:

- alert on slot lag: `SELECT slot_name, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) FROM pg_replication_slots;`
- `max_slot_wal_keep_size` in PostgreSQL limits WAL growth (at the cost of the slot possibly becoming invalid and needing a new snapshot);
- `heartbeat.interval.ms` in the connector moves the position even when the watched tables don't change;
- when you delete a connector, drop its slot: `SELECT pg_drop_replication_slot('debezium_shop');`

## 10.6 Outbox with Debezium

CDC is the best relay for the transactional outbox from module 6: the application writes to `outbox` in the same transaction, Debezium publishes the rows, and the **Outbox Event Router** SMT turns them into clean domain events.

For the `outbox` table from module 6.6 (`id`, `aggregate_id`, `topic`, `payload`):

```json
{
  "name": "shop-outbox",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "shop",
    "database.password": "shop",
    "database.dbname": "shop",
    "topic.prefix": "shop-outbox",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_outbox",
    "table.include.list": "public.outbox",
    "transforms": "outbox",
    "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
    "transforms.outbox.table.field.event.id": "id",
    "transforms.outbox.table.field.event.key": "aggregate_id",
    "transforms.outbox.table.field.event.payload": "payload",
    "transforms.outbox.route.by.field": "topic",
    "transforms.outbox.route.topic.replacement": "${routedByValue}"
  }
}
```

An outbox row with `topic = 'shop.orders.events'` becomes a message in `shop.orders.events` keyed by `aggregate_id` with `payload` as the value. The event id goes into a header, and consumers deduplicate by it.

Outbox rows can be deleted right after insertion (in the same transaction or periodically): Debezium reads the WAL, not the table.

## 10.7 Converters and SMTs

```json
"key.converter": "org.apache.kafka.connect.json.JsonConverter",
"value.converter": "io.confluent.connect.avro.AvroConverter",
"value.converter.schema.registry.url": "http://schema-registry:8081",

"transforms": "unwrap,route",
"transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
"transforms.unwrap.delete.tombstone.handling.mode": "tombstone",
"transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
"transforms.route.regex": "shop\\.public\\.(.*)",
"transforms.route.replacement": "shop.cdc.$1"
```

- `ExtractNewRecordState` keeps only the `after` state, handy for sinks that don't need the Debezium envelope;
- `RegexRouter` renames topics;
- SMTs are for **light** changes. Anything heavier (joins, aggregates, calls to external systems) belongs in Kafka Streams or Flink.

## 10.8 Errors in Connect

```json
"errors.tolerance": "all",
"errors.log.enable": "true",
"errors.log.include.messages": "true",
"errors.deadletterqueue.topic.name": "dlq.clickhouse-sink",
"errors.deadletterqueue.context.headers.enable": "true",
"errors.retry.timeout": "300000"
```

- `errors.tolerance=none` (the default) stops the task on the first bad message;
- `all` skips bad messages (and sends them to the DLQ if one is configured);
- Connect's built-in DLQ exists **only for sink connectors**;
- the context headers (`__connect.errors.*`) keep the error cause and the original topic/partition/offset.

A task in `FAILED` state won't restart by itself: monitor `/connectors/<name>/status` and alert.

### Practice

1. Bring up PostgreSQL and Debezium, create an `orders` table, insert, update and delete a row. Look at the events in `shop.public.orders`.
2. Stop Connect for 10 minutes while generating changes in the database and watch the slot lag grow.
3. Set up the outbox connector and confirm the event shows up in `shop.orders.events` with the right key.

---

# Module 11. Kafka Streams

## 11.1 What it is

Kafka Streams is a **Java library** for stream processing. Not a cluster and not a separate service: you write an ordinary application, run it as several instances, and the library shares the work, keeps state and recovers from failures.

```
  input topics --> [ your application with Kafka Streams ] --> output topics
                      |  state stores (RocksDB, local)
                      |  changelog topics in Kafka (for recovery)
                      v
                 3 instances = work is split by partitions
```

| Task | Kafka Streams | Flink | Consumer + your own logic |
|---|---|---|---|
| Filtering, enrichment, routing | Excellent | Excellent | Fine |
| Aggregates, windows, joins | Excellent | Excellent | Hard and error-prone |
| Exactly-once Kafka → Kafka | One setting | Yes | By hand (module 6.4) |
| Infrastructure | None, it's a library | A Flink cluster | None |
| Languages | Java/Kotlin/Scala | Java, SQL, Python | Any |
| Sources other than Kafka | No | Many | Any |

## 11.2 Core abstractions

| Abstraction | What it is | Analogy |
|---|---|---|
| **KStream** | A stream of events: every message is a separate fact | An operations journal |
| **KTable** | A table: the latest value per key (a compacted topic) | Current state |
| **GlobalKTable** | A table fully copied to every instance | A lookup table |
| **State store** | Local state storage (RocksDB or in-memory) | A local database |
| **Changelog topic** | A copy of a state store in Kafka for recovery | A backup |
| **Task** | The unit of parallelism: one input partition (or a group of partitions) | |

The stream–table duality: a KStream aggregated by key becomes a KTable; a KTable's changes form a KStream.

## 11.3 Example: payments per user per minute

```xml
<dependency>
  <groupId>org.apache.kafka</groupId>
  <artifactId>kafka-streams</artifactId>
  <version>4.3.1</version>
</dependency>
```

```java
Properties props = new Properties();
props.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-stats");       // also the consumer group and the prefix of internal topics
props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9192,localhost:9292");
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);               // for changelog and repartition topics
props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

StreamsBuilder builder = new StreamsBuilder();
KStream<String, String> orders = builder.stream("shop.orders.events");

orders
    .filter((orderId, json) -> json.contains("\"OrderPaid\""))
    .selectKey((orderId, json) -> userIdOf(json))                     // key change -> repartition topic
    .groupByKey()
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
    .count(Materialized.as("paid-per-user-1m"))                       // state store + changelog
    .toStream()
    .map((windowedUser, count) -> KeyValue.pair(
            windowedUser.key() + "@" + windowedUser.window().startTime(), count.toString()))
    .to("shop.stats.paid-per-user");

KafkaStreams streams = new KafkaStreams(builder.build(), props);
Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
streams.start();
```

What the application creates by itself:

- `order-stats-KSTREAM-...-repartition`: after `selectKey` the data must be redistributed by the new key;
- `order-stats-paid-per-user-1m-changelog`: the state store backup;
- the consumer group `order-stats`.

## 11.4 Joins and co-partitioning

```java
KTable<String, String> customers = builder.table("shop.customers");   // key: user_id
KStream<String, String> paidByUser = orders
        .filter((k, v) -> v.contains("\"OrderPaid\""))
        .selectKey((k, v) -> userIdOf(v));

paidByUser
    .join(customers, (order, customer) -> enrich(order, customer))
    .to("shop.orders.enriched");
```

**Co-partitioning:** for a KStream–KTable join both topics must have **the same number of partitions** and the same key with the same partitioner. Then records for one `user_id` from both topics end up in the same task. If not, Streams fails at startup or requires a repartition. A `GlobalKTable` sidesteps the requirement at the cost of a full copy on every instance: fine for small lookup tables.

## 11.5 Time and windows

| Window | What it is | Example |
|---|---|---|
| Tumbling | Fixed, non-overlapping intervals | Orders per minute |
| Hopping | Fixed, overlapping | A 5-minute window every minute |
| Sliding | By time difference between events | Two payments with one card within 10 seconds |
| Session | Activity with gaps no longer than N | A user's session on a site |

Kafka Streams works with **event time** (the record timestamp), not processing time. Late events are accepted within a `grace` period: `TimeWindows.ofSizeAndGrace(Duration.ofMinutes(1), Duration.ofSeconds(30))`. Events later than grace are dropped.

## 11.6 Scaling and state

- The number of tasks equals the number of input partitions. More instances than tasks means idle instances (or ones holding standby replicas).
- When a task moves to another instance, its state store is restored from the changelog. For large state that is slow: enable `num.standby.replicas=1` to keep a warm copy.
- `state.dir` must be on a persistent volume, or every restart means a full restore from the changelog.
- Interactive queries let you read a state store straight from the application over HTTP: a "materialised view" without a separate database.

## 11.7 What's new in 4.2–4.3

- **Streams rebalance protocol** (KIP-1071) became generally available with a limited feature set: the broker, not the clients, assigns tasks. It is enabled with `group.protocol=streams`; check the limitations in the docs for your version before using it in production.
- **DLQ in exception handlers** (KIP-1034): deserialisation and processing error handlers can send problem records to a dead letter queue instead of stopping the application.
- **Anchored punctuation** (KIP-1146): periodic tasks exactly on schedule, for example at the start of every hour.
- **Headers in state stores** (KIP-1271, KIP-1285, 4.3).

## 11.8 Typical mistakes

| Mistake | Consequence | Do this instead |
|---|---|---|
| Two different applications with one `application.id` | They split partitions and corrupt each other's state | A unique `application.id` per application |
| `replication.factor=1` for internal topics | State lost when a broker fails | `replication.factor=3` |
| Joining topics with different partition counts | Startup error or wrong results | Co-partitioning or a GlobalKTable |
| `state.dir` in the container's temp directory | Long recovery on every restart | A persistent volume |
| Changing the topology without changing `application.id` | Incompatible internal topics | Reset with `kafka-streams-application-reset.sh` or a new id |

### Self-check questions

1. How does a KStream differ from a KTable?
2. Why does Kafka Streams create a repartition topic after `selectKey`?
3. What is co-partitioning and when is it required?
4. How does Kafka Streams restore state after a task moves?

---

# Module 12. Share groups: queues in Kafka

## 12.1 The problem share groups solve

In a classic consumer group a partition is read by **exactly one** consumer. That gives ordering but creates two limits:

- processing parallelism is capped by the number of partitions;
- one slow or "poison" message blocks the whole partition: the ones after it wait until it is processed or skipped.

For jobs like "resize an image", "send an email", "generate a report" ordering doesn't matter; what you need is a classic queue: many workers, per-job acknowledgement, redelivery of failed jobs.

**Share groups** (KIP-932, "Queues for Kafka") provide exactly that. Since Kafka 4.2 they are production-ready.

## 12.2 How it works

```
topic jobs: 2 partitions

consumer group (classic)              share group
  p0 -> only consumer A                 p0 -> A, B, C, D read records interleaved
  p1 -> only consumer B                 p1 -> A, B, C, D read records interleaved
  C, D sit idle                         each record goes to one consumer
                                        and is acknowledged individually
```

- The broker hands records to consumers under a **temporary lock** (acquisition lock, 30 seconds by default).
- The consumer acknowledges each record with one of these types:

| Type | Meaning |
|---|---|
| `ACCEPT` | Processed, don't deliver again |
| `RELEASE` | Couldn't do it now, deliver again (to me or someone else) |
| `REJECT` | Can't be processed, don't deliver again |
| `RENEW` (since 4.2) | Still working: extend the lock for long processing |

- If the lock expires without an acknowledgement, the record is delivered again.
- The broker counts deliveries of each record. After the limit (`group.share.delivery.count.limit`, 5 by default) the record is **archived** and not delivered again.
- State is kept in the internal topic `__share_group_state`.

## 12.3 Share group vs consumer group

| | Consumer group | Share group |
|---|---|---|
| Consumers per partition | One | Any number |
| Ordering | Within a partition | **Not guaranteed** |
| Acknowledgement | Offset (everything up to N) | Each record individually |
| Redelivering a single record | No | Yes, with a counter |
| A slow message | Blocks the partition | Doesn't block the others |
| Scaling | Up to the partition count | Not limited by partitions |
| Good for | Event streams where order matters | Task queues |

One topic can be read by a consumer group and a share group at the same time: analytics reads the stream in order while workers treat it as a queue.

## 12.4 A share consumer in Java

```java
Properties props = new Properties();
props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9192,localhost:9292");
props.put(ConsumerConfig.GROUP_ID_CONFIG, "image-workers");
props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
props.put("share.acknowledgement.mode", "explicit");   // we acknowledge every record ourselves

try (KafkaShareConsumer<String, String> consumer = new KafkaShareConsumer<>(props)) {
    consumer.subscribe(List.of("shop.jobs.images"));
    while (running) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
        for (ConsumerRecord<String, String> r : records) {
            try {
                resizeImage(r.value());
                consumer.acknowledge(r, AcknowledgeType.ACCEPT);
            } catch (TemporaryException e) {
                consumer.acknowledge(r, AcknowledgeType.RELEASE);   // try again
            } catch (Exception e) {
                consumer.acknowledge(r, AcknowledgeType.REJECT);    // garbage: do not deliver again
            }
        }
        consumer.commitSync();   // send the acknowledgements to the broker
    }
}
```

In implicit mode (`share.acknowledgement.mode=implicit`, the default) every record returned by `poll()` counts as accepted on the next `poll()` or `commitSync()`.

## 12.5 Configuration and administration

```bash
# if share groups are not enabled in your cluster, enable the feature (check the docs for your version)
kt kafka-features.sh --bootstrap-server kafka-1:19092 describe
kt kafka-features.sh --bootstrap-server kafka-1:19092 upgrade --feature share.version=1

# share group state
kt kafka-share-groups.sh --bootstrap-server kafka-1:19092 --list
kt kafka-share-groups.sh --bootstrap-server kafka-1:19092 --describe --group image-workers

# where a new share group starts (latest by default)
kt kafka-configs.sh --bootstrap-server kafka-1:19092 --alter \
  --entity-type groups --entity-name image-workers \
  --add-config share.auto.offset.reset=earliest
```

Important settings (on the broker or the group):

| Parameter | Default | Meaning |
|---|---|---|
| `group.share.record.lock.duration.ms` | 30,000 | How long a record lock lasts |
| `group.share.delivery.count.limit` | 5 | How many deliveries before a record is archived |
| `share.auto.offset.reset` | `latest` | Where a new group starts |

## 12.6 Limitations to remember

- **No ordering.** If the order of one entity's events matters, this is not your tool.
- **Rejected records and records that exhausted their attempts are archived, not sent to a DLQ automatically.** If you need a DLQ, write the record to a separate topic yourself before `REJECT`.
- Check share group support in your client library: the Java client supports them fully, others are catching up.
- For long processing use `RENEW` or increase the lock duration, or the job will be handed to a second worker while the first is still working on it.

### Self-check questions

1. Why does a slow message block a partition in a classic consumer group but not in a share group?
2. How does `RELEASE` differ from `REJECT`?
3. What happens to a record after the delivery limit is exhausted?
4. For which jobs are share groups the wrong choice?

---

# Module 13. Error handling: retry topics, DLQs and poison pills

## 13.1 Classifying errors

```
error while processing a message
   |
   +-- transient (database down, 503, timeout)       -> retry later
   +-- permanent (broken JSON, no such order)         -> straight to the DLQ
   +-- unknown                                        -> a few retries, then the DLQ
```

The Kafka twist: a group consumer **can't "put aside" one message** and move on, because offsets advance sequentially. Get stuck on a message and the whole partition stops. So a retry strategy in Kafka is an architectural decision, not a single setting.

## 13.2 Blocking retries

Retry processing in place until it succeeds:

```
message 42 -> error -> wait 1s -> error -> wait 5s -> success -> message 43
```

- **Pro:** ordering is preserved.
- **Con:** the whole partition waits while retries run. Wait longer than `max.poll.interval.ms` and a rebalance starts.

Good for short transient failures and for streams where ordering is critical (balance changes, order statuses).

If you need to wait a long time, don't sleep in the loop: call `consumer.pause(partitions)`, keep calling `poll()` (it returns no records from paused partitions but keeps your group membership), and when the delay is over `seek()` to the same message and `resume()`.

## 13.3 Non-blocking retries: retry topics

```
shop.orders.events
   | error
   v
shop.orders.events.retry-1m   (the consumer waits until the message is 1 minute old)
   | error again
   v
shop.orders.events.retry-10m
   | error again
   v
shop.orders.events.dlq        (a human investigates, then re-sends)
```

- **Pro:** the main stream doesn't stop.
- **Con:** **ordering breaks**: later events for the same order may be processed before the failed one. If that is critical, either block the key (route subsequent messages with the same key down the same retry path) or use blocking retries.

A retry consumer must not process a message early. Look at the record timestamp: if it's not time yet, `pause()` the partition until then and `seek()` back to the message.

## 13.4 What to put in a DLQ message

Send the original key and value **unchanged** and add headers:

| Header | Why |
|---|---|
| `dlq-original-topic`, `dlq-original-partition`, `dlq-original-offset` | Find the original and its context |
| `dlq-error-class`, `dlq-error-message` | The cause |
| `dlq-attempts` | How many attempts were made |
| `dlq-failed-at` | When |
| `dlq-consumer-group` | Who couldn't process it |

```java
void sendToDlq(ConsumerRecord<byte[], byte[]> r, Exception e, int attempts) {
    ProducerRecord<byte[], byte[]> dlq = new ProducerRecord<>(r.topic() + ".dlq", r.key(), r.value());
    r.headers().forEach(h -> dlq.headers().add(h));                // keep the original headers
    dlq.headers()
       .add("dlq-original-topic", r.topic().getBytes(UTF_8))
       .add("dlq-original-partition", Integer.toString(r.partition()).getBytes(UTF_8))
       .add("dlq-original-offset", Long.toString(r.offset()).getBytes(UTF_8))
       .add("dlq-error-class", e.getClass().getName().getBytes(UTF_8))
       .add("dlq-error-message", String.valueOf(e.getMessage()).getBytes(UTF_8))
       .add("dlq-attempts", Integer.toString(attempts).getBytes(UTF_8));
    try {
        producer.send(dlq).get();     // wait for the DLQ write BEFORE committing the offset
    } catch (Exception sendError) {
        throw new IllegalStateException("DLQ write failed, not committing the offset", sendError);
    }
}
```

The order is critical: **write to the DLQ first, commit the offset second**. Otherwise a crash between the two loses the message.

Key and value are read as `byte[]`: the DLQ then receives exactly what arrived, even if it can't be deserialised.

## 13.5 Poison pills: messages you can't even read

```
a message with broken bytes
  -> the deserialiser throws inside poll()
  -> the consumer can't get past that offset
  -> restart -> same error -> the partition is stuck forever
```

Protection:

1. **Deserialise yourself, not in the client.** Read `byte[]`, deserialise in your code inside a try/catch and send unreadable messages to the DLQ. Spring Kafka has `ErrorHandlingDeserializer` for this.
2. If an exception does escape `poll()` (`RecordDeserializationException`), it carries the partition and offset: `seek(partition, offset + 1)` after saving the raw data for analysis.
3. Kafka Streams: configure a deserialisation exception handler (`LogAndContinueExceptionHandler` or sending to a DLQ, module 11.7).
4. Kafka Connect: `errors.tolerance=all` + a DLQ topic (module 10.8).
5. Schema Registry on the producer side is the best way to keep broken messages out in the first place.

## 13.6 Reprocessing the DLQ

A DLQ without a triage process is just a graveyard. You need:

- an **alert** on any message in the DLQ;
- a **way to inspect** it: `kafka-console-consumer.sh --topic shop.orders.events.dlq --property print.headers=true` or a UI (Kafka UI, AKHQ, Redpanda Console);
- **re-sending** after the fix: a separate consumer reads the DLQ and publishes messages back to the original topic (or a retry topic), keeping the key;
- DLQ **retention** longer than the main topic: investigations can take weeks.

## 13.7 Choosing a strategy

| Situation | Strategy |
|---|---|
| Per-key ordering is critical (balances, statuses) | Blocking retries with `pause/resume`, then stop and alert, or a DLQ with key blocking |
| Ordering doesn't matter, external services fail | Retry topics with delays + DLQ |
| Independent jobs | Share groups (module 12) + your own DLQ before `REJECT` |
| Broken data | Straight to the DLQ, no retries |
| Kafka Streams | Exception handlers with a DLQ (4.2+) |
| Kafka Connect | `errors.tolerance` + DLQ |

### Practice

1. Write a consumer that reads `byte[]`, fails on invalid JSON and sends such messages to a DLQ with headers. Confirm the offset is committed only after the DLQ write.
2. Implement a retry topic with a 30-second delay using `pause/resume` and check that a message isn't processed early.
3. Send a message that breaks the deserialiser and watch what happens to an unprotected consumer.

---

# Module 14. Multiple data centres: MirrorMaker 2

## 14.1 Topology options

| Topology | How | When |
|---|---|---|
| **Stretch cluster** | One cluster, brokers and controllers across three DCs/zones, `broker.rack` per DC | Zones of one region with single-digit millisecond latency |
| **Active-passive** | A primary cluster + a standby, data copied one way | Disaster recovery between regions |
| **Active-active** | Two clusters, each accepts writes, copying both ways | Users in different regions write locally |
| **Aggregation** | Many regional clusters → a central one | Collecting data for analytics |

A stretch cluster is the simplest for clients (it's one cluster), but every `acks=all` write waits for cross-zone replication. Between regions with tens of milliseconds of latency you don't do this; you run separate clusters and replicate between them.

## 14.2 MirrorMaker 2

MirrorMaker 2 (MM2) is part of Apache Kafka, built on Kafka Connect. It has three connectors:

| Connector | What it does |
|---|---|
| `MirrorSourceConnector` | Copies topics, their configs and ACLs |
| `MirrorCheckpointConnector` | Translates consumer group offsets between clusters |
| `MirrorHeartbeatConnector` | Writes heartbeats that show replication is alive and measure its lag |

```
cluster eu                                  cluster us
shop.orders.events  --MirrorSource-->        eu.shop.orders.events
__consumer_offsets  --MirrorCheckpoint-->    eu.checkpoints.internal
                                             (+ translated group offsets)
```

By default (`DefaultReplicationPolicy`) a replicated topic gets **the source cluster as a prefix**: `eu.shop.orders.events`. That prevents loops in active-active: MM2 won't copy `eu.*` back into `eu`.

## 14.3 Configuration

`mm2.properties`:

```properties
clusters = eu, us
eu.bootstrap.servers = kafka-eu-1:9092,kafka-eu-2:9092,kafka-eu-3:9092
us.bootstrap.servers = kafka-us-1:9092,kafka-us-2:9092,kafka-us-3:9092

# direction: eu -> us
eu->us.enabled = true
eu->us.topics = shop\..*
eu->us.groups = .*

# durability of internal topics
replication.factor = 3
checkpoints.topic.replication.factor = 3
heartbeats.topic.replication.factor = 3
offset-syncs.topic.replication.factor = 3

# translate group offsets and write them to the target cluster
sync.group.offsets.enabled = true
sync.group.offsets.interval.seconds = 10
emit.checkpoints.interval.seconds = 10

# copy topic configs and ACLs
sync.topic.configs.enabled = true
sync.topic.acls.enabled = true
```

```bash
/opt/kafka/bin/connect-mirror-maker.sh mm2.properties
```

In production MM2 usually runs as a set of connectors in an existing Kafka Connect cluster (or via Strimzi `KafkaMirrorMaker2`) next to the **target** cluster: reading over the WAN tolerates latency better than writing.

## 14.4 Offsets differ between clusters

An offset is a position in a specific partition of a specific cluster. The message at offset 1000 in `eu` may be at offset 987 in `us` (retention, compaction, when replication started). So you can't just copy a group's committed offset.

MirrorCheckpointConnector periodically records the offset mapping, and `sync.group.offsets.enabled=true` writes translated offsets directly into the target cluster's `__consumer_offsets`. The translation is **approximate** (it moves in sync steps): after failover, consumers re-read a few messages. Processing must be idempotent.

## 14.5 The failover procedure (active-passive)

```
1. the primary cluster eu is unavailable (or a planned switch)
2. make sure MM2 copied everything it could (heartbeat lag)
3. stop consumers in eu (if they are still alive)
4. consumers in us subscribe to eu.shop.orders.events
   with the translated offsets of their groups
5. producers switch to cluster us and write to shop.orders.events
6. consumers read both topics: eu.shop.orders.events (the tail) and shop.orders.events (new data)
7. once eu recovers, set up reverse replication us -> eu and plan the switch back
```

The order and precision of failover is the hardest part of multi-DC. Rehearse it beforehand, or it won't work on the day of the outage.

## 14.6 Topic names after failover

| Policy | Name in the target cluster | Pros and cons |
|---|---|---|
| `DefaultReplicationPolicy` | `eu.shop.orders.events` | No loops, origin visible; consumers must subscribe to the new name (`.*shop.orders.events` by regex) |
| `IdentityReplicationPolicy` | `shop.orders.events` | Consumers keep their subscription; **can't** be used in active-active (loops) |

### Self-check questions

1. When is a stretch cluster better than two clusters with replication?
2. Why does MM2 add a prefix to the topic name by default?
3. Why can't you just copy committed offsets from one cluster to another?
4. What must consumers do to make failover safe?

---

# Module 15. Performance and tuning

## 15.1 Orders of magnitude

| Scenario | Ballpark on good hardware |
|---|---|
| Writing to one broker, small messages, batching and compression | Hundreds of thousands to millions of msg/s |
| A 3-broker cluster, `acks=all`, RF=3 | Hundreds of MB/s |
| One partition | Tens of MB/s (bounded by one leader and one consumer) |
| End-to-end latency with `acks=all` | Single to tens of milliseconds |

Other people's numbers are useless: measure on your hardware, with your message sizes and your settings.

## 15.2 Benchmarks

```bash
# write: 1M messages of 1 KB, unthrottled
kt kafka-producer-perf-test.sh --topic perf --num-records 1000000 --record-size 1024 \
  --throughput -1 --warmup-records 100000 \
  --producer-props bootstrap.servers=kafka-1:19092 acks=all linger.ms=20 batch.size=131072 compression.type=lz4

# read
kt kafka-consumer-perf-test.sh --bootstrap-server kafka-1:19092 --topic perf \
  --messages 1000000 --group perf-test

# end-to-end latency
kt kafka-e2e-latency.sh --help
```

`--warmup-records` (since 4.2) separates warm-up from steady state: without it the first seconds skew the result.

## 15.3 Tuning the producer

| Goal | Settings |
|---|---|
| Throughput | `linger.ms=10..50`, `batch.size=64..256 KB`, `compression.type=lz4` or `zstd`, more `buffer.memory` |
| Low latency | `linger.ms=0..5`, no compression or `lz4`, small batches |
| Durability | `acks=all`, idempotence (default), `delivery.timeout.ms` matching your SLA |

The main lever is **batch size**. Many small requests of one message each cost network, broker CPU and compression ratio. Check the producer metrics `batch-size-avg` and `records-per-request-avg`: if batches are tiny, increase `linger.ms`.

## 15.4 Tuning the consumer

| Parameter | Default | When to change |
|---|---|---|
| `fetch.min.bytes` | 1 | Raise it (e.g. 64 KB) so the broker returns data in batches |
| `fetch.max.wait.ms` | 500 | How long the broker waits for `fetch.min.bytes` |
| `max.partition.fetch.bytes` | 1 MB | Maximum from one partition per request |
| `max.poll.records` | 500 | Lower it if processing a batch is slow (rebalances) |
| `max.poll.interval.ms` | 300,000 | Raise it if processing a batch is legitimately long |

Most often a consumer is limited not by Kafka but by **its own processing**: synchronous database and API calls, one per message. Batching writes to the database usually gives more than any client setting.

## 15.5 Broker, OS and JVM

```properties
num.network.threads=8        # request-receiving threads
num.io.threads=16            # processing threads (≈ disks × 2 or more)
num.replica.fetchers=4       # replication threads from leaders
log.dirs=/data/kafka1,/data/kafka2   # several disks = parallel I/O
socket.send.buffer.bytes=1048576
socket.receive.buffer.bytes=1048576
```

OS:

- XFS or ext4, mounted with `noatime`;
- `vm.swappiness=1`: swapping kills latency;
- an open files limit of 100,000+ (every segment and index is a file);
- **most of the memory goes to the page cache**: Kafka serves consumers from the OS cache.

JVM:

- a broker heap of 4–8 GB is **enough**; more hurts, since it takes memory from the page cache;
- G1 GC (the default); Java 17 or 21.

A typical broker: 64 GB RAM = 6 GB heap + ~55 GB page cache.

## 15.6 Quotas: protection from noisy neighbours

```bash
# limit a client in bytes per second
kt kafka-configs.sh --bootstrap-server kafka-1:19092 --alter \
  --entity-type clients --entity-name analytics-exporter \
  --add-config 'producer_byte_rate=10485760,consumer_byte_rate=52428800'

# default quota for all users
kt kafka-configs.sh --bootstrap-server kafka-1:19092 --alter \
  --entity-type users --entity-default \
  --add-config 'consumer_byte_rate=104857600'
```

When a quota is exceeded the broker doesn't refuse; it **delays responses**: the client slows down and the others don't suffer.

## 15.7 What actually makes systems faster

1. **Batching** on the producer side and when the consumer writes to its database.
2. **Compression** (`lz4` or `zstd`): saves network, disk and replication.
3. **The right partition count**: enough for parallelism, without thousands of extras.
4. **A good key**: no hot partitions.
5. **Page cache**: don't give all the memory to the heap.
6. **Consumers that keep up**: reading fresh data comes from memory; reading old data hits the disk and evicts the cache.

---

# Module 16. Monitoring: lag, under-replicated partitions, alerts

## 16.1 Where metrics come from

Kafka exposes metrics via **JMX**. The standard path:

```
brokers, clients --JMX--> Prometheus JMX exporter (java agent) --> Prometheus --> Grafana / Alertmanager
```

For group lag a dedicated exporter that reads offsets through the Admin API is more convenient, for example KMinion or Burrow. For day-to-day work use a UI: Kafka UI (kafbat), AKHQ, Redpanda Console.

## 16.2 Broker metrics that matter

| Metric (JMX) | Normal | What a deviation means |
|---|---|---|
| `kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions` | 0 | Replicas lag or a broker is down |
| `kafka.server:type=ReplicaManager,name=UnderMinIsrPartitionCount` | 0 | `acks=all` writes to these partitions are **blocked** |
| `kafka.controller:type=KafkaController,name=OfflinePartitionsCount` | 0 | Partitions without a leader: unavailable |
| `kafka.controller:type=KafkaController,name=ActiveControllerCount` | Sum across the cluster = 1 | 0 means no active controller |
| `kafka.server:type=ReplicaManager,name=IsrShrinksPerSec` / `IsrExpandsPerSec` | About 0 | Replicas "flap": network, disks, GC |
| `kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent` | > 0.3 | The broker is overloaded with request processing |
| `kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent` | > 0.3 | Network threads are overloaded |
| `kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Produce` | Stable | A growing p99 means slow writes |
| `kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec` / `BytesOutPerSec` | — | Load, capacity planning |
| Disk usage | < 70% | At 100% the broker stops |

Kafka 4.3 added metrics for how full partitions are relative to retention limits (KIP-1257): handy for spotting a topic about to start dropping data by size.

## 16.3 Consumer lag

Lag = `log end offset − committed offset` for each partition of a group.

```bash
kt kafka-consumer-groups.sh --bootstrap-server kafka-1:19092 --describe --group billing
```

What to look at:

- lag **growth** matters more than its absolute value: 100,000 messages of stable lag is a normal buffer, while 1,000 growing every minute is a problem;
- lag on one partition while the others are fine means a hot key or a stuck message;
- lag **in time** ("5 minutes behind") is clearer to the business than in messages; exporters can compute it;
- a group in `Empty` state with growing lag means the service isn't running at all.

## 16.4 What to alert on

| Alert | Condition | Why it matters |
|---|---|---|
| **Under-min-ISR partitions** | > 0 | `acks=all` writes are blocked |
| **Offline partitions** | > 0 | Data is unavailable |
| **No active controller** | Sum of `ActiveControllerCount` ≠ 1 | The cluster can't change metadata |
| **Under-replicated partitions** | > 0 for more than 5 minutes | Redundancy is lost |
| **Growing consumer lag** | Growing for 10+ minutes | The consumer can't keep up or has stopped |
| **Disk** | > 75% | At 100% the broker stops |
| **Messages in a DLQ** | Any | Business logic is failing |
| **Debezium replication slot** | Lag growing | The database disk will fill up (module 10.5) |
| **Connect task FAILED** | Any | It won't restart by itself |
| **Authentication errors** | Growing | An attack or a broken deployment |

## 16.5 Everyday commands

```bash
kt kafka-metadata-quorum.sh --bootstrap-server kafka-1:19092 describe --status
kt kafka-broker-api-versions.sh --bootstrap-server kafka-1:19092 | grep -c "id:"
kt kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --under-replicated-partitions
kt kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --under-min-isr-partitions
kt kafka-consumer-groups.sh --bootstrap-server kafka-1:19092 --describe --all-groups
kt kafka-log-dirs.sh --bootstrap-server kafka-1:19092 --describe --topic-list shop.orders.events
kt kafka-get-offsets.sh --bootstrap-server kafka-1:19092 --topic shop.orders.events
```

---

# Module 17. Security: TLS, SASL, ACLs

## 17.1 Three layers of protection

| Layer | Mechanism | What it gives you |
|---|---|---|
| **Encryption** | TLS on listeners | Nobody can read traffic on the network |
| **Authentication** | SASL (SCRAM, OAUTHBEARER, GSSAPI, PLAIN) or mTLS | The broker knows who connected |
| **Authorization** | ACLs + `StandardAuthorizer` | A client can do only what it's allowed to |

By default Kafka does none of this: anyone who can reach the port can read and write everything. The module 2 cluster is for development only.

## 17.2 Authentication mechanisms

| Mechanism | How | When |
|---|---|---|
| `SCRAM-SHA-512` | Username and password; the password isn't sent in clear, it's stored in the cluster metadata | The standard choice for most setups |
| `OAUTHBEARER` | Tokens from an OAuth/OIDC provider | Single sign-on, short-lived tokens, clouds |
| mTLS | A client certificate; the name comes from the certificate DN | When you already have a PKI |
| `GSSAPI` | Kerberos | Corporate environments with Kerberos |
| `PLAIN` | Username and password in clear inside TLS | Only with external verification; never without TLS |

## 17.3 A broker with SASL_SSL and SCRAM

```properties
listeners=SASL_SSL://:9094,CONTROLLER://:9093
advertised.listeners=SASL_SSL://kafka-1.example.com:9094
listener.security.protocol.map=SASL_SSL:SASL_SSL,CONTROLLER:SSL
inter.broker.listener.name=SASL_SSL
controller.listener.names=CONTROLLER

sasl.enabled.mechanisms=SCRAM-SHA-512
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512
listener.name.sasl_ssl.scram-sha-512.sasl.jaas.config=\
  org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="broker" password="<broker-password>";

ssl.keystore.location=/etc/kafka/certs/kafka-1.keystore.p12
ssl.keystore.password=<...>
ssl.key.password=<...>
ssl.truststore.location=/etc/kafka/certs/truststore.p12
ssl.truststore.password=<...>

# authorization
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
super.users=User:broker;User:admin
allow.everyone.if.no.acl.found=false
```

In KRaft, SCRAM credentials are stored in the cluster metadata. The first users (including the inter-broker one) are added when formatting storage, or the brokers can't connect to each other:

```bash
kafka-storage.sh format --config server.properties --cluster-id <id> \
  --add-scram 'SCRAM-SHA-512=[name=broker,password=<broker-password>]' \
  --add-scram 'SCRAM-SHA-512=[name=admin,password=<admin-password>]'
```

Other users are created on the running cluster:

```bash
kafka-configs.sh --bootstrap-server kafka-1.example.com:9094 --command-config admin.properties \
  --alter --entity-type users --entity-name billing \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=<password>]'
```

## 17.4 Client settings

```properties
bootstrap.servers=kafka-1.example.com:9094,kafka-2.example.com:9094,kafka-3.example.com:9094
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="billing" password="<password>";
ssl.truststore.location=/etc/app/truststore.p12
ssl.truststore.password=<...>
```

For `confluent-kafka` (librdkafka) the same parameters are called `security.protocol`, `sasl.mechanisms`, `sasl.username`, `sasl.password`, `ssl.ca.location`.

## 17.5 ACLs

```bash
# consumer billing: read the topic and use its group
kafka-acls.sh --bootstrap-server kafka-1.example.com:9094 --command-config admin.properties \
  --add --allow-principal User:billing \
  --consumer --topic shop.orders.events --group billing

# producer order-service: write to topics prefixed shop.orders. (idempotence included)
kafka-acls.sh --bootstrap-server kafka-1.example.com:9094 --command-config admin.properties \
  --add --allow-principal User:order-service \
  --producer --topic shop.orders. --resource-pattern-type prefixed

# a transactional producer needs rights on its transactional.id
kafka-acls.sh --bootstrap-server kafka-1.example.com:9094 --command-config admin.properties \
  --add --allow-principal User:invoicer \
  --operation Write --operation Describe --transactional-id invoicer- --resource-pattern-type prefixed

kafka-acls.sh --bootstrap-server kafka-1.example.com:9094 --command-config admin.properties \
  --list --topic shop.orders.events
```

`--producer` and `--consumer` are shortcuts for a set of operations (`Write`, `Describe`, `Create` for a producer; `Read`, `Describe` on the topic and `Read` on the group for a consumer).

Prefixed ACLs (`--resource-pattern-type prefixed`) are the main tool for keeping order: the `shop` team gets rights on `shop.*`, and nobody has to grant rights for every new topic.

## 17.6 What else matters

- **TLS on every listener**, including inter-broker and controller ones: replicas carry all the cluster's data.
- **Kafka has no encryption at rest.** Use disk encryption at the OS or cloud level, and encrypt sensitive fields in the application before sending.
- **Quotas** (module 15.6) are security too: they protect against a noisy or compromised client.
- **Secrets don't belong in configs.** For Connect use a `ConfigProvider` (e.g. `FileConfigProvider` or a Vault provider) so database passwords don't sit in the connector JSON in clear text.
- **The Kafka Connect REST API** must not be reachable from outside: through it anyone can create a connector that reads any data.

## 17.7 Security checklist

- [ ] TLS on client, inter-broker and controller listeners
- [ ] SASL (SCRAM or OAUTHBEARER) or mTLS for every client
- [ ] `StandardAuthorizer` and `allow.everyone.if.no.acl.found=false`
- [ ] A separate user per service, rights by topic prefix
- [ ] Minimal `super.users`
- [ ] Client quotas
- [ ] Disk encryption; sensitive fields encrypted in the application
- [ ] Connect secrets via a ConfigProvider
- [ ] Connect REST API and JMX not exposed
- [ ] Password and certificate rotation automated
- [ ] Alerts on authentication errors

---

# Module 18. Kafka in production: Kubernetes, operations, upgrades

## 18.1 Reference architecture

```
                 applications (producers / consumers / Streams)
                                   |
        +--------------------------+--------------------------+
        |                  Kafka cluster                      |
        |  controllers: 3 dedicated nodes (one per zone)      |
        |  brokers: 3+ nodes, broker.rack = zone              |
        |  RF=3, min.insync.replicas=2                        |
        +--------------------------+--------------------------+
             |                |                  |
     Schema Registry    Kafka Connect       MirrorMaker 2 --> standby region
     (2+ instances)     (3+ workers)
             |
      Prometheus + Grafana + Alertmanager, UI (Kafka UI / AKHQ)
```

Dedicated controllers (`process.roles=controller`) are better than combined mode in production: broker load doesn't affect the metadata quorum, and brokers can be restarted and scaled independently.

## 18.2 Sizing

| Load | Configuration |
|---|---|
| Up to 10 MB/s, dozens of topics | 3 nodes in combined mode, 4–8 vCPU, 16–32 GB RAM |
| Up to 100 MB/s | 3 controllers (2 vCPU, 4–8 GB) + 3–6 brokers (8–16 vCPU, 64 GB, NVMe or fast SSD) |
| Hundreds of MB/s and more | 3 controllers + 6–20+ brokers, several disks per broker, 25 Gbit/s network, tiered storage |
| Several regions | A cluster per region + MirrorMaker 2 |

Calculate separately: disk (module 7.7), network (writes × RF + reads × number of groups) and partitions per broker (thousands are fine, tens of thousands deliberately).

## 18.3 Kubernetes and Strimzi

On Kubernetes, Kafka is almost always deployed through an operator. The open-source standard is **Strimzi** (a CNCF project): it manages brokers, controllers, topics, users, Connect and MirrorMaker 2 through custom resources.

```yaml
apiVersion: kafka.strimzi.io/v1beta2    # check the current API version in the Strimzi docs
kind: KafkaNodePool
metadata:
  name: controllers
  labels: { strimzi.io/cluster: shop }
spec:
  replicas: 3
  roles: [controller]
  storage: { type: persistent-claim, size: 20Gi }
---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaNodePool
metadata:
  name: brokers
  labels: { strimzi.io/cluster: shop }
spec:
  replicas: 3
  roles: [broker]
  storage: { type: persistent-claim, size: 500Gi }
---
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: shop
spec:
  kafka:
    version: 4.3.1
    listeners:
      - { name: tls, port: 9093, type: internal, tls: true, authentication: { type: scram-sha-512 } }
    authorization: { type: simple }
    rack: { topologyKey: topology.kubernetes.io/zone }
    config:
      default.replication.factor: 3
      min.insync.replicas: 2
      offsets.topic.replication.factor: 3
      transaction.state.log.replication.factor: 3
      transaction.state.log.min.isr: 2
      auto.create.topics.enable: false
  entityOperator:
    topicOperator: {}
    userOperator: {}
---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: shop.orders.events
  labels: { strimzi.io/cluster: shop }
spec:
  partitions: 12
  replicas: 3
  config: { min.insync.replicas: 2, retention.ms: 604800000 }
```

What matters on Kubernetes:

- **persistent volumes** are mandatory, with a `StorageClass` on fast disks;
- **anti-affinity and rack awareness** across zones: three brokers in one zone won't survive its failure;
- access from outside the cluster is its own problem (`type: loadbalancer`, `nodeport`, `ingress` in Strimzi), because every broker needs its own address (remember `advertised.listeners` from module 2);
- topics and users as `KafkaTopic` and `KafkaUser` in Git: that is your infrastructure as code.

## 18.4 Upgrades

Since 4.0 Kafka is KRaft-only. **A ZooKeeper-based cluster can't be upgraded to 4.x directly**: migrate to KRaft on 3.9 first, then upgrade.

Upgrading a KRaft cluster:

```
1. read the upgrade notes of the target version
2. check health: URP = 0, offline = 0, the quorum is fine
3. upgrade one node at a time: controllers first, then brokers
   - stop the node (controlled shutdown moves leadership away)
   - update binaries or the image
   - start it and wait until URP is 0 again
4. once every node is upgraded and stable, raise the cluster feature version
```

```bash
kafka-features.sh --bootstrap-server kafka-1:19092 describe
kafka-features.sh --bootstrap-server kafka-1:19092 upgrade --release-version 4.3
```

Until the feature (metadata) version is raised you can roll binaries back to the previous version. After raising it, rolling back is harder, so don't rush the last step.

Kafka clients are compatible in both directions across a wide range of versions, but Kafka 4.0 dropped very old protocol versions: clients older than 2.1 can't connect to 4.x brokers. Check your library versions before upgrading brokers.

## 18.5 Production checklist

- [ ] 3 dedicated controllers and 3+ brokers in different zones, `broker.rack` set
- [ ] `default.replication.factor=3`, `min.insync.replicas=2`, internal topics with RF=3
- [ ] `auto.create.topics.enable=false`, topics created via IaC
- [ ] `unclean.leader.election.enable=false`
- [ ] Producers: `acks=all`, idempotence, send errors handled
- [ ] Consumers: manual commit after processing, idempotent processing
- [ ] Schema Registry with a compatibility mode and a CI check
- [ ] A DLQ and an alert on it
- [ ] Monitoring: URP, under-min-ISR, offline, controller, lag, disks
- [ ] TLS, SASL, ACLs, quotas
- [ ] Disks on persistent volumes, usage < 70%
- [ ] Rehearsed: broker failure, zone failure, full disk, rolling upgrade
- [ ] For multi-DC: MirrorMaker 2 and a rehearsed failover procedure

## 18.6 Anti-patterns

| Anti-pattern | Why it's bad | Do this instead |
|---|---|---|
| `replication.factor=1` for important data | A broker dies, the data is gone | RF=3 |
| `acks=all` without `min.insync.replicas=2` | Effectively `acks=1` when the ISR shrinks | Both settings together |
| Auto-created topics | A typo creates a topic with default settings and one partition | Explicit creation via IaC |
| A key with few values (`country`, `tenant`) | Hot partitions | A high-cardinality key |
| Thousands of topics "per customer" | Load on metadata and the controller | A shared topic keyed by customer |
| Auto-commit with complex processing | Losses on crashes | Manual commit after processing |
| Committing before processing | At-most-once | Commit after successful processing |
| Large messages (tens of MB) | Memory and replication pressure | Object storage + a reference in the event |
| Kafka as a queryable database | Kafka can't search by field | Materialise into a database or a state store |
| Request-reply over Kafka | Slow and complicated | HTTP or gRPC |
| No schemas in shared topics | Silent consumer breakage | Schema Registry |
| A 32 GB+ broker heap | Steals the page cache | 4–8 GB heap, the rest for the OS |

---

# Module 19. Capstone project: an event-driven online shop

## 19.1 What you're building

```
                     ┌──────────────┐
  HTTP  ────────────►│  Order API   │── INSERT orders + outbox (one transaction)
                     └──────┬───────┘
                            │ PostgreSQL WAL
                     ┌──────▼───────────────┐
                     │ Debezium (Connect)   │  Outbox Event Router
                     └──────┬───────────────┘
                            │ shop.orders.events (Avro, key order_id)
          ┌─────────────────┼──────────────────────┬───────────────────┐
          ▼                 ▼                      ▼                   ▼
   Payment Service    Warehouse Service     Order Stats (Streams)   ClickHouse sink
   consumer group     consumer group        windows, KTable         (Connect)
   idempotent         writes transactionally exactly_once_v2
          │           to shop.stock.events
          ▼
   shop.payments.events ──► Notification workers (share group) ──► email / push
          │
   retry topics + DLQ at every stage

   Plus:
   - Schema Registry: Avro schemas, BACKWARD_TRANSITIVE, checked in CI
   - monitoring: lag of every group, URP, disks, DLQ, replication slot
   - a 3-node cluster, RF=3, min.insync.replicas=2
```

## 19.2 Requirements

1. Orders are created over HTTP, and the event reaches Kafka via the outbox and Debezium: **no dual writes**.
2. All order events are keyed by `order_id` and go to one topic: the lifecycle order is preserved.
3. Event schemas live in Schema Registry, and an incompatible change fails CI.
4. Payment Service processes idempotently (a `processed_events` table) and commits the offset after the database COMMIT.
5. Warehouse Service reads orders and writes reservations to `shop.stock.events` **in one transaction** with the offset commit.
6. Order Stats on Kafka Streams computes payments per minute and revenue per user with `exactly_once_v2`.
7. Notifications go through a share group: workers scale independently of the partition count.
8. Every consumer has a retry path and a DLQ, and the DLQ alerts.
9. A dashboard: lag of every group, URP, disks, replication slot lag.
10. The system survives stopping any broker with no data loss and no write outage.

## 19.3 Stages

| Stage | What to do |
|---|---|
| 1 | A 3-node cluster, Schema Registry, Connect with Debezium, PostgreSQL |
| 2 | Topics via a script or IaC: partitions, RF, `min.insync.replicas`, retention |
| 3 | Order API: HTTP + PostgreSQL + outbox |
| 4 | The Debezium outbox connector, events with the right key in `shop.orders.events` |
| 5 | Avro schemas, a compatibility mode, a CI check |
| 6 | Payment Service: idempotent consumer, retry topic, DLQ |
| 7 | Warehouse Service: consume-transform-produce with transactions |
| 8 | Order Stats on Kafka Streams |
| 9 | Notification workers on a share group |
| 10 | Monitoring and alerts |
| 11 | Drills: stop a broker, kill a consumer mid-processing, stop Debezium, send a broken message |

## 19.4 How to verify it works

```bash
# load
kt kafka-producer-perf-test.sh --topic shop.orders.events --num-records 200000 --record-size 512 \
  --throughput 5000 --producer-props bootstrap.servers=kafka-1:19092 acks=all

# while the load runs
docker stop kafka-2
kt kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --under-min-isr-partitions   # must be empty
kt kafka-consumer-groups.sh --bootstrap-server kafka-1:19092 --describe --all-groups         # lag grows and drains
docker start kafka-2

# idempotency: stop Payment Service mid-processing and start it again
# the database must not show double charges
```

If after all the drills the lag of every group is back to zero, the DLQ is empty (or holds only deliberately broken messages), the database has no duplicates, and the order count in ClickHouse matches PostgreSQL, you've finished the course.

---

# Kafka CLI cheat sheet

All tools live in `/opt/kafka/bin` (in the `apache/kafka` Docker image). For a secured cluster add `--command-config client.properties`.

```bash
B=kafka-1:19092

# cluster
kafka-metadata-quorum.sh --bootstrap-server $B describe --status
kafka-metadata-quorum.sh --bootstrap-server $B describe --replication
kafka-features.sh --bootstrap-server $B describe
kafka-broker-api-versions.sh --bootstrap-server $B

# topics
kafka-topics.sh --bootstrap-server $B --list
kafka-topics.sh --bootstrap-server $B --create --topic t --partitions 12 --replication-factor 3 --config min.insync.replicas=2
kafka-topics.sh --bootstrap-server $B --describe --topic t
kafka-topics.sh --bootstrap-server $B --alter --topic t --partitions 24
kafka-topics.sh --bootstrap-server $B --delete --topic t
kafka-topics.sh --bootstrap-server $B --describe --under-replicated-partitions
kafka-topics.sh --bootstrap-server $B --describe --under-min-isr-partitions
kafka-topics.sh --bootstrap-server $B --describe --unavailable-partitions
kafka-get-offsets.sh --bootstrap-server $B --topic t

# configs
kafka-configs.sh --bootstrap-server $B --describe --entity-type topics --entity-name t
kafka-configs.sh --bootstrap-server $B --alter --entity-type topics --entity-name t --add-config retention.ms=86400000
kafka-configs.sh --bootstrap-server $B --alter --entity-type topics --entity-name t --delete-config retention.ms
kafka-configs.sh --bootstrap-server $B --describe --entity-type brokers --entity-name 1

# console producer / consumer
kafka-console-producer.sh --bootstrap-server $B --topic t --property parse.key=true --property key.separator=:
kafka-console-consumer.sh --bootstrap-server $B --topic t --from-beginning \
  --property print.key=true --property print.partition=true --property print.offset=true --property print.headers=true
kafka-console-consumer.sh --bootstrap-server $B --topic t --group g

# consumer groups
kafka-consumer-groups.sh --bootstrap-server $B --list
kafka-consumer-groups.sh --bootstrap-server $B --describe --group g
kafka-consumer-groups.sh --bootstrap-server $B --describe --group g --members
kafka-consumer-groups.sh --bootstrap-server $B --group g --topic t --reset-offsets --to-earliest --dry-run
kafka-consumer-groups.sh --bootstrap-server $B --group g --topic t --reset-offsets --to-datetime 2026-09-15T00:00:00.000 --execute
kafka-consumer-groups.sh --bootstrap-server $B --delete --group g

# share groups
kafka-share-groups.sh --bootstrap-server $B --list
kafka-share-groups.sh --bootstrap-server $B --describe --group sg

# replication and leaders
kafka-leader-election.sh --bootstrap-server $B --election-type preferred --all-topic-partitions
kafka-reassign-partitions.sh --bootstrap-server $B --reassignment-json-file plan.json --execute --throttle 50000000
kafka-reassign-partitions.sh --bootstrap-server $B --reassignment-json-file plan.json --verify
kafka-log-dirs.sh --bootstrap-server $B --describe --topic-list t

# ACLs and users
kafka-acls.sh --bootstrap-server $B --list
kafka-acls.sh --bootstrap-server $B --add --allow-principal User:app --consumer --topic t --group g
kafka-configs.sh --bootstrap-server $B --alter --entity-type users --entity-name app --add-config 'SCRAM-SHA-512=[password=secret]'

# performance
kafka-producer-perf-test.sh --topic t --num-records 1000000 --record-size 1024 --throughput -1 --producer-props bootstrap.servers=$B acks=all
kafka-consumer-perf-test.sh --bootstrap-server $B --topic t --messages 1000000

# Kafka Streams
kafka-streams-application-reset.sh --bootstrap-server $B --application-id my-app --input-topics t
```

---

# Configuration cheat sheet

**A topic for business events:**

```
partitions: 12–24 (see the estimate in module 3.4)
replication.factor: 3
min.insync.replicas: 2
cleanup.policy: delete
retention.ms: 604800000 (7 days) or longer
```

**A state topic (latest value per key):**

```
cleanup.policy: compact
min.insync.replicas: 2
delete.retention.ms: 86400000
segment.ms: 3600000        (so compaction doesn't wait a week)
```

**Producer:**

```
acks: all
enable.idempotence: true   (default)
linger.ms: 5–20
batch.size: 64–128 KB
compression.type: lz4 or zstd
delivery.timeout.ms: 120000
+ always check the send() result
```

**Consumer:**

```
group.protocol: consumer   (the new protocol, KIP-848)
enable.auto.commit: false
auto.offset.reset: earliest
max.poll.records: matched to batch processing time
isolation.level: read_committed   (if producers are transactional)
group.instance.id: a stable instance id (static membership)
+ commit after processing, idempotent processing
```

**Broker (production):**

```
process.roles: broker (controllers separate)
default.replication.factor: 3
min.insync.replicas: 2
offsets.topic.replication.factor: 3
transaction.state.log.replication.factor: 3
transaction.state.log.min.isr: 2
auto.create.topics.enable: false
unclean.leader.election.enable: false
broker.rack: <zone>
heap: 4–8 GB, the rest of the memory for the page cache
```

---

# Kafka interview questions with answers

**Junior**

1. **How does Kafka differ from a classic queue?** Kafka is a log: messages aren't deleted after reading, each consumer keeps its own position (offset) and can re-read history. In a queue a message disappears after acknowledgement.
2. **What are a topic and a partition?** A topic is a named stream of events. A partition is an ordered, immutable log inside a topic, the unit of ordering, parallelism and replication.
3. **What is an offset?** The sequence number of a record within a partition. Unique only within the partition and never changes.
4. **How does a message get to a partition?** By key hash (`hash(key) % partitions`); without a key the sticky partitioner distributes batches; the partition can also be set explicitly.
5. **Where is ordering guaranteed?** Only within one partition. That's why one entity's events are sent with the same key.
6. **What is a consumer group?** A set of consumers sharing a topic's partitions: each partition is assigned to one consumer in the group. Different groups read the topic independently.
7. **Why are more consumers than partitions useless?** A partition is read by only one consumer in the group; the extras sit idle.
8. **What is the replication factor?** The number of copies of each partition on different brokers. Usually 3 in production.
9. **What is KRaft?** Kafka's mode without ZooKeeper: metadata is held by a controller quorum using Raft. Since 4.0 it's the only mode.
10. **What does retention do?** Deletes old segments by time (`retention.ms`) or size (`retention.bytes`).

**Middle**

11. **How do acks=0, 1 and all differ?** 0: don't wait for a response; 1: wait for the leader's write; all: wait for every ISR replica.
12. **What is min.insync.replicas for?** It sets the minimum ISR size for `acks=all` writes. Without it, `acks=all` degrades to `acks=1` when the ISR shrinks.
13. **What is the ISR?** In-Sync Replicas: replicas that haven't lagged behind the leader longer than `replica.lag.time.max.ms`. A new leader is elected from the ISR.
14. **What is the high watermark?** The last offset written to every ISR replica. Consumers don't see records past it.
15. **What does the idempotent producer protect against?** Duplicates from network retries within one producer session, using the producer id and sequence numbers.
16. **What is a committed offset and when do you commit it?** The next offset the group should read. Commit after processing: that's at-least-once.
17. **Why is auto-commit dangerous?** The offset may be committed before processing finishes, and a crash loses messages.
18. **When does auto.offset.reset apply?** Only when the group has no committed offset (a new group or the offset was deleted). To re-read for an existing group you reset offsets.
19. **What is a rebalance and why is it harmful?** Redistribution of partitions when group membership changes. The old eager mode stopped the whole group; cooperative mode and the new KIP-848 protocol make it incremental.
20. **What does static membership give you?** `group.instance.id` lets a consumer restart within the session timeout without a rebalance.
21. **What happens if you add partitions?** Keys start landing in other partitions; ordering between old and new events of a key breaks temporarily. You can't decrease the partition count.
22. **What is log compaction?** A mode that keeps the latest value of every key. A tombstone (null value) deletes a key.
23. **Why Schema Registry?** It stores schemas, gives them ids and refuses incompatible changes; a message carries only the schema id.
24. **How does BACKWARD differ from FORWARD?** BACKWARD: the new schema reads old data (upgrade consumers first). FORWARD: the old schema reads new data (upgrade producers first).
25. **What is CDC and how does Debezium work?** Capturing changes from the database transaction log. Debezium reads the WAL/binlog and publishes every row change to Kafka.

**Senior**

26. **How do you get exactly-once?** Kafka → Kafka: a transactional producer with `sendOffsetsToTransaction` and consumers with `read_committed`, or Kafka Streams with `exactly_once_v2`. With external systems: at-least-once plus idempotent writes.
27. **Why must transactional.id be stable?** It survives restarts and lets the broker fence a zombie instance with an older epoch, so two instances can't write under one identity.
28. **How do you solve the dual write to a database and Kafka?** A transactional outbox: the event is written to a table in the same transaction and a relay (e.g. Debezium) publishes it; consumers deduplicate by event id.
29. **What is unclean leader election and why is it turned off?** Electing a lagging replica as leader when the ISR is empty. It restores availability at the cost of losing acknowledged writes.
30. **How do you choose the partition count?** Target throughput divided by the speed of one consumer and one partition, with headroom for growth and the maximum number of instances. Remember that increasing it breaks the key mapping.
31. **How do you handle errors without stopping a partition?** Retry topics with delays and a DLQ, at the cost of ordering; if ordering is critical, blocking retries with `pause/resume`.
32. **What are share groups and when do you need them?** A queue on top of Kafka (KIP-932, production-ready since 4.2): several consumers read one partition, each record is acknowledged individually, and deliveries are counted. For jobs without ordering requirements.
33. **Why can't offsets be carried across clusters during replication?** Offsets differ between clusters; MirrorMaker 2 translates them approximately via checkpoints, so processing must be idempotent after failover.
34. **Why isn't the broker heap made large?** Kafka serves data from the OS page cache; a large heap steals memory from the cache and lengthens GC pauses.
35. **Which metrics do you alert on first?** Under-min-ISR and offline partitions, active controller count, under-replicated partitions, growing consumer lag, disk usage, messages in DLQs.

---

# FAQ

**Does Kafka lose messages?**
With `replication.factor=3`, `min.insync.replicas=2`, `acks=all`, unclean election off and send results checked, no. Most "losses" happen in the application: the `send()` error wasn't checked, the offset was committed before processing, or reading started from `latest`.

**Do I need ZooKeeper?**
No. Since Kafka 4.0 ZooKeeper has been removed completely. ZooKeeper clusters must first migrate to KRaft on 3.9.

**Can I decrease the number of partitions?**
No. Only by creating a new topic and moving the data and consumers.

**How do I re-read a topic from the start?**
Stop the group's consumers and reset offsets: `kafka-consumer-groups.sh --reset-offsets --to-earliest --execute`. Or start a consumer with a new `group.id` and `auto.offset.reset=earliest`.

**Why does my consumer read nothing?**
Most often: the group already has an offset at the end of the topic, or it's a new group with `auto.offset.reset=latest`, or the client can't reach the brokers via `advertised.listeners`.

**How many topics and partitions can a cluster handle?**
KRaft is designed for hundreds of thousands of partitions per cluster, but each partition costs memory, files and recovery time. Thousands per broker are fine; beyond that, calculate deliberately.

**Can Kafka keep data forever?**
Yes: `retention.ms=-1` or compacted topics. For large volumes use tiered storage so you don't keep everything on local disks.

**Can I search messages by a field?**
No, Kafka is not a database. Materialise the data into a database, a search engine or a Kafka Streams state store.

**Kafka or RabbitMQ?**
Kafka when several systems need the event stream and history, replay and data integration matter. RabbitMQ when you need flexible routing, priorities and a classic task queue. For queues inside Kafka there are now share groups.

**Do I need Schema Registry?**
For shared topics read by different teams, yes. For a prototype or a topic internal to one service you can start without it.

**How do I send a large message?**
Put the object in S3 or other storage and send a reference plus metadata to Kafka (the claim check pattern). Raising `max.message.bytes` to tens of megabytes isn't worth it.

**Which client for Go and Python?**
For Go, franz-go (pure Go, full protocol support) or confluent-kafka-go (a librdkafka wrapper). For Python, confluent-kafka (librdkafka). Check support for the features you need (KIP-848, share groups) in the version you pick.

---

# Kafka glossary

| Term | Meaning |
|---|---|
| **Broker** | A Kafka server that stores partitions and serves clients |
| **Controller** | A KRaft quorum node that manages cluster metadata |
| **KRaft** | Kafka's mode without ZooKeeper; metadata is replicated via Raft |
| **Topic** | A named stream of events |
| **Partition** | An ordered log inside a topic |
| **Offset** | A record's position in a partition |
| **Record** | An event: key, value, headers, timestamp |
| **Segment** | A file on disk, part of a partition's log |
| **Replica** | A copy of a partition on a broker |
| **Leader / Follower** | The replica that accepts writes / a replica copying from the leader |
| **ISR** | In-Sync Replicas |
| **High watermark** | The last offset written to every ISR replica |
| **URP** | Under-replicated partitions, partitions with an incomplete ISR |
| **acks** | How many replicas must confirm a write |
| **min.insync.replicas** | The minimum ISR size for `acks=all` writes |
| **Idempotent producer** | A producer whose retries don't create duplicates |
| **Transactional producer** | A producer that writes atomically to several partitions together with offsets |
| **Consumer group** | Consumers sharing partitions |
| **Committed offset** | The next offset the group will read |
| **Lag** | The gap between the log end and the committed offset |
| **Rebalance** | Redistribution of partitions within a group |
| **Static membership** | A stable `group.instance.id` that avoids rebalances on restarts |
| **Share group** | A group that reads partitions like a queue, acknowledging each record |
| **Retention** | The policy for deleting old data |
| **Log compaction** | Keeping the latest value of every key |
| **Tombstone** | A record with a null value that deletes a key in a compacted topic |
| **Tiered storage** | Offloading old segments to object storage |
| **Schema Registry** | A service that stores schemas and checks compatibility |
| **Kafka Connect** | A framework of connectors to external systems |
| **SMT** | Single Message Transform in Kafka Connect |
| **CDC** | Change Data Capture from a database transaction log |
| **Debezium** | A set of CDC connectors for Kafka Connect |
| **Kafka Streams** | A Java stream processing library |
| **KStream / KTable** | A stream of events / a table of latest values per key |
| **Changelog topic** | A topic backing up a Kafka Streams state store |
| **MirrorMaker 2** | Replication of topics and offsets between clusters |
| **DLQ** | Dead letter queue, a topic for messages that can't be processed |
| **Outbox** | An events table written in the same transaction as the business data |

---

# Official sources and what to read next

- **Apache Kafka documentation** — https://kafka.apache.org/documentation
- **Release announcements and upgrade notes** — https://kafka.apache.org/blog/releases/
- **KIPs (Kafka Improvement Proposals)** — https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Improvement+Proposals
- **Source code** — https://github.com/apache/kafka
- **Official Docker image** — https://hub.docker.com/r/apache/kafka
- **Debezium** — https://debezium.io/documentation/
- **Strimzi (Kafka on Kubernetes)** — https://strimzi.io/documentation/
- **franz-go (Go client)** — https://github.com/twmb/franz-go
- **confluent-kafka-python** — https://github.com/confluentinc/confluent-kafka-python
- **Confluent Developer: courses and patterns** — https://developer.confluent.io
- **Books:** "Kafka: The Definitive Guide" (2nd edition), Ben Stopford's "Designing Event-Driven Systems", Martin Kleppmann's "Designing Data-Intensive Applications" (the chapters on logs and stream processing)

---

## Contributing

Found an error, an inaccuracy or an outdated setting? Open an issue or send a pull request. Especially welcome:

- examples in languages not covered here (C#, Node.js, Rust, Kotlin);
- real production stories and incident write-ups;
- corrections for newer Kafka releases.

⭐ If this course helped, star the repo so other developers can find it.

**Licence:** the course text is licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/), and the code samples under the [MIT License](LICENSE). You're free to use, adapt and share the material, including for internal workshops, as long as you credit the source.
