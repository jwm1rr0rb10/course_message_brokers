# NATS Course 2026: a free NATS and JetStream course from zero to pro

![NATS 2.12](https://img.shields.io/badge/NATS-2.12-27AAE1?logo=natsdotio&logoColor=white)
![JetStream](https://img.shields.io/badge/JetStream-persistence-blue)
![Language English](https://img.shields.io/badge/language-english-red)
![Free course](https://img.shields.io/badge/price-free-brightgreen)
![junior to senior](https://img.shields.io/badge/level-junior%20→%20senior-orange)

> **A complete free NATS course.** Theory, practice, Docker, Go, Python and Java, Core NATS and JetStream, subjects and wildcards, queue groups, request-reply, streams and consumers, delivery guarantees and deduplication, Key-Value and Object Store, microservices, clustering, superclusters and leaf nodes, monitoring, security, tuning and production architecture. All in a single README, current for **NATS Server 2.12 (2026)**.

**NATS without the fluff:** every module gives you clear theory, diagrams, commands you can actually run, the mistakes people make, and self-check questions. Use it to learn NATS from scratch, to prepare for a backend, platform or DevOps interview, and to design a reliable NATS-based system in production.

⭐ If the course helps, star the repository so other developers can find it.

🇷🇺 Русская версия: [READMEru.md](READMEru.md)

---

## Who this NATS course is for

| Who you are | What you get |
|---|---|
| **New to messaging** | What NATS is, how it differs from Kafka and RabbitMQ, and how to run it in under a minute |
| **Backend developer** (examples in Go, Python and Java; the patterns carry over to Node.js, .NET and Rust clients) | Pub/sub, request-reply, queue groups, durable delivery with JetStream, idempotency |
| **Microservices developer** | A service layer on NATS instead of HTTP, service discovery out of the box, distributed RPC |
| **Data / platform engineer** | Streams, mirrors and sources, geo-replication, KV and Object Store, an honest comparison with Kafka |
| **DevOps / SRE** | RAFT clustering, superclusters, leaf nodes, monitoring, alerting, sizing, Kubernetes |
| **Architect / Tech Lead** | Subject space design, event-driven architecture, multi-region, anti-patterns |
| **IoT / edge** | Leaf nodes on devices, surviving connectivity loss, the built-in MQTT and WebSocket listeners |
| **Interview prep** | 33 NATS questions with answers at junior, middle and senior level |

## What you'll be able to do after the course

- explain the NATS architecture: subject, message, client, server, cluster, account, JetStream domain;
- run a three-node NATS cluster in Docker and break it while watching RAFT leader elections;
- design a subject space and use the `*` and `>` wildcards properly;
- write publishers and subscribers in Go, Python and Java without losing or duplicating messages;
- use request-reply, queue groups and the scatter-gather pattern;
- tell the difference between Core NATS (at-most-once) and JetStream (at-least-once, exactly-once);
- create streams with the right retention policy: `limits`, `interest`, `workqueue`;
- configure consumers: pull vs push, `AckExplicit`, `AckWait`, `MaxDeliver`, `MaxAckPending`, backoff;
- deduplicate publishes with the `Nats-Msg-Id` header and the duplicate window;
- build a DLQ on top of the `MAX_DELIVERIES` advisory;
- use the Key-Value Store as config, cache, distributed lock and leader election;
- store files in the Object Store;
- write microservices on the Services API with automatic discovery and statistics;
- build a supercluster across data centres and attach leaf nodes at the edge;
- monitor `/varz`, `/jsz`, consumer lag, slow consumers, and set up alerts;
- enable TLS, accounts, NKeys, JWT auth, permissions and limits;
- size memory, disk and node count for production.

---

## Table of contents

- [Who this NATS course is for](#who-this-nats-course-is-for)
- [What you'll be able to do after the course](#what-youll-be-able-to-do-after-the-course)
- [How to take this course](#how-to-take-this-course)
- [Module 0. What NATS is and why it exists](#module-0-what-nats-is-and-why-it-exists)
- [Module 1. NATS architecture: subject, message, server, cluster](#module-1-nats-architecture-subject-message-server-cluster)
- [Module 2. Installing NATS in Docker and first commands](#module-2-installing-nats-in-docker-and-first-commands)
- [Module 3. Subjects and wildcards: designing the namespace](#module-3-subjects-and-wildcards-designing-the-namespace)
- [Module 4. Core NATS: pub/sub, request-reply, queue groups](#module-4-core-nats-pubsub-request-reply-queue-groups)
- [Module 5. JetStream: streams and message storage](#module-5-jetstream-streams-and-message-storage)
- [Module 6. JetStream consumers: pull, push, ack](#module-6-jetstream-consumers-pull-push-ack)
- [Module 7. Delivery guarantees, deduplication and exactly-once](#module-7-delivery-guarantees-deduplication-and-exactly-once)
- [Module 8. Retention, limits and working with disk](#module-8-retention-limits-and-working-with-disk)
- [Module 9. Key-Value Store](#module-9-key-value-store)
- [Module 10. Object Store](#module-10-object-store)
- [Module 11. Microservices on NATS: the Services API](#module-11-microservices-on-nats-the-services-api)
- [Module 12. Clustering, superclusters and leaf nodes](#module-12-clustering-superclusters-and-leaf-nodes)
- [Module 13. Mirrors, sources and geo-replication](#module-13-mirrors-sources-and-geo-replication)
- [Module 14. Error handling: retry, DLQ and poison pills](#module-14-error-handling-retry-dlq-and-poison-pills)
- [Module 15. Performance and tuning](#module-15-performance-and-tuning)
- [Module 16. Monitoring NATS: metrics, lag, alerts](#module-16-monitoring-nats-metrics-lag-alerts)
- [Module 17. NATS security: accounts, NKeys, JWT, TLS](#module-17-nats-security-accounts-nkeys-jwt-tls)
- [Module 18. NATS in production: architecture and operations](#module-18-nats-in-production-architecture-and-operations)
- [Module 19. Capstone project: an event-driven online shop](#module-19-capstone-project-an-event-driven-online-shop)
- [What's new in NATS 2.11 and 2.12](#whats-new-in-nats-211-and-212)
- [NATS CLI cheat sheet](#nats-cli-cheat-sheet)
- [Configuration cheat sheet](#configuration-cheat-sheet)
- [NATS interview questions with answers](#nats-interview-questions-with-answers)
- [FAQ](#faq)
- [NATS glossary](#nats-glossary)
- [Official sources and what to read next](#official-sources-and-what-to-read-next)

---

## How to take this course

1. **Go in order.** Modules 0-6 are the foundation. Without understanding subjects, queue groups, streams and acks, everything else looks like magic.
2. **Run every command.** NATS is learned by hand, and the `nats` CLI gives you feedback in seconds. Reading about redelivery and watching it in `nats stream view` are different levels of understanding.
3. **Break the cluster.** Stop nodes, kill consumers, fill up stream limits. That is how production experience appears.
4. **Answer the questions at the end of each module** out loud, as if you were in an interview.
5. **Do the capstone project.** It pulls every topic into one system.

**What to install:** Docker and Docker Compose, the `nats` CLI (natscli), Git, any IDE. Go 1.26+ for the Go examples (the current `nats.go` requires it), Python 3.10+ for Python, Java 17+ for Java.

**Version:** all examples target NATS Server 2.12.x, image `nats:2.12-alpine`. JetStream is enabled with the `-js` flag and is **off by default**.

**Runnable examples:** the cluster, a smoke test of the CLI commands, Go and Python code and integration tests for the course's claims live in [`examples/`](examples/). CI runs them against 2.12 and the newest 2.x release every week, so if a release changes behaviour described here, the build goes red.

---

# Module 0. What NATS is and why it exists

## 0.1 NATS in plain words

**NATS** is a messaging system for distributed systems: a single process (`nats-server`) of about 20 MB that does three things:

1. **Delivers messages instantly** between services using subject addresses (Core NATS).
2. **Stores messages durably** on disk and redelivers them until a consumer acknowledges them (JetStream).
3. **Holds state**: key-value (KV Store) and files (Object Store) on top of the same engine.

The core idea of NATS is **subject-based messaging**. A publisher sends a message to a subject such as `orders.created.eu` and does not care who receives it. All routing is built on hierarchical names and wildcards, with no exchanges, routing keys or bindings.

NATS was created by Derek Collison, who previously designed messaging systems at TIBCO. The first version was written in Ruby as the message bus of Cloud Foundry; the server was later rewritten in Go. NATS is a CNCF project backed by Synadia and is used in production by a wide range of companies, platforms and IoT deployments: see the adopters list on [nats.io](https://nats.io) for current, verifiable examples.

## 0.2 The problem NATS solves

Picture an online shop. A user places an order and several systems need to know about it:

```
                    +--> Payment Service
                    |
Order Service ------+--> Warehouse
                    |
                    +--> Analytics
                    |
                    +--> Notification Service
                    |
                    +--> Fraud Detection
```

Without a broker, Order Service calls every service directly over HTTP:

```
Order Service
   |
   +--> POST http://payment/...
   +--> POST http://warehouse/...
   +--> POST http://analytics/...
   +--> POST http://notification/...
   +--> POST http://fraud/...
```

Then the problems start:

| Question | The problem with synchronous integration |
|---|---|
| Notification Service is down | The whole order fails, or the notification is silently lost |
| Analytics takes 3 seconds | The user waits 3 seconds at checkout |
| Ten more consumers appear | You have to change and redeploy Order Service |
| A service moves to a new address | Configs, DNS and service mesh all need edits |
| Black Friday traffic spike | Downstream services fall over in a cascade |

This is **tight coupling**: every service knows every other service's address.

## 0.3 The same system with NATS

```
Order Service
     |
     | publish "shop.orders.created"
     v
+---------------------+
|    nats-server      |
+---------------------+
     |      |      |       |
     v      v      v       v
Payment  Warehouse Analytics Fraud
 (subscribe "shop.orders.>")
```

Order Service publishes **one message** to `shop.orders.created` and knows nothing about anyone else. Each consumer subscribes to whatever subject it cares about.

What you gain:

- **Loose coupling.** A new service plugs in without touching Order Service.
- **Location transparency.** Nobody knows anybody's IP or port: the address is a subject, not a host.
- **Service discovery for free.** Subscribing to a subject is registration.
- **Load balancing built in.** Ten instances of a service in one queue group split the load.
- **Durability on demand.** Need replay and guarantees? Turn on JetStream for that subject; publisher code barely changes.

## 0.4 Core NATS and JetStream: two layers, one system

This is **the single most important thing to understand about NATS**. One server contains two layers.

```
+---------------------------- nats-server ----------------------------+
|                                                                     |
|  Core NATS                         JetStream                        |
|  --------------------------        -----------------------------    |
|  in-memory delivery                writes to disk (or RAM)          |
|  at-most-once                      at-least-once / exactly-once     |
|  no subscriber = message           message sits in a stream         |
|  simply vanishes                   and waits for a consumer         |
|  latency ~microseconds             latency ~milliseconds            |
|  no replay                         replay, history, cursors         |
|  fire and forget                   ack, retry, DLQ                  |
+---------------------------------------------------------------------+
```

| Question | Core NATS | JetStream |
|---|---|---|
| Is the message stored? | No | Yes, in file or memory |
| What if there are no subscribers? | The message is lost | It is stored in the stream |
| Acknowledgement of processing | No | Yes (`ack`, `nak`, `term`, `inProgress`) |
| Redelivery | No | Yes, up to `MaxDeliver` times |
| Replay history | No | Yes, from any position or time |
| Typical latency | Tens to hundreds of microseconds | A few milliseconds |
| Use for | Telemetry, RPC, health, cache invalidation | Orders, payments, task queues, domain events |

**Rule of thumb:** Core NATS for things you can afford to lose and need fast. JetStream for anything the business is accountable for.

## 0.5 What a subject is

A **subject** is a string address split into dot-separated tokens:

```
shop.orders.created
shop.orders.cancelled
shop.payments.eu.succeeded
iot.sensors.london.dc1.temperature
```

Rules:

- tokens are separated by dots and are case-sensitive;
- `*` matches **exactly one** token: `shop.*.created`;
- `>` matches **one or more** trailing tokens and may only appear at the end: `shop.orders.>`;
- subjects starting with `$` are reserved by the system (`$JS.API.>`, `$SYS.>`, `$KV.>`).

A subject is not a "queue" and not a "topic you have to create". In Core NATS a subject **needs no creation**: you publish, and it exists exactly at that moment.

## 0.6 What a message is

```
Subject: shop.orders.created
Reply:   _INBOX.7fK2s3Lq.1      (where to reply, optional)
Headers: Nats-Msg-Id: 5f1c2a7e-9b1d-4c1e-8a4e-0c7b2d9f1a11
         Event-Type: OrderCreated
         Trace-Id: 4bf92f3577b34da6
Data:    {"order_id":"order-123","user_id":"user-42","amount":4990}
```

- **Data** is just bytes. NATS neither knows nor validates the format: JSON, Protobuf, Avro, MessagePack, anything.
- **Headers** arrived in NATS 2.2 and work in both Core and JetStream. Use them for `Nats-Msg-Id`, trace ids, event type, schema version.
- **Reply subject** turns a plain publish into an RPC request.
- The default maximum message size (`max_payload`) is **1 MB**. You can raise it (up to 64 MB) but you shouldn't: large messages clog buffers and create slow consumers. Files belong in the Object Store.

## 0.7 NATS vs Kafka vs RabbitMQ vs gRPC

| Criterion | NATS (Core + JetStream) | Apache Kafka | RabbitMQ | HTTP/gRPC |
|---|---|---|---|---|
| Model | Subject-based pub/sub + log (JetStream) | Distributed log | Queue broker (AMQP) | Request-response |
| What you deploy | One ~20 MB binary, no dependencies | JVM cluster, KRaft | Erlang cluster | Nothing |
| Request-reply | Built into the protocol | No, hand-rolled | Via reply-to queues | Native |
| Guarantees | At-most-once (Core) or at-least/exactly-once (JetStream) | At-least/exactly-once | At-least-once | Implementation-dependent |
| Storage and replay | Yes, JetStream | Yes, it's the core product | Limited (Streams) | No |
| Throughput | Millions msg/s (Core), hundreds of thousands (JetStream) | Millions msg/s | Tens to hundreds of thousands msg/s | Service-dependent |
| Latency | Tens of microseconds | Milliseconds | Sub-millisecond to milliseconds | Network-dependent |
| Routing | Hierarchical subjects and wildcards | Topics and partitions | Exchanges, routing keys, headers | URL paths |
| Multi-tenancy | Accounts with full isolation, built in | ACLs and prefixes | Vhosts | No |
| Geo / edge | Superclusters, gateways, leaf nodes | MirrorMaker 2 | Federation, shovel | No |
| Message ordering | Within a stream (and per subject) | Within a partition | Within a queue | No |
| Choose when | Microservices, edge/IoT, low latency, multi-region, "bus + queue + KV in one" | Big data, analytics, CDC, years of history, Connect/Streams ecosystem | Complex routing, priorities, per-message TTL, legacy AMQP | The user is waiting for a synchronous answer |

**An honest word on Kafka:** if you need terabytes of history, Kafka Connect, Kafka Streams, Debezium CDC and warehouse integration, use Kafka. If you need a nervous system for microservices with RPC, low latency, simple operations and reach across continents and the edge, NATS is usually the simpler fit. They are different tools and frequently live side by side in the same company.

### NATS and HTTP together

NATS does not replace HTTP:

```
Client --HTTP--> Order API --(saves order, returns 201)--> Client
                     |
                     +--publish shop.orders.created--> NATS --> everyone else
```

HTTP is for when the user is waiting for an answer. NATS is for asynchronous event distribution and internal service-to-service RPC.

## 0.8 Where NATS is used

| Scenario | How NATS is applied |
|---|---|
| **Microservices** | Internal RPC instead of HTTP, queue groups as the load balancer, Services API as discovery |
| **Event-driven architecture** | Domain events in JetStream, saga choreography, outbox |
| **IoT and edge** | A leaf node on the device or factory floor, buffering during outages, syncing upstream |
| **Telemetry and metrics** | Core NATS: losing a data point is fine, speed matters |
| **Distributed cache / config** | KV Store with watch: services learn about config changes instantly |
| **Task queues** | A `workqueue` stream with pull consumers as a worker pool |
| **Multi-region** | Supercluster: publish in Europe, consume in the US, no MirrorMaker |
| **Control plane** | Managing agents, deploy commands, collecting status from thousands of nodes |
| **Replacing Redis Pub/Sub and part of RabbitMQ** | One service instead of three systems |

## 0.9 When you don't need NATS

- You need years of event history, big-data pipelines, ClickHouse and Snowflake via ready-made connectors: use Kafka.
- You need complex header-based routing, message priorities and per-message TTL: RabbitMQ is more flexible.
- You have one monolith and three background jobs: a PostgreSQL or Redis queue is enough.
- You need a synchronous answer over HTTP: use HTTP.
- You want transactions between broker and database: nobody gives you that; design an outbox and idempotency.

## 0.10 What NATS will NOT do for you

NATS provides infrastructure guarantees. Correctness is still your design:

- idempotent processing in the application;
- event schemas and versioning (there is no Schema Registry in NATS, you own the contracts);
- retry strategy and poison-message handling;
- monitoring and alerting;
- accounts, permissions and limits;
- a well-thought-out subject space: renaming subjects later hurts.

### Self-check questions

1. How does Core NATS differ from JetStream, and when is each enough?
2. Why does a message disappear in Core NATS when there are no subscribers?
3. What is the reply subject for?
4. When would you pick Kafka over NATS?

---

# Module 1. NATS architecture: subject, message, server, cluster

## 1.1 The main hierarchy

```
Supercluster (several clusters joined by gateways)
  └── Cluster (servers joined by routes)
        └── Server (a nats-server process)
              └── Account (isolated namespace)
                    └── Subject (hierarchical address)
                          └── Message (subject + headers + payload)

  and separately, when JetStream is enabled:
  Account
    └── Stream (a set of subjects + storage rules)
          └── Consumer (cursor + delivery and ack policy)
```

Memorise this picture. The whole course hangs on it.

## 1.2 Core components

| Component | What it is | Analogy |
|---|---|---|
| **Message** | subject, headers, reply, payload | A letter with an address |
| **Subject** | Hierarchical message address | Email subject / URL path |
| **Client** | An application connected over TCP | A database client |
| **Server** | The `nats-server` process | A database server |
| **Cluster** | Servers joined by `routes` in a full mesh | A database cluster |
| **Gateway** | A link between clusters (supercluster) | A cross-region link |
| **Leaf node** | A server attached top-down, for edge and isolation | A branch office |
| **Account** | A fully isolated subject namespace | A separate virtual broker |
| **Queue group** | Subscribers sharing messages between them | A worker pool |
| **Stream** | JetStream storage for a set of subjects | A table / log |
| **Consumer** | A read position with an ack policy | Cursor + queue |
| **KV bucket** | Key-value on top of a stream | A Redis hash / etcd |
| **Object bucket** | File storage on top of a stream | An S3 bucket |

## 1.3 A full-mesh cluster

A NATS cluster is a **full mesh**: every server connects directly to every other.

```
        +-----------+
        | nats-1    |
        +-----------+
         /         \
        /           \
+-----------+   +-----------+
| nats-2    |---| nats-3    |
+-----------+   +-----------+
```

Consequences that matter:

- **a client connects to any server** and sees the whole cluster: messages are routed between servers automatically;
- the server tells the client about the other addresses, and the client library **reconnects on its own** when a node dies;
- a message takes at most **one hop** between servers inside a cluster, so latency is predictable;
- a server forwards a message to a peer **only if that peer has an interested subscriber**: interest propagation saves bandwidth;
- 3 or 5 nodes is the norm; rarely more than 7-9 in one cluster, beyond that you build a supercluster.

## 1.4 Interest-based routing

In Core NATS a message is never stored "just in case". The server knows who is subscribed to what and delivers precisely.

```
1. a subscriber on nats-2 issues SUB shop.orders.>
2. nats-2 tells nats-1 and nats-3: "I have interest in shop.orders.>"
3. a publisher on nats-1 issues PUB shop.orders.created
4. nats-1 delivers to local subscribers and forwards a copy to nats-2
5. nats-3 has no subscribers -> nothing is sent there
```

An important property follows: **publishing to a subject nobody is subscribed to costs nothing and goes nowhere.** That is not an error, and the client is never told. This is exactly why it is so easy to "lose" messages in Core NATS when a subscriber hasn't started yet.

## 1.5 How a client connects

```
1. client --> nats://nats-1:4222 : CONNECT {…}
2. server --> client: INFO {server_id, max_payload, connect_urls:[…], headers:true}
3. client --> server: SUB shop.orders.> 1
4. client --> server: PUB shop.orders.created 45 \r\n {payload}
5. server --> client: MSG shop.orders.created 1 45 \r\n {payload}
```

The NATS protocol is **plain text and human-readable**. You can inspect it with telnet:

```bash
telnet localhost 4222
```

The main protocol verbs are `CONNECT`, `PUB`, `HPUB`, `SUB`, `UNSUB`, `MSG`, `HMSG`, `PING`, `PONG`, `+OK`, `-ERR`. That simplicity is one reason NATS client libraries exist for nearly every language and stay small.

## 1.6 JetStream: the storage layer

JetStream is enabled with the `-js` flag or a `jetstream {}` config block. It adds:

- **streams** — message stores that capture given subjects;
- **consumers** — read positions with ack and redelivery policies;
- **KV and Object Store** — abstractions over streams;
- **RAFT replication** — each stream with `replicas=3` forms its own RAFT group.

```
publish "shop.orders.created"
        |
        v
+------------------------------------------+
| Stream ORDERS                            |
|  subjects: shop.orders.>                 |
|  storage:  file                          |
|  replicas: 3   (RAFT: leader + 2)        |
|  retention: limits, max_age: 168h        |
|                                          |
|  seq 1 [shop.orders.created  order-1]    |
|  seq 2 [shop.orders.paid     order-1]    |
|  seq 3 [shop.orders.created  order-2]    |
+------------------------------------------+
        |                 |
        v                 v
 consumer BILLING   consumer ANALYTICS
 (pull, ack explicit) (pull, ack none)
```

The key difference from Core NATS: the message is **written to the stream and acknowledged by the server first**, and only then consumers read it at their own pace.

## 1.7 The numbers that describe a stream

| Term | Meaning |
|---|---|
| **Stream sequence** | Global message number inside the stream, starting at 1 |
| **Consumer sequence** | Delivery counter for a given consumer (increases on redelivery too) |
| **Ack floor** | The stream sequence up to which the consumer has acknowledged everything |
| **Num pending** | How many messages the consumer still has to receive (the NATS equivalent of consumer lag) |
| **Num ack pending** | Delivered but not yet acknowledged |
| **Num redelivered** | Messages currently being redelivered |
| **First / last seq** | What is physically in the stream right now |

`num_pending` **is** the lag you should watch in monitoring.

## 1.8 Accounts: isolation inside one server

An **account** is a separate subject namespace. Two accounts never see each other's messages, even when publishing to identical subjects.

```
account APP_A: shop.orders.created   <-- invisible to account APP_B
account APP_B: shop.orders.created   <-- invisible to account APP_A
```

Crossing the boundary is only possible explicitly, via **exports** and **imports**. This makes NATS genuinely multi-tenant: one cluster serves dozens of teams and customers with no risk that someone subscribes to `>` and reads everyone's data. The system account `$SYS` holds monitoring subjects separately.

## 1.9 Your first mental model

```
Core NATS:
  publish subject -> server finds subscribers ->
  delivers to all (and to one member of each queue group) -> forgets the message

JetStream:
  publish subject -> the stream whose subjects matched stores the message ->
  RAFT replicates to a quorum -> server replies PubAck{stream, seq} ->
  consumer receives it -> processes -> ack ->
  the message stays in the stream until retention removes it
```

### Self-check questions

1. Why does publishing to a subject with no subscribers cost nothing, and why is that dangerous?
2. Why is a NATS cluster a full mesh, and how many hops does a message take?
3. How does stream sequence differ from consumer sequence?
4. What do accounts give you?

---

# Module 2. Installing NATS in Docker and first commands

## 2.1 The fastest way to run NATS

```bash
docker run -d --name nats -p 4222:4222 -p 8222:8222 nats:2.12-alpine -js -m 8222
```

- `4222` — client port;
- `6222` — cluster route port;
- `8222` — HTTP monitoring port;
- `-js` — enable JetStream (**without it there are no streams and no KV**);
- `-m 8222` — enable monitoring.

Check it:

```bash
curl -s localhost:8222/varz | head -20
curl -s localhost:8222/jsz
docker logs nats | grep -i jetstream
```

## 2.2 Install the nats CLI

`nats` is the official CLI. Life without it is an order of magnitude harder.

```bash
# macOS / Linux with Homebrew
brew tap nats-io/nats-tools && brew install nats-io/nats-tools/nats

# Go
go install github.com/nats-io/natscli/nats@latest

# Or just Docker
docker run --rm -it --network host natsio/nats-box:latest
```

Contexts save you from endless flags:

```bash
nats context add local --server nats://localhost:4222 --description "Local NATS"
nats context select local
nats server check connection
```

## 2.3 Your first message: pub/sub

Terminal 1:

```bash
nats sub "shop.orders.>"
```

Terminal 2:

```bash
nats pub shop.orders.created '{"order_id":"order-1","amount":4990}'
nats pub shop.orders.paid    '{"order_id":"order-1"}'
nats pub shop.payments.failed '{"order_id":"order-2"}'
```

The subscriber sees the first two messages and not the third: `shop.orders.>` does not cover `shop.payments.*`.

**The one experiment that matters.** Stop the subscriber, publish a message, start the subscriber again. The message is gone: that's Core NATS, at-most-once. This moment separates people who understand NATS from people who "tried it once".

## 2.4 Request-reply in 30 seconds

Terminal 1 — the service:

```bash
nats reply "service.echo" --echo
```

Terminal 2 — the client:

```bash
nats request "service.echo" "hello" --timeout 2s
```

Now run `nats reply "service.echo" --echo` in **three** terminals: that is automatically one queue group, and requests start spreading across them. Load balancing without a single line of configuration.

## 2.5 A three-node cluster in Docker Compose

Create a directory and the files:

```bash
mkdir nats-course && cd nats-course
```

`nats.conf` (shared by all three nodes, the differences come from flags):

```hcl
# nats.conf
port: 4222
http: 8222

jetstream {
  store_dir: "/data/jetstream"
  max_memory_store: 1GB
  max_file_store: 10GB
}

cluster {
  name: "course"
  port: 6222
  routes: [
    "nats://nats-1:6222"
    "nats://nats-2:6222"
    "nats://nats-3:6222"
  ]
}

accounts {
  # application account with JetStream enabled
  APP: {
    jetstream: enabled
    users: [ { user: "app", password: "app" } ]
  }
  # system account: server monitoring and `nats server ...` commands
  SYS: {
    users: [ { user: "admin", password: "admin" } ]
  }
}
system_account: SYS

# LEARNING ONLY: clients that connect without credentials are logged in as "app".
# Never do this in production (see module 17).
no_auth_user: app
```

Why this block matters: as soon as a config defines **any** user, the server requires authentication. Without `no_auth_user`, every command below that connects without credentials would fail with `Authorization Violation`. And without `jetstream: enabled` on the account, you'd get `jetstream not enabled for account` even though the server itself has JetStream on.

`docker-compose.yml`:

```yaml
x-nats-common: &nats-common
  image: nats:2.12-alpine
  restart: unless-stopped

services:
  nats-1:
    <<: *nats-common
    container_name: nats-1
    command: ["-c", "/etc/nats/nats.conf", "--server_name", "nats-1"]
    ports: ["4222:4222", "8222:8222"]
    volumes:
      - ./nats.conf:/etc/nats/nats.conf:ro
      - nats1-data:/data

  nats-2:
    <<: *nats-common
    container_name: nats-2
    command: ["-c", "/etc/nats/nats.conf", "--server_name", "nats-2"]
    ports: ["4223:4222", "8223:8222"]
    volumes:
      - ./nats.conf:/etc/nats/nats.conf:ro
      - nats2-data:/data

  nats-3:
    <<: *nats-common
    container_name: nats-3
    command: ["-c", "/etc/nats/nats.conf", "--server_name", "nats-3"]
    ports: ["4224:4222", "8224:8222"]
    volumes:
      - ./nats.conf:/etc/nats/nats.conf:ro
      - nats3-data:/data

  nats-box:
    image: natsio/nats-box:latest
    container_name: nats-box
    entrypoint: ["sleep", "infinity"]
    depends_on: [nats-1, nats-2, nats-3]

volumes:
  nats1-data:
  nats2-data:
  nats3-data:
```

Start it:

```bash
docker compose up -d
docker compose ps
```

Check the cluster:

```bash
# application context: lands in the APP account via no_auth_user
nats context add cluster \
  --server "nats://localhost:4222,nats://localhost:4223,nats://localhost:4224"
nats context select cluster

# system context: `nats server ...` commands need the system account
nats context add cluster-sys \
  --server "nats://localhost:4222,nats://localhost:4223,nats://localhost:4224" \
  --user admin --password admin

nats --context cluster-sys server list
nats --context cluster-sys server report jetstream
```

You'll see three servers, their roles and JetStream state. Every `nats server ...` command in this course needs system-account credentials; application commands (`pub`, `sub`, `stream`, `consumer`, `kv`) use the normal context. Clients list **all three addresses** in the connection string: if the first is unreachable, the library goes to the second.

## 2.6 The most common startup mistakes

| Symptom | Cause | Fix |
|---|---|---|
| `nats: no responders available for request` | Nobody is listening on that subject | Check the service is running and the subject matches character for character |
| `jetstream not enabled for account` | Server started without `-js`, or the account has no JetStream enabled | Add `-js` / a `jetstream {}` block to the server and `jetstream: enabled` (or limits) to the account |
| Cluster doesn't form | Nodes can't reach each other over `routes`, or cluster names differ | Check DNS names, port 6222, identical cluster name |
| Stream exists on one node only | JetStream is not clustered, `replicas=1` | Create streams with `--replicas 3` |
| Client drops with `slow consumer` | The subscriber can't keep up | See module 15 |
| `Authorization Violation` | Users are defined in the config and the client sent no credentials | Pass `--user`/`--password` or creds, or (for learning only) set `no_auth_user` |
| `nats server list` returns nothing or an error | The command needs the system account | Use a context with system-account credentials |

## 2.7 Where NATS stores data

```bash
docker exec nats-1 ls -R /data/jetstream
```

```
/data/jetstream/
└── APP/                     # account name ($G on a server with no accounts configured)
    └── streams/
        └── ORDERS/
            ├── msgs/
            │   ├── 1.blk    # message blocks
            │   └── 2.blk
            ├── obs/         # consumer state
            │   └── BILLING/
            └── meta.inf     # stream configuration
```

Core NATS stores **nothing**: stop the server and subscription state is gone. Everything that must survive a restart lives in `store_dir`. Mount it on a persistent volume, otherwise a pod restart in Kubernetes leaves you with an empty stream.

## 2.8 Your first stream and consumer

```bash
nats stream add ORDERS \
  --subjects "shop.orders.>" \
  --storage file \
  --replicas 3 \
  --retention limits \
  --max-age 168h \
  --max-msgs=-1 \
  --max-bytes 1GB \
  --discard old \
  --dupe-window 2m \
  --defaults
```

```bash
nats stream info ORDERS
nats stream ls
```

Publish and confirm messages are now stored:

```bash
nats pub shop.orders.created '{"order_id":"order-1"}'
nats pub shop.orders.created '{"order_id":"order-2"}'
nats stream view ORDERS
```

Create a consumer and read:

```bash
nats consumer add ORDERS BILLING \
  --pull \
  --ack explicit \
  --deliver all \
  --filter "shop.orders.created" \
  --max-deliver 5 \
  --wait 30s \
  --defaults

nats consumer next ORDERS BILLING --count 2
nats consumer info ORDERS BILLING
```

Now stop everything, start it again and read once more: the messages are still there. That's JetStream.

### Practice

1. Bring up the three-node cluster and check `nats server report jetstream`.
2. Publish 10 Core NATS messages with no subscriber and confirm they exist nowhere.
3. Create the `ORDERS` stream with `--replicas 3`, publish 10 messages, stop the stream leader (`nats stream info ORDERS` shows it) and confirm the data is still available.
4. Find your stream in `curl "localhost:8222/jsz?accounts=true&streams=true"`.

---

# Module 3. Subjects and wildcards: designing the namespace

## 3.1 How a subject is built

```
shop . orders . eu . created
 |       |       |     |
 t1      t2      t3    t4     -> dot-separated tokens
```

Constraints:

- empty tokens are invalid (`shop..created`);
- spaces and dots inside a token are not allowed;
- case matters: `Shop.Orders` and `shop.orders` are different subjects;
- practical length limit is around 255 bytes, and sane code stays under ~16 tokens;
- a subject does not need to be "created": it exists the moment you publish.

## 3.2 Wildcards

| Symbol | Matches | Example | Matches | Doesn't match |
|---|---|---|---|---|
| `*` | Exactly one token | `shop.*.created` | `shop.orders.created` | `shop.orders.eu.created` |
| `>` | One or more tokens, only at the end | `shop.orders.>` | `shop.orders.created`, `shop.orders.eu.paid` | `shop.payments.created` |

```
subject:   shop.orders.eu.created

shop.orders.eu.created   ✔ exact match
shop.*.eu.created        ✔
shop.orders.*.created    ✔
shop.orders.>            ✔
shop.>                   ✔
>                        ✔ (everything)
shop.*.created           ✘ too many tokens
shop.orders.eu           ✘ too few tokens
```

Wildcards work **only in subscriptions and in stream configuration**. You cannot publish to a wildcard: `nats pub shop.orders.* ...` sends a message to a subject that literally contains an asterisk, and nobody receives it.

## 3.3 Designing the subject space

Subjects are the API of your system. Changing them later hurts as much as changing a database schema.

**A template that works:**

```
<domain>.<entity>.<event>[.<region|version|id>]

shop.orders.created
shop.orders.cancelled
shop.payments.succeeded
crm.customers.updated
iot.sensors.london.dc1.temperature
```

Rules that will save you in a year:

| Rule | Why |
|---|---|
| Tokens from general to specific | `shop.orders.created`, not `created.orders.shop`: otherwise wildcards are useless |
| One entity, one prefix | Lets a single `shop.orders.>` subscription capture the whole lifecycle |
| Version as a token or a header | `shop.orders.created.v2` or an `Event-Version` header |
| Identifiers at the end | `shop.orders.updated.order-123` lets you subscribe to one order |
| Never put free text in a subject | Spaces and dots inside ids break routing |
| Don't use `>` in production service subscriptions | The service starts receiving everything invented in the future |
| Different prefixes for commands and events | `cmd.billing.charge` vs `evt.billing.charged` |

**Bad:**

```
orders                      # no hierarchy, nothing to filter on
order_created_event_v1      # underscores instead of tokens
shop.orders.created.2026-09-15T10:00:00Z   # timestamps create infinite subjects
```

**Good:**

```
shop.orders.created
shop.orders.created.order-123
evt.shop.orders.v1.created
```

## 3.4 Cardinality: how many subjects are fine

Core NATS handles **a very large number** of unique subjects cheaply (millions). But in JetStream every unique subject inside a stream is tracked in an index, and `max_msgs_per_subject` works off that index. Millions of subjects in one stream means a lot of memory for metadata.

Rule of thumb: `iot.sensors.<device_id>.temperature` with 100,000 devices in one stream is fine. The same thing with a timestamp in the subject is a disaster.

## 3.5 Subscriptions, queue groups and wildcards together

```
service "billing", 3 instances:
  sub "shop.orders.created", queue="billing"

service "analytics", 2 instances:
  sub "shop.>", queue="analytics"
```

For a single `shop.orders.created` message:

- **one** of the three billing instances receives it;
- **one** of the two analytics instances receives it;
- that is, each queue group gets a copy, and inside the group one member gets it.

Exactly the behaviour of Kafka consumer groups, minus partitions and minus rebalancing.

## 3.6 Reserved subjects

| Prefix | Purpose |
|---|---|
| `$JS.API.>` | JetStream API (creating streams and consumers, pull requests) |
| `$JS.ACK.>` | Message acknowledgements |
| `$JS.EVENT.ADVISORY.>` | Advisories: max deliveries, terminated, message loss |
| `$SYS.>` | Server system events: connects, disconnects, statistics |
| `$KV.<bucket>.>` | Key-Value Store |
| `$OBJ.<bucket>.>` | Object Store |
| `_INBOX.>` | Temporary reply subjects for request-reply |

Never publish into these by hand and never subscribe to `>` in an application: you'd get all the system traffic mixed with your business messages.

### Practice

1. Subscribe to `shop.>`, `shop.*.created` and `shop.orders.>` in three terminals and publish `shop.orders.eu.created`. Explain the result.
2. Design a subject space for a delivery service: orders, couriers, statuses, GPS positions.
3. See what happens with `nats pub "shop.*.created" test`.

---

# Module 4. Core NATS: pub/sub, request-reply, queue groups

## 4.1 The three patterns everything is built on

```
1. PUB/SUB (fan-out)          2. QUEUE GROUP (balancing)        3. REQUEST-REPLY (RPC)

   publisher                     publisher                         client
       |                             |                              | request
   +---+---+                     +---+---+                          v
   v   v   v                     v   v   v                      service (queue)
  s1  s2  s3                    w1  w2  w3                          |
 everyone gets it             exactly one gets it                   | reply
                                                                    v
                                                                 client
```

## 4.2 Pub/Sub in Go

```bash
go get github.com/nats-io/nats.go
```

```go
package main

import (
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	nc, err := nats.Connect(
		"nats://localhost:4222,nats://localhost:4223,nats://localhost:4224",
		nats.Name("order-service"),
		nats.MaxReconnects(-1),                 // reconnect forever
		nats.ReconnectWait(500*time.Millisecond),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			log.Println("disconnected:", err)
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			log.Println("reconnected to", c.ConnectedUrl())
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Drain() // graceful shutdown: finish reading and unsubscribe

	sub, err := nc.Subscribe("shop.orders.>", func(m *nats.Msg) {
		log.Printf("subject=%s data=%s", m.Subject, string(m.Data))
	})
	if err != nil {
		log.Fatal(err)
	}
	sub.SetPendingLimits(65536, 64*1024*1024) // slow-consumer protection

	msg := nats.NewMsg("shop.orders.created")
	msg.Header.Set("Event-Type", "OrderCreated")
	msg.Header.Set("Trace-Id", "4bf92f3577b34da6")
	msg.Data = []byte(`{"order_id":"order-1","amount":4990}`)

	if err := nc.PublishMsg(msg); err != nil {
		log.Fatal(err)
	}
	nc.Flush() // wait until the server has taken it

	time.Sleep(time.Second)
}
```

**Rules:**

- use **one connection per application**: it is thread-safe and multiplexes every subscription;
- `Publish()` is **asynchronous** and guarantees nothing: it writes into a socket buffer. If you want an acknowledgement, that's JetStream;
- `Flush()` confirms the server received everything sent so far;
- always use `Drain()` instead of `Close()` on shutdown: it unsubscribes, drains what's in flight and only then tears down the connection;
- always log errors from `nats.ErrorHandler`: that's where `slow consumer` and `permissions violation` show up.

## 4.3 Pub/Sub in Python

```bash
pip install nats-py
```

```python
import asyncio, json
import nats

async def main():
    nc = await nats.connect(
        servers=["nats://localhost:4222", "nats://localhost:4223"],
        name="order-service",
        max_reconnect_attempts=-1,
        reconnect_time_wait=0.5,
    )

    async def handler(msg):
        print(msg.subject, msg.headers, msg.data.decode())

    await nc.subscribe("shop.orders.>", cb=handler)

    await nc.publish(
        "shop.orders.created",
        json.dumps({"order_id": "order-1", "amount": 4990}).encode(),
        headers={"Event-Type": "OrderCreated"},
    )
    await nc.flush()
    await asyncio.sleep(1)
    await nc.drain()

asyncio.run(main())
```

## 4.4 Pub/Sub in Java

```xml
<dependency>
  <groupId>io.nats</groupId>
  <artifactId>jnats</artifactId>
  <version>2.20.5</version>
</dependency>
```

```java
import io.nats.client.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class OrderService {
    public static void main(String[] args) throws Exception {
        Options options = new Options.Builder()
                .servers(new String[]{"nats://localhost:4222", "nats://localhost:4223"})
                .connectionName("order-service")
                .maxReconnects(-1)
                .reconnectWait(Duration.ofMillis(500))
                .build();

        try (Connection nc = Nats.connect(options)) {
            Dispatcher d = nc.createDispatcher(msg ->
                System.out.printf("%s -> %s%n", msg.getSubject(),
                        new String(msg.getData(), StandardCharsets.UTF_8)));
            d.subscribe("shop.orders.>");

            Headers h = new Headers();
            h.add("Event-Type", "OrderCreated");
            nc.publish(NatsMessage.builder()
                    .subject("shop.orders.created")
                    .headers(h)
                    .data("{\"order_id\":\"order-1\"}", StandardCharsets.UTF_8)
                    .build());

            nc.flush(Duration.ofSeconds(2));
            Thread.sleep(1000);
        }
    }
}
```

## 4.5 Queue groups: load balancing with zero configuration

```go
// run three instances of this
_, _ = nc.QueueSubscribe("shop.orders.created", "billing-workers", func(m *nats.Msg) {
    process(m.Data)
})
```

```python
await nc.subscribe("shop.orders.created", queue="billing-workers", cb=handler)
```

What matters:

- **exactly one** group member receives the message (the server picks, usually the topologically closest);
- members can live on **different cluster servers**: NATS prefers a local subscriber to save a hop;
- **there is no rebalance**: a new worker simply starts receiving, a dead one stops;
- **ordering across workers is not guaranteed**: two messages for the same order may be processed in parallel;
- a Core NATS queue group has **no acks**: if a worker dies mid-processing, the message is gone. Need guarantees? JetStream.

## 4.6 Request-Reply

This is what brings many people to NATS: RPC with no service discovery, no load balancer, no DNS.

```go
// the service
nc.QueueSubscribe("service.pricing.calculate", "pricing", func(m *nats.Msg) {
    result := calculate(m.Data)
    m.Respond(result)     // the reply goes to m.Reply
})

// the client
resp, err := nc.Request("service.pricing.calculate", []byte(`{"sku":"A1","qty":3}`), 2*time.Second)
if errors.Is(err, nats.ErrNoResponders) {
    // nobody is listening on that subject: the service isn't up
}
```

Under the hood:

```
1. the client creates a temporary inbox: _INBOX.aB9xK2.1
2. the client subscribes to it
3. the client publishes the request with reply=_INBOX.aB9xK2.1
4. the service processes it and publishes the answer to _INBOX.aB9xK2.1
5. the client gets the answer and unsubscribes
```

Advantages over HTTP:

- **no addresses**: the client has no idea where the service physically lives;
- **free load balancing**: several instances in one queue group;
- **instant fail-fast**: with no subscribers the client gets `no responders` in microseconds instead of waiting for a timeout;
- **works across NAT, clusters and continents**: the connection is always outbound from the service to NATS.

Downsides: no schema or typing out of the box, none of the HTTP ecosystem (caches, CDNs), harder to debug without tracing.

## 4.7 Scatter-Gather: collecting answers from many services

```go
sub, _ := nc.SubscribeSync(nats.NewInbox())
defer sub.Unsubscribe()

msg := nats.NewMsg("service.quotes.request")
msg.Reply = sub.Subject
msg.Data = []byte(`{"route":"LHR-JFK"}`)
nc.PublishMsg(msg)

deadline := time.Now().Add(500 * time.Millisecond)
var quotes [][]byte
for time.Now().Before(deadline) {
    m, err := sub.NextMsg(time.Until(deadline))
    if err != nil {
        break
    }
    quotes = append(quotes, m.Data)
}
```

Every subscriber without a queue group answers, and the client collects as many replies as arrive in the window. This is how price aggregators, cluster-wide status polls and distributed searches are built.

## 4.8 No responders and timeouts

| Situation | What the client gets |
|---|---|
| Nobody subscribed to the subject | `no responders available` immediately (when the server supports headers) |
| Service subscribed but slow | Request timeout |
| Service died after receiving | Request timeout |
| Several services with no queue group | The client takes the **first** reply and ignores the rest |

Always set an explicit timeout, and make it shorter than the timeout of the call above you.

## 4.9 When Core NATS is enough

| Task | Is Core NATS enough? |
|---|---|
| Metrics and telemetry once per second | Yes |
| Cache invalidation | Yes |
| Agent health checks and heartbeats | Yes |
| Service-to-service RPC | Yes |
| Pushing UI updates over WebSocket | Yes |
| "Order paid" event | **No**, use JetStream |
| A task queue with retries | **No**, use JetStream |
| Audit and operation history | **No**, use JetStream |

### Practice

1. Run 3 workers in one queue group, send 100 messages and count the distribution.
2. Kill one worker mid-processing and confirm the message is lost.
3. Build a request-reply service and measure latency: `nats bench service request service.echo --msgs 10000`.
4. Reproduce the `no responders` error.

---

# Module 5. JetStream: streams and message storage

## 5.1 What a stream is

A **stream** is a message store that "captures" the subjects you list.

```
nats stream add ORDERS --subjects "shop.orders.>"
```

From that moment **any** message published to `shop.orders.*` lands in the stream and gets a `stream sequence`. The publisher keeps using the same subject: the code doesn't change, only the fact that it now gets an acknowledgement.

```
publish shop.orders.created
   |
   v
[ Stream ORDERS ]
  seq 1: shop.orders.created  {order-1}
  seq 2: shop.orders.paid     {order-1}
  seq 3: shop.orders.created  {order-2}
   |
   +--> PubAck{stream:"ORDERS", seq:3, duplicate:false}
```

**An important constraint:** one subject can belong to **only one stream** within an account. Creating a second stream with overlapping subjects returns an error. If you want a copy of the data, use `mirror` or `source` (module 13).

## 5.2 Storage: file or memory

| Type | Pros | Cons | When |
|---|---|---|---|
| `file` | Survives restarts, cheap per GB | Tens to hundreds of microseconds more latency | The default for anything that matters |
| `memory` | Minimal latency | Data is lost when the node restarts | Short-lived buffers, dedup, tests |

With `replicas=3` even a memory stream survives one node failing (the data lives on the others), but not a simultaneous restart of all of them.

## 5.3 Retention policy: three models

This is the key stream setting, and getting it wrong is the most expensive mistake here.

| Policy | When a message is removed | Use for |
|---|---|---|
| `limits` (default) | Only when a limit is hit (`max_age`, `max_msgs`, `max_bytes`) | Event logs, audit, replay, several independent consumers |
| `interest` | As soon as **all existing consumers** have acknowledged | Pub/sub with guarantees when history isn't needed |
| `workqueue` | As soon as **one** consumer acknowledges | Task queues: a message is processed by exactly one consumer |

```
LIMITS                       INTEREST                    WORKQUEUE
[1][2][3][4][5]              [1][2][3]                   [1][2][3]
 ^      ^                     ^                           ^
 A      B  read               all ack -> delete           one ack -> delete
 independently, history       (with no consumers,         (multiple consumers need
 lives until max_age          messages vanish at once)     non-overlapping filters)
```

**Traps:**

- `interest`: with no consumers at all, messages are deleted **immediately** after publishing. Create the consumer first, then publish.
- `workqueue`: a stream cannot have two consumers with overlapping `filter_subject`. The server rejects the second one.
- `limits`: messages survive processing, and your disk fills up unless `max_age` or `max_bytes` is set.

## 5.4 Full stream configuration

```bash
nats stream add ORDERS \
  --subjects "shop.orders.>" \
  --storage file \
  --replicas 3 \
  --retention limits \
  --discard old \
  --max-age 720h \
  --max-msgs -1 \
  --max-msgs-per-subject -1 \
  --max-bytes 50GB \
  --max-msg-size 1MB \
  --dupe-window 2m \
  --allow-rollup \
  --deny-delete
```

| Parameter | Meaning | Practice |
|---|---|---|
| `subjects` | Which subjects the stream captures | Keep it narrow: `shop.orders.>`, not `shop.>` |
| `storage` | `file` or `memory` | `file` |
| `replicas` | 1, 3 or 5, a RAFT group | 3 in production |
| `retention` | `limits` / `interest` / `workqueue` | Think for five minutes before choosing |
| `discard` | `old` (drop oldest) or `new` (reject the publisher) | `old` for logs, `new` for task queues with a hard cap |
| `max_age` | Message TTL | Always set it |
| `max_bytes` | Size cap | Always set it, or the disk will run out |
| `max_msgs_per_subject` | How many versions of each subject to keep | `1` turns the stream into a snapshot store |
| `duplicate_window` | Dedup window for `Nats-Msg-Id` | Two minutes is usually enough |
| `allow_rollup` | Permit the `Nats-Rollup` header (collapse a subject's history) | For snapshots |
| `deny_delete` / `deny_purge` | Forbid message deletion via the API | For audit and compliance |

## 5.5 Publishing to JetStream in Go

The modern API is the `jetstream` package:

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	nc, err := nats.Connect("nats://localhost:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Drain()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// create or update the stream straight from code
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       "ORDERS",
		Subjects:   []string{"shop.orders.>"},
		Storage:    jetstream.FileStorage,
		Retention:  jetstream.LimitsPolicy,
		Replicas:   3,
		MaxAge:     30 * 24 * time.Hour,
		MaxBytes:   50 << 30,
		Duplicates: 2 * time.Minute,
	})
	if err != nil {
		log.Fatal(err)
	}

	// synchronous publish with acknowledgement
	msg := &nats.Msg{
		Subject: "shop.orders.created",
		Header:  nats.Header{"Nats-Msg-Id": []string{"order-1-created"}},
		Data:    []byte(`{"order_id":"order-1","amount":4990}`),
	}
	ack, err := js.PublishMsg(ctx, msg)
	if err != nil {
		log.Fatal("publish failed:", err) // the message is NOT stored, retry needed
	}
	log.Printf("stream=%s seq=%d duplicate=%v", ack.Stream, ack.Sequence, ack.Duplicate)
}
```

**Async publishing** for high throughput:

```go
futures := make([]jetstream.PubAckFuture, 0, 1000)
for i := 0; i < 1000; i++ {
	f, err := js.PublishAsync("shop.orders.created", payload(i))
	if err != nil {
		log.Fatal(err)
	}
	futures = append(futures, f)
}

select {
case <-js.PublishAsyncComplete():
case <-time.After(10 * time.Second):
	log.Fatal("acknowledgements never arrived")
}

for _, f := range futures {
	select {
	case <-f.Ok():
	case err := <-f.Err():
		log.Println("not stored:", err) // you must handle this
	default:
	}
}
```

Async publishing multiplies throughput, but **every error has to be inspected**: a silently dropped ack is a silently dropped event.

## 5.6 Publishing in Python and Java

**Python:**

```python
import asyncio, json, nats

async def main():
    nc = await nats.connect("nats://localhost:4222")
    js = nc.jetstream()

    await js.add_stream(name="ORDERS", subjects=["shop.orders.>"],
                        storage="file", num_replicas=3, duplicate_window=120)

    ack = await js.publish(
        "shop.orders.created",
        json.dumps({"order_id": "order-1"}).encode(),
        headers={"Nats-Msg-Id": "order-1-created"},
    )
    print(ack.stream, ack.seq, ack.duplicate)
    await nc.drain()

asyncio.run(main())
```

**Java:**

```java
Connection nc = Nats.connect("nats://localhost:4222");
JetStream js = nc.jetStream();

Headers h = new Headers();
h.add("Nats-Msg-Id", "order-1-created");

PublishAck ack = js.publish(NatsMessage.builder()
        .subject("shop.orders.created")
        .headers(h)
        .data("{\"order_id\":\"order-1\"}", StandardCharsets.UTF_8)
        .build());

System.out.println(ack.getStream() + " seq=" + ack.getSeqno() + " dup=" + ack.isDuplicate());
```

## 5.7 What to do when a publish fails

```
js.Publish() returned an error
   |
   +-- timeout / no responders   -> stream unavailable or no leader (also for a moment right
   |                                after creating a replicated stream): retry with backoff
   +-- maximum messages exceeded -> limit hit with discard=new: alert and investigate
   +-- wrong last sequence       -> an optimistic ExpectedLastSeq condition failed
   +-- no stream matches subject -> typo in the subject, or the stream doesn't exist
```

Production rule: **retry with exponential backoff plus a mandatory `Nats-Msg-Id`**. Then the retry cannot create a duplicate (module 7). If several attempts fail, park the event in a local database outbox and ship it later.

## 5.8 Managing streams from the CLI

```bash
nats stream info ORDERS
nats stream view ORDERS                      # page through messages
nats stream get ORDERS 42                    # fetch by sequence
nats stream subjects ORDERS                  # which subjects exist and how many messages each
nats stream report                           # summary of all streams
nats stream edit ORDERS --max-age 48h
nats stream purge ORDERS --subject "shop.orders.created"
nats stream rm ORDERS
nats stream backup ORDERS ./orders-backup    # snapshot
nats stream restore ./orders-backup
```

### Self-check questions

1. Why can't one subject belong to two streams?
2. What is the difference between `interest` and `workqueue` retention?
3. What happens with `discard=new` when `max_bytes` is reached?
4. Why is async publishing more dangerous than sync?

---

# Module 6. JetStream consumers: pull, push, ack

## 6.1 What a consumer is

A **consumer** is not a "subscriber" but a **server-side object**: a cursor over the stream plus delivery and acknowledgement rules. It exists on the server even while your application is down.

```
Stream ORDERS
  seq: 1  2  3  4  5  6  7  8
        ^        ^        ^
        |        |        |
   ack_floor  delivered  last_seq
        |________|
        num_ack_pending = 2       num_pending = 2  (this is the lag)
```

| Type | How it's created | Lifetime |
|---|---|---|
| **Durable** | With a name (`--durable BILLING`) | Until you delete it |
| **Ephemeral** | Without a name | While a client is connected + `inactive_threshold` |

In production it's almost always durable: restarting a pod must not reset progress.

## 6.2 Pull vs Push

| | **Pull (recommended)** | **Push (legacy)** |
|---|---|---|
| Who initiates | The client asks for N messages | The server pushes to a subject |
| Flow control | Natural: you don't ask, you don't get | Requires `flow_control` and `idle_heartbeat` |
| Scaling | Just add another instance sharing the consumer | Requires a queue group and `deliver_group` |
| Failure behaviour | Client dies, nothing is lost | Messages fly into the void |
| Use when | Almost always | Legacy code, niche scenarios |

**Pull consumers are the recommended default**, and the modern client APIs (the `jetstream` packages) are built around them. Every example below uses them.

## 6.3 Ack policy and what each reply means

| Ack policy | Meaning |
|---|---|
| `explicit` (default and the right choice) | Every message is acknowledged individually |
| `all` | Acking message N acknowledges everything up to N |
| `none` | No acks; the server treats delivered as processed |

What the client can reply:

| Reply | Effect | When |
|---|---|---|
| `Ack()` | Processed, never deliver again | Success |
| `Nak()` / `NakWithDelay(d)` | Requeue and redeliver (now or after d) | Transient failure: database down, upstream 503 |
| `Term()` / `TermWithReason()` | Never deliver again | The message is invalid and can never be processed |
| `InProgress()` | "Still working", reset the `AckWait` timer | Long processing |
| nothing | Redelivered after `AckWait` | Process crash |

**This matters more than it looks.** The difference between `Nak` and `Term` is the difference between "let's try again" and "this is garbage, drop it". An app that `Nak`s on every error will spin on a poison message forever.

## 6.4 Key consumer parameters

```bash
nats consumer add ORDERS BILLING \
  --pull \
  --ack explicit \
  --deliver all \
  --filter "shop.orders.created" \
  --max-deliver 6 \
  --wait 30s \
  --max-pending 1000 \
  --backoff linear \
  --backoff-steps 5 \
  --backoff-min 1s \
  --backoff-max 1m \
  --replicas 3 \
  --defaults
```

| Parameter | Default | Meaning |
|---|---|---|
| `filter_subject(s)` | all stream subjects | What this consumer reads; multiple filters since 2.10 |
| `deliver_policy` | `all` | `all`, `last`, `new`, `by_start_sequence`, `by_start_time`, `last_per_subject` |
| `ack_wait` | 30s | How long the server waits for an ack before redelivering |
| `max_deliver` | -1 (infinite) | How many delivery attempts |
| `max_ack_pending` | 1000 | How many unacknowledged messages may be outstanding: **this is** your concurrency limiter |
| `backoff` | none | Delay array for redeliveries |
| `replicas` | same as stream | Replicas of the consumer state |
| `inactive_threshold` | 5s (ephemeral) | When to delete an unused consumer |

**`max_ack_pending` is the most underrated setting.** It is also backpressure: when workers fall behind, the server stops handing out new messages.

**Two backoff rules that trip people up:**

- when `backoff` is set, it **replaces `ack_wait` for redeliveries**: the first delivery waits `ack_wait`, redelivery N waits `backoff[N-1]` (the last value repeats if there are more attempts than entries);
- `max_deliver` can't be **smaller** than the number of backoff entries (the server rejects the config). Make it one larger, so the last delay is actually used before the consumer gives up.

With the CLI flags above (`linear`, 5 steps, 1s to 1m), natscli computes evenly spaced delays of 1s, ~13s, ~25s, ~36s, ~48s: the last step stays below the maximum. Check the result with `nats consumer info ORDERS BILLING`. For a hand-tuned schedule such as 1s, 5s, 30s, set the array explicitly in code (6.5) or in a JSON config (`nats consumer add ORDERS --config billing.json`).

## 6.5 A pull consumer in Go

```go
cons, err := js.CreateOrUpdateConsumer(ctx, "ORDERS", jetstream.ConsumerConfig{
	Durable:       "BILLING",
	FilterSubject: "shop.orders.created",
	AckPolicy:     jetstream.AckExplicitPolicy,
	DeliverPolicy: jetstream.DeliverAllPolicy,
	MaxDeliver:    5,
	AckWait:       30 * time.Second,
	MaxAckPending: 500,
	BackOff:       []time.Duration{time.Second, 5 * time.Second, 30 * time.Second},
})
if err != nil {
	log.Fatal(err)
}

// option 1: callback (usually what you want)
cc, err := cons.Consume(func(msg jetstream.Msg) {
	if err := process(msg.Data()); err != nil {
		if isTemporary(err) {
			msg.NakWithDelay(5 * time.Second)
		} else {
			msg.Term() // never retry
		}
		return
	}
	msg.Ack()
}, jetstream.PullMaxMessages(128))
if err != nil {
	log.Fatal(err)
}
defer cc.Stop()

// option 2: manual batches
batch, err := cons.Fetch(100, jetstream.FetchMaxWait(5*time.Second))
for msg := range batch.Messages() {
	process(msg.Data())
	msg.Ack()
}
if err := batch.Error(); err != nil {
	log.Println(err)
}
```

Message metadata:

```go
meta, _ := msg.Metadata()
log.Printf("stream_seq=%d consumer_seq=%d deliveries=%d pending=%d ts=%s",
	meta.Sequence.Stream, meta.Sequence.Consumer,
	meta.NumDelivered, meta.NumPending, meta.Timestamp)
```

`meta.NumDelivered > 1` means this is a redelivery. Log it: rising redeliveries are the first sign of trouble.

## 6.6 Pull consumers in Python and Java

**Python:**

```python
js = nc.jetstream()

sub = await js.pull_subscribe(
    "shop.orders.created",
    durable="BILLING",
    stream="ORDERS",
)

while True:
    try:
        msgs = await sub.fetch(batch=50, timeout=5)
    except asyncio.TimeoutError:   # no messages; also catches its subclass nats.errors.TimeoutError
        continue

    for msg in msgs:
        try:
            await process(msg.data)
            await msg.ack()
        except TemporaryError:
            await msg.nak(delay=5)
        except Exception:
            await msg.term()
```

**Java:**

```java
JetStreamManagement jsm = nc.jetStreamManagement();
jsm.addOrUpdateConsumer("ORDERS", ConsumerConfiguration.builder()
        .durable("BILLING")
        .filterSubject("shop.orders.created")
        .ackPolicy(AckPolicy.Explicit)
        .maxDeliver(5)
        .ackWait(Duration.ofSeconds(30))
        .maxAckPending(500)
        .build());

ConsumerContext cc = nc.getStreamContext("ORDERS").getConsumerContext("BILLING");
try (MessageConsumer consumer = cc.consume(msg -> {
        try {
            process(msg.getData());
            msg.ack();
        } catch (TemporaryException e) {
            msg.nakWithDelay(Duration.ofSeconds(5));
        } catch (Exception e) {
            msg.term();
        }
    })) {
    Thread.sleep(Long.MAX_VALUE);
}
```

## 6.7 Scaling: many workers on one consumer

```
       Stream ORDERS
            |
      consumer BILLING (durable, pull)
       /      |      \
  worker-1 worker-2 worker-3
```

All three processes attach to the **same durable consumer** and pull messages. The server makes sure one message doesn't go to two workers at once (until `AckWait` expires).

Differences from Kafka worth internalising:

- **no partitions and no rebalance**: add and remove workers whenever you like;
- **concurrency is not capped by partition count**: 100 workers on one consumer is normal;
- **ordering across workers is not guaranteed**: if you need strict per-entity ordering, use a filtered consumer per key or process serially (`max_ack_pending=1`).

## 6.8 Message ordering in NATS

| What you need | How to get it |
|---|---|
| Global stream ordering | One consumer, one worker, `max_ack_pending=1` |
| Per-entity ordering (order, account) | A subject per entity + a filtered consumer, or shard workers by a subject token |
| Ordering doesn't matter | Many workers, maximum throughput |

Sharding example: subjects `shop.orders.shard-0.…` through `shop.orders.shard-7.…`, the publisher computes `hash(order_id) % 8`, and each worker creates a consumer filtered to its shard. This is a manual equivalent of Kafka partitions.

## 6.9 Ordered consumers

A special mode for reading a stream "like a file": the client library creates an ephemeral consumer, tracks sequence gaps and transparently recreates it from the right position after a disconnect.

```go
cons, _ := js.OrderedConsumer(ctx, "ORDERS", jetstream.OrderedConsumerConfig{
	FilterSubjects: []string{"shop.orders.>"},
})
```

No acks needed, strict ordering guaranteed. Ideal for building projections, materialised views and in-memory caches.

## 6.10 Monitoring a consumer

```bash
nats consumer info ORDERS BILLING
nats consumer report ORDERS
nats consumer next ORDERS BILLING --count 1     # pull one message by hand
nats consumer rm ORDERS BILLING
```

What to watch:

```
Unprocessed Messages: 4,023        <- num_pending, the lag
Outstanding Acks:       417        <- num_ack_pending, hitting max_ack_pending?
Redelivered Messages:    38        <- rising? processing fails or AckWait is too short
Acknowledgment Floor: seq 120,455  <- not moving? the consumer is stuck
```

## 6.11 Moving a consumer's position

There is no "reset offset" command: a consumer's start position is fixed at creation. Re-reading a stream means recreating the consumer:

```bash
nats consumer rm ORDERS BILLING -f
nats consumer add ORDERS BILLING --pull --ack explicit \
  --start-time "2026-09-15T00:00:00Z" --filter "shop.orders.created" --defaults
```

Start options: `--deliver all`, `--deliver last`, `--deliver new`, `--deliver last_per_subject`, `--start-sequence 1000`, `--start-time ...`.

### Practice

1. Create a consumer with `--wait 5s` and don't ack a message: watch the redelivery.
2. Set `--max-deliver 3` and see what happens after the third attempt (`nats consumer info` and the advisories).
3. Run 3 workers on one durable consumer and kill one mid-processing: confirm the message moves to another.
4. Compare throughput with `max_ack_pending=1` and `max_ack_pending=1000`.

---

# Module 7. Delivery guarantees, deduplication and exactly-once

## 7.1 Where duplicates and losses come from

| Scenario | Result | Protection |
|---|---|---|
| Core NATS, subscriber not running | Loss | JetStream |
| Core NATS, subscriber dies mid-processing | Loss | JetStream + ack |
| Publisher didn't get a PubAck and retried | Duplicate in the stream | `Nats-Msg-Id` + `duplicate_window` |
| App crashed between the DB write and the publish | Lost event | Transactional outbox |
| App published and then the DB transaction rolled back | Phantom event | Transactional outbox |
| Consumer processed but crashed before `Ack()` | Reprocessing | Idempotency |
| Processing took longer than `AckWait` | Parallel redelivery | `InProgress()` or a larger `AckWait` |
| `replicas=1` and the node died | Stream lost | `replicas=3` |
| Power loss before fsync (`replicas=1`) | Messages that already got a PubAck are lost | `replicas=3` (a quorum holds the data in memory on other nodes) or `sync_interval: always` (see 15.4) |

## 7.2 The three semantics

```
AT-MOST-ONCE  (Core NATS)
  publish -> deliver to subscribers -> forget
  nobody received it -> the message is gone

AT-LEAST-ONCE (JetStream, baseline)
  publish -> PubAck -> deliver -> process -> Ack
  crash before Ack -> redelivered

EXACTLY-ONCE (JetStream + Nats-Msg-Id + idempotent consumer)
  publish-side dedup plus consumer-side idempotency
```

**NATS honestly calls this "exactly-once semantics"**: a combination of publish deduplication and consumer idempotency, not a magical distributed transaction. Kafka gives the same honest answer.

## 7.3 Publish deduplication: Nats-Msg-Id

A stream remembers message ids for `duplicate_window`. Republishing with the same `Nats-Msg-Id` doesn't create a second message; it returns `PubAck{duplicate: true}`.

```go
msg := nats.NewMsg("shop.orders.created")
msg.Header.Set("Nats-Msg-Id", eventID) // the event UUID, NOT a fresh random per attempt
msg.Data = payload

ack, err := js.PublishMsg(ctx, msg)
if err == nil && ack.Duplicate {
	log.Println("that was a retry, the message is already in the stream")
}
```

Rules:

- `Nats-Msg-Id` must be **stable for a single business event**: generate it once and store it, don't create a new one per attempt;
- `duplicate_window` must be **longer than your retry window** (2-5 minutes typically);
- the dedup window costs memory: the server holds those ids in RAM;
- deduplication works **within one stream**.

```bash
nats stream edit ORDERS --dupe-window 5m
nats stream info ORDERS   # see the "Duplicate Window" section
```

## 7.4 Optimistic concurrency on publish

JetStream can check conditions at write time, giving you CAS semantics without external locks.

| Header | Check |
|---|---|
| `Nats-Expected-Stream` | We're publishing to exactly this stream |
| `Nats-Expected-Last-Sequence` | The stream's last seq equals the given value |
| `Nats-Expected-Last-Subject-Sequence` | The last seq **for this subject** equals the given value |
| `Nats-Expected-Last-Msg-Id` | The last `Nats-Msg-Id` equals the given value |

```go
opts := []jetstream.PublishOpt{
	jetstream.WithExpectLastSequencePerSubject(lastKnownSeq),
}
ack, err := js.Publish(ctx, "shop.orders.updated.order-123", payload, opts...)
// err == "wrong last sequence" -> someone wrote first, re-read the state
```

This is how you build event sourcing with aggregate version checks: if someone changed state between your read and your write, the publish is rejected.

## 7.5 The idempotent consumer

The most reliable way to get "effectively once" when writing to an external database:

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
-- if 0 rows were inserted -> already processed: COMMIT and Ack()
UPDATE accounts SET balance = balance - $2 WHERE id = $3;
COMMIT;
-- only after a successful COMMIT do we call msg.Ack()
```

Alternatives: an `UPSERT` on a natural key, a conditional update on a version column (`WHERE version = $expected`), or storing `stream_sequence` in the same table as the result.

**Order of operations is critical:** process and commit to the database first, then `Ack()`. The reverse turns at-least-once into at-most-once.

## 7.6 Double-ack: acknowledging the acknowledgement

A plain `Ack()` is fire-and-forget: the client doesn't know whether it reached the server. If it is lost, the message is redelivered.

```go
msg.DoubleAck(ctx)   // waits for the server to confirm the ack
```

It costs an extra round-trip but removes a whole class of pointless redeliveries. Use it where reprocessing is expensive and idempotency is imperfect.

## 7.7 Transactional outbox

**The dual-write problem:**

```
1. INSERT INTO orders ...       OK
2. js.Publish(OrderCreated) -> the service crashed
   -> the order exists, the event doesn't, nobody downstream knows
```

**The fix: an outbox table in the same transaction.**

```
+--------------- one database transaction -----------+
| INSERT INTO orders (...)                            |
| INSERT INTO outbox (id, subject, headers, payload)  |
+-----------------------------------------------------+
               |
               v
      outbox relay (a goroutine or separate service)
      reads unpublished rows and publishes to NATS
      with Nats-Msg-Id = outbox.id
               |
               v
        Stream ORDERS
```

```sql
CREATE TABLE outbox (
    id           UUID PRIMARY KEY,
    subject      TEXT NOT NULL,
    headers      JSONB,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX ON outbox (created_at) WHERE published_at IS NULL;
```

The relay may publish twice (for example, crashing between `Publish` and `UPDATE outbox SET published_at`), so `Nats-Msg-Id = outbox.id` is mandatory: stream deduplication removes the duplicate.

## 7.8 Choosing a guarantee

| Task | Recommendation |
|---|---|
| Metrics, telemetry, heartbeats | Core NATS, losses acceptable |
| Service-to-service RPC | Core NATS request-reply with timeouts and retries |
| User notifications | JetStream, at-least-once, a duplicate is harmless |
| Domain business events | JetStream + `Nats-Msg-Id` + outbox + idempotent consumer |
| Money movements | All of the above + `processed_events` + double-ack |
| Task queues | A `workqueue` stream + `max_deliver` + DLQ |

### Self-check questions

1. Why is exactly-once in NATS two separate things rather than one setting?
2. What happens if `duplicate_window` is shorter than the publisher's retry window?
3. What is `Nats-Expected-Last-Subject-Sequence` for?
4. Why must `Ack()` come after the database commit, not before?
5. What problem does the transactional outbox solve, and why doesn't it fully solve it without deduplication?

---

# Module 8. Retention, limits and working with disk

## 8.1 How JetStream stores messages

```
/data/jetstream/APP/streams/ORDERS/
├── msgs/
│   ├── 1.blk       # message block (a few MB by default)
│   ├── 2.blk
│   └── index.db
├── obs/            # consumer state
│   └── BILLING/
└── meta.inf        # stream configuration
```

- messages are written **sequentially** into blocks, and old blocks are deleted whole;
- the per-subject index lives in memory, which is why unique subject count drives RAM usage;
- file streams rely on the OS page cache, like most log systems;
- with `replicas=3` a write is acknowledged once a **majority** of the RAFT group has it (2 of 3);
- by default data is flushed to disk (fsync) every 2 minutes, not on every write. A PubAck means "accepted by a quorum", not "on the platter": on `replicas=1` a power loss can lose acknowledged messages (details in 15.4).

## 8.2 Stream limits

| Limit | What it caps | On hit |
|---|---|---|
| `max_msgs` | Message count | `discard` |
| `max_bytes` | On-disk size (multiply by replicas for real disk) | `discard` |
| `max_age` | Message age | Removed automatically |
| `max_msgs_per_subject` | Messages per unique subject | Oldest for that subject is removed |
| `max_msg_size` | Single message size | Publish rejected |
| `max_consumers` | Consumer count | Creation rejected |

**Discard policy:**

```
discard = old  : new message arrives -> drop the oldest (ring buffer)
discard = new  : new message arrives -> reject the publish with an error
```

Use `old` for logs and telemetry. For task queues where losing work is unacceptable, use `new` plus an alert: better to reject the publisher than to silently drop work.

`--discard-per-subject` (since 2.10) applies `discard=new` per subject: handy when each subject is a separate entity.

## 8.3 Account and server limits

Stream limits won't save you when one tenant eats the whole disk. Set limits a level up:

```hcl
jetstream {
  store_dir: "/data/jetstream"
  max_memory_store: 4GB      # server-wide
  max_file_store: 500GB
}

accounts {
  SHOP: {
    jetstream: {
      max_memory: 1GB
      max_file: 100GB
      max_streams: 50
      max_consumers: 500
    }
    users: [ {user: shop, password: "…"} ]
  }
}
```

```bash
nats account info          # what's used and what's left
nats server report jetstream
```

## 8.4 max_msgs_per_subject: a snapshot instead of history

```bash
nats stream add PROFILES \
  --subjects "profiles.*" \
  --storage file --replicas 3 \
  --max-msgs-per-subject 1 \
  --retention limits --defaults
```

The stream now keeps **the latest state of each profile** and nothing else:

```
publish profiles.user-1  {"name":"Ann"}
publish profiles.user-2  {"name":"Bob"}
publish profiles.user-1  {"name":"Anna"}   <- the previous profiles.user-1 message is removed

in the stream: profiles.user-1 -> Anna, profiles.user-2 -> Bob
```

This is the equivalent of Kafka log compaction, and exactly what the KV Store is built on. Read such a stream with a `--deliver last_per_subject` consumer: a service gets the complete state instantly at startup and then only changes.

## 8.5 Rollup: collapsing history on purpose

With `--allow-rollup` you can publish a message carrying:

| Header | Effect |
|---|---|
| `Nats-Rollup: sub` | Delete all prior history **for that subject**, keeping only the new message |
| `Nats-Rollup: all` | Purge the **entire stream**, keeping only the new message |

Use it for periodic aggregate snapshots so you don't store a million tiny events.

## 8.6 Deleting messages

```bash
nats stream purge ORDERS                                   # everything
nats stream purge ORDERS --subject "shop.orders.created"   # by subject
nats stream purge ORDERS --keep 1000                       # keep the last 1000
nats stream purge ORDERS --seq 5000                        # delete everything below seq
nats stream rmm ORDERS 42                                  # delete one message (GDPR)
```

For compliance streams set `--deny-delete` and `--deny-purge` so nobody wipes the audit trail with one command.

## 8.7 Backup and restore

```bash
nats stream backup ORDERS ./backups/orders-2026-09-16
nats stream restore ./backups/orders-2026-09-16
```

The backup is taken from a **live stream** and includes configuration, messages and consumer state. For large streams it is a heavy operation: run it against a replica during a quiet window. Separately, keep `nats stream info --json` for every stream in Git: that's your infrastructure-as-code.

## 8.8 Sizing disk and memory

```
disk = avg_message_size × messages_per_day × retention_days × replicas × 1.3

example: 2 KB × 20,000,000 × 7 × 3 × 1.3 ≈ 1.1 TB
```

Memory:

- subject index: roughly 100-200 bytes per unique subject in a stream;
- dedup window: `Nats-Msg-Id` size × messages within `duplicate_window`;
- consumer state: proportional to `max_ack_pending` and outstanding messages;
- plus page cache: leave the OS at least half the RAM.

A practical node baseline: 8-16 GB RAM, an NVMe disk separate from the system disk. JetStream is very sensitive to fsync latency; network disks, and gp2 EBS in particular, produce unpleasant surprises.

### Self-check questions

1. How does `discard=old` differ from `discard=new`, and when do you pick each?
2. How do you turn a stream into a latest-state-per-key store?
3. Why does the number of unique subjects drive memory usage?
4. Why must `max_bytes` be multiplied by the replica count when sizing disk?

---

# Module 9. Key-Value Store

## 9.1 What it is and why

The KV Store is a layer over a stream with `max_msgs_per_subject`. There is no separate database: a `CONFIG` bucket is a stream called `KV_CONFIG` holding subjects `$KV.CONFIG.>`.

```
nats kv put CONFIG feature.dark_mode true
       |
       v
publish $KV.CONFIG.feature.dark_mode  "true"  -> stream KV_CONFIG
```

What it gives you:

- `get`, `put`, `delete`, atomic `create` and `update` with revisions;
- **value history** (`--history N`);
- **watch**: real-time subscription to key or prefix changes;
- **TTL** per bucket, and since 2.11 per key;
- replication and fault tolerance inherited from JetStream.

Typical uses: dynamic configuration, feature flags, caches, distributed locks, leader election, service registries, sessions.

## 9.2 CLI

```bash
nats kv add CONFIG --history 5 --replicas 3 --storage file --ttl 0
nats kv put CONFIG feature.dark_mode true
nats kv get CONFIG feature.dark_mode
nats kv ls CONFIG
nats kv history CONFIG feature.dark_mode
nats kv watch CONFIG                     # follow every change
nats kv del CONFIG feature.dark_mode
nats kv rm CONFIG                        # delete the bucket
nats kv info CONFIG
```

## 9.3 KV in Go

```go
kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
	Bucket:   "CONFIG",
	History:  5,
	Replicas: 3,
	Storage:  jetstream.FileStorage,
	TTL:      0,
})

// write
rev, err := kv.Put(ctx, "feature.dark_mode", []byte("true"))

// read
entry, err := kv.Get(ctx, "feature.dark_mode")
if errors.Is(err, jetstream.ErrKeyNotFound) { /* … */ }
log.Println(string(entry.Value()), entry.Revision(), entry.Created())

// atomic create: fails if the key already exists
_, err = kv.Create(ctx, "lock.import-job", []byte("worker-1"))

// optimistic update: fails if the revision changed
_, err = kv.Update(ctx, "feature.dark_mode", []byte("false"), entry.Revision())

// watch: config arrives on its own
w, _ := kv.Watch(ctx, "feature.>")
defer w.Stop()
for e := range w.Updates() {
	if e == nil { // means "initial state fully delivered"
		continue
	}
	log.Printf("%s = %s (rev %d, op %v)", e.Key(), e.Value(), e.Revision(), e.Operation())
}
```

## 9.4 KV in Python

```python
kv = await js.create_key_value(bucket="CONFIG", history=5, replicas=3)

await kv.put("feature.dark_mode", b"true")
entry = await kv.get("feature.dark_mode")
print(entry.value, entry.revision)

watcher = await kv.watch("feature.>")
async for e in watcher:
    if e is None:
        continue
    print(e.key, e.value, e.operation)
```

## 9.5 A distributed lock on KV

```go
// acquire
_, err := kv.Create(ctx, "lock.nightly-report", []byte(instanceID))
if errors.Is(err, jetstream.ErrKeyExists) {
	return ErrAlreadyRunning   // the lock is taken
}
defer kv.Delete(ctx, "lock.nightly-report")

// renew so the lock doesn't expire if the bucket has a TTL
ticker := time.NewTicker(10 * time.Second)
for range ticker.C {
	entry, err := kv.Get(ctx, "lock.nightly-report")
	if err != nil || string(entry.Value()) != instanceID {
		return ErrLockLost
	}
	kv.Update(ctx, "lock.nightly-report", []byte(instanceID), entry.Revision())
}
```

A bucket TTL is mandatory here, otherwise a crashed process holds the lock forever. This is not Chubby and not etcd with fencing tokens, but for 95% of "only one pod should run the nightly report" cases it is plenty.

## 9.6 KV limitations

- values are bytes up to `max_payload` (1 MB by default): large objects go to the Object Store;
- keys follow subject rules: a dot creates hierarchy, spaces are forbidden;
- no query-by-value, no secondary indexes, no multi-key transactions;
- `History` above 64 is not supported;
- deletes leave a marker (`purge` removes it entirely).

### Practice

1. Build a service that reads config from KV and applies changes via watch without restarting.
2. Implement leader election: the winner is whoever can `Create` the key; the rest wait and watch.
3. Look inside the KV store: `nats stream info KV_CONFIG`.

---

# Module 10. Object Store

## 10.1 Why

Messages are capped by `max_payload`. The Object Store splits a file into chunks, stores them in an `OBJ_<bucket>` stream and reassembles them on read. It gives you "S3 lite" where running MinIO would be overkill.

Uses: build artifacts, configs and certificates for edge nodes, IoT firmware, reports, attachments, ML models for edge inference.

## 10.2 CLI

```bash
nats object add ASSETS --replicas 3 --storage file --max-bucket-size 10GB
nats object put ASSETS ./firmware-v2.bin
nats object ls ASSETS
nats object info ASSETS firmware-v2.bin
nats object get ASSETS firmware-v2.bin --output ./downloaded.bin
nats object watch ASSETS
nats object rm ASSETS firmware-v2.bin
```

## 10.3 Object Store in Go

```go
obs, err := js.CreateOrUpdateObjectStore(ctx, jetstream.ObjectStoreConfig{
	Bucket:   "ASSETS",
	Replicas: 3,
	Storage:  jetstream.FileStorage,
	MaxBytes: 10 << 30,
})

// upload
f, _ := os.Open("firmware-v2.bin")
defer f.Close()
info, err := obs.Put(ctx, jetstream.ObjectMeta{
	Name:        "firmware-v2.bin",
	Description: "firmware for X-series sensors",
	Headers:     nats.Header{"Sha256": []string{checksum}},
}, f)
log.Println(info.Size, info.Digest)

// download
r, err := obs.Get(ctx, "firmware-v2.bin")
defer r.Close()
io.Copy(out, r)

// watch for new versions
w, _ := obs.Watch(ctx)
for i := range w.Updates() {
	if i == nil { continue }
	log.Println("new object:", i.Name, i.Size)
}
```

The Object Store keeps metadata and a SHA-256 digest, supports versioning via overwrite, and supports watch. What it lacks: presigned URLs, proper range requests, per-object access policies. It is not an S3 replacement for user content, but it is excellent for distributing files across your infrastructure, especially to leaf nodes.

---

# Module 11. Microservices on NATS: the Services API

## 11.1 What it is

The Services API (the `micro` package in Go, with equivalents in other clients) is a thin layer over request-reply that adds what production usually needs:

- a uniform error format in responses;
- **automatic discovery**: `PING`, `INFO`, `STATS` on system subjects;
- **statistics**: request counts, error counts, average processing time;
- versioning and endpoint groups.

```
$SRV.PING                      -> every service answers
$SRV.PING.pricing              -> every instance of the pricing service answers
$SRV.INFO.pricing.<id>         -> description of a specific instance
$SRV.STATS.pricing             -> statistics
```

## 11.2 A service in Go

```go
import "github.com/nats-io/nats.go/micro"

srv, err := micro.AddService(nc, micro.Config{
	Name:        "pricing",
	Version:     "1.2.0",
	Description: "Price and discount calculation",
})
if err != nil {
	log.Fatal(err)
}
defer srv.Stop()

g := srv.AddGroup("service.pricing")

g.AddEndpoint("calculate", micro.HandlerFunc(func(r micro.Request) {
	var req CalcRequest
	if err := json.Unmarshal(r.Data(), &req); err != nil {
		r.Error("400", "invalid payload", nil)
		return
	}
	result, err := calculate(req)
	if err != nil {
		r.Error("500", err.Error(), nil)
		return
	}
	r.RespondJSON(result)
}), micro.WithEndpointMetadata(map[string]string{"format": "json"}))
```

Clients call it like ordinary request-reply:

```bash
nats request service.pricing.calculate '{"sku":"A1","qty":3}' --timeout 2s
```

## 11.3 Discovery and observability for free

```bash
nats micro ls                      # every service in the system
nats micro info pricing            # description and endpoints
nats micro stats pricing           # requests, errors, latency per instance
nats micro ping pricing            # who's alive and how fast
```

In the HTTP world you pay for this with Consul, a service mesh and half an observability stack. Here it is built into the protocol.

## 11.4 NATS instead of HTTP: the honest trade-offs

| | Upside | Downside |
|---|---|---|
| Addressing | No DNS, ingress or service mesh needed | No familiar URLs or HTTP tooling |
| Load balancing | Queue groups, automatic | No weighted routing or canaries out of the box |
| Resilience | The client reconnects itself, fail-fast `no responders` | You design timeouts and circuit breakers yourself |
| Multi-region | Works transparently through a supercluster | You must understand the topology or requests cross oceans |
| Contracts | Endpoint metadata | No OpenAPI or typing, you own the schemas |
| Debugging | `nats sub`, `nats req` from anywhere | No curl, Postman or browser |

A common compromise: an HTTP gateway at the edge, NATS inside.

---

# Module 12. Clustering, superclusters and leaf nodes

## 12.1 Three levels of topology

```
  LEAF NODES (edge, branches, laptops, devices)
        |  leafnode connection (outbound, through NAT and TLS)
        v
  CLUSTER (3-5 servers in one DC, routes, full mesh)
        |  gateway connection
        v
  SUPERCLUSTER (several clusters in different regions)
```

| Level | Connection | Default port | Purpose |
|---|---|---|---|
| Cluster | `routes` | 6222 | Fault tolerance and scale within a DC |
| Supercluster | `gateways` | 7222 | Linking clusters across regions |
| Leaf node | `leafnodes` | 7422 | Edge, isolation, weak or intermittent links |

## 12.2 Clustering and RAFT

A Core NATS cluster needs no quorum: it simply routes messages. But **JetStream does**: every stream with `replicas > 1` is a RAFT group, plus there is a meta group holding the list of streams and consumers.

```
meta RAFT group (all cluster servers):
  who leads JetStream, where to place streams

stream RAFT group (one per stream):
  ORDERS: leader=nats-2, followers=nats-1, nats-3
```

Consequences:

| Nodes | Survives failure of | Comment |
|---|---|---|
| 1 | 0 | Development only |
| 2 | 0 | **The worst option**: no quorum, worse than a single node |
| 3 | 1 | The production standard |
| 5 | 2 | Large installations, higher write cost |

An even node count adds nothing. `replicas=2` technically exists but survives no failures without losing write availability.

```bash
# `nats server ...` commands need a system-account context
nats server report jetstream           # who leads, how many streams per node
nats stream cluster step-down ORDERS   # force a stream leader re-election
nats server cluster step-down          # re-elect the meta leader (system account)
```

## 12.3 Superclusters and gateways

```
       ┌──────────── cluster EU ────────────┐
       │  nats-eu-1  nats-eu-2  nats-eu-3   │
       └──────────────┬─────────────────────┘
                      │ gateway
       ┌──────────────┴─────────────────────┐
       │  nats-us-1  nats-us-2  nats-us-3   │
       └──────────── cluster US ────────────┘
```

```hcl
gateway {
  name: "EU"
  port: 7222
  gateways: [
    { name: "EU", urls: ["nats://nats-eu-1:7222", "nats://nats-eu-2:7222"] }
    { name: "US", urls: ["nats://nats-us-1:7222", "nats://nats-us-2:7222"] }
  ]
}
```

How it works:

- you list **one or a few seed URLs per remote cluster**: servers discover the rest of the remote cluster via gossip, so adding a node doesn't mean editing every config;
- a message crosses the ocean **only if there is interested subscription on the other side**;
- request-reply prefers a local responder: if an instance exists in EU, the request never goes to the US;
- **loop detection** is built in: messages don't circulate between clusters.

This is the main argument for NATS in multi-region setups: a geo-distributed bus without MirrorMaker, without manual topic mirroring and without a separate routing layer.

## 12.4 Leaf nodes

A leaf node is a separate `nats-server` that makes an **outbound** connection to a cluster and bridges one of its local accounts with an account on the hub. Which hub account you land in is decided by the credentials; the optional `account` field names the local account on the leaf. Use one authentication method per remote: credentials, or user and password, not both.

```hcl
# leaf server config on a factory floor / in a shop / on a developer laptop
leafnodes {
  remotes: [
    {
      url: "tls://nats.company.com:7422"
      # the creds decide WHICH account on the hub this leaf binds to
      credentials: "/etc/nats/edge.creds"
      # optional: the LOCAL account on this leaf server to bridge
      # account: "SHOP"
    }
  ]
}

jetstream { store_dir: "/data/js", domain: "edge-lon-42" }
```

Why you'd want it:

- **works during outages**: local services keep talking through their own leaf server;
- **a local JetStream with its own domain**: data accumulates locally and syncs upstream when the link returns;
- **bandwidth savings**: only messages with upstream interest travel;
- **through NAT and firewalls**: the connection is always outbound, no inbound ports to open;
- **isolation**: a leaf only sees the account it attaches to.

```bash
curl -s localhost:8222/leafz | jq '.leafs[] | {name, account, ip, rtt}'   # on the hub
```

A typical IoT setup: every site runs a leaf node with JetStream domain `edge-<id>`, data is written locally, and the central cluster collects it with a `source` referencing the domain (module 13).

## 12.5 JetStream domains

A domain names an isolated JetStream space. You need it to distinguish "my local JetStream" from "JetStream at HQ":

```bash
nats --js-domain hub stream ls          # streams in the central domain
nats --js-domain edge-lon-42 stream ls  # streams on this leaf node
```

Without domains a JetStream-enabled leaf and the central cluster start fighting over the same API subjects. Always set a domain when you enable JetStream on a leaf.

## 12.6 MQTT and WebSocket: built-in listeners

`nats-server` speaks MQTT 3.1.1 and WebSocket natively, which covers IoT devices and browsers without a separate gateway.

```hcl
server_name: "edge-lon-42"       # required for MQTT
jetstream { store_dir: "/data/js" }   # MQTT sessions and QoS 1 are stored in JetStream

mqtt {
  port: 1883
}

websocket {
  port: 8080
  no_tls: true                  # development only; use TLS in production
}
```

How MQTT maps onto NATS:

| MQTT | NATS |
|---|---|
| `/` topic separator | `.` token separator |
| `+` single-level wildcard | `*` |
| `#` multi-level wildcard | `>` |
| QoS 0 | Core NATS delivery |
| QoS 1, retained messages, sessions | JetStream streams created by the server |

So a device publishing to `factory/line1/temp` is visible to NATS services on `factory.line1.temp`, and vice versa. Avoid dots in MQTT topic names: they become token separators.

Over WebSocket, browser code uses the official JavaScript client and gets the same subjects, permissions and accounts as any other client.

### Self-check questions

1. Why is a two-node cluster worse than a single node for JetStream?
2. How does a gateway differ from a route?
3. Why does a request-reply call in a supercluster normally stay in-region?
4. Why does a leaf node need its own JetStream domain?
5. Why does MQTT support require JetStream?

---

# Module 13. Mirrors, sources and geo-replication

## 13.1 Mirror and Source

A subject can belong to only one stream. Copies of data are made with special streams:

| | **Mirror** | **Source** |
|---|---|---|
| Number of origins | Exactly one | One or many |
| Own subjects | No, copy only | May have them, plus data from sources |
| Direct publishing | Forbidden | Allowed |
| Ordering and seq | Preserves the origin's sequence | Renumbers |
| Use for | Read replica, backup, geo copy | Aggregating several streams into one |

```
                      ORDERS (EU, primary)
                       |            |
              mirror   |            |  source
                       v            v
              ORDERS_MIRROR_US    ALL_EVENTS (ORDERS + PAYMENTS + SHIPPING)
```

## 13.2 Mirrors for geo-replication

```bash
nats stream add ORDERS_US \
  --mirror ORDERS \
  --storage file --replicas 3 \
  --defaults
```

With a filter and a start position, a JSON config is the clearest and version-independent way:

```json
{
  "name": "ORDERS_ARCHIVE",
  "storage": "file",
  "num_replicas": 3,
  "mirror": {
    "name": "ORDERS",
    "filter_subject": "shop.orders.created",
    "opt_start_seq": 1
  }
}
```

```bash
nats stream add ORDERS_ARCHIVE --config orders-archive.json
```

Reader services in the US attach to `ORDERS_US` and don't cross the ocean per message. Publishing still goes to EU.

## 13.3 Sources: aggregation

```bash
nats stream add ALL_EVENTS \
  --source ORDERS \
  --source PAYMENTS \
  --source SHIPPING \
  --storage file --replicas 3
```

Useful for analytics and audit: one stream for the whole BI team while the domain streams stay separate.

A source can live in another JetStream domain (for example on a leaf node). The domain is addressed through its API prefix, `$JS.<domain>.API`:

```json
{
  "name": "EDGE_TELEMETRY",
  "storage": "file",
  "num_replicas": 3,
  "sources": [
    {
      "name": "TELEMETRY",
      "external": { "api": "$JS.edge-lon-42.API" }
    }
  ]
}
```

```bash
nats stream add EDGE_TELEMETRY --config edge-telemetry.json
```

This is how a central cluster pulls data from hundreds of edge nodes while those nodes keep working through outages.

## 13.4 Disaster recovery

| Scenario | Solution |
|---|---|
| One cluster node dies | `replicas=3`, RAFT re-elects a leader in seconds |
| A whole DC dies | A mirror in another region plus publisher failover |
| The application corrupted data | Scheduled `nats stream backup` |
| Migration between clusters | `backup` / `restore`, or a mirror followed by cutover |

Test your restores regularly. A backup you've never restored is not a backup.

---

# Module 14. Error handling: retry, DLQ and poison pills

## 14.1 Classifying errors

```
error while processing a message
   |
   +-- transient (DB down, 503, timeout) -> Nak with delay, we'll retry
   +-- permanent (invalid JSON, no such order) -> Term, send to DLQ
   +-- unknown -> Nak, but count attempts: after N, Term
```

An application that doesn't distinguish these is doomed: it will either spin on garbage forever or throw away valid messages over a brief network blip.

## 14.2 Backoff instead of a fixed interval

```bash
nats consumer add ORDERS BILLING --pull --ack explicit \
  --max-deliver 6 \
  --backoff linear --backoff-steps 5 --backoff-min 1s --backoff-max 5m
```

```
attempt 1 -> error -> wait 1s
attempt 2 -> error -> wait ~1m
attempt 3 -> error -> wait ~2m
attempt 4 -> error -> wait ~3m
attempt 5 -> error -> wait ~4m
attempt 6 -> error -> MAX_DELIVERIES advisory -> no more deliveries
```

`linear` spreads the delays evenly from min towards max (with natscli the last delay stays one step below max). If you want a growing schedule such as 1s, 15s, 1m, 3m, 5m, set the array explicitly (`BackOff` in code or `"backoff"` in a JSON consumer config). `max_deliver` can't be smaller than the number of backoff entries; here 6 attempts for 5 delays uses every delay.

Without backoff, a struggling database gets a storm of retries at the exact moment it is least able to cope.

## 14.3 A DLQ built on advisories

NATS has no built-in DLQ, but it has advisory events from which you can build one:

```
$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.<stream>.<consumer>
$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.<stream>.<consumer>
```

**A trap first.** Advisories are ordinary Core NATS messages. If your DLQ service simply subscribes to them and happens to be down (a deploy, a crash) when an advisory fires, that advisory is gone and the failed message never reaches the DLQ. That's exactly the "Core NATS for things you can afford to lose" rule from module 0. So the advisories themselves go into a stream first, and the DLQ mover reads them with a durable consumer.

```go
// 1. capture advisories durably
js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
	Name: "ADVISORIES",
	Subjects: []string{
		"$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>",
		"$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.>",
	},
	Storage:  jetstream.FileStorage,
	Replicas: 3,
	MaxAge:   7 * 24 * time.Hour,
})

// 2. a stream for dead messages
js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
	Name:       "DLQ",
	Subjects:   []string{"dlq.>"},
	Storage:    jetstream.FileStorage,
	Replicas:   3,
	MaxAge:     30 * 24 * time.Hour,
	Duplicates: 10 * time.Minute,
})

// 3. a durable consumer that moves originals into the DLQ
mover, _ := js.CreateOrUpdateConsumer(ctx, "ADVISORIES", jetstream.ConsumerConfig{
	Durable:    "DLQ_MOVER",
	AckPolicy:  jetstream.AckExplicitPolicy,
	MaxDeliver: 20,
})

mover.Consume(func(m jetstream.Msg) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// both advisory types carry these fields
	var adv struct {
		Stream     string `json:"stream"`
		Consumer   string `json:"consumer"`
		StreamSeq  uint64 `json:"stream_seq"`
		Deliveries uint64 `json:"deliveries"`
		Reason     string `json:"reason"` // MSG_TERMINATED only
	}
	if err := json.Unmarshal(m.Data(), &adv); err != nil {
		m.Term()
		return
	}
	if adv.Stream == "DLQ" || adv.Stream == "ADVISORIES" {
		m.Ack() // never loop on our own streams
		return
	}

	stream, err := js.Stream(ctx, adv.Stream)
	if err != nil {
		m.NakWithDelay(5 * time.Second)
		return
	}
	orig, err := stream.GetMsg(ctx, adv.StreamSeq)
	if err != nil {
		log.Println("original already removed by retention:", err)
		m.Ack()
		return
	}

	// copy headers into a NEW map: orig.Header may be nil,
	// and the original Nats-Msg-Id must not trigger dedup in the DLQ
	h := nats.Header{}
	for k, v := range orig.Header {
		if k != "Nats-Msg-Id" {
			h[k] = v
		}
	}
	h.Set("Dlq-Original-Subject", orig.Subject)
	h.Set("Dlq-Original-Stream", adv.Stream)
	h.Set("Dlq-Original-Seq", strconv.FormatUint(adv.StreamSeq, 10))
	h.Set("Dlq-Deliveries", strconv.FormatUint(adv.Deliveries, 10))
	if adv.Reason != "" {
		h.Set("Dlq-Reason", adv.Reason)
	}
	// a stable id makes the move itself idempotent if the mover retries
	h.Set("Nats-Msg-Id", fmt.Sprintf("dlq-%s-%d", adv.Stream, adv.StreamSeq))

	dlqMsg := &nats.Msg{
		Subject: "dlq." + adv.Stream + "." + adv.Consumer,
		Header:  h,
		Data:    orig.Data,
	}
	if _, err := js.PublishMsg(ctx, dlqMsg); err != nil {
		m.NakWithDelay(5 * time.Second)
		return
	}

	alert("message moved to DLQ", adv)
	m.Ack()
})
```

Notes:

- the original is fetched by `stream_seq`, so this works as long as retention hasn't removed it yet: keep `max_age` on the source stream comfortably longer than your total retry time;
- with `workqueue` retention a message that exhausted `max_deliver` stays in the stream (it was never acked), so it is still available here, but it also still counts against limits: purge it after moving if you need to;
- the application user needs permission to subscribe to `$JS.EVENT.ADVISORY.>` inside its account (module 17).

Reprocessing after you fix the bug:

```bash
nats stream view DLQ
nats consumer add DLQ REPROCESS --pull --ack explicit --defaults
# a service reads the DLQ and republishes to the original subject
```

## 14.4 Poison pills: how not to take down the queue

```
a message with broken JSON
  -> the handler panics
  -> no ack is sent
  -> AckWait expires and it is redelivered
  -> the handler panics again
  -> ... forever, workers do nothing else
```

Defences:

1. **Always a finite `max_deliver`.** `-1` in production is a landmine.
2. **Validate on entry**: if it doesn't parse, `Term()` immediately, no retries.
3. **Recover from panics** in the handler, or the whole worker dies.
4. **`max_ack_pending`** caps how many poison messages can occupy workers at once.
5. **Alert on rising `num_redelivered`** — the earliest signal something is wrong.

## 14.5 Useful advisories

| Advisory | About |
|---|---|
| `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>` | A message exhausted its attempts (capture into a stream, see 14.3) |
| `$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.>` | A client called `Term()` |
| `$JS.EVENT.ADVISORY.STREAM.CREATED/DELETED/UPDATED.>` | Configuration changes |
| `$JS.EVENT.ADVISORY.API` | All JetStream API calls (audit) |
| `$SYS.ACCOUNT.*.DISCONNECT` | Client disconnects, including error cases |
| `$SYS.SERVER.*.CLIENT.AUTH.ERR` | Authentication failures |

```bash
nats events                                  # interactive viewer
nats sub "$JS.EVENT.ADVISORY.>" --headers-only
```

### Practice

1. Write a handler that always fails and watch the backoff chain.
2. Implement the DLQ above and confirm messages land in it. Then stop the mover, trigger a failure, start the mover again and confirm the message still reaches the DLQ.
3. Publish broken JSON and make it reach the DLQ on the first attempt via `Term()`.

---

# Module 15. Performance and tuning

## 15.1 Orders of magnitude

| Scenario | Ballpark on one decent server |
|---|---|
| Core NATS fan-out, small messages | Millions msg/s, tens of microseconds latency |
| Core NATS request-reply | Hundreds of thousands req/s |
| JetStream memory, replicas=1 | Hundreds of thousands msg/s |
| JetStream file, replicas=1 | 100-300k msg/s (disk-dependent) |
| JetStream file, replicas=3 | Tens of thousands msg/s synchronous, far more async |

The takeaway: **moving from Core to JetStream costs an order of magnitude, and moving to replicas=3 costs several times more again.** Don't enable persistence where it isn't needed.

## 15.2 Benchmark it yourself

```bash
# Core NATS pub/sub
nats bench pub bench.core --msgs 1000000 --size 128 --clients 5
nats bench sub bench.core --msgs 1000000 --clients 5

# request-reply
nats bench service serve bench.rpc --clients 5
nats bench service request bench.rpc --msgs 100000 --clients 10

# JetStream
nats bench js pub ORDERS --msgs 200000 --size 512 --clients 5
nats bench js pub ORDERS --msgs 200000 --size 512 --clients 5 --batch 500   # async
```

Always measure **on your hardware, with your message sizes and your topology**. Other people's numbers are useless.

## 15.3 Slow consumers: the classic Core NATS failure

When a subscriber can't keep up, the server buffers messages for it. When the buffer overflows, the server **drops messages and logs `slow consumer`**.

```bash
curl -s localhost:8222/varz | grep -i slow
nats server report connections --sort subs
```

What to do:

| Cause | Fix |
|---|---|
| Heavy work inside the callback | Push into a channel/queue and process in a worker pool |
| One subscriber for the whole firehose | Queue group and horizontal scaling |
| Small client buffer | `SetPendingLimits`, larger sync queue |
| Slow network to the client | Move it closer or use a leaf node |
| You actually need guarantees, not speed | JetStream: no loss from slow clients, just `max_ack_pending` |

## 15.4 Server settings

```hcl
max_payload: 1MB          # don't raise it without a very good reason
max_pending: 64MB         # outbound buffer per client
max_connections: 64000
write_deadline: "10s"
ping_interval: "2m"
ping_max: 2

jetstream {
  store_dir: "/data/jetstream"
  max_file_store: 500GB
  max_memory_store: 4GB
  sync_interval: "2m"     # fsync frequency (the default); "always" is safer but much slower
}
```

**What `sync_interval` means for durability.** By default JetStream acknowledges a publish once the write is accepted by a quorum, and flushes to disk every 2 minutes. With `replicas=3` that's safe in practice: losing acknowledged data would require a simultaneous power loss on a majority of nodes. With `replicas=1`, a power loss or kernel panic can lose up to the last couple of minutes of **acknowledged** messages. If you run single-replica streams with data you can't lose, use `sync_interval: always` and budget for the throughput hit, or simply use `replicas=3`.

Linux settings:

```
# file descriptors
ulimit -n 1000000

# network
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.ipv4.tcp_rmem = 4096 87380 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216
```

Use local NVMe for JetStream. Network disks with variable fsync latency turn RAFT into a lottery.

## 15.5 Client tuning

| Setting | Why |
|---|---|
| One connection per process | The connection multiplexes every subscription |
| `PublishAsync` + a pending cap | Multiplies JetStream throughput |
| Larger `Fetch(batch)` | Fewer round trips per message |
| `max_ack_pending` matched to worker count | Too low means idling, too high means overload |
| `AckWait` slightly above p99 processing time | Otherwise you get needless redeliveries |
| `Drain()` instead of `Close()` | Don't lose messages during deploys |
| No heavy logic in callbacks | The classic source of slow consumers |

## 15.6 What actually makes systems faster

1. **Don't enable JetStream where Core suffices.** The cheapest optimisation there is.
2. **Don't publish what nobody wants.** Interest routing only saves bandwidth when subscriptions are narrow.
3. **Batch.** Both on publish and on fetch.
4. **Compress payloads in the application** when messages are large and textual.
5. **Keep messages small.** Files in the Object Store, a reference in the event.
6. **Count your replicas.** `replicas=3` on a telemetry stream nobody reads after an hour is wasted money.

---

# Module 16. Monitoring NATS: metrics, lag, alerts

## 16.1 HTTP monitoring endpoints

Enabled with `-m 8222` or `http: 8222`.

| Endpoint | What it shows |
|---|---|
| `/varz` | General server info: version, uptime, memory, connections, slow consumers |
| `/connz` | Connections: who, how many messages, which subscriptions |
| `/routez` | Connections to other cluster servers |
| `/gatewayz` | Gateway connections |
| `/leafz` | Leaf connections |
| `/subsz` | Subscription table |
| `/jsz` | JetStream: accounts, streams, consumers, disk usage |
| `/healthz` | Health check for Kubernetes |
| `/accountz` | Accounts |

```bash
curl -s localhost:8222/varz | jq '{connections, slow_consumers, mem, in_msgs, out_msgs}'
curl -s "localhost:8222/jsz?streams=1&consumers=1" | jq '.account_details'
curl -s "localhost:8222/healthz?js-enabled-only=true"
```

## 16.2 Prometheus

```yaml
# docker-compose fragment
  nats-exporter:
    image: natsio/prometheus-nats-exporter:latest
    command: ["-varz", "-connz", "-routez", "-subz", "-jsz=all", "http://nats-1:8222"]
    ports: ["7777:7777"]
```

Key metrics:

| Metric | What it tells you |
|---|---|
| `gnatsd_varz_slow_consumers` | Rising = clients can't keep up |
| `gnatsd_varz_connections` | Connection count |
| `gnatsd_varz_mem` / `cpu` | Server resources |
| `gnatsd_varz_in_msgs` / `out_msgs` | Throughput |
| `jetstream_consumer_num_pending` | **Consumer lag**, the headline metric |
| `jetstream_consumer_num_ack_pending` | Outstanding acks |
| `jetstream_consumer_num_redelivered` | Redeliveries |
| `jetstream_stream_total_bytes` / `total_messages` | Stream fill level |
| `jetstream_server_jetstream_disk_used` | Free space |

## 16.3 What to alert on

| Alert | Condition | Why it matters |
|---|---|---|
| **Consumer lag growing** | `num_pending` rising for 10 minutes | The consumer can't keep up or is down |
| **Redeliveries** | `num_redelivered > 0` persistently | Processing errors, poison messages |
| **Ack pending at the ceiling** | `num_ack_pending >= max_ack_pending` | Processing has stalled |
| **Slow consumers** | Any increase in the counter | Message loss in Core NATS |
| **JetStream disk** | Over 75% used | At 100% publishing stops or starts dropping |
| **No stream leader** | Empty `leader` in `nats stream info` | Quorum lost |
| **Messages in the DLQ** | Any | Business logic is failing |
| **Server count drop** | `< 3` in the cluster | Quorum at risk |
| **Auth errors** | `$SYS.SERVER.*.CLIENT.AUTH.ERR` | An attack or a broken deploy |

## 16.4 Everyday commands

```bash
# `server`, `latency`, `events` and `traffic` need a system-account context
nats server list
nats server info nats-1
nats server report jetstream
nats server report connections --sort in-msgs
nats server report accounts
nats stream report
nats consumer report ORDERS
nats latency --server-b nats://nats-2:4222 --duration 10s   # server-to-server latency
nats events                                                  # system event stream
nats traffic                                                 # live traffic
```

## 16.5 Message tracing

Since NATS 2.11 delivery tracing is available: the server reports a message's path through the cluster.

```bash
nats trace shop.orders.created
```

It shows which servers the message traversed, which subscribers received it and whether it landed in a stream. Indispensable for "why isn't my service getting the event".

---

# Module 17. NATS security: accounts, NKeys, JWT, TLS

## 17.1 Three authentication models

| Model | How | When |
|---|---|---|
| **Tokens and passwords in config** | `authorization { users: [...] }` | Development, small installs |
| **NKeys** | Ed25519 keys, challenge-response, the private key never leaves the client | Mid-size installs without central management |
| **JWT + operator model** | An operator signs accounts, accounts sign users | Production, multi-tenancy, dynamic access control |

## 17.2 A simple config with users and permissions

```hcl
accounts {
  SHOP: {
    users: [
      {
        user: "order-service", password: "$2a$11$…"   # bcrypt via `nats server passwd`
        permissions: {
          publish:   { allow: ["shop.orders.>", "$JS.API.STREAM.MSG.GET.ORDERS"] }
          subscribe: { allow: ["_INBOX.>", "shop.payments.>"] }
        }
      },
      {
        user: "billing-service", password: "$2a$11$…"
        permissions: {
          publish: {
            allow: [
              "shop.payments.>",                             # its own events
              "$JS.API.CONSUMER.INFO.ORDERS.BILLING",        # clients look up the consumer first
              "$JS.API.CONSUMER.MSG.NEXT.ORDERS.BILLING",    # a pull request is a PUBLISH
              "$JS.ACK.ORDERS.BILLING.>"                     # acks for this consumer only
            ]
          }
          # messages and PubAcks arrive on the inbox
          subscribe: { allow: ["_INBOX.>"] }
        }
      }
    ]
    jetstream: { max_file: 100GB, max_streams: 20 }
  }

  ANALYTICS: {
    users: [ { user: "analytics", password: "$2a$11$…",
               permissions: { subscribe: { allow: ["shop.>"] }, publish: { deny: [">"] } } } ]
  }

  SYS: { users: [ { user: "admin", password: "$2a$11$…" } ] }
}
system_account: SYS
# no `no_auth_user` here: in production every client must authenticate
```

A common mistake is to think of a pull request as a subscription. It isn't: the client **publishes** to `$JS.API.CONSUMER.MSG.NEXT.<stream>.<consumer>` with an inbox as the reply subject, and the server delivers messages to that inbox. The same applies to every JetStream API call. If a service fails with `permissions violation`, the server log names the exact subject that was denied.

`_INBOX.>` is broad: any user allowed to subscribe to it could in principle see other users' replies. For stricter isolation, give each service its own inbox prefix (`nats.CustomInboxPrefix("_INBOX_billing")` in Go) and allow only `_INBOX_billing.>`.

**Least privilege matters especially here:** subscribing to `>` within an account means access to everything. Always list concrete subjects.

## 17.3 Exports and imports between accounts

```hcl
accounts {
  SHOP: {
    exports: [
      { stream: "shop.orders.created" }                       # a public event stream
      { service: "service.pricing.calculate" }                # a public service
    ]
  }
  PARTNER: {
    imports: [
      { stream:  { account: "SHOP", subject: "shop.orders.created" } }
      { service: { account: "SHOP", subject: "service.pricing.calculate" } }
    ]
  }
}
```

An optional `prefix` on a stream import prepends a token: `prefix: "upstream"` makes the events appear in `PARTNER` as `upstream.shop.orders.created`. That's handy when importing from several accounts, and confusing when the prefix repeats the first token.

This is the only way to cross an account boundary, and it is explicit. Want to give a partner exactly one subject? You give exactly one.

## 17.4 The operator model: nsc

```bash
nsc add operator --generate-signing-key --sys --name SHOP_OPERATOR
nsc add account --name SHOP
nsc edit account SHOP --js-mem-storage 1G --js-disk-storage 100G --js-streams 50
nsc add user --account SHOP --name order-service \
  --allow-pub "shop.orders.>" --allow-sub "_INBOX.>"
nsc generate creds --account SHOP --name order-service > order-service.creds

# the server has to know about the operator: generate its resolver config once
nsc generate config --nats-resolver --sys-account SYS > resolver.conf
# add `include resolver.conf` to nats.conf, restart, then push accounts
nsc push --all
```

```go
nc, _ := nats.Connect("nats://nats.company.com:4222",
	nats.UserCredentials("/etc/nats/order-service.creds"))
```

What this gives you:

- private keys **never travel to the server**;
- permissions are baked into a signed JWT: the server keeps no user list;
- users can be issued and revoked without restarting servers (via a NATS account server or the built-in resolver);
- expiry, connection limits, data limits and subscription limits all live in the JWT.

## 17.5 TLS

```hcl
tls {
  cert_file: "/etc/nats/certs/server-cert.pem"
  key_file:  "/etc/nats/certs/server-key.pem"
  ca_file:   "/etc/nats/certs/ca.pem"
  verify:    true          # require a client certificate (mTLS)
  timeout:   5
}

cluster {
  name: "course"
  port: 6222
  tls { cert_file: "…", key_file: "…", ca_file: "…", verify: true }
  routes: [ … ]
}
```

TLS is configured separately for clients, routes, gateways and leafnodes. Encrypt intra-cluster traffic too: a route connection sees every message in the cluster.

With mTLS you can map users to certificate CNs:

```hcl
tls { verify_and_map: true }   # certificate CN = user name
```

## 17.6 Limits as a security control

```hcl
accounts {
  PARTNER: {
    limits: {
      max_connections: 100
      max_subscriptions: 1000
      max_payload: 256KB
      max_data: 10GB
    }
  }
}
```

A noisy neighbour opening 50,000 connections or subscribing to `>` is more dangerous than an external attacker. Account limits aren't paranoia, they're hygiene.

## 17.7 Production security checklist

- [ ] TLS for clients, routes, gateways and leafnodes
- [ ] No `no_auth_user`, no anonymous access (the module 2 compose file uses it for learning only)
- [ ] A separate account per team or tenant
- [ ] Permissions on concrete subjects, never `>` in allow
- [ ] A system account (`system_account`) with separate credentials, SRE-only
- [ ] JetStream limits per account
- [ ] Monitoring port 8222 not exposed publicly
- [ ] Automated rotation of creds and certificates
- [ ] Alerts on `AUTH.ERR` and connection spikes
- [ ] `deny_delete` and `deny_purge` on audit streams

---

# Module 18. NATS in production: architecture and operations

## 18.1 Reference architecture

```
        applications                      applications
          (EU)                              (US)
            |                                 |
   ┌────────┴─────────┐             ┌─────────┴────────┐
   │  cluster EU       │◄──gateway──►│  cluster US      │
   │  3 x nats-server  │             │  3 x nats-server │
   │  JetStream file   │             │  JetStream file  │
   └────────┬──────────┘             └──────────────────┘
            │ leafnodes
   ┌────────┴──────────┐
   │  edge: shops,     │
   │  factories, IoT   │
   │  leaf + JS domain │
   └───────────────────┘
```

## 18.2 Sizing

| Load | Configuration |
|---|---|
| Up to 10k msg/s, Core only | 3 nodes × 2 vCPU / 4 GB, JetStream optional |
| Up to 50k msg/s with JetStream | 3 nodes × 4-8 vCPU / 16 GB / NVMe 500 GB |
| 100k+ msg/s with JetStream | 5 nodes × 8-16 vCPU / 32 GB / NVMe 1+ TB, spread streams across nodes |
| Multi-region | One cluster per region + gateways, mirrors for reads |

Size Core and JetStream traffic **separately**: they are fundamentally different in cost.

## 18.3 Kubernetes

```bash
helm repo add nats https://nats-io.github.io/k8s/helm/charts/
helm install nats nats/nats \
  --set config.jetstream.enabled=true \
  --set config.jetstream.fileStore.pvc.size=100Gi \
  --set config.cluster.enabled=true \
  --set config.cluster.replicas=3
```

What matters:

- **StatefulSet and persistent volumes** are mandatory: without them JetStream loses data when a pod moves;
- `podAntiAffinity` across zones: three replicas in one zone won't survive a zone failure;
- `/healthz` as readiness and liveness probe, with `js-enabled-only` on JetStream nodes;
- **rolling updates one pod at a time**, waiting for quorum to recover between steps;
- before draining a node, run `nats server cluster step-down` if it is the meta leader.

## 18.4 Upgrading

```bash
# 1. check the state
nats server report jetstream
nats stream report

# 2. move leadership off the node you're upgrading
nats server cluster step-down
nats stream cluster step-down ORDERS

# 3. upgrade one node, wait for it to rejoin the quorum
# 4. repeat for the rest
```

NATS is protocol-compatible across minor versions, but upgrading **one node at a time with quorum checks** is a rule without exceptions.

## 18.5 Production checklist

- [ ] 3 or 5 cluster nodes spread across zones
- [ ] `replicas=3` on every important stream
- [ ] `max_age` **and** `max_bytes` set on every stream
- [ ] JetStream limits on every account
- [ ] Finite `max_deliver` on every consumer
- [ ] A DLQ and an alert when anything lands in it
- [ ] Scheduled stream backups and a tested restore
- [ ] Stream and consumer config in Git, applied by automation, not by hand
- [ ] Monitoring: lag, redeliveries, slow consumers, disk, quorum
- [ ] TLS and least-privilege accounts
- [ ] `Drain()` on application shutdown
- [ ] `Nats-Msg-Id` on every business event
- [ ] Idempotent handlers
- [ ] Game days done: kill a node, kill a leader, fill the disk

## 18.6 Anti-patterns

| Anti-pattern | Why it's bad | Do this instead |
|---|---|---|
| Core NATS for business events | Silent data loss | JetStream |
| A stream on `shop.>` "just in case" | One stream captures everything, nothing else can be created | Narrow per-domain subjects |
| `max_deliver: -1` | A poison message loops forever | A finite limit + DLQ |
| A stream with no `max_bytes` or `max_age` | The disk fills at the worst possible time | Always set limits |
| `replicas: 2` | No quorum | 1, 3 or 5 |
| Subscribing to `>` in an app | You get system traffic and other people's data | Concrete subjects |
| Ack before processing | At-most-once instead of at-least-once | Ack after a successful commit |
| Huge messages (tens of MB) | Clogged buffers, slow consumers | Object Store + a reference in the event |
| Timestamps or UUIDs in stream subjects | Explosive growth of unique subjects and memory | Identifiers in the payload, or at the end of the subject deliberately |
| One account for the whole company | No isolation, everyone sees everything | One account per team |

---

# Module 19. Capstone project: an event-driven online shop

## 19.1 What you're building

```
                    ┌─────────────┐
   HTTP  ──────────►│ Order API   │
                    └──────┬──────┘
                           │ JetStream: shop.orders.created
                    ┌──────▼───────────────────────────────┐
                    │        Stream ORDERS                  │
                    │  subjects: shop.orders.>              │
                    │  limits, 30d, replicas 3              │
                    └──┬────────┬────────────┬──────────────┘
                       │        │            │
              consumer │        │            │ consumer
              PAYMENT  │        │ consumer   │ ANALYTICS (ordered)
                       │        │ WAREHOUSE  │
                ┌──────▼─┐  ┌───▼────┐  ┌────▼─────┐
                │Payment │  │Warehouse│  │Analytics │
                └───┬────┘  └────────┘  └──────────┘
                    │ shop.payments.succeeded / failed
                    ▼
              Stream PAYMENTS ──► consumer NOTIFY ──► Core NATS ──► WebSocket clients

   Plus:
   - KV bucket CONFIG: limits, feature flags, live updates via watch
   - KV bucket IDEMPOTENCY: processed event ids
   - Object bucket RECEIPTS: PDF receipts
   - Stream DLQ: everything that couldn't be processed
   - Services API: service.pricing.calculate, service.fraud.check
```

## 19.2 Requirements

1. Orders are created over HTTP, and the event is published to JetStream **in the same transaction as the database write** (outbox).
2. Every event carries a `Nats-Msg-Id` and is never duplicated on retries.
3. Payment Service processes idempotently, `Nak`s with backoff on transient errors and `Term`s on invalid data.
4. Warehouse reserves stock and survives redelivery.
5. Analytics reads the stream with an ordered consumer and builds an in-memory projection.
6. Notification pushes updates through Core NATS to a WebSocket gateway.
7. Anything that exhausts its attempts goes to the DLQ with an alert.
8. Feature flags are read from KV and applied without restarts.
9. There's a dashboard: per-consumer lag, redeliveries, DLQ size.
10. The system survives losing any single NATS node without data loss.

## 19.3 Stages

| Stage | What to do |
|---|---|
| 1 | Bring up a 3-node cluster, create `ORDERS`, `PAYMENTS`, `DLQ` |
| 2 | Order API: HTTP + PostgreSQL + outbox + relay to NATS |
| 3 | Payment Service: pull consumer, idempotency via a `processed_events` table |
| 4 | Warehouse: its own consumer on the same stream with its own filter |
| 5 | Analytics: an ordered consumer and a "revenue per hour" projection |
| 6 | Services API: `service.pricing.calculate`, called from Order API via request-reply |
| 7 | KV `CONFIG` + watch: toggle the fraud check on the fly |
| 8 | DLQ via the `MAX_DELIVERIES` advisory plus a reprocessing command |
| 9 | Monitoring: exporter + Prometheus + Grafana, alerts on lag and DLQ |
| 10 | Game day: kill the stream leader, kill a worker mid-processing, fill the disk |

## 19.4 How to verify it works

```bash
# load
nats bench js pub ORDERS --msgs 100000 --size 512 --clients 5

# during the load
docker stop nats-2
nats stream info ORDERS      # leader re-elected, data intact
nats consumer report ORDERS  # lag spiked and drained

# idempotency check
# publish the same event three times with the same Nats-Msg-Id
# the stream should hold 1 message, the database 1 row
```

If after all the chaos `num_pending` is back to zero, the DLQ is empty and there are no duplicates in the database, you've finished the course.

---

# What's new in NATS 2.11 and 2.12

The modules above use features that have been stable for years. Recent releases added several that change how you'd solve some of the course's problems. Field and header names below are given for orientation: check the release notes and ADRs for your exact server and client versions before relying on them.

**NATS 2.11**

- **Per-message TTL.** A stream that allows it can expire individual messages via a TTL header, not just the stream-wide `max_age`. It powers per-key TTL in the KV Store (module 9).
- **Consumer pause.** A consumer can be paused until a given time and resumed, instead of being deleted and recreated. Useful during maintenance of a downstream database.
- **Priority groups.** Pull consumers can pin delivery to one client (with automatic failover) or send overflow to standby clients above a pending threshold. That's a server-side alternative to part of the manual sharding in 6.8.
- **Message tracing.** `nats trace` follows a message through the cluster (16.5).

**NATS 2.12**

- **Atomic batch publishing.** A group of messages is committed to a stream all together or not at all, which helps with events that must appear together.
- **Distributed counters.** A stream can be configured to treat messages as increments on a per-subject counter, merged correctly across mirrors and sources.

When you use any of these, pin the minimum server version in your deployment docs: older servers in a mixed cluster may ignore or reject the new fields.

---

# NATS CLI cheat sheet

```bash
# contexts
nats context add prod --server nats://a:4222,nats://b:4222 --creds ./app.creds
nats context select prod
nats context ls

# basics
nats pub shop.orders.created '{"id":1}' --count 10
nats pub shop.orders.created '{"id":1}' -H "Nats-Msg-Id:evt-1"
nats sub "shop.>" --headers-only
nats sub "shop.orders.created" --queue workers
nats request service.echo "ping" --timeout 2s
nats reply service.echo --echo

# streams
nats stream ls
nats stream add ORDERS --subjects "shop.orders.>" --defaults
nats stream info ORDERS
nats stream view ORDERS
nats stream get ORDERS 42
nats stream subjects ORDERS
nats stream edit ORDERS --max-age 48h
nats stream purge ORDERS --subject "shop.orders.created"
nats stream rmm ORDERS 42
nats stream backup ORDERS ./bk && nats stream restore ./bk
nats stream report

# consumers
nats consumer add ORDERS BILLING --pull --ack explicit --defaults
nats consumer ls ORDERS
nats consumer info ORDERS BILLING
nats consumer next ORDERS BILLING --count 5
nats consumer report ORDERS
nats consumer rm ORDERS BILLING

# KV
nats kv add CONFIG --history 5
nats kv put CONFIG key value
nats kv get CONFIG key
nats kv watch CONFIG
nats kv history CONFIG key

# object store
nats object add ASSETS
nats object put ASSETS ./file.bin
nats object get ASSETS file.bin --output ./out.bin
nats object ls ASSETS

# microservices
nats micro ls
nats micro info pricing
nats micro stats pricing

# server and diagnostics (system account context)
nats server list
nats server report jetstream
nats server report connections
nats server check stream --stream ORDERS --peer-expect 3
nats server cluster step-down
nats stream cluster step-down ORDERS
nats latency --server-b nats://b:4222
nats events
nats traffic
nats bench pub bench.test --msgs 100000
```

---

# Configuration cheat sheet

**Stream (durable business events):**

```
storage: file
replicas: 3
retention: limits
discard: old
max_age: 720h
max_bytes: 50GB
duplicate_window: 2m
```

**Stream (task queue):**

```
storage: file
replicas: 3
retention: workqueue
discard: new
max_msgs: 1000000
max_age: 24h
```

**Consumer (typical service):**

```
pull
ack_policy: explicit
ack_wait: 30s            (slightly above p99 processing time)
max_deliver: 5
max_ack_pending: 500     (≈ worker count × 10)
backoff: 1s, 10s, 1m, 5m   (replaces ack_wait for redeliveries; max_deliver >= 4, and 5 uses every delay)
filter_subject: a concrete subject
```

**Client:**

```
every cluster address in the connection string
max_reconnects: -1
reconnect_wait: 500ms
Drain() on shutdown
Nats-Msg-Id on every business event
Ack() after the database commit
```

**Server:**

```
max_payload: 1MB
jetstream.store_dir on local NVMe
http: 8222 (not publicly exposed)
TLS everywhere: clients, routes, gateways, leafnodes
JetStream limits on every account
```

---

# NATS interview questions with answers

**Junior**

1. **What is a subject and how is it different from a queue?** A subject is a hierarchical message address. It isn't an object, needs no creation, and stores nothing by itself.
2. **How does `*` differ from `>`?** `*` matches exactly one token; `>` matches one or more and only at the end of a subscription.
3. **What happens if you publish to a subject with no subscribers?** In Core NATS, nothing: the message vanishes. If a JetStream stream captures that subject, it is stored.
4. **What is a queue group?** A set of subscribers across which the server distributes messages: one member gets each copy.
5. **How does request-reply work?** The client creates a temporary inbox, sets it as the reply subject, and the service publishes its answer there.
6. **How does Core NATS differ from JetStream?** Core is in-memory delivery with no guarantees (at-most-once); JetStream adds storage, acknowledgements and redelivery.
7. **What are streams and consumers?** A stream stores messages for given subjects; a consumer is a server-side cursor with delivery and ack policies.
8. **What does `Ack()` do?** Confirms processing: the server stops treating the message as outstanding and won't redeliver it.
9. **Why use `Drain()`?** To shut down gracefully: unsubscribe, finish in-flight work, then close the connection.
10. **What's the maximum message size?** 1 MB by default, configurable, but raising it is discouraged.

**Middle**

11. **How does `Nak` differ from `Term`?** `Nak` requeues for redelivery; `Term` says "never deliver this again".
12. **What is `InProgress()` for?** To reset the `AckWait` timer during long processing so the server doesn't start a parallel redelivery.
13. **What is `max_ack_pending` and why does it matter?** The cap on unacknowledged messages per consumer: it is both your concurrency limit and your backpressure.
14. **Name the three retention policies and when to use each.** `limits` for event logs with replay, `interest` for guaranteed pub/sub without history, `workqueue` for task queues.
15. **Why can't one subject be in two streams?** It would be ambiguous where to store the message; use mirrors and sources for copies.
16. **How does deduplication work?** The stream remembers `Nats-Msg-Id` for `duplicate_window` and replies `PubAck{duplicate:true}` to repeats.
17. **How do you achieve exactly-once?** Publish deduplication plus idempotent consumer processing. There is no single setting.
18. **Pull or push consumers, and why?** Pull: natural flow control, easier scaling, nothing is lost when the client dies.
19. **How do you scale processing?** Several instances on one durable pull consumer; there are no partitions and no rebalance.
20. **How do you guarantee per-entity ordering?** A subject per entity with a filtered consumer, sharding by subject token, or `max_ack_pending=1`.
21. **What is a slow consumer?** A subscriber that can't keep up; the server drops its messages and increments a counter in `/varz`.
22. **What is `num_pending`?** The number of messages the consumer still has to receive, i.e. the lag.
23. **How do you build a DLQ?** Subscribe to the `MAX_DELIVERIES` advisory, fetch the original by `stream_seq` and move it to a separate stream.
24. **How is the KV Store implemented?** A stream with `max_msgs_per_subject` and `$KV.<bucket>.>` subjects, plus a client API with revisions and watch.
25. **What are accounts for?** Full subject-namespace isolation: multi-tenancy without separate clusters.

**Senior**

26. **Why is a two-node cluster worse than a single node?** JetStream uses RAFT: with two nodes quorum is lost when either fails, and the stream becomes unavailable for writes.
27. **How do superclusters avoid pointless cross-ocean traffic?** Gateways forward a message to another cluster only when there is interest there, and request-reply prefers a local responder.
28. **When do you need a leaf node?** Edge and branch sites: local operation during outages, its own JetStream domain, outbound connections through NAT, account isolation.
29. **Mirror or source?** A mirror is an exact copy of one stream preserving sequence numbers; a source aggregates several streams and renumbers messages.
30. **How do you design a subject space that lasts five years?** General to specific, domain-entity-event, versioning kept separate, deliberate use of identifiers, no timestamps, permissions granted by prefix.
31. **How do you avoid losing an event when a service dies between the DB write and NATS?** A transactional outbox plus `Nats-Msg-Id` so the relay's retries deduplicate.
32. **What do you do when `num_redelivered` grows?** Find the poison messages, check `AckWait` against real p99, make sure the handler distinguishes transient from permanent errors, inspect the DLQ.
33. **How do you choose between NATS and Kafka?** Weigh history retention and data ecosystem needs against low latency, RPC, multi-region reach and operational simplicity.

---

# FAQ

**Does NATS lose messages?**
Core NATS does, by design (at-most-once). JetStream doesn't, when configured properly (`replicas=3`, ack after processing, a finite `max_deliver`).

**Do I need to create a subject before publishing?**
No. A subject exists the moment you publish. Only streams, consumers and buckets need creating.

**Can NATS replace Kafka?**
Sometimes. For event exchange between microservices, RPC, edge and multi-region, NATS is usually more convenient. For big-data pipelines, CDC and the Connect/Streams ecosystem, Kafka stays.

**Why do I get `no responders available`?**
Nobody is subscribed to the request subject: the service isn't running, the subject has a typo, or permissions block it. It's not a timeout, it's an immediate server response.

**Why `jetstream not enabled for account`?**
The server started without `-js`, or the account has no JetStream enabled (`jetstream: enabled` or limits in the account config).

**How do I re-read a stream from the start?**
Recreate the consumer with `--deliver all` or `--start-time`. There is no "reset offset" command.

**How many messages can a stream hold?**
Only disk and limits constrain it. Terabytes are possible, but NATS wasn't designed as multi-year archival storage: offload to S3 or Kafka for that.

**How do I change stream config without losing data?**
`nats stream edit`. A few fundamentals (like storage type) can't change in place. For those, create a new stream that **sources** from the old one, wait until it catches up, switch publishers to the new stream's subjects, then remove the old stream. A source rather than a mirror, because a mirror doesn't accept direct publishes.

**How does `interest` retention differ from `workqueue`?**
`interest` removes a message once **all** consumers ack it; `workqueue` once **any one** does.

**Do I need a Schema Registry?**
There is no built-in one. Contracts live in a separate repository (Protobuf, Avro, JSON Schema) and are versioned via the subject or a header.

**Does NATS support MQTT and WebSocket?**
Yes, natively: `mqtt { port: 1883 }` and `websocket { port: 8080 }`. MQTT requires JetStream and a `server_name`. Details in 12.6.

**How do I migrate from RabbitMQ?**
Queues → `workqueue` streams, fanout exchanges → wildcard subscriptions, RPC → request-reply, DLX → a DLQ on advisories. Complex header routing has no direct equivalent and moves into the subject hierarchy.

---

# NATS glossary

| Term | Meaning |
|---|---|
| **Subject** | A hierarchical message address of dot-separated tokens |
| **Wildcard** | `*` (one token) and `>` (trailing tokens) in subscriptions |
| **Core NATS** | The in-memory delivery layer, at-most-once |
| **JetStream** | The persistence layer: streams, consumers, KV, Object Store |
| **Queue group** | Subscribers sharing messages among themselves |
| **Request-Reply** | Built-in RPC via a reply subject |
| **Inbox** | A temporary `_INBOX.…` subject for replies |
| **Stream** | Message storage for a set of subjects |
| **Consumer** | A server-side cursor with its own delivery and ack policy |
| **Durable / Ephemeral** | A consumer that persists / one that lives only while a client is attached |
| **Stream sequence** | A message's number within a stream |
| **Ack floor** | The sequence up to which the consumer has acked everything |
| **num_pending** | How many messages the consumer still has to receive (lag) |
| **AckWait** | How long the server waits for an ack before redelivering |
| **MaxDeliver** | Maximum delivery attempts |
| **MaxAckPending** | Cap on unacknowledged messages per consumer |
| **Nak / Term / InProgress** | Requeue / never redeliver / extend processing |
| **Nats-Msg-Id** | The header used for publish deduplication |
| **Duplicate window** | How long a stream remembers `Nats-Msg-Id` values |
| **Retention** | `limits`, `interest` or `workqueue` |
| **Discard** | `old` or `new`: what to do when a limit is reached |
| **Mirror / Source** | An exact copy of one stream / aggregation of several |
| **Advisory** | A JetStream system event (`$JS.EVENT.ADVISORY.>`) |
| **KV bucket** | Key-value on a stream with `max_msgs_per_subject` |
| **Object bucket** | File storage split into chunks |
| **Account** | An isolated subject namespace |
| **Export / Import** | Explicit subject sharing between accounts |
| **NKey** | An Ed25519 key used for authentication |
| **Creds / JWT** | A file holding a user's signed permissions |
| **Route** | A connection between servers in one cluster |
| **Gateway** | A connection between clusters (supercluster) |
| **Leaf node** | An edge server attached by an outbound connection |
| **JetStream domain** | The name of an isolated JetStream space (hub and edge) |
| **Slow consumer** | A subscriber that can't keep up; its messages are dropped |
| **RAFT group** | The quorum of servers replicating a stream or the meta state |

---

# Official sources and what to read next

- **NATS documentation** — https://docs.nats.io
- **Server repository** — https://github.com/nats-io/nats-server
- **nats CLI** — https://github.com/nats-io/natscli
- **Examples in every language (NATS by Example)** — https://natsbyexample.com
- **Go client** — https://github.com/nats-io/nats.go (look at the `jetstream` package)
- **Python client** — https://github.com/nats-io/nats.py
- **Java client** — https://github.com/nats-io/nats.java
- **Helm charts and Kubernetes** — https://github.com/nats-io/k8s
- **nsc (accounts and JWT management)** — https://github.com/nats-io/nsc
- **ADRs: NATS architecture and design** — https://github.com/nats-io/nats-architecture-and-design
- **Slack community** — https://slack.nats.io

---

## Contributing

Found an error, an inaccuracy or an outdated setting? Open an issue or send a pull request. Especially welcome:

- examples in languages not covered here (Rust, C#, Node.js, Elixir);
- real production stories and incident write-ups;
- corrections for newer NATS Server releases.

⭐ If this course helped, star the repo so other developers can find it.

**Licence:** the course text is licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/), and the code samples under the [MIT License](../LICENSE). You're free to use, adapt and share the material, including for internal workshops, as long as you credit the source.
