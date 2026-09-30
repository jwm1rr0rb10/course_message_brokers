# RabbitMQ Course 2026: a free RabbitMQ course from zero to pro

![RabbitMQ 4.3](https://img.shields.io/badge/RabbitMQ-4.3-FF6600?logo=rabbitmq&logoColor=white)
![Quorum queues](https://img.shields.io/badge/quorum%20queues-Raft-blue)
![Language English](https://img.shields.io/badge/language-english-red)
![Free course](https://img.shields.io/badge/price-free-brightgreen)
![junior to senior](https://img.shields.io/badge/level-junior%20→%20senior-orange)

> **A complete free RabbitMQ course.** Theory, practice, Docker, Python, Go and Java, AMQP 0-9-1, exchanges and routing, classic and quorum queues, streams, publisher confirms, acks and prefetch, delivery guarantees and idempotency, dead letter exchanges, TTL and delayed retries, clustering and Raft, RPC, AMQP 1.0 and MQTT, Federation and Shovel, monitoring, security and production architecture. All in a single README, current for **RabbitMQ 4.3 (2026)**.

**RabbitMQ without the fluff:** every module gives you clear theory, diagrams, commands you can actually run, the mistakes people make, and self-check questions. Use it to learn RabbitMQ from scratch, to prepare for a backend or DevOps interview, and to design a reliable RabbitMQ-based system in production.

⭐ If the course helps, star the repository so other developers can find it.

🇷🇺 Русская версия: [READMEru.md](READMEru.md)

---

## Who this RabbitMQ course is for

| Who you are | What you get |
|---|---|
| **New to messaging** | What a broker is, how a queue differs from a log, how to run RabbitMQ in a minute |
| **Backend developer** (examples in Python, Go and Java) | Publisher confirms, acks and prefetch, retries and DLX, idempotent processing |
| **Microservices developer** | Routing through exchanges, RPC, work queues, outbox, sagas |
| **DevOps / SRE** | Clustering, quorum queues, Khepri and Raft, monitoring, memory and disk alarms, upgrades, Kubernetes |
| **Architect / Tech Lead** | Topology design, choosing a queue type, RabbitMQ vs Kafka and NATS, anti-patterns |
| **IoT** | MQTT and WebSocket straight into RabbitMQ |
| **Interview prep** | 35 RabbitMQ questions with answers at junior, middle and senior level |

## What you'll be able to do after the course

- explain the AMQP 0-9-1 model: connection, channel, exchange, queue, binding, routing key, virtual host;
- run a three-node RabbitMQ cluster in Docker and break it while watching quorum queue leader elections;
- choose an exchange type (direct, fanout, topic, headers) and design routing;
- choose a queue type: classic, quorum or stream, and know why;
- publish without losses: persistent messages, publisher confirms, `mandatory` and returns;
- consume correctly: manual ack, `nack` and `reject`, prefetch, redelivery;
- build delayed retries and dead letter exchanges, and limit delivery attempts;
- understand how a Raft-based cluster works and what happens when nodes fail;
- use streams for replay and large flows;
- do RPC over RabbitMQ with direct reply-to;
- connect clients over AMQP 1.0, MQTT and STOMP;
- link clusters with Federation and Shovel;
- constrain resources with policies and understand memory and disk alarms;
- monitor queues, consumers and nodes with Prometheus;
- enable TLS, configure users, permissions and OAuth 2;
- upgrade a cluster without downtime.

---

## Table of contents

- [Who this RabbitMQ course is for](#who-this-rabbitmq-course-is-for)
- [What you'll be able to do after the course](#what-youll-be-able-to-do-after-the-course)
- [How to take this course](#how-to-take-this-course)
- [Module 0. What RabbitMQ is and why you need it](#module-0-what-rabbitmq-is-and-why-you-need-it)
- [Module 1. Architecture: connection, channel, exchange, queue, binding](#module-1-architecture-connection-channel-exchange-queue-binding)
- [Module 2. Installing RabbitMQ in Docker and first commands](#module-2-installing-rabbitmq-in-docker-and-first-commands)
- [Module 3. Exchanges and routing](#module-3-exchanges-and-routing)
- [Module 4. Queues: classic, quorum and streams](#module-4-queues-classic-quorum-and-streams)
- [Module 5. Publisher: persistent messages, confirms and mandatory](#module-5-publisher-persistent-messages-confirms-and-mandatory)
- [Module 6. Consumer: ack, prefetch and redelivery](#module-6-consumer-ack-prefetch-and-redelivery)
- [Module 7. Delivery guarantees, idempotency and outbox](#module-7-delivery-guarantees-idempotency-and-outbox)
- [Module 8. Dead letter exchanges, TTL and delayed retries](#module-8-dead-letter-exchanges-ttl-and-delayed-retries)
- [Module 9. Cluster and quorum queues: Raft, Khepri, fault tolerance](#module-9-cluster-and-quorum-queues-raft-khepri-fault-tolerance)
- [Module 10. Streams and super streams](#module-10-streams-and-super-streams)
- [Module 11. Patterns: work queues, pub/sub, RPC, priorities](#module-11-patterns-work-queues-pubsub-rpc-priorities)
- [Module 12. Protocols: AMQP 1.0, MQTT, STOMP, WebSocket](#module-12-protocols-amqp-10-mqtt-stomp-websocket)
- [Module 13. Multiple data centres: Federation and Shovel](#module-13-multiple-data-centres-federation-and-shovel)
- [Module 14. Policies, limits and resource management](#module-14-policies-limits-and-resource-management)
- [Module 15. Performance and tuning](#module-15-performance-and-tuning)
- [Module 16. Monitoring and alerts](#module-16-monitoring-and-alerts)
- [Module 17. Security: TLS, users, permissions, OAuth 2](#module-17-security-tls-users-permissions-oauth-2)
- [Module 18. RabbitMQ in production: Kubernetes, operations, upgrades](#module-18-rabbitmq-in-production-kubernetes-operations-upgrades)
- [Module 19. Capstone project: an event-driven online shop](#module-19-capstone-project-an-event-driven-online-shop)
- [RabbitMQ CLI and HTTP API cheat sheet](#rabbitmq-cli-and-http-api-cheat-sheet)
- [Configuration cheat sheet](#configuration-cheat-sheet)
- [RabbitMQ interview questions with answers](#rabbitmq-interview-questions-with-answers)
- [FAQ](#faq)
- [RabbitMQ glossary](#rabbitmq-glossary)
- [Official sources and what to read next](#official-sources-and-what-to-read-next)

---

## How to take this course

1. **Go in order.** Modules 0–6 are the foundation. Without exchanges, bindings and acks everything else looks like magic.
2. **Run every command.** RabbitMQ is learned by hand, and the management UI shows everything happening inside: queues, rates, unacknowledged messages.
3. **Break the cluster.** Stop nodes, kill consumers mid-processing, fill a queue to its limit. That is how production experience appears.
4. **Answer the questions at the end of each module** out loud, as if you were in an interview.
5. **Do the capstone project.** It pulls every topic into one system.

**What to install:** Docker and Docker Compose, Git, any IDE. Python 3.10+ for the Python examples, a current Go for Go, Java 17+ for Java.

**Version:** all examples target RabbitMQ 4.3, image `rabbitmq:4.3-management`. Recent changes you'll meet in the course:

- since 4.0 **classic mirrored queues are gone**: replicated queues are quorum queues and streams;
- since 4.3 cluster metadata is stored only in **Khepri** (Raft-based); Mnesia and partition handling strategies are removed;
- since 4.3 non-durable non-exclusive queues and `global` prefetch are **denied by default**;
- the minimum Erlang version is 27 starting with 4.3.3 (and 4.2.9); 4.3.0–4.3.2 still ran on Erlang 26.2.

**Runnable examples:** the cluster, a smoke test of the commands, Go and Python code and integration tests for the course's claims live in [`examples/`](examples/). CI ([`.github/workflows/examples.yml`](../.github/workflows/examples.yml)) runs them on every push and once a week: `go vet` and unit tests, then the smoke test and the integration tests on a three-node cluster, on the `rabbitmq:4.3-management` image and on the newest RabbitMQ image. If a release changes behaviour described here, the build goes red.

---

# Module 0. What RabbitMQ is and why you need it

## 0.1 RabbitMQ in plain words

**RabbitMQ** is a message broker: a server that accepts messages from some programs, puts them into queues and hands them to other programs.

RabbitMQ does three things:

1. **Routes**: by rules (exchanges and bindings) it decides which queues a message goes to.
2. **Stores**: it keeps messages in a queue until a consumer takes and acknowledges them.
3. **Delivers**: it hands messages to consumers, tracks acknowledgements and redelivers what wasn't acknowledged.

The core idea of RabbitMQ is **a smart broker, simple clients**. The producer doesn't know which queues exist or who reads them: it sends a message to an exchange with a routing key, and the broker decides where to put it. The consumer just reads its queue.

RabbitMQ appeared in 2007 as an implementation of the open AMQP protocol and is written in Erlang, a language built for fault-tolerant telecom systems. It is developed by a team at Broadcom (previously VMware), and its source is open under the Mozilla Public License 2.0.

## 0.2 The problem RabbitMQ solves

An online shop: a user places an order, and then you need to send an email, charge the card, reserve stock and generate a PDF receipt. If you do it all synchronously in the HTTP handler:

```
POST /orders
   |
   +--> write the order to the database    20 ms
   +--> charge the card                    300 ms
   +--> reserve stock                      150 ms
   +--> generate a PDF receipt             2,000 ms
   +--> send the email                     800 ms
   |
the user gets an answer after 3+ seconds, and any error breaks the whole order
```

| Question | The problem with synchronous processing |
|---|---|
| The mail service is down | The order fails, even though the email could be sent later |
| PDF generation is slow | The user waits |
| Black Friday, 10× the orders | Every service falls over at once |
| One more action needs adding | Change and redeploy the order service |
| A task failed halfway | Nobody knows what's already done |

## 0.3 The same system with RabbitMQ

```
POST /orders -> write order -> publish "order.created" -> answer in 30 ms
                                         |
                                         v
                              +---------------------+
                              |  exchange "orders"  |
                              +---------------------+
                              /      |       |       \
                             v       v       v        v
                        [payments] [stock] [receipts] [emails]   <- queues
                            |        |       |          |
                         workers  workers  workers    workers
```

What you gain:

- **A fast response.** Long work goes to the background.
- **A buffer for spikes.** The queue accumulates tasks, and workers process them at their own pace.
- **Independence.** The mail service is down? Emails wait in the queue while everything else works.
- **Scaling.** Add workers to a queue and tasks are processed faster.
- **Flexible routing.** A new queue attaches to the exchange with no change to the producer.
- **Redelivery.** A worker died without acknowledging? The broker gives the message to another one.

## 0.4 Queue vs log: the most important thing to understand about RabbitMQ

This is **the key idea of the course**, especially if you know Kafka.

```
QUEUE (RabbitMQ classic and quorum queues)    LOG (Kafka, RabbitMQ streams)
------------------------------------------    ---------------------------------
message deleted after ack                      message stays until retention
each message goes to one consumer              any number of independent readers
broker tracks delivered and acked              reader tracks its own position (offset)
no re-reading                                  re-read from any position
ack/nack each message individually             a position is committed
parallelism = consumers on the queue           parallelism = number of partitions
```

| Question | RabbitMQ queue | Log |
|---|---|---|
| After processing | The message is deleted | It stays, the offset moves |
| Ten systems need one event | One queue each, the exchange copies the message | One log, each with its own offset |
| Re-read yesterday | Impossible | Possible |
| A slow message | Doesn't hold others back: other consumers take the next ones | Blocks the partition |
| Postpone and retry one message | Yes: nack, DLX, TTL, delayed retry | Hard |
| Routing | Rich: exchanges, keys, headers | By topic and partition key |

**Rule of thumb:** RabbitMQ queues when you need to hand tasks to workers, route flexibly and reliably get every message processed. A log when history and replay matter. RabbitMQ has both: for a log there are **streams** (module 10).

## 0.5 What a message is

```
Exchange:    orders
Routing key: order.created.eu
Properties:
  delivery_mode:  2                 (persistent: survive a restart)
  content_type:   application/json
  message_id:     5f1c2a7e-9b1d-4c1e-8a4e-0c7b2d9f1a11
  correlation_id: (for RPC)
  reply_to:       (for RPC)
  expiration:     "60000"           (message TTL, ms)
  priority:       5
  timestamp:      1789466400
  headers:        {event-type: OrderCreated, trace-id: 4bf92f3577b34da6}
Body:        {"order_id":"order-123","user_id":"user-42","amount":4990}
```

- **Body** is just bytes. You pick the format (JSON, Protobuf, Avro).
- **Routing key** is the string the exchange uses to decide where the message goes.
- **Properties** are standard AMQP fields. The most important: `delivery_mode` (persistent or not), `message_id` (for deduplication), `correlation_id` and `reply_to` (for RPC), `headers` (your own metadata).

The default maximum message size is **16 MB** (`max_message_size`, upper bound 512 MB). But large messages are a bad idea: they pressure memory and the network. Files go to object storage and the message carries a reference.

## 0.6 RabbitMQ vs Kafka, NATS and Redis

| Criterion | RabbitMQ | Apache Kafka | NATS JetStream | Redis Streams / Pub/Sub |
|---|---|---|---|---|
| Model | Queue broker + streams | Distributed log | Subjects + streams | In-memory data structures |
| Routing | Rich: direct, topic, fanout, headers | Topic and key | Hierarchical subjects | Channels and keys |
| Acknowledging a message | Each individually | Committing a position | Each individually | XACK for Streams |
| Replay | Streams | Yes, the core product | Yes | Streams |
| Delayed retries | DLX + TTL, delayed retry in quorum queues | By hand | NakWithDelay, backoff | By hand |
| Priorities | Yes | No | No | No |
| Protocols | AMQP 0-9-1, AMQP 1.0, MQTT, STOMP, Stream | Kafka protocol | NATS, MQTT, WebSocket | RESP |
| Strength | Flexible routing, task queues, maturity | Throughput, history, data integration | Simplicity, low latency, edge | Speed, if you already run Redis |

**An honest word on choosing:** RabbitMQ is an excellent choice for task queues, service integration with non-trivial routing, and enterprise systems that need delivery guarantees for every message. For a years-long event log, CDC and analytics pipelines, use Kafka. For lightweight RPC and edge, look at NATS. Many companies run RabbitMQ for tasks and commands and Kafka for data streams.

### RabbitMQ and HTTP together

```
Client --HTTP--> Order API --(saves the order, returns 201)--> Client
                     |
                     +--publish order.created--> RabbitMQ --> workers
```

HTTP is for when the user is waiting for an answer. RabbitMQ is for background work, asynchronous events and decoupling services.

## 0.7 Where RabbitMQ is used

| Scenario | How it's applied |
|---|---|
| **Task queues** | Report generation, image processing, sending emails |
| **Microservice integration** | Events and commands through topic exchanges |
| **RPC** | A request through a queue, the answer via reply-to |
| **A buffer in front of a slow system** | The queue smooths load on a database or external API |
| **Delayed tasks and retries** | TTL + DLX, delayed retry in quorum queues |
| **IoT** | MQTT devices connect straight to the broker |
| **Notification fan-out** | A fanout exchange hands an event to every subscriber |
| **Event log with replay** | Streams |

## 0.8 When you don't need RabbitMQ

- You need years of event history and analytics: Kafka.
- One or two background jobs in a monolith: a PostgreSQL or Redis queue is enough.
- You need a synchronous answer for the user: HTTP or gRPC.
- You need millions of messages per second with replay: Kafka or RabbitMQ streams, but not classic queues.
- You want transactions between a database and the broker: nobody offers them; design an outbox (module 7).

## 0.9 What RabbitMQ will NOT do for you

- idempotent processing in the application;
- message schemas and versioning (RabbitMQ has no Schema Registry);
- retry strategy and handling "poison" messages;
- monitoring queue lengths and consumers;
- access control and limits;
- a well-thought-out topology: renaming exchanges and queues later is painful.

### Self-check questions

1. How does a queue differ from a log, and what does that mean for consumers?
2. Why doesn't a RabbitMQ producer know which queues a message will reach?
3. Which message properties does RPC need?
4. When would you pick Kafka over RabbitMQ?

---

# Module 1. Architecture: connection, channel, exchange, queue, binding

## 1.1 The main hierarchy

```
RabbitMQ cluster (nodes rabbit@rabbit-1, rabbit@rabbit-2, rabbit@rabbit-3)
  └── Virtual host (an isolated namespace, e.g. "/" or "shop")
        ├── Exchanges (entry points: you publish here)
        │     └── Bindings (rules: exchange -> queue by key)
        └── Queues (messages live here)
              └── Consumers (receive messages from a queue)

and separately, on the client side:
  Connection (one TCP connection)
    └── Channels (lightweight logical channels inside the connection)
```

Memorise this picture. The whole course hangs on it.

## 1.2 Core components

| Component | What it is | Analogy |
|---|---|---|
| **Message** | Body + properties + routing key | A letter with an address |
| **Producer** | A client that publishes messages | Sender |
| **Exchange** | Accepts messages and routes them to queues | A sorting centre |
| **Queue** | An ordered buffer of messages | A mailbox |
| **Binding** | A rule linking an exchange to a queue | A sorting rule |
| **Routing key** | The string routing works on | The address on the envelope |
| **Consumer** | A client that receives messages from a queue | Recipient |
| **Connection** | A client's TCP connection to the broker | A phone line |
| **Channel** | A logical channel inside a connection | A call on the line |
| **Virtual host** | An isolated namespace | A separate broker |

## 1.3 A message's path

```
producer                                                        consumer
   |                                                               ^
   | basic.publish(exchange="orders", routing_key="order.created") |
   v                                                               |
+----------+   binding "order.*"    +-----------------+    deliver |
| exchange | ---------------------> | queue: payments | -----------+
|  orders  |                        +-----------------+
|  (topic) |   binding "order.#"    +-----------------+
|          | ---------------------> | queue: audit    | ---> another consumer
+----------+                        +-----------------+
      |
      | no binding matched -> the message is dropped
      |   (or returned to the producer if mandatory=true)
```

Key consequences:

- **you always publish to an exchange**, never directly to a queue. Even "publishing to a queue" is publishing to the default exchange (module 3);
- one message can land **in several queues**: the broker copies it into each matching one;
- if no queue matches, the message **silently disappears**. This is one of the most common causes of "lost" messages (the protections are `mandatory` and an alternate exchange, modules 3 and 5);
- each queue delivers each message to **one** of its consumers.

## 1.4 Connection and channel

```
application
  |
  +-- Connection (TCP, TLS, authentication, heartbeats)
        +-- Channel 1: publishing orders
        +-- Channel 2: consumer of the payments queue
        +-- Channel 3: consumer of the stock queue
```

- A **connection** is expensive: TCP handshake, TLS, authentication, memory on the broker. Open one or two per process and keep them long.
- A **channel** is a cheap logical connection inside a connection. All AMQP operations (declare, publish, consume, ack) go through a channel.
- **Channels are not thread-safe** in most clients: one channel, one thread (or one goroutine).
- A protocol error (for example, publishing to a non-existent exchange) **closes the channel**, not the connection. The application must be able to open the channel again.
- Don't open a connection per message: it's a classic anti-pattern that takes the broker down under load.

A practical rule: one connection for publishing and one for consuming (so publishing flow control doesn't slow deliveries to consumers), channels per thread.

## 1.5 Virtual hosts: isolation inside a broker

A **virtual host** (vhost) is a separate namespace: its own exchanges, queues, bindings, permissions and limits. Messages don't travel between vhosts.

```
vhost "/"        — the default
vhost "shop"     — exchanges orders, payments; queues payments, stock
vhost "billing"  — queues with the same names, but completely separate
```

A vhost is a convenient boundary for a team or an environment: user permissions are granted per vhost, and limits (number of queues and connections) are set per vhost.

## 1.6 Queue properties

| Property | Meaning |
|---|---|
| **durable** | The queue survives a broker restart (its definition is kept) |
| **exclusive** | The queue belongs to one connection and is deleted when it closes |
| **auto-delete** | The queue is deleted when its last consumer unsubscribes |
| **arguments** | Extra parameters: type (`x-queue-type`), TTL, limits, DLX |

An important distinction: a **durable queue** and a **persistent message** are different things. A durable queue keeps its definition. For the messages themselves to survive a restart they need `delivery_mode=2` (quorum queues and streams always store messages on disk).

Since 4.3, queues that are both `durable=false` and `exclusive=false` are **denied by default**. For temporary queues use `exclusive=true` or a durable queue with a queue TTL (`x-expires`).

## 1.7 Three queue types

| Type | Replication | Storage | When |
|---|---|---|---|
| **Classic** | No (lives on one node) | Memory + disk | Temporary and exclusive queues, data you can afford to lose |
| **Quorum** | Yes, Raft across several nodes | Disk | Everything important: the production standard |
| **Stream** | Yes, log replication | Disk, a log | Replay, many readers, large volumes |

Details in module 4. The main point now: **use quorum queues for important data**. Classic mirrored queues, which used to provide replication, were removed in 4.0.

## 1.8 The numbers that describe a queue

| Figure | Meaning |
|---|---|
| **Ready** | Messages in the queue ready to deliver |
| **Unacked** | Delivered to consumers but not yet acknowledged |
| **Total** | Ready + Unacked |
| **Consumers** | How many consumers are subscribed |
| **Publish rate / Deliver rate / Ack rate** | Inbound, delivery and acknowledgement rates |

**Growing Ready** means consumers can't keep up or there are none. **Growing Unacked** means consumers took messages but don't acknowledge them (stuck, slow, or they forgot to ack).

## 1.9 Your first mental model

```
Publishing:
  producer -> channel -> exchange -> bindings decide which queues ->
  a copy of the message in each matching queue ->
  (with publisher confirms) the broker confirms to the producer

Consuming:
  consumer subscribed to a queue -> broker delivers up to prefetch messages ->
  consumer processes -> ack -> the message is deleted from the queue
  nack/reject or a consumer crash -> the message returns to the queue
  (or goes to a dead letter exchange)
```

### Self-check questions

1. Why does a producer publish to an exchange and not to a queue?
2. What happens to a message if no binding matches?
3. How does a connection differ from a channel, and why not open a connection per message?
4. How does a durable queue differ from a persistent message?
5. What does growing Unacked mean?

---

# Module 2. Installing RabbitMQ in Docker and first commands

## 2.1 The fastest way to run RabbitMQ

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 \
  -e RABBITMQ_DEFAULT_USER=admin -e RABBITMQ_DEFAULT_PASS=admin \
  rabbitmq:4.3-management
```

- `5672` is AMQP 0-9-1 and AMQP 1.0;
- `15672` is the management UI and HTTP API: open http://localhost:15672 (admin / admin);
- the `-management` tag enables the management plugin.

The default `guest` user can connect **only from localhost** inside the container, so create your own user right away.

Check it:

```bash
docker exec rabbitmq rabbitmq-diagnostics status
docker exec rabbitmq rabbitmq-diagnostics check_port_connectivity
docker exec rabbitmq rabbitmqctl list_queues name type messages consumers
```

## 2.2 The main tools

| Tool | What for |
|---|---|
| `rabbitmqctl` | Node management: users, permissions, vhosts, policies, cluster, object listings |
| `rabbitmq-diagnostics` | Health and diagnostics: status, checks, alarms, memory |
| `rabbitmq-queues` | Operations on quorum queues and streams: replicas, leaders |
| `rabbitmq-plugins` | Enabling and disabling plugins |
| `rabbitmq-upgrade` | Preparing a node for an upgrade (drain) |
| `rabbitmqadmin` | A CLI on top of the HTTP API: declare an exchange or queue, publish a message |
| Management UI | All of the above in a browser, plus charts |

The second-generation `rabbitmqadmin` (v2) is a standalone binary that works through the HTTP API and needs no access to the node. In this course objects are managed through the HTTP API with `curl` or `rabbitmqadmin`, and node operations with `rabbitmqctl` inside the container.

## 2.3 A three-node cluster in Docker Compose

For learning you need a real cluster: then you can stop nodes and watch quorum queues elect new leaders.

RabbitMQ cluster nodes must:

- share **the same Erlang cookie**, a secret that nodes and CLI tools use to trust each other;
- reach each other by **stable hostnames**: the node name is `rabbit@<hostname>`;
- know whom to cluster with: in Compose, **peer discovery** via `classic_config` is convenient.

Create a directory:

```bash
mkdir rabbitmq-course && cd rabbitmq-course
```

`rabbitmq.conf`:

```ini
# nodes find each other from this list and form a cluster by themselves
cluster_formation.peer_discovery_backend = classic_config
cluster_formation.classic_config.nodes.1 = rabbit@rabbit-1
cluster_formation.classic_config.nodes.2 = rabbit@rabbit-2
cluster_formation.classic_config.nodes.3 = rabbit@rabbit-3

# Prometheus (port 15692): /metrics serves aggregated metrics, which is the default;
# per-queue metrics are on /metrics/per-object and /metrics/detailed (module 16.1)
prometheus.return_per_object_metrics = false

# stream protocol (module 10.4): advertise localhost to clients, not the container name
stream.advertised_host = localhost
```

Each node publishes the stream port on its own host port (5552, 5553, 5554), and must advertise exactly that port to clients:

```bash
mkdir stream
for i in 1 2 3; do echo "stream.advertised_port = $((5551 + i))" > stream/rabbit-$i.conf; done
```

`enabled_plugins`:

```erlang
[rabbitmq_management,rabbitmq_prometheus,rabbitmq_stream,rabbitmq_stream_management].
```

`docker-compose.yml`:

```yaml
x-rabbit-common: &rabbit-common
  image: rabbitmq:4.3-management
  restart: unless-stopped
  environment:
    RABBITMQ_DEFAULT_USER: admin
    RABBITMQ_DEFAULT_PASS: admin
    # shared secret of the cluster nodes; in production a long random string from a secret
    RABBITMQ_COOKIE: "course-secret-cookie"
  # write the cookie file before start: the image fixes its owner and permissions
  entrypoint:
    - /bin/sh
    - -c
    - |
      echo "$$RABBITMQ_COOKIE" > /var/lib/rabbitmq/.erlang.cookie
      chmod 400 /var/lib/rabbitmq/.erlang.cookie
      exec docker-entrypoint.sh rabbitmq-server

services:
  rabbit-1:
    <<: *rabbit-common
    hostname: rabbit-1
    container_name: rabbit-1
    ports: ["5672:5672", "15672:15672", "15692:15692", "5552:5552"]
    volumes:
      - ./rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf:ro
      - ./enabled_plugins:/etc/rabbitmq/enabled_plugins:ro
      - ./stream/rabbit-1.conf:/etc/rabbitmq/conf.d/20-stream.conf:ro
      - rabbit1-data:/var/lib/rabbitmq

  rabbit-2:
    <<: *rabbit-common
    hostname: rabbit-2
    container_name: rabbit-2
    ports: ["5673:5672", "15673:15672", "5553:5552"]
    volumes:
      - ./rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf:ro
      - ./enabled_plugins:/etc/rabbitmq/enabled_plugins:ro
      - ./stream/rabbit-2.conf:/etc/rabbitmq/conf.d/20-stream.conf:ro
      - rabbit2-data:/var/lib/rabbitmq

  rabbit-3:
    <<: *rabbit-common
    hostname: rabbit-3
    container_name: rabbit-3
    ports: ["5674:5672", "15674:15672", "5554:5552"]
    volumes:
      - ./rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf:ro
      - ./enabled_plugins:/etc/rabbitmq/enabled_plugins:ro
      - ./stream/rabbit-3.conf:/etc/rabbitmq/conf.d/20-stream.conf:ro
      - rabbit3-data:/var/lib/rabbitmq

volumes:
  rabbit1-data:
  rabbit2-data:
  rabbit3-data:
```

**Why `hostname` is mandatory.** A RabbitMQ node's name is `rabbit@<hostname>`, and the node's data is tied to that name. If the container gets a random hostname, after it is recreated it comes up as a "new node" with empty data and doesn't recognise the cluster.

Start the cluster:

```bash
docker compose up -d
docker compose ps
```

Check the cluster:

```bash
docker exec rabbit-1 rabbitmqctl cluster_status
docker exec rabbit-1 rabbitmq-diagnostics check_running
docker exec rabbit-1 rabbitmqctl list_feature_flags name state
```

In `cluster_status` you'll see three nodes under *Disk Nodes* and all three under *Running Nodes*. If a node didn't join, check its logs: `docker logs rabbit-2`.

The management UI is available on every node: http://localhost:15672, http://localhost:15673, http://localhost:15674. The stream protocol is on `localhost:5552`, `5553` and `5554`.

## 2.4 The first queue and the first message

Declare a queue through the HTTP API (the management UI does the same):

```bash
# quorum queue tasks
curl -s -u admin:admin -X PUT http://localhost:15672/api/queues/%2F/tasks \
  -H "content-type: application/json" \
  -d '{"durable": true, "arguments": {"x-queue-type": "quorum"}}'

# publish via the default exchange (routing key = queue name)
curl -s -u admin:admin -X POST http://localhost:15672/api/exchanges/%2F/amq.default/publish \
  -H "content-type: application/json" \
  -d '{"routing_key": "tasks", "payload": "{\"task\":\"resize\",\"image\":\"1.png\"}",
       "payload_encoding": "string", "properties": {"delivery_mode": 2}}'

# fetch a message (for debugging; applications should consume)
curl -s -u admin:admin -X POST http://localhost:15672/api/queues/%2F/tasks/get \
  -H "content-type: application/json" \
  -d '{"count": 1, "ackmode": "ack_requeue_false", "encoding": "auto"}'
```

`%2F` is the encoded name of the vhost `/`.

Look at the queue:

```bash
docker exec rabbit-1 rabbitmqctl list_queues name type messages_ready messages_unacknowledged consumers
docker exec rabbit-1 rabbitmq-queues quorum_status tasks
```

`quorum_status` shows three replicas of the queue on three nodes and which of them is the leader.

**The experiment that separates understanding from "I tried it".** Publish a message to the `amq.direct` exchange with the key `nobody`, for which there is no binding:

```bash
curl -s -u admin:admin -X POST http://localhost:15672/api/exchanges/%2F/amq.direct/publish \
  -H "content-type: application/json" \
  -d '{"routing_key": "nobody", "payload": "lost", "payload_encoding": "string", "properties": {}}'
```

The response is `{"routed": false}`: the broker accepted the message and threw it away. A producer without `mandatory` will never know.

## 2.5 The first producer and consumer in Python

```bash
pip install pika
```

`consumer.py`:

```python
import pika

params = pika.URLParameters("amqp://admin:admin@localhost:5672/%2F")
conn = pika.BlockingConnection(params)
ch = conn.channel()
ch.queue_declare("tasks", durable=True, arguments={"x-queue-type": "quorum"})
ch.basic_qos(prefetch_count=10)

def on_message(ch, method, properties, body):
    print("received:", body.decode(), "redelivered:", method.redelivered)
    ch.basic_ack(delivery_tag=method.delivery_tag)   # acknowledge after processing

ch.basic_consume("tasks", on_message)
print("waiting for messages, Ctrl+C to exit")
ch.start_consuming()
```

`producer.py`:

```python
import pika

conn = pika.BlockingConnection(pika.URLParameters("amqp://admin:admin@localhost:5672/%2F"))
ch = conn.channel()
ch.queue_declare("tasks", durable=True, arguments={"x-queue-type": "quorum"})
ch.confirm_delivery()   # publisher confirms: basic_publish raises if the broker did not accept

for i in range(5):
    ch.basic_publish(
        exchange="",                 # default exchange
        routing_key="tasks",         # = queue name
        body=f'{{"task":"resize","n":{i}}}',
        properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"),
        mandatory=True,              # return it if there is no queue
    )
print("sent 5 messages")
conn.close()
```

Run the consumer in one terminal and the producer in another. Then run **two** consumers and send 10 messages: they are split between them. That's competing consumers, the foundation of task queues.

## 2.6 Where RabbitMQ stores data

```bash
docker exec rabbit-1 ls /var/lib/rabbitmq/mnesia
```

```
/var/lib/rabbitmq/
├── .erlang.cookie
└── mnesia/                         # the data directory, named so for historical reasons
    └── rabbit@rabbit-1/
        ├── coordination/           # Khepri (metadata) and Raft logs
        ├── quorum/                 # quorum queue data
        ├── stream/                 # stream data
        └── msg_stores/             # classic queue data
```

The data directory is called `mnesia` for historical reasons, even though since 4.3 metadata is stored in Khepri. It must live on a persistent volume and be tied to a **stable node name**.

## 2.7 The most common startup mistakes

| Symptom | Cause | Fix |
|---|---|---|
| `ACCESS_REFUSED` for `guest` | `guest` may connect only from localhost | Create your own user (`RABBITMQ_DEFAULT_USER`) |
| Nodes don't form a cluster | Different Erlang cookies or hostnames don't resolve | The same cookie, stable `hostname`s |
| `Cookie file ... must be accessible by owner only` | Permissions on the cookie are too open | `chmod 400`, owned by the `rabbitmq` user |
| The node is empty after recreating the container | The hostname, and so the node name, changed | Set `hostname` explicitly |
| `PRECONDITION_FAILED - inequivalent arg` | The queue already exists with different arguments | Declare it the same way everywhere or delete the queue |
| `PRECONDITION_FAILED` declaring a non-durable queue | Since 4.3 non-durable non-exclusive queues are denied | `durable=True` or `exclusive=True` |
| Publishes "disappear" | No binding matches the routing key | `mandatory=True`, an alternate exchange, check the bindings |
| The cluster is unavailable after two of three nodes die | Khepri needs a quorum (a majority of nodes) | Bring back at least one node |

### Practice

1. Bring up the three-node cluster and check `rabbitmqctl cluster_status`.
2. Create the quorum queue `tasks`, look at `rabbitmq-queues quorum_status tasks` and find the leader.
3. Stop the leader node (`docker stop rabbit-N`) and check the status again: who became leader? Keep publishing and reading.
4. Run two consumers on one queue and send 10 messages. How were they split?

---

# Module 3. Exchanges and routing

## 3.1 Four exchange types

| Type | How it routes | Example |
|---|---|---|
| **direct** | The message's routing key **equals** the binding key | `payment.succeeded` → the queue bound with `payment.succeeded` |
| **fanout** | To **every** bound queue, the key is ignored | An event for all subscribers |
| **topic** | By pattern: `*` is one word, `#` is zero or more words | `order.*.eu`, `order.#` |
| **headers** | By message headers, the key is ignored | `x-match=all`, `format=pdf`, `region=eu` |

```
DIRECT                      FANOUT                       TOPIC
routing key "pay"           any routing key              routing key "order.created.eu"
   |                           |                            |
   +--> [q1] binding "pay"     +--> [q1]                    +--> [q1] "order.*.eu"     ✔
   +-x- [q2] binding "ship"    +--> [q2]                    +--> [q2] "order.#"        ✔
                               +--> [q3]                    +-x- [q3] "order.*"        ✘ (three words)
```

## 3.2 The default exchange

Every vhost has a **default exchange**, a direct exchange with the empty name `""`. Every queue is automatically bound to it by its own name.

```
publish(exchange="", routing_key="tasks")  ==  "put it straight into the tasks queue"
```

It's handy for simple task queues but bad for architecture: the producer starts knowing queue names, and therefore consumers. For events and service integration, publish to a **named exchange**.

## 3.3 The topic exchange: the main tool

A routing key in a topic exchange is words separated by dots. Binding patterns:

| Pattern | Matches | Doesn't match |
|---|---|---|
| `order.created` | `order.created` | `order.created.eu` |
| `order.*` | `order.created`, `order.paid` | `order.created.eu` |
| `order.#` | `order`, `order.created`, `order.created.eu` | `payment.created` |
| `*.created.*` | `order.created.eu` | `order.created` |
| `#.eu` | `order.created.eu`, `eu` | `order.created` |
| `#` | everything | — |

A key template that works:

```
<entity>.<event>[.<region|version>]

order.created
order.paid
order.cancelled
payment.succeeded.eu
stock.reserved
```

Rules:

- words from general to specific: `order.created`, not `created.order`;
- one entity, one prefix: `order.#` catches the whole lifecycle;
- don't put identifiers in the key: you don't need millions of unique keys, routing works on patterns;
- use `#` at the end of a pattern; since 4.3.5 a binding key may contain at most two `#`.

## 3.4 An online shop topology, by example

```bash
API=http://localhost:15672/api
AUTH="-u admin:admin -H content-type:application/json"

# exchange for order events
curl -s $AUTH -X PUT $API/exchanges/%2F/shop.events -d '{"type":"topic","durable":true}'

# service queues
for q in payments stock notifications audit; do
  curl -s $AUTH -X PUT $API/queues/%2F/$q -d '{"durable":true,"arguments":{"x-queue-type":"quorum"}}'
done

# bindings: who needs what
curl -s $AUTH -X POST $API/bindings/%2F/e/shop.events/q/payments      -d '{"routing_key":"order.created"}'
curl -s $AUTH -X POST $API/bindings/%2F/e/shop.events/q/stock         -d '{"routing_key":"order.created"}'
curl -s $AUTH -X POST $API/bindings/%2F/e/shop.events/q/stock         -d '{"routing_key":"order.cancelled"}'
curl -s $AUTH -X POST $API/bindings/%2F/e/shop.events/q/notifications -d '{"routing_key":"*.succeeded"}'
curl -s $AUTH -X POST $API/bindings/%2F/e/shop.events/q/audit         -d '{"routing_key":"#"}'
```

```
                                  +--> [payments]       order.created
publish order.created  ---+       +--> [stock]          order.created, order.cancelled
                          |       |
                    [shop.events] +--> [notifications]  *.succeeded
                       (topic)    |
                                  +--> [audit]          #   (everything)
```

One `order.created` lands in three queues: `payments`, `stock`, `audit`. A new service attaches by adding a queue and a binding, **with no change to the producer**.

## 3.5 Fanout: to every subscriber

```bash
curl -s $AUTH -X PUT $API/exchanges/%2F/cache.invalidate -d '{"type":"fanout","durable":true}'
```

A typical technique: every instance of a service creates **its own exclusive queue** with a server-generated name and binds it to the fanout exchange. Then every instance receives every event (cache invalidation, configuration updates):

```python
q = ch.queue_declare(queue="", exclusive=True)          # the server picks the name: amq.gen-...
ch.queue_bind(exchange="cache.invalidate", queue=q.method.queue)
```

An exclusive queue disappears with its connection: a crashed instance leaves no garbage behind.

## 3.6 The headers exchange

Routing by headers instead of a key:

```bash
curl -s $AUTH -X PUT $API/exchanges/%2F/reports -d '{"type":"headers","durable":true}'
curl -s $AUTH -X POST $API/bindings/%2F/e/reports/q/pdf-eu \
  -d '{"routing_key":"","arguments":{"x-match":"all","format":"pdf","region":"eu"}}'
```

`x-match=all` means all headers must match, `any` means at least one. The headers exchange is slower and rarely used: a topic exchange with a well-designed key almost always does the job.

## 3.7 Alternate exchange: don't lose unrouted messages

If a message matched no binding, instead of dropping it you can send it to an **alternate exchange**:

```bash
curl -s $AUTH -X PUT $API/exchanges/%2F/shop.unrouted -d '{"type":"fanout","durable":true}'
curl -s $AUTH -X PUT $API/queues/%2F/unrouted -d '{"durable":true,"arguments":{"x-queue-type":"quorum"}}'
curl -s $AUTH -X POST $API/bindings/%2F/e/shop.unrouted/q/unrouted -d '{"routing_key":""}'

# policy: give every exchange prefixed shop. an alternate exchange
docker exec rabbit-1 rabbitmqctl set_policy AE '^shop\.' \
  '{"alternate-exchange":"shop.unrouted"}' --apply-to exchanges
```

Now a message with a typo in its key lands in the `unrouted` queue, where monitoring will see it, instead of vanishing.

## 3.8 Exchange-to-exchange bindings

An exchange can be bound to another exchange. That lets you build a hierarchy: a shared entry exchange and separate team exchanges that subscribe to the keys they need.

```bash
curl -s $AUTH -X PUT $API/exchanges/%2F/billing.in -d '{"type":"topic","durable":true}'
curl -s $AUTH -X POST $API/bindings/%2F/e/shop.events/e/billing.in -d '{"routing_key":"payment.#"}'
```

The billing team manages its exchange `billing.in` and its queues without touching the shared `shop.events`.

## 3.9 Built-in exchanges

| Exchange | Type |
|---|---|
| `""` (default) | direct, bound to every queue by name |
| `amq.direct` | direct |
| `amq.fanout` | fanout |
| `amq.topic` | topic (the MQTT plugin uses it too) |
| `amq.headers`, `amq.match` | headers |

For your own systems create your own exchanges with clear names: built-in ones can't be deleted and are awkward to separate by permissions.

### Practice

1. Build the topology from 3.4 and publish `order.created`, `order.cancelled`, `payment.succeeded`. Check which queues each message reached.
2. Publish a message with the key `ordr.created` (a typo) without and with an alternate exchange. Where does it end up?
3. Design routing keys for a delivery service: orders, couriers, statuses, locations. Which bindings will notifications and analytics need?

---

# Module 4. Queues: classic, quorum and streams

## 4.1 Which queue type to choose

| | **Classic** | **Quorum** | **Stream** |
|---|---|---|---|
| Replication | No, the queue lives on one node | Yes, Raft (usually 3 replicas) | Yes, log replication |
| Survives a node failure | No: unavailable until the node returns | Yes, while a quorum is alive | Yes, while a quorum is alive |
| Message after ack | Deleted | Deleted | **Stays** until retention |
| Replay | No | No | Yes, from any offset |
| Redelivery limit | No | Yes, `delivery-limit` (20 by default) | Not applicable |
| Exclusive / non-durable | Yes | No | No |
| Priorities | Yes (`x-max-priority`) | Yes | No |
| When | Temporary, exclusive queues, RPC replies | Everything that matters | History, many readers, large volumes |

**Default rule:** important data — **quorum**; temporary replies and exclusive queues — **classic**; replay and fan-out to many readers — **stream**.

## 4.2 Classic queues

A classic queue stores messages on one node: in memory, with paging to disk (since 4.0 only the second storage version, CQv2, is used: the first, CQv1, was removed except for the migration code; `x-queue-mode=lazy` has done nothing since 3.12, and since 4.3 declaring a queue with `x-queue-mode` or `x-queue-version=1` fails).

What to know:

- if the queue's node fails, the queue is **unavailable** until it returns; persistent messages in a durable queue survive a node restart, but not the loss of its disk;
- classic mirrored queues were **removed in 4.0**, so classic queues are no longer replicated at all;
- they're good for temporary queues: exclusive, with TTL, for RPC replies.

## 4.3 Quorum queues

A quorum queue is a replicated queue based on the **Raft** protocol:

```
            quorum queue "payments" (3 replicas)
   rabbit-1               rabbit-2               rabbit-3
   [LEADER]  ---------->  [follower]  ------->  [follower]
      ^
      | publish/consume go through the leader,
      | a write is confirmed once a majority (2 of 3) has stored it
```

- All operations go through the **leader**; clients can be connected to any node, and the broker forwards the work.
- A message is confirmed to the publisher once a **majority** of replicas has written it to disk.
- If the leader fails, the replicas elect a new one within seconds. The queue is available while a majority is alive: 2 of 3, 3 of 5.
- The group size is set at declaration (`x-quorum-initial-group-size`, 3 by default, or fewer if there are fewer nodes).

Useful built-in features:

| Feature | What it gives you |
|---|---|
| `delivery-limit` | After N failed deliveries the message goes to the DLX or is dropped. Since 4.0 the default is **20**: protection against an endless loop of "poison" messages |
| `x-delivery-count` header | How many deliveries failed (`reject`, a lost connection). Since 4.3 `nack` doesn't increment it; `x-acquired-count` counts every hand-out to a consumer |
| At-least-once dead lettering | Reliable forwarding to the DLX without losses (module 8) |
| Priorities | Since 4.3, strict, 32 levels (0–31), with correct redelivery ordering; 4.0–4.2 had only two levels: normal (0–4) and high (5+) |
| Delayed retry (4.3) | A returned message waits before redelivery, the delay grows with the number of attempts (module 8) |
| Consumer timeout (4.3) | Messages a consumer holds without ack for too long return to the queue |

What quorum queues **can't** do: be exclusive or non-durable, or work with `global` prefetch. For temporary queues use classic.

```python
ch.queue_declare("payments", durable=True, arguments={
    "x-queue-type": "quorum",
    "x-delivery-limit": 5,                  # at most 5 deliveries of one message
    "x-dead-letter-exchange": "shop.dlx",   # where exhausted messages go
})
```

## 4.4 Streams

A stream is a **log** inside RabbitMQ: messages are appended at the end and aren't deleted after reading. Every consumer reads from its own position.

```
stream "shop.events.log"
  offset: 0   1   2   3   4   5   6   7 ...
                  ^           ^           ^
           analytics       audit       new events
           (re-reading)  (catching up)
```

```python
ch.queue_declare("shop.events.log", durable=True, arguments={
    "x-queue-type": "stream",
    "x-max-age": "7D",                          # keep for 7 days
    "x-stream-max-segment-size-bytes": 100_000_000,
})

ch.basic_qos(prefetch_count=100)                # prefetch is mandatory for streams
ch.basic_consume("shop.events.log", on_message,
                 arguments={"x-stream-offset": "first"})   # first | last | next | number | timestamp
```

Streams are covered in module 10. In short: they give replay, fan-out to hundreds of readers without copying messages into hundreds of queues, and huge throughput through a dedicated stream protocol (port 5552).

## 4.5 Queue arguments

| Argument | Types | Meaning |
|---|---|---|
| `x-queue-type` | all | `classic`, `quorum`, `stream` |
| `x-message-ttl` | classic, quorum | Message lifetime in the queue, ms |
| `x-expires` | classic, quorum | Delete the queue if unused for N ms |
| `x-max-length` | classic, quorum | Maximum number of messages |
| `x-max-length-bytes` | all | Maximum bytes (for a stream: size-based retention) |
| `x-overflow` | classic, quorum | What to do on overflow: `drop-head` (default), `reject-publish`, `reject-publish-dlx` (classic only) |
| `x-dead-letter-exchange`, `x-dead-letter-routing-key` | classic, quorum | Where "dead" messages go |
| `x-delivery-limit` | quorum | Delivery limit |
| `x-single-active-consumer` | classic, quorum | Only one active consumer (module 6.8) |
| `x-max-priority` | classic | Number of priority levels |
| `x-quorum-initial-group-size` | quorum | Number of replicas |
| `x-max-age` | stream | Time-based retention (`7D`, `12h`) |

**The `drop-head` trap:** on overflow a queue by default **silently deletes the oldest messages**. For work you can't lose, set `x-overflow=reject-publish`: then a publisher with confirms gets a `nack` and learns about the problem.

Classic queues enforce the limit exactly. For quorum queues the limit is **soft**: the queue may accept one or two messages over the limit before it starts answering `nack` (the tests in `examples/` check exactly this).

## 4.6 Policies instead of arguments

Arguments are set at declaration and **can't change**: re-declaring with different arguments returns `PRECONDITION_FAILED`. So TTLs, limits and DLX are better set with **policies**: they can be changed on the fly without recreating queues.

```bash
docker exec rabbit-1 rabbitmqctl set_policy shop-limits '^shop\.' \
  '{"max-length": 100000, "overflow": "reject-publish", "dead-letter-exchange": "shop.dlx", "delivery-limit": 10}' \
  --apply-to queues --priority 10

docker exec rabbit-1 rabbitmqctl list_policies
```

- A policy applies to every queue whose name matches the regular expression.
- A queue gets **one** policy, the one with the highest priority (plus an operator policy from the administrator).
- If a value is set both as an argument and by a policy, for most keys **the argument wins**.
- `x-queue-type` is set only as an argument: a policy can't change a queue's type.

The rule: in code declare only the queue type and what the application can't work without; keep limits, TTL and DLX in policies.

## 4.7 A default queue type per vhost

To stop developers from creating classic queues by accident, set a default type on the vhost:

```bash
docker exec rabbit-1 rabbitmqctl add_vhost shop --default-queue-type quorum
```

Queues declared in `shop` without `x-queue-type` become quorum (except exclusive ones, which stay classic).

### Self-check questions

1. Why, after the removal of classic mirrored queues, does important data need quorum queues specifically?
2. What happens to a quorum queue when one node of three fails? Two?
3. Why do quorum queues need `delivery-limit`, and what is its default?
4. Why is `x-overflow=drop-head` dangerous?
5. Why are TTLs and limits better set with policies than with arguments?

---

# Module 5. Publisher: persistent messages, confirms and mandatory

## 5.1 Three questions a publisher must answer

```
publish(message)
   |
   +-- 1. Will the message survive a broker restart?     -> durable queue + persistent message
   +-- 2. Did the broker accept the message?             -> publisher confirms
   +-- 3. Did the message reach at least one queue?      -> mandatory + basic.return
```

`basic.publish` in AMQP 0-9-1 is **asynchronous with no reply**: the client wrote bytes to the socket, and that's it. Without confirms the producer doesn't know whether the message arrived, whether the broker stored it, or whether it threw it away for lack of a queue.

## 5.2 Persistent messages

| Queue | Message | After a broker restart |
|---|---|---|
| durable classic | `delivery_mode=2` | The message is there |
| durable classic | `delivery_mode=1` | The queue exists, the message is **lost** |
| quorum / stream | any | The message is there (always on disk) |
| exclusive / auto-delete | any | The queue is gone |

For quorum queues and streams `delivery_mode` doesn't affect storage, but setting `delivery_mode=2` is still worthwhile: the message may pass through a DLX or a shovel into a classic queue.

## 5.3 Publisher confirms

The channel is switched to confirm mode (`confirm.select`), and the broker answers every publish:

```
producer                          broker
   |-- publish (seq 1) -------------->|
   |-- publish (seq 2) -------------->|
   |-- publish (seq 3) -------------->|
   |<-------------- basic.ack (seq 2, multiple=true)   # 1 and 2 accepted
   |<-------------- basic.nack (seq 3)                 # 3 not accepted: retry or alert
```

When the broker sends the `ack`:

| Queue | The confirm arrives when… |
|---|---|
| Quorum | A majority of replicas has written the message |
| Stream | A majority of replicas has written the message |
| Classic durable + persistent | The message is written to disk (or delivered and acked by a consumer) |
| No matching queue | Immediately (preceded by `basic.return` if `mandatory=true`) |

A `nack` arrives if the broker couldn't accept the message: for example, a queue with `x-overflow=reject-publish` is full. If a quorum queue has lost its quorum of replicas, usually neither an `ack` nor a `nack` arrives: the confirm waits for a leader to be elected (sometimes the broker answers with a `nack`). So always put a timeout on waiting for a confirm.

**The main rule:** consider a message sent only after the `ack`. If a `nack` arrived or waiting timed out, resend (with the same `message_id`) or save the message to an outbox (module 7).

## 5.4 Mandatory and returns

```
publish(mandatory=true, routing_key="ordr.created")   # a typo
   -> no queue matched
   -> the broker sends basic.return (NO_ROUTE) back to the producer
   -> then basic.ack (the message was "handled" by the broker)
```

Without `mandatory` the message silently disappears. With `mandatory` the producer learns about the problem, but **only if it listens for returns**. The broker-side alternative is an alternate exchange (module 3.7).

## 5.5 Publisher in Python (pika)

```python
import json
import uuid

import pika
from pika.exceptions import NackError, UnroutableError

conn = pika.BlockingConnection(pika.URLParameters("amqp://admin:admin@localhost:5672/%2F"))
ch = conn.channel()
ch.exchange_declare("shop.events", exchange_type="topic", durable=True)
ch.confirm_delivery()          # every basic_publish waits for the broker's ack

event = {"order_id": "order-1", "amount": 4990}
try:
    ch.basic_publish(
        exchange="shop.events",
        routing_key="order.created",
        body=json.dumps(event),
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
            message_id=str(uuid.uuid4()),        # a stable event id for deduplication
            headers={"event-type": "OrderCreated"},
        ),
        mandatory=True,
    )
    print("accepted by the broker")
except UnroutableError:
    print("no queue is bound for this key")     # alert: the topology is broken
except NackError:
    print("the broker did not accept the message")               # retry or outbox
finally:
    conn.close()
```

`BlockingConnection` with `confirm_delivery()` waits for a confirm on **every** message: simple and reliable, but slow. For high throughput you need asynchronous confirms (as in Go and Java below) or an asynchronous client (`aio-pika`).

## 5.6 Publisher in Go (amqp091-go)

```bash
go get github.com/rabbitmq/amqp091-go
```

```go
package main

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp.Dial("amqp://admin:admin@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer ch.Close()

	if err := ch.ExchangeDeclare("shop.events", "topic", true, false, false, false, nil); err != nil {
		log.Fatal(err)
	}
	if err := ch.Confirm(false); err != nil { // enable publisher confirms
		log.Fatal(err)
	}
	returns := ch.NotifyReturn(make(chan amqp.Return, 16)) // returns for mandatory

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dc, err := ch.PublishWithDeferredConfirmWithContext(ctx,
		"shop.events", "order.created",
		true,  // mandatory
		false, // immediate (unsupported, always false)
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			MessageId:    "order-1-created",
			Body:         []byte(`{"order_id":"order-1","amount":4990}`),
		})
	if err != nil {
		log.Fatal(err)
	}

	acked, err := dc.WaitContext(ctx) // wait for the broker's ack/nack
	if err != nil || !acked {
		log.Fatalf("not confirmed: acked=%v err=%v", acked, err) // retry or outbox
	}
	select {
	case r := <-returns:
		log.Fatalf("returned: %s (%d), key %s", r.ReplyText, r.ReplyCode, r.RoutingKey)
	default:
		log.Println("accepted by the broker and routed to a queue")
	}
}
```

For high throughput, publish a batch of messages collecting `DeferredConfirmation`s, then wait for all of them. Returns arrive asynchronously and before the ack, so in production they are read in a separate goroutine. An example with a batch and re-sending unconfirmed messages after a dropped connection is in [`examples/go/cmd/publisher`](examples/go/cmd/publisher/main.go).

## 5.7 Publisher in Java

```xml
<dependency>
  <groupId>com.rabbitmq</groupId>
  <artifactId>amqp-client</artifactId>
  <version>5.25.0</version> <!-- use the current version -->
</dependency>
```

```java
import com.rabbitmq.client.*;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class OrderPublisher {
    public static void main(String[] args) throws Exception {
        ConnectionFactory f = new ConnectionFactory();
        f.setUri("amqp://admin:admin@localhost:5672/%2F");
        f.setAutomaticRecoveryEnabled(true);          // the Java client can reconnect by itself

        try (Connection conn = f.newConnection("order-service");
             Channel ch = conn.createChannel()) {
            ch.exchangeDeclare("shop.events", BuiltinExchangeType.TOPIC, true);
            ch.confirmSelect();

            // asynchronous confirms: keep unconfirmed messages by sequence number
            ConcurrentNavigableMap<Long, String> outstanding = new ConcurrentSkipListMap<>();
            ch.addConfirmListener(
                (seq, multiple) -> clear(outstanding, seq, multiple),               // ack
                (seq, multiple) -> {                                                // nack
                    System.err.println("nack up to " + seq + ": retry or outbox");
                    clear(outstanding, seq, multiple);
                });
            ch.addReturnListener(r ->
                System.err.println("returned: " + r.getReplyText() + " key " + r.getRoutingKey()));

            for (int i = 1; i <= 100; i++) {
                String body = "{\"order_id\":\"order-" + i + "\"}";
                outstanding.put(ch.getNextPublishSeqNo(), body);
                ch.basicPublish("shop.events", "order.created", true,
                    new AMQP.BasicProperties.Builder()
                        .deliveryMode(2)
                        .contentType("application/json")
                        .messageId("order-" + i + "-created")
                        .headers(Map.of("event-type", "OrderCreated"))
                        .build(),
                    body.getBytes(StandardCharsets.UTF_8));
            }
            ch.waitForConfirmsOrDie(10_000);   // wait for all confirms before exiting
        }
    }

    static void clear(ConcurrentNavigableMap<Long, String> m, long seq, boolean multiple) {
        if (multiple) m.headMap(seq, true).clear(); else m.remove(seq);
    }
}
```

## 5.8 Flow control and blocked connections

When the broker runs short of memory or disk, an **alarm** fires (module 14): the broker **blocks every publishing connection** in the cluster. Publishers hang on `publish`, while consumers keep working and drain the queues.

```
memory_high_watermark exceeded (or free disk below disk_free_limit)
   -> connection.blocked to every publisher
   -> publishing stops, consumers keep working
   -> memory freed -> connection.unblocked
```

What follows:

- **separate connections** for publishing and consuming: otherwise a blocked connection also delays the consumer's acks;
- subscribe to `connection.blocked`/`unblocked` (`NotifyBlocked` in Go, `BlockedListener` in Java) and expose a metric: a blocked publisher is the first sign of trouble;
- timeouts on publishing, or your service's HTTP requests will hang together with the broker.

## 5.9 What to do when a publish fails

```
publish not confirmed
   |
   +-- nack                        -> queue full (reject-publish) or unavailable: retry with backoff, alert
   +-- confirm wait timed out      -> connection or broker trouble: reconnect and retry
   +-- basic.return (NO_ROUTE)     -> broken topology: don't retry forever, alert
   +-- channel closed with error   -> protocol error (no exchange, no permission): fix the code or permissions
```

A resend can create a **duplicate**: the broker may have accepted the message while the confirm got lost. So every event must have a stable `message_id`, and consumers must be idempotent (module 7). Queues in RabbitMQ have no built-in publish deduplication (streams do, via the stream protocol, module 10).

### Self-check questions

1. Why doesn't a durable queue save a message with `delivery_mode=1`?
2. When does the broker send a confirm for a quorum queue?
3. What happens to a message without `mandatory` when there's no matching queue?
4. Why keep separate connections for publishing and consuming?
5. Why can a resend after a confirm timeout create a duplicate?

---

# Module 6. Consumer: ack, prefetch and redelivery

## 6.1 How the broker delivers messages

```
queue [m1][m2][m3][m4][m5][m6] ...
            |
            | basic.consume: the broker pushes messages itself,
            | but no more than prefetch unacked per consumer
            v
   consumer A: m1, m3, m5  (unacked)      consumer B: m2, m4, m6  (unacked)
            |                                   |
         ack m1  -> m1 deleted              crashes -> m2, m4, m6 return to the queue
```

- **basic.consume** (push) is the right way: the broker delivers messages as they arrive.
- **basic.get** (pull a single message) is for debugging only: every message is a separate network request, which is slow and loads the broker.

## 6.2 Acknowledgement modes

| Mode | How | Guarantee |
|---|---|---|
| **auto ack** (`auto_ack=True`) | The broker considers the message acknowledged as soon as it's sent | At-most-once: the consumer crashes, the message is lost |
| **manual ack** | The consumer explicitly acknowledges after processing | At-least-once: a crash before ack means redelivery |

You almost always want **manual ack**. Auto ack is acceptable for data you can afford to lose (metrics, logs), and it's dangerous for another reason too: the broker sends messages with no prefetch limit, so a fast broker can flood a slow consumer and exhaust its memory.

## 6.3 ack, nack, reject

| Reply | Effect | When |
|---|---|---|
| `basic.ack` | Processed, delete it | Success |
| `basic.reject(requeue=true)` / `basic.nack(requeue=true)` | Return it to the queue | A transient error, but carefully, see below |
| `basic.nack(requeue=false)` / `basic.reject(requeue=false)` | Drop it or send it to the DLX | The message is invalid |
| nothing, the consumer crashed | All its unacked messages return to the queue | |

The `multiple=true` flag acknowledges every message up to the given `delivery_tag` at once: handy for batches.

**The `requeue=true` trap:** the message returns **to the head of the queue** and is delivered again immediately. A "poison" message that always fails becomes an endless CPU-eating loop. In quorum queues `delivery-limit` protects you (20 by default): after the limit the message goes to the DLX or is dropped. But since 4.3 the limit counts only **failed** deliveries: `basic.reject(requeue=true)` and a lost connection increment `x-delivery-count`, while `basic.nack(requeue=true)` doesn't, so such a loop can spin forever. So handle a transient error with `reject(requeue=true)`. Proper delayed retries are in module 8.

**Important:** a `delivery_tag` is unique **within a channel**. A message must be acknowledged on the same channel it was received on. If the channel closed, acking by the old tag is impossible, and the message will be delivered again.

## 6.4 Prefetch (QoS)

`basic.qos(prefetch_count=N)` is how many **unacknowledged** messages the broker may keep with one consumer.

```
prefetch=1      the broker waits for an ack before sending the next: even, but slow (a round trip per message)
prefetch=10-50  a good start for most workloads
prefetch=500+   fast consumers and small messages
no limit        dangerous: the whole queue ends up in one consumer's memory
```

How to choose:

- long processing time (hundreds of ms and more): a small prefetch (1–10), or one consumer "grabs" a batch while the others sit idle;
- fast processing, the network is the bottleneck: a larger prefetch (100–300);
- prefetch is mandatory for streams.

Prefetch is set **per consumer** (`global=false`). The `global=true` mode (a shared limit per channel) is denied by default since 4.3.

## 6.5 Consumer in Python (pika)

```python
import json
import pika

conn = pika.BlockingConnection(pika.URLParameters("amqp://admin:admin@localhost:5672/%2F"))
ch = conn.channel()
ch.queue_declare("payments", durable=True, arguments={"x-queue-type": "quorum"})
ch.basic_qos(prefetch_count=20)

class TemporaryError(Exception):
    pass

def on_message(ch, method, props, body):
    deliveries = (props.headers or {}).get("x-delivery-count", 0)   # quorum queues
    try:
        event = json.loads(body)
        charge(event)                                   # must be idempotent
        ch.basic_ack(method.delivery_tag)
    except json.JSONDecodeError:
        ch.basic_reject(method.delivery_tag, requeue=False)   # garbage: to the DLX
    except TemporaryError:
        ch.basic_reject(method.delivery_tag, requeue=True)    # try again (reject counts toward delivery-limit)

ch.basic_consume("payments", on_message)
try:
    ch.start_consuming()
except KeyboardInterrupt:
    ch.stop_consuming()
conn.close()
```

`BlockingConnection` is single-threaded: while `on_message` runs, pika doesn't serve heartbeats. If processing a message takes longer than the heartbeat timeout (60 s by default), the broker closes the connection. Move long work to a separate thread and acknowledge via `conn.add_callback_threadsafe`, or use an asynchronous client.

## 6.6 Consumer in Go (amqp091-go)

```go
conn, err := amqp.Dial("amqp://admin:admin@localhost:5672/")
if err != nil {
	log.Fatal(err)
}
defer conn.Close()
ch, err := conn.Channel()
if err != nil {
	log.Fatal(err)
}
defer ch.Close()

if err := ch.Qos(20, 0, false); err != nil { // prefetch 20 per consumer
	log.Fatal(err)
}
msgs, err := ch.Consume("payments", "billing-1",
	false, // autoAck: off
	false, false, false, nil)
if err != nil {
	log.Fatal(err)
}

for d := range msgs { // closes when the connection or AMQP channel closes
	count, _ := d.Headers["x-delivery-count"].(int64)
	if err := process(d.Body); err != nil {
		if isTemporary(err) {
			d.Reject(true) // requeue; unlike Nack, it counts toward delivery-limit
		} else {
			d.Reject(false) // to the DLX
		}
		log.Printf("error (delivery %d): %v", count, err)
		continue
	}
	d.Ack(false)
}
log.Println("delivery channel closed: reconnect needed")
```

By default `amqp091-go` **does not reconnect by itself**. When the connection drops, the `msgs` channel closes; the application must subscribe to `conn.NotifyClose` and `ch.NotifyCancel` (the broker cancels the subscription if the queue is deleted or its node goes away), wait with an exponentially growing pause, then reopen the connection and channel, declare the topology and subscribe again. A complete example with such a loop and a clean SIGTERM shutdown (`ch.Cancel`, finish and ack the deliveries already received, close the channel) is in [`examples/go/cmd/consumer`](examples/go/cmd/consumer/main.go). Since v1.12 the library also has built-in recovery that you enable explicitly: `amqp.DialConfig(url, amqp.Config{Recovery: &amqp.Recovery{}})`. It restores the connection, channels, topology and subscriptions, but publisher confirms still outstanding when the connection dropped complete as not acked: those messages still have to be re-sent.

## 6.7 Consumer in Java

```java
ConnectionFactory f = new ConnectionFactory();
f.setUri("amqp://admin:admin@localhost:5672/%2F");
f.setAutomaticRecoveryEnabled(true);           // restores the connection, channels and subscriptions
Connection conn = f.newConnection("billing");
Channel ch = conn.createChannel();
ch.basicQos(20);

ch.basicConsume("payments", false, "billing-1", new DefaultConsumer(ch) {
    @Override
    public void handleDelivery(String tag, Envelope env, AMQP.BasicProperties props, byte[] body)
            throws IOException {
        long deliveryTag = env.getDeliveryTag();
        try {
            charge(body);                                     // idempotent
            getChannel().basicAck(deliveryTag, false);
        } catch (InvalidMessageException e) {
            getChannel().basicReject(deliveryTag, false);     // to the DLX
        } catch (Exception e) {
            getChannel().basicReject(deliveryTag, true);      // requeue (counts toward delivery-limit)
        }
    }
});
```

## 6.8 Ordering, single active consumer and consumer priorities

**Ordering.** A queue hands out messages in order, but with several consumers they are processed **in parallel**, and after a `nack` or a consumer crash a message returns and may be processed after later ones. If order matters:

- **single active consumer** (`x-single-active-consumer=true`): many consumers, but only one receives messages; if it dies, the broker switches to the next. Order is preserved and there's a hot standby;
- **several queues by key**: one order's events always go to one queue (the consistent hash exchange plugin or super streams, module 10).

**Consumer priorities** (`x-priority` in the `basic.consume` arguments): the broker hands messages to the highest-priority consumers while they have room in their prefetch, and only then to the others. Useful when you have fast "primary" workers and slow standby ones.

## 6.9 Consumer timeout

If a consumer holds a message without ack for longer than `consumer_timeout` (30 minutes by default), the broker returns the messages to the queue. Since 4.3 the timeout is evaluated by **the quorum queue itself**: it no longer applies to classic queues and streams. For quorum queues it can be set with the `x-consumer-timeout` argument, the `consumer-timeout` policy key, or globally in `rabbitmq.conf`.

If processing legitimately takes hours, that's a signal to rethink the design (split the task or keep progress outside the broker), not just to raise the timeout.

## 6.10 Scaling consumption

```
queue payments
  1 consumer     -> it gets every message
  5 consumers    -> messages are spread round-robin, respecting prefetch
  50 consumers on one queue -> the queue (one Erlang process, one leader) may become the bottleneck
```

Unlike Kafka, the number of consumers on a queue is **not bounded by a partition count**: add workers while the queue keeps up. A single queue is one process on the leader, and its limit is tens of thousands of messages per second. If you need more, shard: several queues with routing by key (consistent hash exchange) or super streams.

### Self-check questions

1. Why is auto ack at-most-once?
2. Why is `requeue=true` dangerous for a "poison" message, what protects against it in quorum queues, and why does it matter in 4.3 whether it's `reject` or `nack`?
3. How do you choose prefetch for slow and for fast processing?
4. How do you preserve processing order with several consumers?
5. Why can't a `delivery_tag` be acknowledged on another channel?

---

# Module 7. Delivery guarantees, idempotency and outbox

## 7.1 Where losses and duplicates come from

| Scenario | Result | Protection |
|---|---|---|
| No matching queue, `mandatory=false` | Loss | `mandatory`, alternate exchange |
| The publisher didn't wait for a confirm, the broker crashed | Loss | Publisher confirms + retry |
| The confirm got lost, the publisher resent | Duplicate in the queue | `message_id` + idempotent consumer |
| Classic queue, `delivery_mode=1`, restart | Loss | Persistent messages or a quorum queue |
| Classic queue, the node's disk is lost | Loss | Quorum queue |
| Queue full, `overflow=drop-head` | Old messages silently deleted | `reject-publish` + confirms |
| Auto ack, the consumer crashed | Loss | Manual ack |
| The consumer processed, crashed before ack | Reprocessing | Idempotency |
| The consumer closed the channel before ack | Redelivery | Idempotency |
| Crash between the DB write and the publish | Lost event | Transactional outbox |
| Published, then the DB transaction rolled back | Phantom event | Transactional outbox |

## 7.2 The three semantics

```
AT-MOST-ONCE
  auto ack, no confirms
  something fails -> the message is lost

AT-LEAST-ONCE (the standard)
  confirms + manual ack after processing
  something fails -> redelivery, a duplicate is possible

EXACTLY-ONCE
  not a broker property in RabbitMQ;
  achieved as at-least-once + idempotent processing
```

**Honestly:** RabbitMQ doesn't promise exactly-once. "Exactly one effect" comes from the broker guaranteeing delivery at least once and the application making reprocessing harmless.

## 7.3 The idempotent consumer

```sql
CREATE TABLE processed_messages (
    message_id   TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

```python
def on_message(ch, method, props, body):
    with db.transaction() as tx:
        inserted = tx.execute(
            "INSERT INTO processed_messages (message_id) VALUES (%s) ON CONFLICT DO NOTHING",
            (props.message_id,),
        ).rowcount
        if inserted:                    # 0 rows -> already processed, just acknowledge
            charge(tx, json.loads(body))
    ch.basic_ack(method.delivery_tag)   # ack only AFTER a successful COMMIT
```

Rules:

- the `message_id` is generated **once per business event** and doesn't change on resend;
- ack **after** the database commit. The other way round is at-most-once;
- the table of processed ids is cleaned periodically (for example, older than the window in which a resend is possible);
- alternatives: an `UPSERT` on a natural key, a conditional update on a version column (`WHERE version = $expected`).

Classic and quorum queues have no built-in deduplication. A third-party header-based deduplication plugin exists, but consumer idempotency is more reliable and doesn't depend on the broker.

## 7.4 AMQP transactions: why not

AMQP 0-9-1 has channel transactions (`tx.select`, `tx.commit`). They atomically commit publishes and acks **inside the broker**, but:

- they're very slow (a synchronous round trip on every commit);
- they don't extend to the application's database;
- they don't give exactly-once.

Use publisher confirms instead of transactions. `tx.*` shows up only in old code.

## 7.5 Transactional outbox

**The dual-write problem:**

```
1. INSERT INTO orders ...           OK
2. basic_publish(order.created)     -> the service crashed
   -> the order exists, the event doesn't
```

**The fix: an outbox table in the same transaction:**

```
+--------------- one database transaction ------------+
| INSERT INTO orders (...)                             |
| INSERT INTO outbox (id, exchange, routing_key, body) |
+------------------------------------------------------+
                |
                v
   relay: reads unsent rows,
   publishes with confirms and message_id = outbox.id,
   marks the row as sent after the ack
                |
                v
         exchange shop.events
```

```sql
CREATE TABLE outbox (
    id           UUID PRIMARY KEY,
    exchange     TEXT NOT NULL,
    routing_key  TEXT NOT NULL,
    body         JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX ON outbox (created_at) WHERE published_at IS NULL;
```

The relay can send an event twice (crash between the broker's ack and marking the row), so `message_id = outbox.id` is mandatory: consumers deduplicate by it. Instead of polling the table you can read the WAL via CDC (Debezium can publish to RabbitMQ through Debezium Server).

## 7.6 Choosing guarantees

| Task | Recommendation |
|---|---|
| Metrics, logs | Auto ack is acceptable, a classic queue |
| Notifications | Quorum queue, confirms, manual ack; a duplicate is harmless or filtered by id |
| Business events | Quorum queue, confirms, `mandatory`, outbox, idempotent consumer |
| Money | All of the above + `processed_messages` + reconciliation |
| Task queue | Quorum queue + `delivery-limit` + DLX |

### Self-check questions

1. Why doesn't RabbitMQ give exactly-once, and how do you get "exactly one effect"?
2. Why must the ack come after the database commit?
3. Why are publisher confirms better than AMQP transactions?
4. Why does the outbox need `message_id = outbox.id`?

---

# Module 8. Dead letter exchanges, TTL and delayed retries

## 8.1 When a message becomes "dead"

A message is sent to a **dead letter exchange** (DLX) if:

| Reason (`x-death` reason) | What happened |
|---|---|
| `rejected` | The consumer did `reject` or `nack` with `requeue=false` |
| `expired` | The message or queue TTL expired |
| `maxlen` | The queue overflowed (`drop-head` or `reject-publish-dlx`) |
| `delivery_limit` | Quorum queue: the delivery limit was exhausted |

Without a DLX such a message is simply deleted.

## 8.2 Setting up a DLX

```bash
API=http://localhost:15672/api
AUTH="-u admin:admin -H content-type:application/json"

curl -s $AUTH -X PUT $API/exchanges/%2F/shop.dlx -d '{"type":"topic","durable":true}'
curl -s $AUTH -X PUT $API/queues/%2F/shop.dead -d '{"durable":true,"arguments":{"x-queue-type":"quorum"}}'
curl -s $AUTH -X POST $API/bindings/%2F/e/shop.dlx/q/shop.dead -d '{"routing_key":"#"}'

# policy: the payments, stock and notifications queues dead-letter into shop.dlx
docker exec rabbit-1 rabbitmqctl set_policy shop-dlx '^(payments|stock|notifications)$' \
  '{"dead-letter-exchange":"shop.dlx","delivery-limit":5}' --apply-to queues
```

By default a message goes to the DLX with its **original routing key**. You can set another one with `dead-letter-routing-key`.

## 8.3 The x-death header

The broker writes the history into the dead message's headers:

```
x-first-death-reason:   rejected
x-first-death-queue:    payments
x-first-death-exchange: shop.events
x-death: [
  {reason: rejected, queue: payments, exchange: shop.events,
   routing-keys: [order.created], count: 1, time: ...}
]
```

`x-death` shows where the message came from and how many times it died in each queue. For quorum queues the delivery count is also in `x-delivery-count`.

## 8.4 Dead lettering in quorum queues: at-most-once and at-least-once

By default forwarding to the DLX is **at-most-once**: if the target queue is unavailable, the dead message is lost. For reliable forwarding:

```bash
docker exec rabbit-1 rabbitmqctl set_policy payments-dlx '^payments$' \
  '{"dead-letter-exchange":"shop.dlx","dead-letter-strategy":"at-least-once","overflow":"reject-publish"}' \
  --apply-to queues
```

In `at-least-once` mode the quorum queue keeps the message until the DLX queue confirms receipt. The requirement is `overflow=reject-publish` (otherwise overflow would force it to delete messages).

## 8.5 TTL

| Where | How | Quirk |
|---|---|---|
| TTL for a queue's messages | `x-message-ttl` or the `message-ttl` policy | Every message in the queue lives at most N ms |
| TTL of a single message | the `expiration` property (a string, ms) | **Expires only once it reaches the head of the queue** |
| Queue TTL | `x-expires` or the `expires` policy | Delete an unused queue |

**The per-message TTL trap.** A queue (classic and quorum alike) checks expiry only for the message at the head. If a message with a one-hour TTL is in front of one with a one-second TTL, the second waits an hour. So for delay queues use **a queue TTL**, not a per-message one.

## 8.6 Delayed retries with TTL + DLX

The classic pattern: a failed message goes to a waiting queue with no consumers, sits there for N seconds, then returns to the work queue through the DLX.

```
                      nack(requeue=false)
[payments] ─────────────────────────────────> exchange shop.retry
   ^                                               |
   |                                   by key -> [payments.retry.10s]  x-message-ttl=10000
   |                                               |   (no consumers)
   |           TTL expired -> DLX = default         |
   +───────────────────────────────────────────────+
```

```bash
# a waiting queue: TTL 10 seconds, then back to the work queue
curl -s $AUTH -X PUT $API/exchanges/%2F/shop.retry -d '{"type":"direct","durable":true}'
curl -s $AUTH -X PUT $API/queues/%2F/payments.retry.10s -d '{"durable":true,"arguments":{
  "x-queue-type":"quorum",
  "x-message-ttl":10000,
  "x-dead-letter-exchange":"",
  "x-dead-letter-routing-key":"payments"}}'
curl -s $AUTH -X POST $API/bindings/%2F/e/shop.retry/q/payments.retry.10s -d '{"routing_key":"payments"}'
```

The consumer itself decides where a failed message goes: to the retry queue (publishing a copy and acking the original) or to the final DLQ if there have been too many attempts. For several stages (10 s, 1 min, 10 min) you create several retry queues.

```python
MAX_ATTEMPTS = 5

def on_message(ch, method, props, body):
    headers = props.headers or {}
    attempt = headers.get("x-attempt", 0)
    try:
        process(body)
        ch.basic_ack(method.delivery_tag)
    except TemporaryError:
        target = "shop.retry" if attempt < MAX_ATTEMPTS else "shop.dlx"
        # publish the copy reliably first (confirm-mode channel), then ack the original
        ch.basic_publish(target, "payments", body, pika.BasicProperties(
            delivery_mode=2, message_id=props.message_id,
            headers={**headers, "x-attempt": attempt + 1}))
        ch.basic_ack(method.delivery_tag)
    except ValueError:
        ch.basic_reject(method.delivery_tag, requeue=False)   # straight to the DLX
```

The order matters: **publish the copy with a confirm first, then ack the original**. Otherwise a crash between the two steps loses the message.

## 8.7 Delayed retry in quorum queues (4.3)

In RabbitMQ 4.3 quorum queues can delay redelivery by themselves, with no extra queues. A returned message (`reject`, `nack`) waits before redelivery, and the delay grows with the number of attempts:

```
delay = min(delayed-retry-min × delivery_count, delayed-retry-max)
```

```bash
docker exec rabbit-1 rabbitmqctl set_policy payments-retry '^payments$' \
  '{"delayed-retry-type":"failed","delayed-retry-min":1000,"delayed-retry-max":60000,
    "delivery-limit":10,"dead-letter-exchange":"shop.dlx"}' --apply-to queues
```

- `delayed-retry-type`: `disabled`, `all`, `failed` or `returned`: which returns to delay: `failed` means those with an incremented `delivery-count` (`reject`, a lost connection), `returned` those without (`nack`);
- combined with `delivery-limit` and a DLX, it gives a complete "retries with growing delay, then DLQ" scheme with a single queue;
- delays are in milliseconds; the same can be set with the queue arguments `x-delayed-retry-type`, `x-delayed-retry-min`, `x-delayed-retry-max`; enable it once every node in the cluster runs 4.3.

## 8.8 The delayed message exchange plugin

The `rabbitmq_delayed_message_exchange` plugin added the `x-delayed-message` exchange type: a message with an `x-delay` header was delivered after the given time. **It doesn't work with 4.3**: the plugin kept delayed messages in Mnesia, which 4.3 removed, and the RabbitMQ team no longer maintains it (the last release targets 4.2). Even on older versions it had limitations:

- delayed messages are stored **on one node and aren't replicated**;
- it scales poorly to millions of delayed messages;
- it's a separate plugin you have to install and upgrade yourself.

For retries use delayed retry in quorum queues (8.7) or TTL + DLX (8.6). For long delays (days) it's often simpler to keep the schedule in a database.

## 8.9 Poison messages and a parking lot

A "poison" message fails every time it's processed. Protection:

1. **`delivery-limit`** in quorum queues (20 by default; 3–10 is better for tasks).
2. **Validation on entry**: if it doesn't parse, `reject(requeue=false)` immediately, no retries.
3. **A DLQ as a parking lot**: dead messages accumulate in a separate queue, with an **alert** on any message in it.
4. **A re-send tool**: after fixing the bug, move messages back (Shovel from module 13 or a script), keeping the `message_id`.

### Practice

1. Set up a DLX for the `payments` queue, send a message and `reject(requeue=false)` it. Look at the `x-death` headers in `shop.dead`.
2. Build a TTL + DLX retry with a 10-second delay and confirm the message doesn't return early.
3. Send a message that always fails to a quorum queue with `delivery-limit=3` and `reject(requeue=true)`. How many times is it delivered, and where does it end up? What changes if you do `nack(requeue=true)` instead of `reject`?

---

# Module 9. Cluster and quorum queues: Raft, Khepri, fault tolerance

## 9.1 What is replicated in a cluster

```
a 3-node cluster
  ├── metadata (vhosts, users, exchanges, bindings, queues, policies)
  │     -> Khepri: a Raft group across all nodes
  ├── quorum queues   -> each has its own Raft group (usually 3 replicas)
  ├── streams         -> each has its own replica group
  └── classic queues  -> NOT replicated, live on one node
```

A client can connect to **any** node: it sees every exchange and queue in the cluster, and the broker forwards operations to the leader node of the queue.

## 9.2 Khepri: metadata on Raft

Since RabbitMQ 4.3 metadata is stored only in **Khepri**, a Raft-based store. Mnesia and its network partition handling strategies (`pause_minority`, `autoheal`) are removed.

In practice:

- the cluster needs **a majority of nodes online**: 2 of 3, 3 of 5. Without a quorum you can't declare queues, change permissions or create bindings;
- recovery after failures and partitions is uniform: metadata, quorum queues and streams recover by Raft rules;
- **an odd number of nodes**: 3 or 5. A second node added to one doesn't add fault tolerance, nor does a fourth added to three.

## 9.3 Cluster size

| Nodes | Survives the loss of | Comment |
|---|---|---|
| 1 | 0 | Development |
| 2 | 0 | **Worse than one**: losing either node loses the quorum |
| 3 | 1 | The production standard |
| 5 | 2 | Large installations, higher write cost |
| 7+ | 3+ | Rarely justified |

## 9.4 Quorum queue leaders

Every quorum queue is a separate Raft group with its own leader. Leaders are spread across nodes, so the load is shared.

```bash
docker exec rabbit-1 rabbitmq-queues quorum_status payments   # replicas, leader, lag
docker exec rabbit-1 rabbitmqctl list_queues name type leader members online
```

Where a new queue's leader is created is controlled by `queue_leader_locator` (`client-local`, on the node the client is connected to, or `balanced`, on the least loaded one). After a node restart leadership becomes skewed; restore balance with

```bash
docker exec rabbit-1 rabbitmq-queues rebalance quorum
```

## 9.5 Managing replicas

```bash
# add replicas of every quorum queue on a new node
docker exec rabbit-1 rabbitmq-queues grow rabbit@rabbit-4 all

# remove replicas from a node before decommissioning it
docker exec rabbit-1 rabbitmq-queues shrink rabbit@rabbit-3

# for a single queue
docker exec rabbit-1 rabbitmq-queues add_member payments rabbit@rabbit-4
docker exec rabbit-1 rabbitmq-queues delete_member payments rabbit@rabbit-3

# is it safe to stop this node now?
docker exec rabbit-1 rabbitmq-queues check_if_node_is_quorum_critical
```

A new replica first catches up with the leader as a **non-voter** and only then gets a vote: adding a replica doesn't weaken the quorum.

`check_if_node_is_quorum_critical` is a mandatory check before stopping a node: if stopping it would make some queue lose its quorum, the command returns an error.

## 9.6 What happens on failures

| Failure | Metadata (Khepri) | Quorum queues | Classic queues |
|---|---|---|---|
| 1 of 3 nodes | Works | Leaders move, queues available | That node's queues unavailable |
| 2 of 3 nodes | **Unavailable** (can't declare or change the topology) | Queues without a quorum unavailable | The failed nodes' queues unavailable |
| Network partition 2 + 1 | The majority side works | The side with a majority of replicas works | Depends on luck |
| A node's disk is lost | The node must rejoin the cluster | Replicas recover from leaders | Data lost |

## 9.7 How clients survive a node failure

- Give the client **several node addresses** or a load balancer address (a TCP load balancer in front of the nodes).
- The client must **reconnect** and re-declare channels and subscriptions. The Java client does it itself (`automatic recovery`), `amqp091-go` only if you enable `Config.Recovery` (since v1.12), pika doesn't: you have to implement it (example: [`examples/go/cmd/consumer`](examples/go/cmd/consumer/main.go)).
- After reconnecting, unacknowledged messages are delivered again: another reason for idempotency.
- Publisher confirms not received before the disconnect are unknown: those messages must be resent.

## 9.8 Node maintenance

```bash
# put the node into maintenance: move leaders away, close client connections
docker exec rabbit-2 rabbitmq-upgrade drain

# ... upgrade, reboot ...

# bring the node back
docker exec rabbit-2 rabbitmq-upgrade revive

# remove the node from the cluster for good (run on another node, the removed one is stopped)
docker exec rabbit-1 rabbitmqctl forget_cluster_node rabbit@rabbit-3
```

### Practice

1. Create 10 quorum queues and see how leaders are distributed. Stop a node, bring it back and run `rebalance quorum`.
2. Check `check_if_node_is_quorum_critical`, then stop two nodes of three. What happens to publishing to a quorum queue and to declaring a new queue?
3. Run `drain` on a node and watch where leaders and connections went.

---

# Module 10. Streams and super streams

## 10.1 Why streams if there are queues

| Task | Queue | Stream |
|---|---|---|
| One event is needed by 50 services | 50 queues, 50 copies of every message | One stream, 50 readers with their own offsets |
| Re-read yesterday's events | Impossible | Move the offset |
| Millions of messages per second | You hit the limit of one queue | Stream protocol, batched writes |
| Keep a week of history | The queue grows and slows down | Time- or size-based retention |

A stream is a log inside RabbitMQ with the same model as a Kafka partition: append at the end, read from an offset, delete by retention.

## 10.2 Creation and retention

```bash
curl -s -u admin:admin -X PUT http://localhost:15672/api/queues/%2F/shop.events.log \
  -H "content-type: application/json" \
  -d '{"durable":true,"arguments":{
        "x-queue-type":"stream",
        "x-max-age":"7D",
        "x-max-length-bytes":20000000000,
        "x-stream-max-segment-size-bytes":500000000}}'
```

| Parameter | Meaning |
|---|---|
| `x-max-age` | Delete segments older than (`30s`, `12h`, `7D`, `1M`) |
| `x-max-length-bytes` | Delete old segments when the size is exceeded |
| `x-stream-max-segment-size-bytes` | Segment size (deletion is by whole segments) |
| `x-initial-cluster-size` | Number of replicas |

As in Kafka, data is deleted **in whole segments**, so messages may live a little longer than configured.

## 10.3 Reading over AMQP 0-9-1

Streams can be read by ordinary AMQP clients:

```python
def on_message(ch, method, props, body):
    offset = props.headers.get("x-stream-offset")   # the message's position in the stream
    handle(body)
    ch.basic_ack(method.delivery_tag)               # ack is needed for flow control; the message is not deleted

ch.basic_qos(prefetch_count=500)                   # mandatory for streams
ch.basic_consume(
    "shop.events.log", on_message,
    arguments={"x-stream-offset": "first"},        # first | last | next | <number> | <timestamp>
)
ch.start_consuming()
```

Over AMQP 0-9-1 the broker **doesn't store the reader's position**: on restart the consumer decides where to read from. Save the last processed offset (in the database next to the result) and subscribe with `x-stream-offset: <saved + 1>`.

## 10.4 The stream protocol

For high throughput streams have a dedicated binary protocol (port **5552**, plugin `rabbitmq_stream`) and dedicated clients: Java, Go, .NET, Python, Rust.

A stream client connects to any node, asks it for the addresses of the leader and replicas, and then connects to them directly. So a node must advertise an address the client can reach: by default that's its hostname (`rabbit-2`), which doesn't resolve on the developer's machine. The cluster from module 2.3 sets `stream.advertised_host = localhost` and a per-node `stream.advertised_port` (5552, 5553, 5554) for this. Clients inside the Docker network or Kubernetes don't need these settings: node names resolve there.

What it offers beyond AMQP:

| Feature | What it is |
|---|---|
| Batched writes and reads | An order of magnitude more throughput |
| **Server-side offset tracking** | A named consumer stores its position on the broker |
| **Publish deduplication** | A named producer with an increasing `publishing id`: the broker drops repeats |
| Single active consumer | One active reader with automatic failover |
| Filtering | The broker sends only chunks that contain messages with the requested filter value |

Deduplication is a rare case where RabbitMQ drops duplicates on the broker side: the producer must have a **stable name** and a **monotonically increasing** publishing number (for example, an id from the outbox).

## 10.5 Filtering

The producer tags messages with a filter value (the `x-stream-filter-value` header in AMQP 0-9-1), and the consumer asks only for the values it needs:

```python
# producer
ch.basic_publish("", "shop.events.log", body,
                 pika.BasicProperties(headers={"x-stream-filter-value": "eu"}))

# consumer
ch.basic_consume("shop.events.log", on_message, arguments={
    "x-stream-offset": "first",
    "x-stream-filter": "eu",
    "x-stream-match-unfiltered": False,
})
```

The filter works at the chunk level (a Bloom filter): the broker skips chunks that definitely contain no matching values, but the chunks that arrive may contain other messages too. **The consumer must filter again itself.** Since 4.2, AMQP 1.0 clients also have server-side SQL filter expressions.

## 10.6 Super streams: a partitioned stream

One stream is one sequence on one leader. For scaling there is the **super stream**: a set of ordinary streams as partitions plus an exchange that distributes messages by key.

```bash
docker exec rabbit-1 rabbitmq-streams add_super_stream invoices --partitions 3
```

```
producer --(key: customer_id)--> exchange invoices --hash--> invoices-0
                                                        +--> invoices-1
                                                        +--> invoices-2
```

- Messages with one key land in one partition: per-key order is preserved.
- With single active consumer each partition is read by one application instance, and partitions are spread across instances: Kafka's consumer group model.
- Since 4.2.9/4.3.3 the number of super stream partitions is capped at 1000 by default (`stream.max_super_stream_partitions`).

## 10.7 Streams or Kafka

| You need | Choice |
|---|---|
| RabbitMQ is already there and you need a log with replay | Streams |
| Fan-out of one event to hundreds of readers | Streams |
| A connector ecosystem, CDC, Kafka Streams, Schema Registry | Kafka |
| Years of history, petabytes | Kafka with tiered storage |
| A mixed system: task queues + a log | RabbitMQ with quorum queues and streams |

### Self-check questions

1. Why is a stream better than 50 queues for fan-out to 50 readers?
2. Who stores the reader's position when a stream is read over AMQP 0-9-1, and who over the stream protocol?
3. What does a producer need for deduplication in a stream?
4. Why must a consumer filter messages again even with a filter set?

---

# Module 11. Patterns: work queues, pub/sub, RPC, priorities

## 11.1 Work queue (task queue)

```
producer --> [tasks] --> worker 1
                    \--> worker 2
                    \--> worker 3
```

The RabbitMQ classic: tasks are spread across workers, each is processed once. The recipe:

- a quorum queue, `delivery-limit`, DLX;
- manual ack after the work is done;
- prefetch matched to processing time (1–5 for long tasks);
- idempotent tasks.

## 11.2 Publish/subscribe

```
producer --> [exchange events (fanout or topic)] --> [queue svc-a] --> service A
                                                  \-> [queue svc-b] --> service B
```

Every service owns **its own durable queue**. Within a service, instances read one queue (competing consumers); across services, each gets a copy. This is the main pattern for integrating microservices on RabbitMQ.

A naming rule: name the queue after the **consumer**, not the event: `billing.order-events`, not `order.created`. Then it's clear whose queue it is, and its backlog is the backlog of a specific service.

## 11.3 RPC: request and reply

```
client                                              server
  |-- publish rpc.pricing                              |
  |     reply_to = amq.rabbitmq.reply-to               |
  |     correlation_id = 42  ------------------------> | processes
  |<------------ publish to reply_to, correlation_id=42 |
```

**Direct reply-to** is the pseudo-queue `amq.rabbitmq.reply-to`: no need to create a reply queue, the answer goes straight to the client's channel. Since 4.2 it works for AMQP 1.0 too, including across protocols.

The client:

```python
import time
import uuid
import pika

conn = pika.BlockingConnection(pika.URLParameters("amqp://admin:admin@localhost:5672/%2F"))
ch = conn.channel()
response = {}

def on_reply(ch, method, props, body):
    response[props.correlation_id] = body

# subscribe to the pseudo-queue BEFORE publishing the request, in auto-ack mode
ch.basic_consume("amq.rabbitmq.reply-to", on_reply, auto_ack=True)

corr_id = str(uuid.uuid4())
ch.basic_publish("", "rpc.pricing", b'{"sku":"A1","qty":3}',
                 pika.BasicProperties(reply_to="amq.rabbitmq.reply-to", correlation_id=corr_id,
                                      expiration="5000"))   # the request is useless after 5 seconds
deadline = time.monotonic() + 5                               # wait for the reply at most 5 seconds
while corr_id not in response and time.monotonic() < deadline:
    # may return early after handling any event, so loop until the reply or the deadline
    conn.process_data_events(time_limit=max(0, deadline - time.monotonic()))
print(response.get(corr_id, "timeout"))
```

The server:

```python
ch.queue_declare("rpc.pricing", durable=True, arguments={"x-queue-type": "quorum"})
ch.basic_qos(prefetch_count=10)

def on_request(ch, method, props, body):
    result = calculate(body)
    ch.basic_publish("", props.reply_to, result,
                     pika.BasicProperties(correlation_id=props.correlation_id))
    ch.basic_ack(method.delivery_tag)

ch.basic_consume("rpc.pricing", on_request)
ch.start_consuming()
```

Rules for RPC through a broker:

- **always a timeout** on the client and an `expiration` on the request: a reply nobody waits for is useless;
- `correlation_id` ties the reply to the request;
- the server must be idempotent: on timeout the client may repeat the request;
- if you need fast synchronous RPC between services, consider gRPC or HTTP: the broker adds latency and a point of failure.

## 11.4 Priorities

```bash
# classic: the number of levels is set at declaration
curl -s -u admin:admin -X PUT http://localhost:15672/api/queues/%2F/reports \
  -H "content-type: application/json" \
  -d '{"durable":true,"arguments":{"x-queue-type":"classic","x-max-priority":5}}'
```

```python
ch.basic_publish("", "reports", body, pika.BasicProperties(priority=5, delivery_mode=2))
```

- Messages with a higher `priority` are delivered first.
- Priority works only when **there is a backlog**: if consumers take everything instantly, there's nothing to sort. A small prefetch strengthens the effect.
- Quorum queues support priorities without `x-max-priority`. Since 4.3 they are strict, 32 levels (0–31), and a message without `priority` counts as priority 4; 4.0–4.2 had only two levels: normal (0–4) and high (5 and above). Messages returned to the queue are redelivered in the order they were returned, regardless of priority.
- Don't create dozens of levels: 2–5 is almost always enough.

## 11.5 Competing consumers and ordering

Several consumers on one queue means scaling at the cost of ordering (module 6.8). Options if you need order:

| Option | How | Cost |
|---|---|---|
| Single active consumer | One reads, the others stand by | No parallelism |
| Sharding by key | Consistent hash exchange → N queues, each with a single active consumer | A more complex topology |
| Super streams + SAC | Partitions by key, one active reader per partition | Stream clients |

## 11.6 Sagas and choreography

In an event-driven system the steps of a business process are performed by different services linked by events:

```
order.created -> payments: charge -> payment.succeeded -> stock: reserve -> stock.reserved -> order confirmed
                                  \-> payment.failed   -> orders: cancel the order
                                                          stock.failed -> payments: refund
```

- every step is idempotent and has a **compensating action**;
- events are published through an outbox;
- for complex processes with timeouts and branches an orchestrator (a separate service that holds the process state) is easier than pure choreography.

### Self-check questions

1. Why is a pub/sub queue named after the consumer and not the event?
2. Why does RPC need `correlation_id`, `expiration` and a client-side timeout?
3. Why don't priorities work when the queue is empty?
4. How do you preserve per-key order and still scale?

---

# Module 12. Protocols: AMQP 1.0, MQTT, STOMP, WebSocket

## 12.1 Which protocols RabbitMQ speaks

| Protocol | Port | Plugin | For whom |
|---|---|---|---|
| AMQP 0-9-1 | 5672 | built in | Classic clients: pika, amqp091-go, Java amqp-client |
| AMQP 1.0 | 5672 | built in since 4.0 | New RabbitMQ clients, Azure Service Bus-compatible and ActiveMQ clients |
| Stream | 5552 | `rabbitmq_stream` | High-throughput work with streams |
| MQTT 3.1.1 and 5.0 | 1883 | `rabbitmq_mqtt` | IoT devices |
| STOMP | 61613 | `rabbitmq_stomp` | Simple text clients |
| Web MQTT / Web STOMP | 15675 / 15674 | `rabbitmq_web_mqtt` / `rabbitmq_web_stomp` | Browsers over WebSocket |

All protocols work with **the same** exchanges and queues: a device publishes over MQTT and a service reads those messages over AMQP.

## 12.2 AMQP 1.0

Since RabbitMQ 4.0, AMQP 1.0 is **a core protocol of the broker** on the same port 5672, with no separate plugin. It has new official clients (Java, .NET, Go, Python) that can also manage the topology.

Addressing in AMQP 1.0 (address format v2):

| Address | Meaning |
|---|---|
| `/queues/payments` | Publish to or consume from a queue |
| `/exchanges/shop.events/order.created` | Publish to an exchange with a key |
| `/exchanges/shop.events` | Publish to an exchange, the key is set in the message |

Differences from AMQP 0-9-1 worth knowing:

- richer delivery outcomes: `accepted`, `released` (return without increasing the delivery count), `rejected` (to the DLX), `modified` (return with changed annotations);
- since 4.3 the publisher gets the queue name and the reason (`maxlen` or `unavailable`) in a `rejected` outcome;
- server-side stream filtering (Property Filter Expressions since 4.1, SQL expressions since 4.2);
- since 4.2 direct reply-to works across protocols.

If you start a new project and a client for your language is ready, AMQP 1.0 with the official RabbitMQ client is worth considering. Old v1-format addresses are denied by default since 4.3.

## 12.3 MQTT

```bash
docker exec rabbit-1 rabbitmq-plugins enable rabbitmq_mqtt
```

(Add `rabbitmq_mqtt` to `enabled_plugins` and port `1883` to compose so the plugin is enabled on every node.)

How MQTT maps onto RabbitMQ's model:

| MQTT | RabbitMQ |
|---|---|
| Publishing to topic `sensors/eu/temp` | Publishing to `amq.topic` with key `sensors.eu.temp` |
| `/` in a topic | `.` in a routing key |
| `+` (one level) | `*` |
| `#` (many levels) | `#` |
| A QoS 0 subscription | A special MQTT QoS 0 queue type (no storage, as fast as possible) |
| A QoS 1 subscription | An ordinary queue (classic or quorum, by configuration) |

```
sensor --MQTT publish sensors/eu/temp--> amq.topic --binding sensors.#--> [telemetry] --AMQP--> analytics service
```

RabbitMQ supports MQTT 5.0 and handles hundreds of thousands to millions of MQTT connections per cluster when tuned properly. For IoT with a huge number of devices at the edge, a separate MQTT broker with a bridge into RabbitMQ is sometimes used.

## 12.4 STOMP and WebSocket

STOMP is a simple text protocol: handy for scripts and older systems. Web STOMP and Web MQTT give browser access over WebSocket:

```javascript
// browser: Web MQTT via mqtt.js
const client = mqtt.connect("ws://localhost:15675/ws", { username: "web", password: "..." });
client.subscribe("orders/+/status");
client.on("message", (topic, payload) => console.log(topic, payload.toString()));
```

Give browser clients **a separate user** with rights only on the topics they need (topic permissions, module 17): the browser is an untrusted environment.

### Self-check questions

1. How does the MQTT topic `sensors/eu/temp` become a RabbitMQ routing key?
2. How does the `released` outcome in AMQP 1.0 differ from `rejected`?
3. Why do browser clients need a separate user with restricted rights?

---

# Module 13. Multiple data centres: Federation and Shovel

## 13.1 Why not stretch a cluster across regions

A RabbitMQ cluster is built on Raft: every write to a quorum queue and to metadata waits for a majority of nodes. Between regions with tens of milliseconds of latency that means:

- slow publishing and slow topology changes;
- a risk of losing the quorum when the inter-region network has problems.

Nodes of one cluster must be in **one region** (they can be in different availability zones). Between regions messages are carried by **Federation** or **Shovel**.

## 13.2 Federation

Federation links brokers **asynchronously** at the level of exchanges or queues.

**Federated exchange:** messages published to an exchange on the upstream broker are copied to the exchange on the downstream broker, if it has interested bindings.

```
region EU (upstream)                         region US (downstream)
exchange shop.events  ---federation link--->  exchange shop.events -> [us.analytics]
```

**Federated queue:** the downstream queue pulls messages from the upstream queue of the same name when it has idle consumers. That way consumers in two regions work through one logical queue.

```bash
docker exec rabbit-us rabbitmq-plugins enable rabbitmq_federation rabbitmq_federation_management

# on the US broker: define the upstream
docker exec rabbit-us rabbitmqctl set_parameter federation-upstream eu \
  '{"uri":"amqps://federation:secret@rabbit-eu.example.com:5671","ack-mode":"on-confirm","expires":3600000}'

# policy: federate exchanges shop.*
docker exec rabbit-us rabbitmqctl set_policy federate-shop '^shop\.' \
  '{"federation-upstream-set":"all"}' --apply-to exchanges

docker exec rabbit-us rabbitmqctl federation_status   # or Admin → Federation Status in the UI
```

`ack-mode=on-confirm` means a message is acknowledged to the upstream only after a confirm on the downstream: no losses when the link fails.

## 13.3 Shovel

Shovel is a simpler, more direct tool: **take messages from a source and publish them to a destination**. The source and destination are queues or exchanges on the same or another broker, over AMQP 0-9-1 or AMQP 1.0.

```bash
docker exec rabbit-1 rabbitmq-plugins enable rabbitmq_shovel rabbitmq_shovel_management

docker exec rabbit-1 rabbitmqctl set_parameter shovel orders-to-dr \
  '{"src-protocol":"amqp091","src-uri":"amqp://","src-queue":"orders.outbound",
    "dest-protocol":"amqp091","dest-uri":"amqps://shovel:secret@rabbit-dr.example.com:5671",
    "dest-exchange":"shop.events","ack-mode":"on-confirm"}'

docker exec rabbit-1 rabbitmqctl shovel_status
```

Typical uses of Shovel:

- moving messages between brokers during a migration (for example, from an old cluster to a new one, blue-green);
- re-sending messages from a DLQ back to the work queue after a bug fix;
- one-way delivery of data to another region or an isolated environment.

Since 4.3.5 dynamic shovels have a `src-delete-after-duration` parameter: the shovel deletes itself after the given time (in seconds, at least 60 by default), handy for one-off moves.

## 13.4 Federation or Shovel

| | Federation | Shovel |
|---|---|---|
| Level | Exchanges and queues, by policies | A source → destination pair |
| Setup | An upstream + a policy, applied to many objects | Each shovel separately |
| Direction | The downstream pulls from the upstream | As configured |
| When | A permanent link between regions, a shared topology | Migrations, re-sends, point-to-point bridges |

## 13.5 Disaster recovery

| Scenario | Solution |
|---|---|
| One node fails | Quorum queues, 3 nodes |
| An availability zone fails | Nodes in different zones of one region |
| A region fails | A second cluster + Federation/Shovel for data, definitions kept in sync |
| The application corrupted data | Definitions exported to Git; messages in queues aren't backed up: important events must live in their source (outbox, database) |

Definitions (exchanges, queues, bindings, policies, users) are exported and imported with one command:

```bash
docker exec rabbit-1 rabbitmqctl export_definitions /tmp/definitions.json
docker exec rabbit-1 rabbitmqctl import_definitions /tmp/definitions.json
```

Keep definitions in Git and apply them automatically: then the standby cluster always has the same topology.

### Practice

1. Bring up two separate brokers and set up a federated exchange: publish to the upstream and receive the message in a downstream queue.
2. Set up a shovel that moves messages from `shop.dead` back to `payments`, and turn it off after the move.
3. Export the cluster's definitions and import them into a clean broker.

---

# Module 14. Policies, limits and resource management

## 14.1 Policies and operator policies

**Policies** (module 4.6) set the behaviour of queues and exchanges by name pattern: TTL, length limits, DLX, delivery-limit, federation. Application owners change them.

**Operator policies** are set by the cluster administrator. They apply on top of regular policies and exist to **constrain** applications: for numeric keys (such as `max-length`, `message-ttl`, `delivery-limit`) the **lower** value of the policy and the operator policy wins.

```bash
# a regular policy of the shop team
docker exec rabbit-1 rabbitmqctl set_policy -p shop shop-queues '.*' \
  '{"max-length": 500000, "dead-letter-exchange": "shop.dlx"}' --apply-to queues

# an administrator cap: no queue in shop holds more than 1M messages
docker exec rabbit-1 rabbitmqctl set_operator_policy -p shop cap '.*' \
  '{"max-length": 1000000, "overflow": "reject-publish"}' --apply-to queues

docker exec rabbit-1 rabbitmqctl list_operator_policies -p shop
```

## 14.2 Limits per vhost and user

```bash
# vhost: at most 256 connections and 1024 queues
docker exec rabbit-1 rabbitmqctl set_vhost_limits -p shop '{"max-connections": 256, "max-queues": 1024}'

# user: at most 20 connections and 200 channels
docker exec rabbit-1 rabbitmqctl set_user_limits billing '{"max-connections": 20, "max-channels": 200}'
```

Limits protect the cluster from one application leaking connections or creating queues endlessly, a typical cause of incidents.

`rabbitmq.conf` also has node-wide limits: `max_connections` (connections per node, unlimited by default) and `channel_max_per_node` (channels per node). Don't confuse them with `max_channels_per_connection`: that's the maximum number of channels in one connection, negotiated with the client when it connects (2047 by default). The keys `max_connections` and `max_channels_per_connection` have these names since 4.2.7/4.3.1; the old names `connection_max` and `channel_max` still work as aliases.

## 14.3 The memory alarm

```ini
# rabbitmq.conf
vm_memory_high_watermark.relative = 0.6      # 60% of available memory (the default)
# or absolute:
# vm_memory_high_watermark.absolute = 6GB
```

When the RabbitMQ process uses more than the threshold, a **memory alarm** fires:

```
memory > vm_memory_high_watermark
   -> an alarm on the node
   -> EVERY publishing connection in the cluster is blocked (connection.blocked)
   -> consumers keep working and drain the queues
   -> memory freed -> publishing resumes
```

In a container RabbitMQ sees the cgroup memory limit, so the threshold is computed from the container's limit. Don't set it above 0.7–0.8: Erlang needs memory for garbage collection, and on a shortage the OS kills the process.

## 14.4 The disk alarm

```ini
disk_free_limit.absolute = 4GB
# or relative to the amount of RAM:
# disk_free_limit.relative = 1.5
```

If free disk space drops below the limit, publishing is blocked just like with the memory alarm. **The default (50 MB) is far too small for production**: quorum queues and streams write a lot, and the disk can fill up before the alarm fires. Use several gigabytes or `relative = 1.0–2.0`.

```bash
docker exec rabbit-1 rabbitmq-diagnostics check_local_alarms
docker exec rabbit-1 rabbitmq-diagnostics alarms
docker exec rabbit-1 rabbitmq-diagnostics memory_breakdown
```

## 14.5 Flow control on connections

Besides global alarms there is **credit-based flow control**: if a queue or a node can't keep up with publishes, the broker slows down specific publishing connections. In the management UI such a connection shows the `flow` state.

A short `flow` is normal. A constant `flow` means publishers write faster than the queue can accept: you need more queues (sharding), faster disks or less load.

## 14.6 Long queues are a problem

RabbitMQ is fastest when queues are **short**: messages arrive and leave for consumers almost immediately. A queue with millions of messages:

- takes memory and disk;
- recovers more slowly after a restart;
- for a quorum queue, bloats the Raft log and snapshots.

Rules:

- monitor queue length and message age, not only rates;
- a length limit + `reject-publish` on every important queue, so overflow is visible instead of unbounded;
- if you need to keep a lot for a long time, that's a job for streams.

## 14.7 Feature flags and deprecated features

**Feature flags** enable new capabilities once every node in the cluster is upgraded:

```bash
docker exec rabbit-1 rabbitmqctl list_feature_flags name state
docker exec rabbit-1 rabbitmqctl enable_feature_flag all     # after every node is upgraded
```

**Deprecated features** go through stages: "permitted by default" → "denied by default" → removed. Since 4.3, for example, `transient_nonexcl_queues` and `global_qos` are denied by default. They can be temporarily allowed in `rabbitmq.conf` on **every** node:

```ini
deprecated_features.permit.transient_nonexcl_queues = true
```

Better not to enable them but to rewrite the code: these features will be removed.

### Self-check questions

1. How does an operator policy differ from a regular policy?
2. What happens to publishers and consumers during a memory alarm?
3. Why is the default `disk_free_limit` dangerous?
4. Why are long queues a problem for RabbitMQ?

---

# Module 15. Performance and tuning

## 15.1 Orders of magnitude

| Scenario | Ballpark on good hardware |
|---|---|
| One classic queue, small messages | Tens of thousands of msg/s |
| One quorum queue, confirms | Tens of thousands of msg/s (bounded by one leader and the disk) |
| A 3-node cluster, many queues | Hundreds of thousands of msg/s |
| A stream over the stream protocol | Hundreds of thousands to millions of msg/s |
| Latency with short queues | Single-digit milliseconds |

The key point: **one queue is one process on the leader**. RabbitMQ scales through **the number of queues**, not by "speeding up" one.

## 15.2 Benchmark: PerfTest

```bash
docker run -it --rm --network host pivotalrabbitmq/perf-test:latest \
  --uri amqp://admin:admin@localhost:5672 \
  --queue perf-q --quorum-queue \
  --producers 2 --consumers 2 \
  --confirm 100 --qos 100 \
  --size 1000 --time 60
```

- `--confirm 100` means at most 100 unconfirmed publishes per producer;
- `--qos 100` is the consumer prefetch;
- PerfTest prints send and receive rates and latency (percentiles).

Measure on your hardware, with your message sizes and your settings. Other people's numbers are useless.

## 15.3 What to tune on the publisher

| Setting | Effect |
|---|---|
| Asynchronous confirms with a window of 100–1000 | The main throughput lever |
| Waiting synchronously for a confirm per message | Reliable but slow: for low loads |
| Long-lived connections and channels | A connection per message kills the broker |
| Message size | Small messages mean more overhead per unit of data; huge ones pressure memory |
| A separate connection for publishing | Flow control doesn't slow down consumers |

## 15.4 What to tune on the consumer

| Setting | Effect |
|---|---|
| Prefetch | 1 is slow; 10–300 is usually optimal; no limit is dangerous |
| Batched acks (`multiple=true`) | Fewer network operations |
| Number of consumers | More consumers, more parallelism, until the queue becomes the bottleneck |
| Processing speed | Most often the bottleneck is the database and external APIs, not the broker |

## 15.5 The server

- **Disks:** fast SSD or NVMe for quorum queues and streams: they write with fsync.
- **CPU:** RabbitMQ uses several cores well (Erlang schedulers). 4–8 cores per node is a typical start.
- **Memory:** keep headroom below `vm_memory_high_watermark`; a memory alarm stops every publisher.
- **File descriptors:** a limit of 100,000+; every connection is a descriptor.
- **Network:** cluster nodes in one region, with low latency between them.
- **Fewer short-lived queues:** mass creation and deletion of queues (a queue per request, per user) loads the cluster metadata.

## 15.6 What actually makes systems faster

1. **Short queues**: consumers keep up with publishers.
2. **Asynchronous confirms with a window** instead of waiting for every message.
3. **The right prefetch**.
4. **More queues** (sharding) instead of one huge one.
5. **Streams** for large flows with replay.
6. **Long-lived connections and channels**.
7. **Turning off extra metrics**: if you use Prometheus, the management plugin's stats collection can be disabled (`management_agent.disable_metrics_collector = true`).

---

# Module 16. Monitoring and alerts

## 16.1 Where metrics come from

The `rabbitmq_prometheus` plugin serves metrics on port **15692**:

| Endpoint | What it returns |
|---|---|
| `/metrics` | Aggregated node metrics (cheap, enough for most dashboards) |
| `/metrics/per-object` | Metrics per queue and connection (expensive with thousands of objects) |
| `/metrics/detailed?family=...&vhost=...` | Selected detailed metrics, for example for the queues of one vhost |

```bash
curl -s localhost:15692/metrics | grep -E '^rabbitmq_(queue_messages_ready|connections|alarms)' | head
curl -s "localhost:15692/metrics/detailed?family=queue_coarse_metrics&vhost=%2F" | head
```

The RabbitMQ team publishes ready Grafana dashboards (Overview, Quorum Queues Raft, Streams). Raft metrics were renamed in 4.2: update dashboards and alerts when you upgrade.

The management UI is convenient for investigating a situation but doesn't replace Prometheus: it keeps history only briefly, and collecting its stats costs resources.

## 16.2 Key metrics

| Metric | What it shows |
|---|---|
| `rabbitmq_queue_messages_ready` | Messages waiting for a consumer |
| `rabbitmq_queue_messages_unacked` | Delivered but not acknowledged |
| `rabbitmq_queue_consumers` | Consumers on a queue (0 with a growing queue means an outage) |
| `rabbitmq_connections`, `rabbitmq_channels` | Connection and channel counts (growth means a leak) |
| `rabbitmq_alarms_memory_used_watermark` | 1 means a memory alarm, publishing blocked |
| `rabbitmq_alarms_free_disk_space_watermark` | 1 means a disk alarm |
| `rabbitmq_process_resident_memory_bytes` | Process memory |
| `rabbitmq_disk_space_available_bytes` | Free space |
| `rabbitmq_global_messages_unroutable_dropped_total` | Messages dropped for lack of a route |
| `rabbitmq_global_messages_redelivered_total` | Redeliveries |
| `rabbitmq_global_messages_confirmed_total` | Confirmed publishes |

With `prometheus.return_per_object_metrics = false` (the default, as in the course cluster) the `rabbitmq_queue_*` metrics on `/metrics` are **per-node sums with no `queue` label**. For per-queue graphs and alerts scrape `/metrics/detailed?family=queue_coarse_metrics&family=queue_consumer_count`: there the same per-queue values are called `rabbitmq_detailed_queue_messages_ready`, `rabbitmq_detailed_queue_messages_unacked`, `rabbitmq_detailed_queue_messages` and `rabbitmq_detailed_queue_consumers`. `/metrics/per-object` also has the `queue` label, but it gets expensive with thousands of queues.

## 16.3 Health checks

```bash
docker exec rabbit-1 rabbitmq-diagnostics ping                      # the node responds
docker exec rabbit-1 rabbitmq-diagnostics check_running             # the RabbitMQ application is running
docker exec rabbit-1 rabbitmq-diagnostics check_local_alarms        # no alarms
docker exec rabbit-1 rabbitmq-diagnostics check_port_connectivity   # listening on client ports
docker exec rabbit-1 rabbitmq-diagnostics check_virtual_hosts       # vhosts are running
docker exec rabbit-1 rabbitmq-queues check_if_node_is_quorum_critical
```

For Kubernetes: liveness is `rabbitmq-diagnostics ping` (the node is alive), readiness is `rabbitmq-diagnostics check_port_connectivity` (it accepts clients). Don't make the liveness probe strict: restarting a node because of an alarm only makes things worse.

## 16.4 What to alert on

| Alert | Condition | Why it matters |
|---|---|---|
| **Memory or disk alarm** | Any | Publishing is blocked across the cluster |
| **A queue without consumers** | `consumers == 0` and `messages_ready > 0` | A service isn't working |
| **A growing queue** | `messages_ready` growing for 10+ minutes | Consumers can't keep up |
| **Growing unacked** | High for a long time | Consumers are stuck or don't ack |
| **Messages in a DLQ** | Any | Business logic is failing |
| **Unroutable dropped** | Growing | Broken routing, messages are being lost |
| **A node is down** | Fewer nodes than expected | The quorum is at risk |
| **A quorum queue without a leader or with lagging replicas** | Any | The queue is unavailable or losing redundancy |
| **Connection or channel growth** | Sharp | A connection leak in an application |
| **Free space** | < 20% | A disk alarm is coming |
| **Authentication errors** | Growing | An attack or a broken deployment |

Per-queue alerts are built on metrics from `/metrics/detailed` (see 16.2), alarms and node counters on the plain `/metrics`. This is how it looks in Prometheus for the cluster configuration from module 2.3:

```yaml
# prometheus.yml: aggregated node metrics + per-queue metrics
scrape_configs:
  - job_name: rabbitmq
    static_configs: [{targets: ["rabbit-1:15692", "rabbit-2:15692", "rabbit-3:15692"]}]
  - job_name: rabbitmq-queues
    metrics_path: /metrics/detailed
    params: {family: [queue_coarse_metrics, queue_consumer_count]}
    static_configs: [{targets: ["rabbit-1:15692", "rabbit-2:15692", "rabbit-3:15692"]}]

# rules.yml
groups:
  - name: rabbitmq
    rules:
      - alert: RabbitMQAlarm             # memory or disk alarm
        expr: max(rabbitmq_alarms_memory_used_watermark) == 1 or max(rabbitmq_alarms_free_disk_space_watermark) == 1
      - alert: QueueWithoutConsumers     # a queue without consumers
        expr: rabbitmq_detailed_queue_consumers == 0 and on(vhost, queue) rabbitmq_detailed_queue_messages_ready > 0
        for: 5m
      - alert: QueueGrowing              # a queue growing for 10+ minutes
        expr: deriv(rabbitmq_detailed_queue_messages_ready[10m]) > 0
        for: 10m
      - alert: MessagesInDLQ             # messages in a DLQ (shop.dead and the like)
        expr: rabbitmq_detailed_queue_messages{queue=~".*\\.dead"} > 0
      - alert: UnroutableDropped         # messages dropped without a route
        expr: rate(rabbitmq_global_messages_unroutable_dropped_total[5m]) > 0
```

## 16.5 Everyday commands

```bash
docker exec rabbit-1 rabbitmqctl cluster_status
docker exec rabbit-1 rabbitmqctl list_queues name type messages_ready messages_unacknowledged consumers
docker exec rabbit-1 rabbitmqctl list_connections name user state channels
docker exec rabbit-1 rabbitmqctl list_consumers queue_name channel_pid prefetch_count
docker exec rabbit-1 rabbitmq-queues quorum_status payments
docker exec rabbit-1 rabbitmq-diagnostics memory_breakdown
docker exec rabbit-1 rabbitmq-diagnostics log_tail -N 100
```

---

# Module 17. Security: TLS, users, permissions, OAuth 2

## 17.1 First steps

- **Delete or don't use `guest`.** It can only connect from localhost, but it has no business in production.
- **A separate user per service** with minimal permissions.
- **A separate vhost** per team or environment.
- **TLS** on client ports, the management UI over HTTPS.
- **The Erlang cookie** is a secret: with it you can take full control of a node. Keep it in secrets, not in the image or the repository.

## 17.2 Users, tags and permissions

```bash
docker exec rabbit-1 rabbitmqctl add_vhost shop
docker exec rabbit-1 rabbitmqctl add_user billing 'long-random-password'
docker exec rabbit-1 rabbitmqctl set_user_tags billing        # no tags: no UI access

# permissions: configure (declare), write (publish, bind a queue to an exchange), read (consume, use an exchange as a binding source)
docker exec rabbit-1 rabbitmqctl set_permissions -p shop billing \
  '^billing\..*' \
  '^(shop\.events|billing\..*)$' \
  '^(shop\.events|billing\..*)$'

docker exec rabbit-1 rabbitmqctl list_user_permissions billing
```

The three regular expressions are the **configure**, **write** and **read** permissions on resource names (exchanges and queues):

| Operation | Permission needed |
|---|---|
| Declare or delete a queue or exchange | configure on that object |
| Publish to an exchange | write on the exchange |
| Read from a queue (consume, get) | read on the queue |
| Bind a queue to an exchange | write on the queue, read on the exchange |

**User tags** control access to the management UI and HTTP API:

| Tag | Access |
|---|---|
| `management` | Own vhosts and objects |
| `policymaker` | + policies and parameters |
| `monitoring` | + read access to the state of the whole cluster |
| `administrator` | Everything |

## 17.3 Topic permissions

For topic exchanges you can restrict **which routing keys** a user may publish with and subscribe to:

```bash
# order-service may publish to shop.events only order.* events
docker exec rabbit-1 rabbitmqctl set_topic_permissions -p shop order-service shop.events '^order\..*' '^$'
```

This is especially important for MQTT and browser clients (module 12): a device must not publish to other devices' topics.

## 17.4 TLS

```ini
# rabbitmq.conf
listeners.tcp = none                       # disable the plain-text port
listeners.ssl.default = 5671

ssl_options.cacertfile = /etc/rabbitmq/tls/ca.pem
ssl_options.certfile   = /etc/rabbitmq/tls/server.pem
ssl_options.keyfile    = /etc/rabbitmq/tls/server-key.pem
ssl_options.verify     = verify_peer
ssl_options.fail_if_no_peer_cert = false   # true = require a client certificate (mTLS)

management.ssl.port       = 15671
management.ssl.cacertfile = /etc/rabbitmq/tls/ca.pem
management.ssl.certfile   = /etc/rabbitmq/tls/server.pem
management.ssl.keyfile    = /etc/rabbitmq/tls/server-key.pem
```

The client connects via `amqps://host:5671`. With mTLS and the `rabbitmq_auth_mechanism_ssl` plugin a client can authenticate with its certificate (the `EXTERNAL` mechanism) without a password.

Don't forget inter-node traffic: for clusters in an untrusted network, TLS is also configured for Erlang distribution between nodes.

## 17.5 OAuth 2 and LDAP

**OAuth 2** (plugin `rabbitmq_auth_backend_oauth2`): clients connect with a JWT instead of a password, and permissions come from the token's scopes:

```
rabbitmq.read:shop/billing.*      read from billing.* resources in vhost shop
rabbitmq.write:shop/shop.events   publish to shop.events
rabbitmq.tag:monitoring           a user tag
```

It works with Keycloak, Entra ID, Okta and other OIDC providers, including management UI login. Tokens are short-lived, and in 4.x a client can refresh the token on an open connection.

**LDAP** (`rabbitmq_auth_backend_ldap`): users and groups from the corporate directory.

Backends can be combined: for example, internal users for service accounts and OAuth 2 for people.

## 17.6 Secrets in configuration

- Values in `rabbitmq.conf` can be stored encrypted (`encrypted:...`); in 4.2–4.3 many keys support this.
- `protected_users` prevents critical users from being deleted through the HTTP API.
- In Kubernetes, the cookie, passwords and certificates go only through Secrets.

## 17.7 Security checklist

- [ ] `guest` deleted or unused
- [ ] A separate user per service, permissions by regular expressions on its own resources
- [ ] Topic permissions for publishing to shared topic exchanges
- [ ] Separate vhosts for teams and environments, limits per vhost and user
- [ ] TLS on client ports, the plain-text port disabled
- [ ] Management UI over HTTPS and not exposed to the internet
- [ ] The Erlang cookie in secrets, long and random
- [ ] OAuth 2 or LDAP for people
- [ ] Secrets in configuration encrypted
- [ ] Alerts on authentication errors
- [ ] Erlang distribution ports (4369, 25672) not reachable from outside the cluster

---

# Module 18. RabbitMQ in production: Kubernetes, operations, upgrades

## 18.1 Reference architecture

```
              applications (publishers / consumers)
                          |
                 TCP load balancer (or an address list in the client)
                          |
       +------------------+------------------+
       |         RabbitMQ 4.3 cluster        |
       |  3 nodes in different zones of one  |
       |  region; quorum queues (3 replicas),|
       |  streams; Khepri metadata on Raft   |
       +------------------+------------------+
                          |
          Federation / Shovel --> a cluster in the standby region
                          |
      Prometheus + Grafana + Alertmanager, definitions in Git
```

## 18.2 Sizing

| Load | Configuration |
|---|---|
| Up to a few thousand msg/s | 3 nodes × 2–4 vCPU, 8 GB RAM, SSD |
| Tens of thousands of msg/s | 3 nodes × 4–8 vCPU, 16–32 GB RAM, NVMe |
| Hundreds of thousands of msg/s | 3–5 nodes × 8–16 vCPU, 32–64 GB RAM, NVMe, queue sharding, streams |
| Several regions | A cluster per region + Federation/Shovel |

Calculate separately: the number of queues and connections (every connection costs memory), the worst-case volume of messages in queues (how much piles up if consumers are down for an hour), and disk for quorum queue and stream Raft logs.

## 18.3 Kubernetes: RabbitMQ Cluster Operator

The official way is the **RabbitMQ Cluster Operator** (clusters) and the **Messaging Topology Operator** (queues, exchanges, bindings, users, policies as Kubernetes resources).

```yaml
apiVersion: rabbitmq.com/v1beta1
kind: RabbitmqCluster
metadata:
  name: shop
spec:
  replicas: 3
  image: rabbitmq:4.3-management
  resources:
    requests: { cpu: "2", memory: 8Gi }
    limits:   { cpu: "2", memory: 8Gi }
  persistence:
    storageClassName: fast-ssd
    storage: 100Gi
  rabbitmq:
    additionalPlugins: [rabbitmq_stream, rabbitmq_prometheus]
    additionalConfig: |
      disk_free_limit.absolute = 8GB
      vm_memory_high_watermark.relative = 0.6
  affinity:
    podAntiAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        - labelSelector:
            matchLabels: { app.kubernetes.io/name: shop }
          topologyKey: topology.kubernetes.io/zone
---
apiVersion: rabbitmq.com/v1beta1
kind: Queue
metadata:
  name: payments
spec:
  name: payments
  type: quorum
  durable: true
  rabbitmqClusterReference: { name: shop }
---
apiVersion: rabbitmq.com/v1beta1
kind: Policy
metadata:
  name: shop-limits
spec:
  name: shop-limits
  pattern: "^(payments|stock)$"
  applyTo: queues
  definition:
    max-length: 500000
    overflow: reject-publish
    dead-letter-exchange: shop.dlx
    delivery-limit: 10
  rabbitmqClusterReference: { name: shop }
```

What the operator does for you: a StatefulSet with stable node names, the Erlang cookie and credentials in Secrets, peer discovery via the Kubernetes API, a safe rolling restart that respects the quorum.

What to remember:

- **persistent volumes** are mandatory;
- **anti-affinity across zones**: three pods in one zone won't survive its failure;
- the container memory limit is the basis of the memory alarm: don't make it too small;
- topology as resources in Git is infrastructure as code and protection against "someone deleted a queue by hand".

## 18.4 Upgrades

Rules from the release notes worth knowing:

- **you can only upgrade to the next series**: to 4.3 only from the latest 4.2.x patch; to 4.2 from 4.1, 4.0 or 3.13;
- **enable all feature flags before upgrading**: `rabbitmqctl enable_feature_flag all`, otherwise new nodes won't start in the cluster;
- **Erlang**: starting with 4.3.3 and 4.2.9, Erlang 27+ is required. Official Docker images already contain a suitable Erlang;
- **mixed versions** in a cluster are acceptable only during a rolling upgrade: a few hours, not days.

Rolling upgrade:

```bash
# 1. check health and that the node can be stopped
docker exec rabbit-1 rabbitmq-diagnostics check_local_alarms
docker exec rabbit-1 rabbitmq-queues check_if_node_is_quorum_critical

# 2. put the node into maintenance
docker exec rabbit-1 rabbitmq-upgrade drain

# 3. update the image or package and restart the node

# 4. bring it back and wait for replicas to catch up
docker exec rabbit-1 rabbitmq-upgrade revive
docker exec rabbit-1 rabbitmq-queues quorum_status payments

# 5. repeat for the other nodes one at a time
# 6. once every node is upgraded:
docker exec rabbit-1 rabbitmqctl enable_feature_flag all
docker exec rabbit-1 rabbitmq-queues rebalance quorum
```

**Blue-green** for big version jumps or migrating from old clusters (for example, from classic mirrored queues on 3.x): a new cluster is brought up, the topology is moved via definitions, messages via Shovel or Federation, clients switch over, and the old cluster is retired.

## 18.5 Production checklist

- [ ] 3 (or 5) nodes in different zones of one region
- [ ] Quorum queues for everything important; classic only for temporary and exclusive ones
- [ ] Publisher confirms, `mandatory` or an alternate exchange
- [ ] Manual ack after processing, a sensible prefetch
- [ ] `delivery-limit`, a DLX and an alert on the DLQ
- [ ] Queue length limits with `reject-publish`
- [ ] `disk_free_limit` in gigabytes, memory headroom below the alarm
- [ ] Limits per vhost and user
- [ ] Separate, long-lived connections for publishing and consuming
- [ ] Client reconnection implemented and tested
- [ ] Idempotent consumers, an outbox for business events
- [ ] Prometheus, dashboards, the alerts from module 16
- [ ] TLS, separate users, topic permissions
- [ ] Definitions and policies in Git, applied automatically
- [ ] Rehearsed: node failure, zone failure, disk alarm, rolling upgrade

## 18.6 Anti-patterns

| Anti-pattern | Why it's bad | Do this instead |
|---|---|---|
| A connection per message | Broker load, exhausted ports and memory | Long-lived connections and channels |
| Auto ack for important data | Losses when a consumer crashes | Manual ack after processing |
| Publishing without confirms | You don't know whether the message arrived | Publisher confirms |
| Classic queues for important data | No replication | Quorum queues |
| `nack(requeue=true)` on any error | An endless loop of "poison" messages (in 4.3 `nack` doesn't count toward delivery-limit) | `reject(requeue=true)` + delivery-limit, DLX, delayed retries |
| Prefetch without a limit | Consumer memory and uneven distribution | A sensible prefetch |
| Queues with millions of messages | Memory, slow recovery | Short queues, limits, streams |
| A queue per request or per user | Load on cluster metadata | Shared queues, direct reply-to |
| A cluster stretched across regions | Slow, quorum loss | A cluster per region + Federation/Shovel |
| Two nodes in a cluster | No quorum when either is lost | 3 or 5 nodes |
| `guest` in production | A security hole | Separate users |
| Queue arguments for TTL and limits | Can't be changed without recreating | Policies |
| RabbitMQ as a database with history | Queues aren't for storage | Streams or a database |

---

# Module 19. Capstone project: an event-driven online shop

## 19.1 What you're building

```
                     ┌──────────────┐
  HTTP  ────────────►│  Order API   │── INSERT orders + outbox (one transaction)
                     └──────┬───────┘
                            │ outbox relay (confirms, message_id)
                            ▼
                  exchange shop.events (topic)
       ┌───────────────┬──────────┴──────────┬───────────────────┐
       ▼               ▼                     ▼                   ▼
  [billing.orders] [stock.orders]   [notifications.events]  stream shop.events.log
   quorum,          quorum,          quorum,                  analytics and audit
   delivery-limit   single active     priority                (replay)
       │            consumer
       ▼               │
  Payment Service   Warehouse          Notification workers (email / push)
  idempotent        order per warehouse
       │
  payment.succeeded / payment.failed -> shop.events

  Plus:
  - rpc.pricing: synchronous price calculation via direct reply-to
  - retries: delayed retry in quorum queues, DLX shop.dlx -> [shop.dead] + an alert
  - MQTT: courier devices publish statuses to couriers/+/status
  - monitoring: queue lengths, unacked, consumers, alarms, DLQ
```

## 19.2 Requirements

1. Orders are created over HTTP, and the event is published through an outbox with confirms: **no dual writes**.
2. Every important queue is quorum, with `delivery-limit`, a DLX and a length limit (`reject-publish`), configured by policies.
3. Payment Service processes idempotently (`processed_messages`) and acks after the COMMIT.
4. Warehouse preserves event order with a single active consumer.
5. Transient errors are retried with a growing delay; messages that exhausted their attempts go to the DLQ with an alert.
6. Notifications use priorities: payment emails before marketing ones.
7. Prices are calculated over RPC with direct reply-to and a timeout.
8. Analytics reads the stream `shop.events.log` and can replay a week of history.
9. Courier statuses arrive over MQTT and are routed to the delivery service's queue.
10. The system survives stopping any node with no message loss and no publishing outage.

## 19.3 Stages

| Stage | What to do |
|---|---|
| 1 | A 3-node cluster, plugins, a vhost `shop` with quorum as the default queue type |
| 2 | Topology via definitions or the Messaging Topology Operator: exchanges, queues, bindings, policies |
| 3 | Order API + outbox + a relay with confirms |
| 4 | Payment Service: idempotent consumer, prefetch, ack after COMMIT |
| 5 | Delayed retries and a DLX, a parking lot, an alert |
| 6 | Warehouse with a single active consumer |
| 7 | Notifications with priorities |
| 8 | RPC `rpc.pricing` |
| 9 | A stream for analytics, reading from a saved offset |
| 10 | MQTT for couriers, topic permissions |
| 11 | Monitoring and alerts |
| 12 | Drills: stop a node, kill a consumer mid-processing, fill a queue to its limit, send a broken message |

## 19.4 How to verify it works

```bash
# load
docker run -it --rm --network host pivotalrabbitmq/perf-test:latest \
  --uri amqp://admin:admin@localhost:5672/shop --exchange shop.events --type topic \
  --routing-key order.created --producers 2 --consumers 0 --confirm 100 --time 120 --predeclared

# while the load runs
docker stop rabbit-2
docker exec rabbit-1 rabbitmqctl list_queues -p shop name messages_ready consumers   # queues grow and drain
docker exec rabbit-1 rabbitmq-queues quorum_status -p shop billing.orders            # a new leader
docker start rabbit-2

# idempotency: stop Payment Service mid-processing and start it again
# the database must not show double charges
```

If after all the drills the queues are back to zero, the DLQ holds only deliberately broken messages, the database has no duplicates, and the order count in analytics matches the database, you've finished the course.

---

# RabbitMQ CLI and HTTP API cheat sheet

```bash
# node and cluster
rabbitmq-diagnostics status
rabbitmq-diagnostics ping
rabbitmq-diagnostics check_running
rabbitmq-diagnostics check_local_alarms
rabbitmq-diagnostics check_port_connectivity
rabbitmq-diagnostics memory_breakdown
rabbitmq-diagnostics log_tail -N 100
rabbitmqctl cluster_status
rabbitmqctl list_feature_flags name state
rabbitmqctl enable_feature_flag all

# vhosts, users, permissions
rabbitmqctl add_vhost shop --default-queue-type quorum
rabbitmqctl add_user app 'password'
rabbitmqctl set_user_tags app monitoring
rabbitmqctl set_permissions -p shop app '^app\..*' '^(shop\.events|app\..*)$' '^(shop\.events|app\..*)$'
rabbitmqctl set_topic_permissions -p shop app shop.events '^order\..*' '^$'
rabbitmqctl list_users
rabbitmqctl list_permissions -p shop

# objects
rabbitmqctl list_exchanges -p shop name type
rabbitmqctl list_queues -p shop name type messages_ready messages_unacknowledged consumers
rabbitmqctl list_bindings -p shop
rabbitmqctl list_connections name user state channels
rabbitmqctl list_consumers -p shop
rabbitmqctl purge_queue -p shop payments
rabbitmqctl delete_queue -p shop payments

# policies and limits
rabbitmqctl set_policy -p shop limits '^(payments|stock)$' '{"max-length":500000,"overflow":"reject-publish"}' --apply-to queues
rabbitmqctl set_operator_policy -p shop cap '.*' '{"max-length":1000000}' --apply-to queues
rabbitmqctl list_policies -p shop
rabbitmqctl set_vhost_limits -p shop '{"max-connections":256,"max-queues":1024}'
rabbitmqctl set_user_limits app '{"max-connections":20,"max-channels":200}'

# quorum queues and streams
rabbitmq-queues quorum_status -p shop payments
rabbitmq-queues check_if_node_is_quorum_critical
rabbitmq-queues rebalance quorum
rabbitmq-queues grow rabbit@rabbit-4 all
rabbitmq-queues shrink rabbit@rabbit-3
rabbitmq-streams add_super_stream invoices --partitions 3

# maintenance
rabbitmq-upgrade drain
rabbitmq-upgrade revive
rabbitmqctl forget_cluster_node rabbit@rabbit-3

# definitions
rabbitmqctl export_definitions /tmp/definitions.json
rabbitmqctl import_definitions /tmp/definitions.json

# HTTP API
curl -u admin:admin http://localhost:15672/api/overview
curl -u admin:admin http://localhost:15672/api/queues/shop
curl -u admin:admin -X PUT http://localhost:15672/api/queues/shop/payments \
  -H content-type:application/json -d '{"durable":true,"arguments":{"x-queue-type":"quorum"}}'
curl -u admin:admin -X POST http://localhost:15672/api/exchanges/shop/shop.events/publish \
  -H content-type:application/json -d '{"routing_key":"order.created","payload":"{}","payload_encoding":"string","properties":{"delivery_mode":2}}'
curl -u admin:admin http://localhost:15672/api/definitions > definitions.json
```

---

# Configuration cheat sheet

**An important queue:**

```
x-queue-type: quorum                      (argument)
delivery-limit: 5–10                      (policy)
dead-letter-exchange: <dlx>               (policy)
dead-letter-strategy: at-least-once       (policy, together with overflow=reject-publish)
max-length: sized for the worst-case backlog (policy)
overflow: reject-publish                  (policy)
```

**A queue for replies and temporary data:**

```
x-queue-type: classic
exclusive: true   (or durable + x-expires)
```

**A log with replay:**

```
x-queue-type: stream
x-max-age: 7D
x-max-length-bytes: within the disk budget
```

**Publisher:**

```
a long-lived connection used only for publishing
publisher confirms, asynchronous with a window of 100–1000
delivery_mode: 2
message_id: a stable event id
mandatory: true (and handle returns) or an alternate exchange
a timeout on waiting for confirms
```

**Consumer:**

```
manual ack after processing (after the database COMMIT)
prefetch: 1–10 for long tasks, 50–300 for fast ones
reject(requeue=false) for invalid messages
reconnection and re-subscription
idempotent processing by message_id
```

**Node (rabbitmq.conf):**

```
vm_memory_high_watermark.relative = 0.6
disk_free_limit.absolute = 4GB (or more)
listeners.tcp = none + listeners.ssl.default = 5671
max_connections / max_channels sized to the expected load
cluster_formation.* (or the operator on Kubernetes)
```

---

# RabbitMQ interview questions with answers

**Junior**

1. **What is RabbitMQ?** A message broker: it accepts messages from producers, routes them through exchanges into queues and delivers them to consumers with acknowledgements.
2. **What are an exchange, a queue and a binding?** An exchange accepts publishes and routes them, a queue stores messages, and a binding is a rule linking an exchange to a queue by key.
3. **Which exchange types exist?** Direct (exact key match), fanout (to every queue), topic (by pattern with `*` and `#`), headers (by headers).
4. **What is the default exchange?** A direct exchange with an empty name to which every queue is bound by its own name.
5. **How does a connection differ from a channel?** A connection is a TCP connection and expensive; a channel is a lightweight logical channel inside it through which operations go.
6. **What happens to a message with no matching queue?** It's dropped; with `mandatory=true` it's returned to the producer, with an alternate exchange it goes there.
7. **What is an ack?** A consumer's confirmation that a message is processed; after it the broker deletes the message from the queue.
8. **How does a durable queue differ from a persistent message?** Durable keeps the queue's definition across restarts; persistent (`delivery_mode=2`) keeps the message itself.
9. **What is a vhost?** An isolated namespace inside the broker with its own objects, permissions and limits.
10. **What are competing consumers?** Several consumers on one queue among which messages are distributed.

**Middle**

11. **How does auto ack differ from manual ack?** Auto ack acknowledges a message when it's sent (at-most-once); manual ack after processing (at-least-once).
12. **What do nack and reject do with requeue=true and false?** `requeue=true` returns the message to the queue; `false` drops it or sends it to the DLX.
13. **What is prefetch and how do you choose it?** A limit of unacknowledged messages per consumer: small for long tasks, larger for fast ones; no limit is dangerous.
14. **What are publisher confirms?** A channel mode in which the broker acknowledges (ack) or rejects (nack) every publish; a message counts as sent only after the ack.
15. **When does a confirm arrive for a quorum queue?** When a majority of replicas has written the message.
16. **How do classic, quorum and stream differ?** Classic isn't replicated, quorum is replicated via Raft and deletes messages after ack, a stream is a replicated log with replay.
17. **What is a DLX and when does a message go there?** A dead letter exchange: messages rejected without requeue, expired by TTL, pushed out on overflow, or past the delivery limit go there.
18. **How do you build a delayed retry?** With a retry queue that has a queue TTL and a DLX back to the work queue, or with delayed retry in 4.3 quorum queues.
19. **Why is a per-message TTL dangerous in classic queues?** Expiry is checked only at the head of the queue: a message with a short TTL waits behind one with a long TTL.
20. **Why policies, if queues have arguments?** Arguments can't change without recreating the queue; policies change on the fly and apply to groups of queues.
21. **What is an alternate exchange?** An exchange that receives messages which matched no binding.
22. **How do you preserve order with several consumers?** A single active consumer or sharding by key across several queues (consistent hash, super streams).
23. **How do you do RPC over RabbitMQ?** A request with `reply_to` and `correlation_id`, the answer to reply-to; easiest with direct reply-to (`amq.rabbitmq.reply-to`) and a mandatory timeout.
24. **What is a memory alarm?** When `vm_memory_high_watermark` is exceeded the broker blocks every publisher in the cluster while consumers keep working.
25. **Why separate connections for publishing and consuming?** Flow control and alarms block publishing connections; a consumer's acks on the same connection would get stuck too.

**Senior**

26. **How does a quorum queue work?** A Raft group of several replicas with a leader: a write is confirmed by a majority, a new leader is elected from the replicas when the leader fails, and the queue is available while a quorum is alive.
27. **What did Khepri change in 4.3?** Metadata is stored only in Khepri on Raft; the cluster needs a majority of nodes online, Mnesia's partition handling strategies are removed, and recovery follows Raft uniformly.
28. **Why is a two-node cluster worse than one node?** Losing either node loses the quorum of both the metadata and the quorum queues.
29. **How do you protect against "poison" messages?** A delivery-limit in quorum queues, validation with reject and no requeue, a DLQ with an alert and a re-send tool.
30. **How do you get the effect of exactly-once?** At-least-once (confirms, manual ack) plus idempotent processing by a stable `message_id`; for publishing from a transaction, an outbox.
31. **When do you choose streams over queues?** When you need replay, fan-out to many readers without copying messages, or very high throughput.
32. **How do you link clusters in different regions?** Don't stretch a cluster; use Federation (exchanges and queues by policy) or Shovel (moving from a source to a destination), with `ack-mode=on-confirm`.
33. **How do you upgrade a cluster without downtime?** Enable all feature flags, then node by node: check quorum criticality, drain, upgrade, revive, wait for replicas; after all nodes, enable the new feature flags and rebalance. For big jumps, blue-green.
34. **Why are long queues a problem?** They take memory and disk, recover slowly and bloat Raft logs; RabbitMQ is at its best with short queues.
35. **Which metrics do you alert on first?** Memory and disk alarms, queues without consumers, growing queues and unacked, messages in DLQs, unroutable messages, node availability and quorum queue quorum.

---

# FAQ

**Does RabbitMQ lose messages?**
With quorum queues, publisher confirms, `mandatory` or an alternate exchange, manual ack after processing and limits with `reject-publish`, no. Most "losses" are publishing without confirms, auto ack, a missing binding or `drop-head` on overflow.

**Classic or quorum?**
Quorum for everything important. Classic for temporary and exclusive queues and RPC replies.

**Can I change the type of an existing queue?**
No. A new queue is created, traffic is switched over, and the old one is drained and deleted (or moved with Shovel).

**Why does declaring my queue fail with PRECONDITION_FAILED?**
Either the queue already exists with different arguments, or you're declaring a non-durable non-exclusive queue, which is denied by default since 4.3.

**How do I re-read messages?**
From a queue, you can't: after the ack they're gone. If you need history, use streams.

**How many queues can a cluster handle?**
Tens of thousands of queues are fine with enough memory, but every quorum queue is a separate Raft group. Don't create a queue per request or per user.

**RabbitMQ or Kafka?**
RabbitMQ for task queues, flexible routing, per-message acknowledgement, retries and priorities. Kafka for a years-long event log, CDC, analytics and a connector ecosystem. RabbitMQ streams cover part of Kafka's use cases.

**Do I need the delayed message exchange plugin for delayed messages?**
Not for retries: there's TTL + DLX and delayed retry in 4.3 quorum queues. The plugin is handy for "send in N minutes", but it doesn't replicate delayed messages.

**How do I send a large file?**
Put it in object storage and send a reference in the message. The default message limit is 16 MB, but even megabytes in every message is a bad idea.

**Which clients should I use?**
Python: pika (synchronous) or aio-pika (asynchronous); Go: `github.com/rabbitmq/amqp091-go`; Java: `com.rabbitmq:amqp-client` or Spring AMQP. For AMQP 1.0, the new official RabbitMQ clients; for streams, the stream clients.

**What do I do with the Erlang cookie?**
The same long random secret on every node, stored in secrets. Whoever knows the cookie and has network access to the Erlang distribution port controls the node.

---

# RabbitMQ glossary

| Term | Meaning |
|---|---|
| **AMQP 0-9-1** | RabbitMQ's classic protocol |
| **AMQP 1.0** | A standard protocol, core in RabbitMQ since 4.0 |
| **Connection** | A client's TCP connection to the broker |
| **Channel** | A logical channel inside a connection |
| **Virtual host** | An isolated namespace |
| **Exchange** | A publishing point that routes messages |
| **Direct / Fanout / Topic / Headers** | Exchange types |
| **Default exchange** | The direct exchange `""`, bound to every queue by name |
| **Binding** | A rule linking an exchange to a queue or another exchange |
| **Routing key** | A message's routing key |
| **Queue** | A message buffer |
| **Classic queue** | A non-replicated queue |
| **Quorum queue** | A replicated queue based on Raft |
| **Stream** | A replicated log with replay |
| **Super stream** | A partitioned stream |
| **Durable** | A queue or exchange definition survives a restart |
| **Persistent message** | A message with `delivery_mode=2` |
| **Exclusive queue** | A queue belonging to one connection |
| **Publisher confirms** | Publish acknowledgements from the broker |
| **Mandatory** | A flag: return the message if it went nowhere |
| **Alternate exchange** | An exchange for unrouted messages |
| **Ack / Nack / Reject** | An acknowledgement / a refusal with optional requeue |
| **Prefetch (QoS)** | A limit of unacknowledged messages per consumer |
| **Delivery tag** | A delivery number within a channel |
| **Redelivered** | The redelivery flag |
| **Delivery limit** | The delivery limit in a quorum queue |
| **DLX** | Dead letter exchange |
| **TTL** | Time to live of a message or queue |
| **Single active consumer** | Only one active consumer on a queue |
| **Consumer priority** | A consumer's priority when messages are handed out |
| **Direct reply-to** | A pseudo-queue for RPC replies |
| **Policy / Operator policy** | Settings by name pattern / administrator caps |
| **Memory / disk alarm** | Publishing blocked for lack of memory or disk |
| **Flow control** | Slowing down publishing connections |
| **Khepri** | The Raft-based metadata store |
| **Raft** | The consensus protocol of quorum queues, streams and Khepri |
| **Feature flag** | A mechanism for enabling new capabilities in a cluster |
| **Federation** | Asynchronous linking of brokers by exchanges and queues |
| **Shovel** | Moving messages from a source to a destination |
| **Definitions** | A JSON export of the topology, users and policies |
| **Erlang cookie** | The shared secret of cluster nodes |

---

# Official sources and what to read next

- **RabbitMQ documentation** — https://www.rabbitmq.com/docs
- **Release notes** — https://github.com/rabbitmq/rabbitmq-server/releases
- **Source code** — https://github.com/rabbitmq/rabbitmq-server
- **Official Docker image** — https://hub.docker.com/_/rabbitmq
- **Tutorials for every language** — https://www.rabbitmq.com/tutorials
- **Cluster Operator for Kubernetes** — https://www.rabbitmq.com/kubernetes/operator/operator-overview
- **PerfTest** — https://perftest.rabbitmq.com
- **pika** — https://github.com/pika/pika
- **amqp091-go** — https://github.com/rabbitmq/amqp091-go
- **Java client** — https://github.com/rabbitmq/rabbitmq-java-client
- **RabbitMQ blog** — https://www.rabbitmq.com/blog
- **Books:** "RabbitMQ in Depth" (Gavin M. Roy), "Enterprise Integration Patterns" (Gregor Hohpe, Bobby Woolf), the classic on messaging patterns

---

## Contributing

Found an error, an inaccuracy or an outdated setting? Open an issue or send a pull request. Especially welcome:

- examples in languages not covered here (C#, Node.js, Rust, Kotlin);
- real production stories and incident write-ups;
- corrections for newer RabbitMQ releases.

⭐ If this course helped, star the repo so other developers can find it.

**Licence:** the course text is licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/), and the code samples under the [MIT License](../LICENSE). You're free to use, adapt and share the material, including for internal workshops, as long as you credit the source.
